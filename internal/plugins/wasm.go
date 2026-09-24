package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
)

// MaxModuleBytes caps the size of a WASM module accepted by WasmHost.
const MaxModuleBytes = 32 << 20

// HookProtocolVersion is the version of the JSON envelope exchanged with
// WASM plugins (see WasmHost.RunHook).
const HookProtocolVersion = 1

// maxPluginErrorText bounds a plugin-reported error message.
const maxPluginErrorText = 256

var (
	// ErrWASMRuntimeUnavailable is returned when the host's WebAssembly
	// runtime could not be created (or was never configured). Callers must
	// treat it as a hard failure rather than as a successful no-op.
	ErrWASMRuntimeUnavailable = errors.New("plugins: wasm runtime not available")
	// ErrPluginFailed reports a plugin that answered {"error": "..."}.
	ErrPluginFailed = errors.New("plugins: plugin reported an error")
	// ErrInvalidPluginOutput reports plugin stdout that violates the hook
	// protocol.
	ErrInvalidPluginOutput = errors.New("plugins: invalid plugin output")
)

// wasmMagic is the 8-byte preamble of a WebAssembly binary module (v1).
var wasmMagic = []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}

// ValidateWASM performs a cheap structural check: the module must fit the
// size cap and start with the WebAssembly v1 magic and version. Full
// validation happens when a module is compiled (WasmHost.LoadPlugin).
func ValidateWASM(b []byte) error {
	if len(b) == 0 {
		return errors.New("plugins: empty wasm module")
	}
	if len(b) > MaxModuleBytes {
		return fmt.Errorf("plugins: wasm module exceeds %d bytes", MaxModuleBytes)
	}
	if len(b) < len(wasmMagic) || !bytes.Equal(b[:len(wasmMagic)], wasmMagic) {
		return errors.New("plugins: not a WebAssembly v1 module (bad magic/version)")
	}
	return nil
}

// WasmHostOptions configures NewWasmHostWithOptions.
type WasmHostOptions struct {
	// Runtime is a shared runtime to execute plugins on. The host does not
	// close it. When nil the host creates (and owns) its own runtime from
	// RuntimeConfig.
	Runtime *wasmrt.Runtime
	// RuntimeConfig configures the owned runtime. Zero fields take wasmrt
	// defaults, except MaxModuleBytes (default MaxModuleBytes) and Timeout
	// (default: the hook timeout).
	RuntimeConfig wasmrt.Config
	// Timeout bounds one hook invocation (default DefaultHookTimeout).
	Timeout time.Duration
}

// WasmHost loads WebAssembly plugins and runs their lifecycle hooks in a
// wasmrt sandbox: no filesystem, network, host environment or real clock,
// with memory, CPU-time, input and output caps (see package wasmrt).
//
// A plugin is a WASI preview1 command (e.g. a Go program built with
// GOOS=wasip1 GOARCH=wasm). For each hook it receives on stdin
//
//	{"version":1,"plugin_id":"<id>","hook":"OnRequest","payload":{...}}
//
// and answers on stdout with one of
//
//	{"payload":{...}}   replace the payload
//	{} / empty output   leave the payload unchanged
//	{"error":"reason"}  fail the hook (ErrPluginFailed)
//
// A non-zero exit code fails the hook with a *wasmrt.ExitError carrying the
// plugin's (capped) stderr. WasmHost is safe for concurrent use.
type WasmHost struct {
	mu      sync.RWMutex
	modules map[string]*wasmrt.Module
	enabled map[string]bool
	store   interface{}
	closed  bool
	rt      *wasmrt.Runtime
	ownsRT  bool
	rtErr   error
	timeout time.Duration
}

// NewWasmHost creates a host with its own default runtime. If the runtime
// cannot be created, loading and running plugins fail with
// ErrWASMRuntimeUnavailable.
func NewWasmHost(store interface{}) *WasmHost {
	h, err := NewWasmHostWithOptions(store, WasmHostOptions{})
	if err != nil {
		return &WasmHost{
			modules: make(map[string]*wasmrt.Module),
			enabled: make(map[string]bool),
			store:   store,
			rtErr:   err,
			timeout: DefaultHookTimeout,
		}
	}
	return h
}

// NewWasmHostWithOptions creates a host from opts.
func NewWasmHostWithOptions(store interface{}, opts WasmHostOptions) (*WasmHost, error) {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultHookTimeout
	}
	h := &WasmHost{
		modules: make(map[string]*wasmrt.Module),
		enabled: make(map[string]bool),
		store:   store,
		rt:      opts.Runtime,
		timeout: timeout,
	}
	if h.rt == nil {
		cfg := opts.RuntimeConfig
		if cfg.MaxModuleBytes == 0 {
			cfg.MaxModuleBytes = MaxModuleBytes
		}
		if cfg.Timeout == 0 {
			cfg.Timeout = timeout
		}
		rt, err := wasmrt.New(cfg)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrWASMRuntimeUnavailable, err)
		}
		h.rt, h.ownsRT = rt, true
	}
	return h, nil
}

func (h *WasmHost) runtime() (*wasmrt.Runtime, error) {
	if h.rt == nil {
		if h.rtErr != nil {
			return nil, h.rtErr
		}
		return nil, ErrWASMRuntimeUnavailable
	}
	return h.rt, nil
}

// LoadPlugin validates, compiles and stores a module under id (enabled),
// replacing any module already loaded under that id. The bytes are copied,
// so the caller may reuse its buffer. Modules that are not valid WASI
// commands, import anything but wasi_snapshot_preview1, or exceed the
// runtime's limits are rejected here rather than at hook time.
func (h *WasmHost) LoadPlugin(ctx context.Context, id string, wasmBytes []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateID(id); err != nil {
		return err
	}
	if len(wasmBytes) == 0 {
		return fmt.Errorf("empty wasm bytes for plugin %q", id)
	}
	if err := ValidateWASM(wasmBytes); err != nil {
		return fmt.Errorf("plugin %q: %w", id, err)
	}
	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()
	if closed {
		return ErrHostClosed
	}
	rt, err := h.runtime()
	if err != nil {
		return fmt.Errorf("plugin %q: %w", id, err)
	}
	mod, err := rt.Compile(ctx, wasmBytes)
	if err != nil {
		if errors.Is(err, wasmrt.ErrClosed) {
			return ErrHostClosed
		}
		return fmt.Errorf("plugin %q: %w", id, err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrHostClosed
	}
	h.modules[id] = mod
	h.enabled[id] = true
	return nil
}

// LoadPluginFile loads name from within dir. The file is opened through an
// os.Root confined to dir, so "..", absolute paths and symlinks that escape
// dir are rejected by the kernel-level lookup rather than by string checks.
// Only regular files up to MaxModuleBytes are accepted.
func (h *WasmHost) LoadPluginFile(ctx context.Context, id, dir, name string) error {
	if dir == "" {
		return errors.New("plugins: plugin directory is required")
	}
	if name == "" || filepath.IsAbs(name) {
		return fmt.Errorf("plugins: plugin file name %q must be relative to the plugin directory", name)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("plugins: open plugin directory: %w", err)
	}
	defer root.Close()
	f, err := root.Open(name)
	if err != nil {
		return fmt.Errorf("plugins: open plugin file: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("plugins: %q is not a regular file", name)
	}
	if fi.Size() > MaxModuleBytes {
		return fmt.Errorf("plugins: %q exceeds %d bytes", name, MaxModuleBytes)
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxModuleBytes+1))
	if err != nil {
		return err
	}
	if len(b) > MaxModuleBytes {
		return fmt.Errorf("plugins: %q exceeds %d bytes", name, MaxModuleBytes)
	}
	return h.LoadPlugin(ctx, id, b)
}

// SetEnabled toggles a loaded plugin.
func (h *WasmHost) SetEnabled(id string, enabled bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrHostClosed
	}
	if _, ok := h.modules[id]; !ok {
		return fmt.Errorf("%w: %q", ErrPluginNotFound, id)
	}
	h.enabled[id] = enabled
	return nil
}

// Unload removes a plugin module.
func (h *WasmHost) Unload(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrHostClosed
	}
	if _, ok := h.modules[id]; !ok {
		return fmt.Errorf("%w: %q", ErrPluginNotFound, id)
	}
	delete(h.modules, id)
	delete(h.enabled, id)
	return nil
}

// Loaded returns the IDs of loaded modules, sorted.
func (h *WasmHost) Loaded() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.modules))
	for id := range h.modules {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// ModuleHash returns the hex SHA-256 of a loaded plugin's module.
func (h *WasmHost) ModuleHash(id string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	m, ok := h.modules[id]
	if !ok {
		return "", false
	}
	return m.Hash(), true
}

// RuntimeAvailable reports whether this host can execute WASM.
func (h *WasmHost) RuntimeAvailable() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.rt != nil && !h.closed
}

// hookInput is the stdin envelope sent to a plugin.
type hookInput struct {
	Version  int                    `json:"version"`
	PluginID string                 `json:"plugin_id"`
	Hook     Hook                   `json:"hook"`
	Payload  map[string]interface{} `json:"payload"`
}

// hookOutput is the stdout envelope expected from a plugin.
type hookOutput struct {
	Payload json.RawMessage `json:"payload"`
	Error   string          `json:"error"`
}

// RunHook executes hook on plugin id and returns the resulting payload (a
// copy of the input payload if the plugin left it unchanged). The caller's
// payload is never mutated. Execution failures are returned as *HookError;
// timeouts match ErrHookTimeout, plugin-reported errors ErrPluginFailed and
// protocol violations ErrInvalidPluginOutput. Unknown or disabled plugins
// report ErrPluginNotFound / ErrPluginDisabled.
func (h *WasmHost) RunHook(ctx context.Context, id string, hook Hook, payload map[string]interface{}) (map[string]interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !hook.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidHook, hook)
	}
	h.mu.RLock()
	closed := h.closed
	mod, loaded := h.modules[id]
	enabled := h.enabled[id]
	h.mu.RUnlock()
	switch {
	case closed:
		return nil, ErrHostClosed
	case !loaded:
		return nil, fmt.Errorf("%w: %q", ErrPluginNotFound, id)
	case !enabled:
		return nil, fmt.Errorf("%w: %q", ErrPluginDisabled, id)
	}
	rt, err := h.runtime()
	if err != nil {
		return nil, &HookError{PluginID: id, Hook: hook, Err: err}
	}
	if payload == nil {
		payload = map[string]interface{}{}
	}
	in, err := json.Marshal(hookInput{Version: HookProtocolVersion, PluginID: id, Hook: hook, Payload: payload})
	if err != nil {
		return nil, &HookError{PluginID: id, Hook: hook, Err: fmt.Errorf("payload is not JSON-serialisable: %w", err)}
	}
	res, err := rt.RunModule(ctx, mod, in, wasmrt.RunOptions{Timeout: h.timeout})
	if err != nil {
		switch {
		case errors.Is(err, wasmrt.ErrTimeout):
			err = fmt.Errorf("%w after %s: %w", ErrHookTimeout, h.timeout, err)
		case errors.Is(err, wasmrt.ErrPanic):
			err = fmt.Errorf("%w: %w", ErrPluginPanic, err)
		case errors.Is(err, wasmrt.ErrClosed):
			return nil, ErrHostClosed
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil, err
		}
		return nil, &HookError{PluginID: id, Hook: hook, Err: err}
	}
	out, err := decodeHookOutput(res.Stdout)
	if err != nil {
		return nil, &HookError{PluginID: id, Hook: hook, Err: err}
	}
	if out == nil {
		return CopyPayload(payload), nil
	}
	return out, nil
}

// decodeHookOutput parses a plugin's stdout. A nil map means "unchanged".
func decodeHookOutput(stdout []byte) (map[string]interface{}, error) {
	trimmed := bytes.TrimSpace(stdout)
	if len(trimmed) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	var out hookOutput
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidPluginOutput, wasmrt.SanitizeText(err.Error(), maxPluginErrorText))
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing data after the JSON object", ErrInvalidPluginOutput)
	}
	if out.Error != "" {
		return nil, fmt.Errorf("%w: %s", ErrPluginFailed, wasmrt.SanitizeText(out.Error, maxPluginErrorText))
	}
	p := bytes.TrimSpace(out.Payload)
	if len(p) == 0 || bytes.Equal(p, []byte("null")) {
		return nil, nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(p, &m); err != nil {
		return nil, fmt.Errorf("%w: payload must be a JSON object", ErrInvalidPluginOutput)
	}
	return m, nil
}

// RunAll runs hook on every enabled plugin in ascending ID order, threading
// the payload through exactly like NativeHost.RunHook. The first failure
// aborts the chain.
func (h *WasmHost) RunAll(ctx context.Context, hook Hook, payload map[string]interface{}) (map[string]interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !hook.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidHook, hook)
	}
	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()
	if closed {
		return nil, ErrHostClosed
	}
	current := CopyPayload(payload)
	if current == nil {
		current = map[string]interface{}{}
	}
	for _, id := range h.Loaded() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out, err := h.RunHook(ctx, id, hook, current)
		if errors.Is(err, ErrPluginDisabled) || errors.Is(err, ErrPluginNotFound) {
			continue // disabled, or unloaded concurrently
		}
		if err != nil {
			return nil, err
		}
		current = out
	}
	return current, nil
}

// AsHost adapts the WasmHost to the Host interface (RunAll semantics), so
// WASM plugins can be used wherever a NativeHost is.
func (h *WasmHost) AsHost() Host { return wasmChain{h} }

type wasmChain struct{ h *WasmHost }

func (c wasmChain) RunHook(ctx context.Context, hook Hook, payload map[string]interface{}) (map[string]interface{}, error) {
	return c.h.RunAll(ctx, hook, payload)
}

// Tool exposes loaded plugin id as an agent tool (see WasmTool). The tool
// shares the host's runtime and hook timeout, and keeps working after the
// plugin is unloaded, until the host is closed.
func (h *WasmHost) Tool(id, name, description string, params map[string]interface{}) (*WasmTool, error) {
	h.mu.RLock()
	closed := h.closed
	mod, ok := h.modules[id]
	h.mu.RUnlock()
	if closed {
		return nil, ErrHostClosed
	}
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrPluginNotFound, id)
	}
	rt, err := h.runtime()
	if err != nil {
		return nil, err
	}
	return newWasmTool(name, description, params, rt, mod, h.timeout)
}

// Close unloads every plugin and, if the host created its runtime, closes
// it (interrupting in-flight hooks). Further calls return ErrHostClosed.
func (h *WasmHost) Close(ctx context.Context) error {
	_ = ctx
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.modules = map[string]*wasmrt.Module{}
	h.enabled = map[string]bool{}
	h.closed = true
	rt, owns := h.rt, h.ownsRT
	h.mu.Unlock()
	if owns && rt != nil {
		return rt.Close()
	}
	return nil
}
