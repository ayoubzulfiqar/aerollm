package plugins

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// MaxModuleBytes caps the size of a WASM module accepted by WasmHost.
const MaxModuleBytes = 32 << 20

// ErrWASMRuntimeUnavailable is returned by WasmHost.RunHook: this build has
// no WebAssembly engine linked in, so modules can be validated, stored and
// managed but not executed. Callers must treat this as a hard failure rather
// than as a successful no-op.
var ErrWASMRuntimeUnavailable = errors.New("plugins: wasm runtime not available")

// wasmMagic is the 8-byte preamble of a WebAssembly binary module (v1).
var wasmMagic = []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}

// ValidateWASM performs a cheap structural check: the module must fit the
// size cap and start with the WebAssembly v1 magic and version.
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

// WasmHost manages WASM plugin modules. It validates and stores modules and
// tracks their enabled state, but it does NOT execute them: no WebAssembly
// runtime is linked into this build, so RunHook returns
// ErrWASMRuntimeUnavailable for every loaded plugin.
type WasmHost struct {
	mu      sync.RWMutex
	modules map[string][]byte
	enabled map[string]bool
	store   interface{}
	closed  bool
}

// NewWasmHost creates a new host.
func NewWasmHost(store interface{}) *WasmHost {
	return &WasmHost{
		modules: make(map[string][]byte),
		enabled: make(map[string]bool),
		store:   store,
	}
}

// LoadPlugin validates and stores a module under id (enabled). The bytes are
// copied, so the caller may reuse its buffer.
func (h *WasmHost) LoadPlugin(ctx context.Context, id string, wasmBytes []byte) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
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
	mod := append([]byte(nil), wasmBytes...)
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

// RuntimeAvailable reports whether this host can execute WASM. It is always
// false in this build.
func (h *WasmHost) RuntimeAvailable() bool { return false }

// RunHook would execute hook on plugin id. Because no WASM runtime is linked
// in, it returns ErrWASMRuntimeUnavailable for a loaded, enabled plugin
// instead of pretending to succeed; unknown or disabled plugins report
// ErrPluginNotFound / ErrPluginDisabled.
func (h *WasmHost) RunHook(ctx context.Context, id string, hook Hook, payload map[string]interface{}) (map[string]interface{}, error) {
	_ = payload
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if !hook.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidHook, hook)
	}
	h.mu.RLock()
	closed := h.closed
	_, loaded := h.modules[id]
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
	return nil, fmt.Errorf("plugin %q hook %s: %w", id, hook, ErrWASMRuntimeUnavailable)
}

// Close releases resources. Further calls return ErrHostClosed.
func (h *WasmHost) Close(ctx context.Context) error {
	_ = ctx
	h.mu.Lock()
	defer h.mu.Unlock()
	h.modules = map[string][]byte{}
	h.enabled = map[string]bool{}
	h.closed = true
	return nil
}
