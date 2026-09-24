// Package sandbox executes agent tools with isolation and resource limits.
//
// Two executors exist:
//   - FuncExecutor runs registered, in-process Go tool functions with a
//     per-call timeout, a concurrency limit, argument/output size limits and
//     panic isolation. It never executes shell commands or loads files.
//   - WasmExecutor runs user-supplied WebAssembly (WASI preview1) modules in
//     the wasmrt sandbox: no filesystem, network, host environment, real
//     clock or real randomness; memory, wall-clock, input, output and
//     concurrency limits; tools exchange JSON over stdin/stdout.
package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
)

// ToolExecutor defines an interface for executing tools in isolation.
type ToolExecutor interface {
	Execute(ctx context.Context, toolName string, arguments map[string]interface{}) (interface{}, error)
}

// Errors.
var (
	ErrWasmRuntimeUnavailable = errors.New("sandbox: WebAssembly runtime is not available")
	ErrUnknownTool            = errors.New("sandbox: unknown tool")
	ErrInvalidToolName        = errors.New("sandbox: invalid tool name")
	ErrArgumentsTooLarge      = errors.New("sandbox: tool arguments too large")
	ErrOutputTooLarge         = errors.New("sandbox: tool output too large")
	ErrTimeout                = errors.New("sandbox: tool execution timed out")
	ErrBusy                   = errors.New("sandbox: too many concurrent tool executions")
	ErrToolPanicked           = errors.New("sandbox: tool panicked")
	ErrInvalidOutput          = errors.New("sandbox: tool output is not a single JSON value")
	ErrExecutorClosed         = errors.New("sandbox: executor closed")
)

var toolNameRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.\-]{0,63}$`)

// ValidToolName reports whether name is an acceptable tool name.
func ValidToolName(name string) bool { return toolNameRe.MatchString(name) }

// Limits bounds tool execution. Zero values select the defaults.
type Limits struct {
	Timeout        time.Duration // per call (default 10s)
	MaxConcurrent  int           // simultaneous executions (default 16)
	MaxArgBytes    int           // JSON-encoded arguments (default 256 KiB)
	MaxOutputBytes int           // JSON-encoded output (default 1 MiB)
	// QueueTimeout bounds how long a call waits for a free slot (default 1s).
	QueueTimeout time.Duration
	// MaxMemoryBytes caps a WASM tool's linear memory (WasmExecutor only;
	// default 64 MiB, at most MaxWasmMemory).
	MaxMemoryBytes uint64
}

// DefaultLimits returns the default limits.
func DefaultLimits() Limits {
	return Limits{Timeout: 10 * time.Second, MaxConcurrent: 16, MaxArgBytes: 256 << 10, MaxOutputBytes: 1 << 20, QueueTimeout: time.Second, MaxMemoryBytes: 64 << 20}
}

func (l Limits) withDefaults() Limits {
	d := DefaultLimits()
	if l.Timeout <= 0 {
		l.Timeout = d.Timeout
	}
	if l.MaxConcurrent <= 0 {
		l.MaxConcurrent = d.MaxConcurrent
	}
	if l.MaxArgBytes <= 0 {
		l.MaxArgBytes = d.MaxArgBytes
	}
	if l.MaxOutputBytes <= 0 {
		l.MaxOutputBytes = d.MaxOutputBytes
	}
	if l.QueueTimeout <= 0 {
		l.QueueTimeout = d.QueueTimeout
	}
	if l.MaxMemoryBytes == 0 {
		l.MaxMemoryBytes = d.MaxMemoryBytes
	}
	if l.MaxMemoryBytes > MaxWasmMemory {
		l.MaxMemoryBytes = MaxWasmMemory
	}
	return l
}

// ToolFunc is an in-process tool implementation. It must honour ctx
// cancellation; the executor cannot forcibly stop a goroutine, so a tool
// that ignores ctx keeps its concurrency slot until it returns.
type ToolFunc func(ctx context.Context, args map[string]interface{}) (interface{}, error)

// FuncExecutor runs registered Go tool functions under Limits. It is safe
// for concurrent use.
type FuncExecutor struct {
	mu     sync.RWMutex
	tools  map[string]ToolFunc
	limits Limits
	slots  chan struct{}
}

// NewFuncExecutor creates an executor with the given limits.
func NewFuncExecutor(limits Limits) *FuncExecutor {
	limits = limits.withDefaults()
	return &FuncExecutor{tools: map[string]ToolFunc{}, limits: limits, slots: make(chan struct{}, limits.MaxConcurrent)}
}

// Register adds or replaces a tool.
func (e *FuncExecutor) Register(name string, fn ToolFunc) error {
	if !ValidToolName(name) {
		return ErrInvalidToolName
	}
	if fn == nil {
		return errors.New("sandbox: nil tool function")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tools[name] = fn
	return nil
}

// Tools returns the registered tool names.
func (e *FuncExecutor) Tools() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.tools))
	for n := range e.tools {
		out = append(out, n)
	}
	return out
}

type toolResult struct {
	out interface{}
	err error
}

// Execute runs a registered tool with the executor's limits.
func (e *FuncExecutor) Execute(ctx context.Context, toolName string, arguments map[string]interface{}) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !ValidToolName(toolName) {
		return nil, ErrInvalidToolName
	}
	e.mu.RLock()
	fn, ok := e.tools[toolName]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownTool, toolName)
	}
	argBytes, err := json.Marshal(arguments)
	if err != nil {
		return nil, fmt.Errorf("sandbox: arguments are not JSON-serialisable: %w", err)
	}
	if len(argBytes) > e.limits.MaxArgBytes {
		return nil, ErrArgumentsTooLarge
	}
	// Give the tool its own copy of the arguments.
	var args map[string]interface{}
	if err := json.Unmarshal(argBytes, &args); err != nil {
		return nil, err
	}

	queue := time.NewTimer(e.limits.QueueTimeout)
	defer queue.Stop()
	select {
	case e.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-queue.C:
		return nil, ErrBusy
	}

	runCtx, cancel := context.WithTimeout(ctx, e.limits.Timeout)
	done := make(chan toolResult, 1)
	go func() {
		defer func() { <-e.slots }() // release only when the tool really returns
		defer func() {
			if p := recover(); p != nil {
				done <- toolResult{err: fmt.Errorf("%w: %v", ErrToolPanicked, p)}
			}
		}()
		out, err := fn(runCtx, args)
		done <- toolResult{out: out, err: err}
	}()

	select {
	case res := <-done:
		cancel()
		if res.err != nil {
			return nil, res.err
		}
		outBytes, err := json.Marshal(res.out)
		if err != nil {
			return nil, fmt.Errorf("sandbox: tool output is not JSON-serialisable: %w", err)
		}
		if len(outBytes) > e.limits.MaxOutputBytes {
			return nil, ErrOutputTooLarge
		}
		return res.out, nil
	case <-runCtx.Done():
		cancel()
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, ErrTimeout
		}
		return nil, ctx.Err()
	}
}

// WasmToolPayload is an ad-hoc WASM execution request (see
// WasmExecutor.RunPayload).
type WasmToolPayload struct {
	Module    []byte
	Input     string        // written to stdin
	Timeout   time.Duration // 0 selects the executor's Limits.Timeout
	MaxMemory uint64        // bytes; 0 selects Limits.MaxMemoryBytes
}

// WASM payload limits.
const (
	MaxWasmModuleBytes = 16 << 20
	MaxWasmMemory      = 256 << 20
	MaxWasmTimeout     = time.Minute
)

// Validate checks the module header and resource requests.
func (p WasmToolPayload) Validate() error {
	if len(p.Module) < 8 || string(p.Module[:4]) != "\x00asm" {
		return errors.New("sandbox: module is not a WebAssembly binary")
	}
	if err := wasmrt.CheckHeader(p.Module); err != nil {
		return fmt.Errorf("sandbox: %w", err)
	}
	if len(p.Module) > MaxWasmModuleBytes {
		return errors.New("sandbox: module too large")
	}
	if p.MaxMemory > MaxWasmMemory {
		return errors.New("sandbox: requested memory exceeds limit")
	}
	if p.Timeout < 0 || p.Timeout > MaxWasmTimeout {
		return errors.New("sandbox: invalid timeout")
	}
	return nil
}

// WasmExecutor runs WebAssembly tools in the wasmrt sandbox. Tools are WASI
// preview1 commands registered by name; Execute writes the JSON-encoded
// arguments object to the module's stdin and decodes its stdout as a
// single JSON value (empty stdout yields nil). A non-zero exit fails the
// call with a *wasmrt.ExitError carrying the tool's capped stderr.
// WasmExecutor is safe for concurrent use.
type WasmExecutor struct {
	mu     sync.RWMutex
	tools  map[string]*wasmrt.Module
	closed bool
	rt     *wasmrt.Runtime
	ownsRT bool
	rtErr  error
	limits Limits
}

// NewWasmExecutor creates an executor with DefaultLimits and its own
// runtime. If the runtime cannot be created every call fails with
// ErrWasmRuntimeUnavailable.
func NewWasmExecutor() *WasmExecutor {
	e, err := NewWasmExecutorWithLimits(Limits{})
	if err != nil {
		return &WasmExecutor{tools: map[string]*wasmrt.Module{}, limits: DefaultLimits(), rtErr: err}
	}
	return e
}

// NewWasmExecutorWithLimits creates an executor that owns a runtime built
// from limits (zero fields take DefaultLimits values).
func NewWasmExecutorWithLimits(limits Limits) (*WasmExecutor, error) {
	limits = limits.withDefaults()
	rt, err := wasmrt.New(wasmrt.Config{
		MaxModuleBytes: MaxWasmModuleBytes,
		MaxMemoryPages: uint32(MaxWasmMemory / wasmrt.PageSize), // per-call caps lower this
		Timeout:        limits.Timeout,
		MaxInputBytes:  max(limits.MaxArgBytes, 1),
		MaxOutputBytes: limits.MaxOutputBytes,
		MaxConcurrent:  limits.MaxConcurrent,
		QueueTimeout:   limits.QueueTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWasmRuntimeUnavailable, err)
	}
	return &WasmExecutor{tools: map[string]*wasmrt.Module{}, rt: rt, ownsRT: true, limits: limits}, nil
}

// NewWasmExecutorWithRuntime creates an executor on a shared runtime, which
// it does not close. The runtime's own caps (memory, input, output,
// concurrency) still apply on top of limits.
func NewWasmExecutorWithRuntime(rt *wasmrt.Runtime, limits Limits) *WasmExecutor {
	e := &WasmExecutor{tools: map[string]*wasmrt.Module{}, rt: rt, limits: limits.withDefaults()}
	if rt == nil {
		e.rtErr = ErrWasmRuntimeUnavailable
	}
	return e
}

func (e *WasmExecutor) runtime() (*wasmrt.Runtime, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, ErrExecutorClosed
	}
	if e.rt == nil {
		if e.rtErr != nil {
			return nil, e.rtErr
		}
		return nil, ErrWasmRuntimeUnavailable
	}
	return e.rt, nil
}

// Register validates and compiles module and registers (or replaces) it as
// tool name. The bytes are copied.
func (e *WasmExecutor) Register(name string, module []byte) error {
	if !ValidToolName(name) {
		return ErrInvalidToolName
	}
	if len(module) > MaxWasmModuleBytes {
		return errors.New("sandbox: module too large")
	}
	rt, err := e.runtime()
	if err != nil {
		return err
	}
	m, err := rt.Compile(context.Background(), module)
	if err != nil {
		return fmt.Errorf("sandbox: tool %q: %w", name, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrExecutorClosed
	}
	e.tools[name] = m
	return nil
}

// Unregister removes a tool and reports whether it existed.
func (e *WasmExecutor) Unregister(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.tools[name]
	delete(e.tools, name)
	return ok
}

// Tools returns the registered tool names, sorted.
func (e *WasmExecutor) Tools() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.tools))
	for n := range e.tools {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// memoryPages converts a byte budget (0 = the executor default, capped at
// Limits.MaxMemoryBytes) to 64 KiB pages, rounding up.
func (e *WasmExecutor) memoryPages(n uint64) uint32 {
	if n == 0 || n > e.limits.MaxMemoryBytes {
		n = e.limits.MaxMemoryBytes
	}
	return uint32((n + wasmrt.PageSize - 1) / wasmrt.PageSize)
}

// Execute runs a registered WASM tool with arguments as JSON on stdin.
func (e *WasmExecutor) Execute(ctx context.Context, toolName string, arguments map[string]interface{}) (interface{}, error) {
	if toolName == "" {
		return nil, fmt.Errorf("toolName is empty")
	}
	if !ValidToolName(toolName) {
		return nil, ErrInvalidToolName
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rt, err := e.runtime()
	if err != nil {
		return nil, err
	}
	e.mu.RLock()
	m, ok := e.tools[toolName]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownTool, toolName)
	}
	if arguments == nil {
		arguments = map[string]interface{}{}
	}
	in, err := json.Marshal(arguments)
	if err != nil {
		return nil, fmt.Errorf("sandbox: arguments are not JSON-serialisable: %w", err)
	}
	if len(in) > e.limits.MaxArgBytes {
		return nil, ErrArgumentsTooLarge
	}
	res, err := rt.RunModule(ctx, m, in, wasmrt.RunOptions{Timeout: e.limits.Timeout, MaxMemoryPages: e.memoryPages(0)})
	if err != nil {
		return nil, mapWasmError(ctx, err)
	}
	if len(res.Stdout) > e.limits.MaxOutputBytes {
		return nil, ErrOutputTooLarge
	}
	return decodeJSONValue(res.Stdout)
}

// RunPayload validates p and runs its module once with p.Input on stdin,
// returning the raw result (stdout, capped stderr, exit code).
func (e *WasmExecutor) RunPayload(ctx context.Context, p WasmToolPayload) (wasmrt.Result, error) {
	if err := p.Validate(); err != nil {
		return wasmrt.Result{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if len(p.Input) > e.limits.MaxArgBytes {
		return wasmrt.Result{}, ErrArgumentsTooLarge
	}
	rt, err := e.runtime()
	if err != nil {
		return wasmrt.Result{}, err
	}
	timeout := p.Timeout
	if timeout == 0 {
		timeout = e.limits.Timeout
	}
	m, err := rt.Compile(ctx, p.Module)
	if err != nil {
		return wasmrt.Result{}, mapWasmError(ctx, err)
	}
	res, err := rt.RunModule(ctx, m, []byte(p.Input), wasmrt.RunOptions{Timeout: timeout, MaxMemoryPages: e.memoryPages(p.MaxMemory)})
	if err != nil {
		return res, mapWasmError(ctx, err)
	}
	return res, nil
}

// Close releases the executor's runtime (if it owns it), interrupting
// in-flight executions. Further calls fail with ErrExecutorClosed.
func (e *WasmExecutor) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.tools = map[string]*wasmrt.Module{}
	rt, owns := e.rt, e.ownsRT
	e.mu.Unlock()
	if owns && rt != nil {
		return rt.Close()
	}
	return nil
}

// mapWasmError translates wasmrt errors to the sandbox error set while
// keeping the original in the chain.
func mapWasmError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	case errors.Is(err, wasmrt.ErrTimeout):
		return fmt.Errorf("%w: %w", ErrTimeout, err)
	case errors.Is(err, wasmrt.ErrBusy):
		return fmt.Errorf("%w: %w", ErrBusy, err)
	case errors.Is(err, wasmrt.ErrOutputTooLarge):
		return fmt.Errorf("%w: %w", ErrOutputTooLarge, err)
	case errors.Is(err, wasmrt.ErrInputTooLarge):
		return fmt.Errorf("%w: %w", ErrArgumentsTooLarge, err)
	case errors.Is(err, wasmrt.ErrPanic):
		return fmt.Errorf("%w: %w", ErrToolPanicked, err)
	case errors.Is(err, wasmrt.ErrClosed):
		return fmt.Errorf("%w: %w", ErrExecutorClosed, err)
	}
	return err
}

// decodeJSONValue decodes stdout as exactly one JSON value; empty output
// decodes to nil.
func decodeJSONValue(stdout []byte) (interface{}, error) {
	trimmed := bytes.TrimSpace(stdout)
	if len(trimmed) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidOutput, wasmrt.SanitizeText(err.Error(), 256))
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing data", ErrInvalidOutput)
	}
	return v, nil
}

var _ ToolExecutor = (*WasmExecutor)(nil)

// ExecuteAgentTool runs an agent tool definition through the executor and
// records the execution time.
func ExecuteAgentTool(ctx context.Context, executor ToolExecutor, tool models.ToolDefinition, arguments map[string]interface{}) (models.ToolResult, error) {
	if executor == nil {
		err := fmt.Errorf("nil executor")
		return models.ToolResult{Name: tool.Name, Error: err}, err
	}
	start := time.Now()
	out, err := executor.Execute(ctx, tool.Name, arguments)
	return models.ToolResult{
		Name:     tool.Name,
		Content:  out,
		Error:    err,
		Duration: time.Since(start),
	}, err
}
