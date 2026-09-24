// Package sandbox executes agent tools with isolation and resource limits.
//
// Two executors exist:
//   - FuncExecutor runs registered, in-process Go tool functions with a
//     per-call timeout, a concurrency limit, argument/output size limits and
//     panic isolation. It never executes shell commands or loads files.
//   - WasmExecutor is a placeholder for user-supplied WebAssembly modules. No
//     WASM runtime is linked into this build, so it validates its input and
//     then fails explicitly with ErrWasmRuntimeUnavailable instead of
//     pretending to run anything.
package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ToolExecutor defines an interface for executing tools in isolation.
type ToolExecutor interface {
	Execute(ctx context.Context, toolName string, arguments map[string]interface{}) (interface{}, error)
}

// Errors.
var (
	ErrWasmRuntimeUnavailable = errors.New("sandbox: no WebAssembly runtime is available in this build")
	ErrUnknownTool            = errors.New("sandbox: unknown tool")
	ErrInvalidToolName        = errors.New("sandbox: invalid tool name")
	ErrArgumentsTooLarge      = errors.New("sandbox: tool arguments too large")
	ErrOutputTooLarge         = errors.New("sandbox: tool output too large")
	ErrTimeout                = errors.New("sandbox: tool execution timed out")
	ErrBusy                   = errors.New("sandbox: too many concurrent tool executions")
	ErrToolPanicked           = errors.New("sandbox: tool panicked")
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
}

// DefaultLimits returns the default limits.
func DefaultLimits() Limits {
	return Limits{Timeout: 10 * time.Second, MaxConcurrent: 16, MaxArgBytes: 256 << 10, MaxOutputBytes: 1 << 20, QueueTimeout: time.Second}
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

// WasmToolPayload is the expected input for WASM tool binaries.
type WasmToolPayload struct {
	Module    []byte
	Input     string
	Timeout   time.Duration
	MaxMemory uint64
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

// WasmExecutor would run user-provided WebAssembly binaries in a sandboxed
// runtime. No runtime is linked into this build: Execute always fails with
// ErrWasmRuntimeUnavailable (after validating its input).
type WasmExecutor struct{}

// NewWasmExecutor creates a new WASM executor.
func NewWasmExecutor() *WasmExecutor {
	return &WasmExecutor{}
}

// Execute validates the request and reports that no WASM runtime exists.
func (e *WasmExecutor) Execute(ctx context.Context, toolName string, arguments map[string]interface{}) (interface{}, error) {
	if toolName == "" {
		return nil, fmt.Errorf("toolName is empty")
	}
	if !ValidToolName(toolName) {
		return nil, ErrInvalidToolName
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, ErrWasmRuntimeUnavailable
}

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
