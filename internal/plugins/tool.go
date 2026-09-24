package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
)

// ErrInvalidToolName reports a tool name that is not accepted by the agent
// tool registry.
var ErrInvalidToolName = errors.New("plugins: invalid tool name")

// ErrInvalidToolOutput reports WASM tool stdout that is not a single JSON
// value.
var ErrInvalidToolOutput = errors.New("plugins: invalid tool output")

// toolNamePattern mirrors agent.ToolRegistry's (OpenAI function-name) rule.
var toolNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// WasmTool exposes a WebAssembly module as an agent tool. It satisfies
// agent.Tool (and tools.Tool) structurally:
//
//	Name() string
//	Description() string
//	Parameters() map[string]interface{}
//	Execute(ctx, args map[string]interface{}) (interface{}, error)
//
// Execute writes the JSON-encoded arguments object to the module's stdin and
// decodes its stdout as a single JSON value (empty stdout yields nil). A
// non-zero exit fails the call with a *wasmrt.ExitError. The module runs
// under all wasmrt sandbox limits. WasmTool is immutable and safe for
// concurrent use.
type WasmTool struct {
	name        string
	description string
	params      map[string]interface{}
	rt          *wasmrt.Runtime
	mod         *wasmrt.Module
	timeout     time.Duration
}

// NewWasmTool compiles wasm on rt and wraps it as a tool. params is the
// JSON-schema of the arguments (default: an object without properties); it
// is deep-copied. The runtime's default timeout applies to each call; rt is
// not owned by the tool.
func NewWasmTool(name, description string, params map[string]interface{}, wasm []byte, rt *wasmrt.Runtime) (*WasmTool, error) {
	if rt == nil {
		return nil, ErrWASMRuntimeUnavailable
	}
	if !toolNamePattern.MatchString(name) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidToolName, name)
	}
	mod, err := rt.Compile(context.Background(), wasm)
	if err != nil {
		return nil, fmt.Errorf("wasm tool %q: %w", name, err)
	}
	return newWasmTool(name, description, params, rt, mod, 0)
}

func newWasmTool(name, description string, params map[string]interface{}, rt *wasmrt.Runtime, mod *wasmrt.Module, timeout time.Duration) (*WasmTool, error) {
	if !toolNamePattern.MatchString(name) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidToolName, name)
	}
	if params == nil {
		params = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
	}
	return &WasmTool{name: name, description: description, params: CopyPayload(params), rt: rt, mod: mod, timeout: timeout}, nil
}

// Name returns the tool name.
func (t *WasmTool) Name() string { return t.name }

// Description returns the tool description.
func (t *WasmTool) Description() string { return t.description }

// Parameters returns a copy of the tool's JSON-schema parameters.
func (t *WasmTool) Parameters() map[string]interface{} { return CopyPayload(t.params) }

// ModuleHash returns the hex SHA-256 of the tool's module.
func (t *WasmTool) ModuleHash() string { return t.mod.Hash() }

// Execute runs the module with args as JSON on stdin.
func (t *WasmTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	if args == nil {
		args = map[string]interface{}{}
	}
	in, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("wasm tool %q: arguments are not JSON-serialisable: %w", t.name, err)
	}
	res, err := t.rt.RunModule(ctx, t.mod, in, wasmrt.RunOptions{Timeout: t.timeout})
	if err != nil {
		return nil, fmt.Errorf("wasm tool %q: %w", t.name, err)
	}
	out, err := decodeJSONOutput(res.Stdout)
	if err != nil {
		return nil, fmt.Errorf("wasm tool %q: %w", t.name, err)
	}
	return out, nil
}

// decodeJSONOutput decodes module stdout as exactly one JSON value
// (surrounding whitespace allowed). Empty output decodes to nil. Numbers
// decode as float64, as with encoding/json defaults.
func decodeJSONOutput(stdout []byte) (interface{}, error) {
	trimmed := bytes.TrimSpace(stdout)
	if len(trimmed) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidToolOutput, wasmrt.SanitizeText(err.Error(), maxPluginErrorText))
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing data after the JSON value", ErrInvalidToolOutput)
	}
	return v, nil
}
