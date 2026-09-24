package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// scriptedProvider returns scripted responses in order and records a deep
// copy of every request it receives. After the script is exhausted it returns
// a plain "done" answer.
type scriptedProvider struct {
	mu        sync.Mutex
	responses []*models.LLMResponse
	requests  []models.LLMRequest
}

func (p *scriptedProvider) CallLLM(_ context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := *req
	cp.Messages = append([]models.Message(nil), req.Messages...)
	cp.Tools = append([]models.ToolDefinition(nil), req.Tools...)
	p.requests = append(p.requests, cp)
	if len(p.responses) == 0 {
		return &models.LLMResponse{ID: "final", Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: ptr("done")}, FinishReason: "stop"}}}, nil
	}
	r := p.responses[0]
	p.responses = p.responses[1:]
	return r, nil
}

func (p *scriptedProvider) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.requests)
}

func callResp(id string, usage *models.Usage, calls ...models.ToolCall) *models.LLMResponse {
	return &models.LLMResponse{
		ID:    id,
		Model: "m",
		Choices: []models.Choice{{
			Message:      models.Message{Role: models.RoleAssistant, ToolCalls: calls},
			FinishReason: "tool_calls",
		}},
		Usage: usage,
	}
}

func tc(id, name, args string) models.ToolCall {
	return models.ToolCall{ID: id, Type: "function", Function: models.ToolFunction{Name: name, Arguments: args}}
}

// countingTool counts executions and tracks peak concurrency.
type countingTool struct {
	name    string
	delay   time.Duration
	calls   atomic.Int32
	active  atomic.Int32
	peak    atomic.Int32
	result  interface{}
	err     error
	panics  bool
	ignores bool // ignores ctx cancellation while sleeping
}

func (c *countingTool) Name() string        { return c.name }
func (c *countingTool) Description() string { return "test tool " + c.name }
func (c *countingTool) Parameters() map[string]interface{} {
	return map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
}
func (c *countingTool) Execute(ctx context.Context, _ map[string]interface{}) (interface{}, error) {
	c.calls.Add(1)
	n := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		p := c.peak.Load()
		if n <= p || c.peak.CompareAndSwap(p, n) {
			break
		}
	}
	if c.panics {
		panic("kaboom")
	}
	if c.delay > 0 {
		if c.ignores {
			time.Sleep(c.delay)
		} else {
			select {
			case <-time.After(c.delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	if c.err != nil {
		return nil, c.err
	}
	if c.result != nil {
		return c.result, nil
	}
	return "ok", nil
}

func userReq(tools ...string) *models.LLMRequest {
	req := &models.LLMRequest{Model: "m", Messages: []models.Message{{Role: models.RoleUser, Content: ptr("hi")}}}
	for _, name := range tools {
		req.Tools = append(req.Tools, models.ToolDefinition{Name: name})
	}
	return req
}

func TestLoopWithoutToolsIsTransparent(t *testing.T) {
	upstream := &models.LLMResponse{
		ID: "chatcmpl-1", Model: "gpt-x", Object: "chat.completion",
		Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, ToolCalls: []models.ToolCall{tc("c1", "echo", `{}`)}}, FinishReason: "tool_calls"}},
		Usage:   &models.Usage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7},
	}
	p := &scriptedProvider{responses: []*models.LLMResponse{upstream}}
	reg := NewToolRegistry()
	echo := &countingTool{name: "echo"}
	_ = reg.Register(echo)
	e := NewAgentEngine(p, reg)

	got, err := e.RunToolExecutionLoop(context.Background(), userReq())
	if err != nil {
		t.Fatal(err)
	}
	if got != upstream {
		t.Fatalf("expected the upstream response to be returned unchanged")
	}
	if p.calls() != 1 || echo.calls.Load() != 0 {
		t.Fatalf("expected exactly one provider call and no tool execution, got %d calls / %d executions", p.calls(), echo.calls.Load())
	}
}

func TestLoopReturnsClientToolCalls(t *testing.T) {
	upstream := callResp("r1", nil, tc("c1", "client_lookup", `{"q":"x"}`))
	p := &scriptedProvider{responses: []*models.LLMResponse{upstream}}
	reg := NewToolRegistry()
	echo := &countingTool{name: "echo"}
	_ = reg.Register(echo)
	e := NewAgentEngine(p, reg)

	got, err := e.RunToolExecutionLoop(context.Background(), userReq("echo", "client_lookup"))
	if err != nil {
		t.Fatal(err)
	}
	if got != upstream || p.calls() != 1 {
		t.Fatalf("client-side tool calls must be returned to the client unchanged")
	}
	if len(got.Choices[0].Message.ToolCalls) != 1 || got.Choices[0].Message.ToolCalls[0].Function.Name != "client_lookup" {
		t.Fatalf("tool calls not preserved: %+v", got.Choices[0].Message)
	}
}

func TestLoopExecutesServerToolsAndAggregatesUsage(t *testing.T) {
	p := &scriptedProvider{responses: []*models.LLMResponse{
		callResp("r1", &models.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15}, tc("c1", "echo", `{"message":"hey"}`), tc("c2", "calculator", `{"expression":"2+3*4"}`)),
		{ID: "r2", Model: "m", Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: ptr("answer")}, FinishReason: "stop"}}, Usage: &models.Usage{PromptTokens: 20, CompletionTokens: 7, TotalTokens: 27}},
	}}
	reg := NewToolRegistry()
	for _, tool := range BuiltinTools() {
		_ = reg.Register(tool)
	}
	e := NewAgentEngine(p, reg)
	req := userReq("echo", "calculator")

	resp, err := e.RunToolExecutionLoop(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ID != "r2" || resp.Choices[0].FinishReason != "stop" || *resp.Choices[0].Message.Content != "answer" {
		t.Fatalf("unexpected final response: %+v", resp)
	}
	if resp.Usage == nil || resp.Usage.PromptTokens != 30 || resp.Usage.CompletionTokens != 12 || resp.Usage.TotalTokens != 42 {
		t.Fatalf("usage not aggregated: %+v", resp.Usage)
	}
	if len(req.Messages) != 1 {
		t.Fatalf("caller request must not be mutated, got %d messages", len(req.Messages))
	}

	second := p.requests[1].Messages
	if len(second) != 4 {
		t.Fatalf("expected user, assistant, 2 tool messages; got %d: %+v", len(second), second)
	}
	if second[1].Role != models.RoleAssistant || len(second[1].ToolCalls) != 2 {
		t.Fatalf("assistant tool_calls message not preserved: %+v", second[1])
	}
	for i, id := range []string{"c1", "c2"} {
		m := second[2+i]
		if m.Role != models.RoleTool || m.ToolCallID == nil || *m.ToolCallID != id || m.Content == nil {
			t.Fatalf("bad tool message %d: %+v", i, m)
		}
	}
	if !strings.Contains(*second[2].Content, `"echo":"hey"`) {
		t.Fatalf("echo result not JSON-encoded: %q", *second[2].Content)
	}
	if !strings.Contains(*second[3].Content, `"result":14`) {
		t.Fatalf("calculator result wrong: %q", *second[3].Content)
	}
	// The registry definition was filled in for tools requested by name only.
	if p.requests[0].Tools[0].Parameters == nil || p.requests[0].Tools[0].Description == "" {
		t.Fatalf("registry tool definition not filled in: %+v", p.requests[0].Tools[0])
	}
}

func TestLoopFeedsToolErrorsBack(t *testing.T) {
	p := &scriptedProvider{responses: []*models.LLMResponse{callResp("r1", nil, tc("c1", "flaky", `{}`))}}
	reg := NewToolRegistry()
	_ = reg.Register(&countingTool{name: "flaky", err: errors.New("backend down")})
	resp, err := NewAgentEngine(p, reg).RunToolExecutionLoop(context.Background(), userReq("flaky"))
	if err != nil {
		t.Fatalf("tool error must not abort the loop: %v", err)
	}
	if *resp.Choices[0].Message.Content != "done" {
		t.Fatalf("unexpected response %+v", resp)
	}
	toolMsg := p.requests[1].Messages[2]
	if !strings.HasPrefix(*toolMsg.Content, "error: ") || !strings.Contains(*toolMsg.Content, "backend down") {
		t.Fatalf("tool error not fed back: %q", *toolMsg.Content)
	}
}

func TestLoopNeverExecutesUnofferedOrUnknownTools(t *testing.T) {
	p := &scriptedProvider{responses: []*models.LLMResponse{
		callResp("r1", nil, tc("c1", "danger", `{}`), tc("c2", "made_up", `{}`), tc("c3", "echo", `{}`)),
	}}
	reg := NewToolRegistry()
	danger := &countingTool{name: "danger"}
	echo := &countingTool{name: "echo"}
	_ = reg.Register(danger)
	_ = reg.Register(echo)

	_, err := NewAgentEngine(p, reg).RunToolExecutionLoop(context.Background(), userReq("echo"))
	if err != nil {
		t.Fatal(err)
	}
	if danger.calls.Load() != 0 {
		t.Fatal("a registered tool the client did not offer must never run")
	}
	if echo.calls.Load() != 1 {
		t.Fatalf("offered tool should run once, ran %d", echo.calls.Load())
	}
	msgs := p.requests[1].Messages
	for _, i := range []int{2, 3} {
		if !strings.Contains(*msgs[i].Content, "unknown tool") {
			t.Fatalf("expected unknown tool error for message %d, got %q", i, *msgs[i].Content)
		}
	}
}

func TestBaseLoopRefusesApprovalTools(t *testing.T) {
	p := &scriptedProvider{responses: []*models.LLMResponse{callResp("r1", nil, tc("c1", "wire_money", `{}`))}}
	reg := NewToolRegistry()
	inner := &countingTool{name: "wire_money"}
	_ = reg.Register(NewApprovalTool(inner))
	_, err := NewAgentEngine(p, reg).RunToolExecutionLoop(context.Background(), userReq("wire_money"))
	if err != nil {
		t.Fatal(err)
	}
	if inner.calls.Load() != 0 {
		t.Fatal("approval-required tool executed without approval")
	}
	if !strings.Contains(*p.requests[1].Messages[2].Content, "requires human approval") {
		t.Fatalf("expected approval refusal, got %q", *p.requests[1].Messages[2].Content)
	}
}

func TestExecuteToolsBoundedConcurrency(t *testing.T) {
	reg := NewToolRegistry()
	slow := &countingTool{name: "slow", delay: 20 * time.Millisecond}
	_ = reg.Register(slow)
	e := NewAgentEngine(nil, reg)
	e.MaxConcurrent = 2
	calls := make([]models.ToolCall, 8)
	for i := range calls {
		calls[i] = tc("c", "slow", `{}`)
	}
	results, err := e.ExecuteTools(context.Background(), calls)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 8 || slow.calls.Load() != 8 {
		t.Fatalf("expected 8 executions, got %d", slow.calls.Load())
	}
	if peak := slow.peak.Load(); peak > 2 {
		t.Fatalf("concurrency limit exceeded: peak %d", peak)
	}
}

func TestExecuteToolsPerToolTimeoutAndPanic(t *testing.T) {
	reg := NewToolRegistry()
	hang := &countingTool{name: "hang", delay: 2 * time.Second, ignores: true}
	boom := &countingTool{name: "boom", panics: true}
	fast := &countingTool{name: "fast"}
	for _, tool := range []Tool{hang, boom, fast} {
		_ = reg.Register(tool)
	}
	e := NewAgentEngine(nil, reg)
	e.ToolTimeout = 50 * time.Millisecond

	start := time.Now()
	results, err := e.ExecuteTools(context.Background(), []models.ToolCall{tc("1", "hang", ""), tc("2", "boom", ""), tc("3", "fast", "")})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("a tool ignoring its context stalled the batch")
	}
	if results[0].Error == nil || !strings.Contains(results[0].Error.Error(), "timed out") {
		t.Fatalf("expected timeout, got %v", results[0].Error)
	}
	if results[1].Error == nil || !strings.Contains(results[1].Error.Error(), "panicked") {
		t.Fatalf("expected recovered panic, got %v", results[1].Error)
	}
	if results[2].Error != nil || results[2].ContentToString() != "ok" {
		t.Fatalf("fast tool should succeed: %+v", results[2])
	}
}

func TestLoopMaxIterationsSkipsWastedToolRun(t *testing.T) {
	p := &scriptedProvider{responses: []*models.LLMResponse{callResp("r1", &models.Usage{TotalTokens: 9}, tc("c1", "echo", `{}`))}}
	reg := NewToolRegistry()
	echo := &countingTool{name: "echo"}
	_ = reg.Register(echo)
	e := NewAgentEngine(p, reg)
	e.MaxIterations = 1
	_, err := e.RunToolExecutionLoop(context.Background(), userReq("echo"))
	var maxErr *MaxIterationsError
	if !errors.As(err, &maxErr) {
		t.Fatalf("expected MaxIterationsError, got %v", err)
	}
	if maxErr.Usage == nil || maxErr.Usage.TotalTokens != 9 {
		t.Fatalf("expected usage on the error, got %+v", maxErr.Usage)
	}
	if echo.calls.Load() != 0 {
		t.Fatal("tools must not run when no iteration is left to use their output")
	}
}

func TestLoopGeneratesMissingToolCallIDs(t *testing.T) {
	p := &scriptedProvider{responses: []*models.LLMResponse{callResp("r1", nil, models.ToolCall{Function: models.ToolFunction{Name: "echo", Arguments: `{}`}})}}
	reg := NewToolRegistry()
	_ = reg.Register(&EchoTool{})
	if _, err := NewAgentEngine(p, reg).RunToolExecutionLoop(context.Background(), userReq("echo")); err != nil {
		t.Fatal(err)
	}
	msgs := p.requests[1].Messages
	id := msgs[1].ToolCalls[0].ID
	if id == "" || msgs[1].ToolCalls[0].Type != "function" || msgs[2].ToolCallID == nil || *msgs[2].ToolCallID != id {
		t.Fatalf("tool call id/type not repaired consistently: %+v / %+v", msgs[1], msgs[2])
	}
}

func TestLoopInputValidation(t *testing.T) {
	if _, err := NewAgentEngine(nil, nil).RunToolExecutionLoop(context.Background(), userReq()); !errors.Is(err, ErrNoProvider) {
		t.Fatalf("expected ErrNoProvider, got %v", err)
	}
	if _, err := NewAgentEngine(&scriptedProvider{}, nil).RunToolExecutionLoop(context.Background(), nil); !errors.Is(err, ErrNilRequest) {
		t.Fatalf("expected ErrNilRequest, got %v", err)
	}
	nilResp := &mockToolProvider{}
	if _, err := NewAgentEngine(nilResp, nil).RunToolExecutionLoop(context.Background(), userReq()); !errors.Is(err, ErrNilResponse) {
		t.Fatalf("expected ErrNilResponse, got %v", err)
	}
}

func TestLoopTruncatesHugeToolOutput(t *testing.T) {
	p := &scriptedProvider{responses: []*models.LLMResponse{callResp("r1", nil, tc("c1", "big", `{}`))}}
	reg := NewToolRegistry()
	_ = reg.Register(&countingTool{name: "big", result: strings.Repeat("é", 1000)})
	e := NewAgentEngine(p, reg)
	e.MaxToolOutputBytes = 101
	if _, err := e.RunToolExecutionLoop(context.Background(), userReq("big")); err != nil {
		t.Fatal(err)
	}
	out := *p.requests[1].Messages[2].Content
	if len(out) > 101 || !strings.HasSuffix(out, "[truncated]") || !json.Valid([]byte(`"`+strings.TrimSuffix(out, "\n...[truncated]")+`"`)) {
		t.Fatalf("bad truncation (%d bytes): %q", len(out), out)
	}
}

func TestRegistryValidationAndArgs(t *testing.T) {
	reg := NewToolRegistry()
	if err := reg.Register(&countingTool{name: "bad name!"}); err == nil {
		t.Fatal("expected invalid name to be rejected")
	}
	if err := reg.Register(nil); err == nil {
		t.Fatal("expected nil tool to be rejected")
	}
	_ = reg.Register(&EchoTool{})
	if _, err := reg.Execute(context.Background(), "echo", ""); err != nil {
		t.Fatalf("empty args should be treated as {}: %v", err)
	}
	if _, err := reg.Execute(context.Background(), "echo", `[1,2]`); err == nil {
		t.Fatal("non-object args must be rejected")
	}
	if got := reg.All(); len(got) != 1 || got[0] != "echo" {
		t.Fatalf("unexpected tool list %v", got)
	}
	if defs := reg.Definitions(); len(defs) != 1 || defs[0].Parameters == nil {
		t.Fatalf("unexpected definitions %+v", defs)
	}
	if !reg.Unregister("echo") || reg.Unregister("echo") {
		t.Fatal("unregister semantics wrong")
	}
}

func TestContentToString(t *testing.T) {
	cases := map[string]*ToolResult{
		"plain":       {Content: "plain"},
		`{"a":1}`:     {Content: map[string]int{"a": 1}},
		"":            {Content: nil},
		"raw":         {Content: []byte("raw")},
		`[1,2]`:       {Content: []int{1, 2}},
		`"quoted"`:    {Content: json.RawMessage(`"quoted"`)},
		"from-string": {Content: ptr("from-string")},
	}
	for want, r := range cases {
		if got := r.ContentToString(); got != want {
			t.Errorf("ContentToString(%#v) = %q, want %q", r.Content, got, want)
		}
	}
}

func TestCalculator(t *testing.T) {
	good := map[string]float64{
		"2+2":         4,
		"2+3*4":       14,
		"(2+3)*4":     20,
		"-3 + 5":      2,
		"10/4":        2.5,
		"2*(3+(4-1))": 12,
		"--2":         2,
		".5 + .25":    0.75,
	}
	for expr, want := range good {
		got, err := evaluateSimpleExpr(expr)
		if err != nil || got != want {
			t.Errorf("%q = %v, %v; want %v", expr, got, err, want)
		}
	}
	for _, bad := range []string{"", "1/0", "2^3", "1e3+1", "(1+2", "1+", "abc", "1..2", strings.Repeat("(", 100) + "1" + strings.Repeat(")", 100), strings.Repeat("1+", 400) + "1"} {
		if _, err := evaluateSimpleExpr(bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}
