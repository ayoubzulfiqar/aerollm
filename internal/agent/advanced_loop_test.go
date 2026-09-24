package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

type fakeProvider struct {
	responses []*models.LLMResponse
	idx       int
}

func (f *fakeProvider) CallLLM(_ context.Context, _ *models.LLMRequest) (*models.LLMResponse, error) {
	if f.idx >= len(f.responses) {
		return &models.LLMResponse{Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: ptr("done")}}}}, nil
	}
	resp := f.responses[f.idx]
	f.idx++
	return resp, nil
}

type captureHook struct {
	beforeLLM   int
	afterLLM    int
	beforeTools int
	afterTools  int
	toolDeficit []ToolDeficitSignal
}

func (c *captureHook) BeforeLLM(_ context.Context, _ *models.LLMRequest)  { c.beforeLLM++ }
func (c *captureHook) AfterLLM(_ context.Context, _ *models.LLMResponse)  { c.afterLLM++ }
func (c *captureHook) BeforeTools(_ context.Context, _ []models.ToolCall) { c.beforeTools++ }
func (c *captureHook) AfterTools(_ context.Context, _ []*ToolResult)      { c.afterTools++ }
func (c *captureHook) OnToolDeficit(_ context.Context, s ToolDeficitSignal) {
	c.toolDeficit = append(c.toolDeficit, s)
}

type errorTool struct {
	msg   string
	calls atomic.Int32
}

func (e *errorTool) Name() string        { return "echo" }
func (e *errorTool) Description() string { return "always errors" }
func (e *errorTool) Parameters() map[string]interface{} {
	return map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
}
func (e *errorTool) Execute(_ context.Context, _ map[string]interface{}) (interface{}, error) {
	e.calls.Add(1)
	return nil, errors.New(e.msg)
}

func ptr(s string) *string { return &s }

func toolResponse(name string) *models.LLMResponse {
	tc := models.ToolCall{ID: "1", Function: models.ToolFunction{Name: name, Arguments: `{}`}}
	msg := models.Message{Role: models.RoleAssistant, ToolCalls: []models.ToolCall{tc}}
	return &models.LLMResponse{Choices: []models.Choice{{Message: msg}}}
}

func textResponse() *models.LLMResponse {
	return &models.LLMResponse{Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: ptr("done")}}}}
}

func TestRunAdvancedExecutionLoopCallsHooks(t *testing.T) {
	provider := &fakeProvider{responses: []*models.LLMResponse{toolResponse("echo"), textResponse()}}
	reg := NewToolRegistry()
	_ = reg.Register(&EchoTool{})

	hook := &captureHook{}
	engine := &AgentEngine{Provider: provider, Registry: reg, MaxIterations: 2, ToolTimeout: time.Second}
	resp, err := engine.RunAdvancedExecutionLoop(context.Background(), &models.LLMRequest{
		Messages: []models.Message{{Role: models.RoleUser, Content: ptr("hi")}},
		Tools:    []models.ToolDefinition{{Name: "echo"}},
	}, AdvancedLoopOptions{Hooks: []LoopHook{hook}})
	if err != nil {
		t.Fatalf("loop failed: %v", err)
	}
	if resp == nil || *resp.Choices[0].Message.Content != "done" {
		t.Fatalf("unexpected response: %v", resp)
	}
	if hook.beforeLLM != 2 || hook.afterLLM != 2 || hook.beforeTools != 1 || hook.afterTools != 1 {
		t.Fatalf("unexpected hook counts: %+v", hook)
	}
}

func TestExecuteToolsWithRetryFeedsErrorBack(t *testing.T) {
	provider := &fakeProvider{responses: []*models.LLMResponse{toolResponse("echo"), textResponse()}}
	reg := NewToolRegistry()
	tool := &errorTool{msg: "boom"}
	_ = reg.Register(tool)

	engine := &AgentEngine{Provider: provider, Registry: reg, MaxIterations: 2, ToolTimeout: time.Second}
	resp, err := engine.RunAdvancedExecutionLoop(context.Background(), &models.LLMRequest{
		Messages: []models.Message{{Role: models.RoleUser, Content: ptr("hi")}},
		Tools:    []models.ToolDefinition{{Name: "echo"}},
	}, AdvancedLoopOptions{RetryToolErrors: true, MaxToolRetries: 2, ToolRetryDelay: time.Millisecond})
	if err != nil {
		t.Fatalf("tool errors must be fed back to the model, not abort the loop: %v", err)
	}
	if resp == nil || *resp.Choices[0].Message.Content != "done" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if got := tool.calls.Load(); got != 3 {
		t.Fatalf("expected 1 attempt + 2 retries, got %d", got)
	}
}

func TestRunAdvancedExecutionLoopSignalsDeficit(t *testing.T) {
	provider := &fakeProvider{responses: []*models.LLMResponse{toolResponse("web_search"), textResponse()}}
	reg := NewToolRegistry()
	_ = reg.Register(&EchoTool{})
	hook := &captureHook{}
	engine := &AgentEngine{Provider: provider, Registry: reg, MaxIterations: 3}
	_, err := engine.RunAdvancedExecutionLoop(context.Background(), &models.LLMRequest{
		Messages: []models.Message{{Role: models.RoleUser, Content: ptr("hi")}},
		Tools:    []models.ToolDefinition{{Name: "echo"}},
	}, AdvancedLoopOptions{Hooks: []LoopHook{hook}})
	if err != nil {
		t.Fatalf("loop failed: %v", err)
	}
	if len(hook.toolDeficit) != 1 || hook.toolDeficit[0].MissingTool != "web_search" {
		t.Fatalf("expected deficit for web_search, got %+v", hook.toolDeficit)
	}
}

func TestToolDeficitHandlerRecordGet(t *testing.T) {
	h := NewToolDeficitHandler()
	signal := ToolDeficitSignal{RequestID: "req-1", MissingTool: "search", Reason: "missing"}
	h.Record("req-1", signal)
	got, ok := h.Get("req-1")
	if !ok || got.MissingTool != "search" {
		t.Fatalf("unexpected deficit handler state: %v %v", ok, got)
	}
}
