package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// mockToolProvider is a mock implementation of ToolProvider.
type mockToolProvider struct {
	mu       sync.Mutex
	calls    int
	response *models.LLMResponse
	err      error
}

func (m *mockToolProvider) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return m.response, nil
}

func TestNewAgentEngine(t *testing.T) {
	a := NewAgentEngine(nil, nil)
	if a == nil {
		t.Fatal("expected non-nil agent engine")
	}
	if a.MaxIterations != 10 {
		t.Fatalf("expected default max iterations 10, got %d", a.MaxIterations)
	}
}

func TestRunToolExecutionLoopNoTools(t *testing.T) {
	resp := &models.LLMResponse{
		Choices: []models.Choice{{
			Message: models.Message{Role: models.RoleAssistant, Content: strPtr("hello")},
		}},
	}
	a := NewAgentEngine(&mockToolProvider{response: resp}, nil)
	result, err := a.RunToolExecutionLoop(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Choices) != 1 {
		t.Fatalf("expected 1 choice, got %d", len(result.Choices))
	}
}

func TestRunToolExecutionLoopWithTools(t *testing.T) {
	firstResp := &models.LLMResponse{
		Choices: []models.Choice{{
			Message: models.Message{
				Role: models.RoleAssistant,
				ToolCalls: []models.ToolCall{{
					ID:   "call-1",
					Type: "function",
					Function: models.ToolFunction{
						Name:      "test_tool",
						Arguments: `{"arg":"value"}`,
					},
				}},
			},
		}},
	}

	a := NewAgentEngine(&mockToolProvider{response: firstResp}, nil)
	a.MaxIterations = 5

	_, err := a.RunToolExecutionLoop(context.Background(), &models.LLMRequest{})
	if err == nil {
		t.Fatal("expected max iterations error when provider always returns tool calls")
	}
}

func TestRunToolExecutionLoopMaxIterations(t *testing.T) {
	respWithTools := &models.LLMResponse{
		Choices: []models.Choice{{
			Message: models.Message{
				Role: models.RoleAssistant,
				ToolCalls: []models.ToolCall{{
					ID:   "call-1",
					Type: "function",
					Function: models.ToolFunction{
						Name:      "test_tool",
						Arguments: `{"arg":"value"}`,
					},
				}},
			},
		}},
	}
	a := NewAgentEngine(&mockToolProvider{response: respWithTools}, nil)
	a.MaxIterations = 2
	_, err := a.RunToolExecutionLoop(context.Background(), &models.LLMRequest{})
	if err == nil {
		t.Fatal("expected max iterations error")
	}
	var maxErr *MaxIterationsError
	if !errors.As(err, &maxErr) {
		t.Fatalf("expected MaxIterationsError, got %T", err)
	}
}

func TestExecuteToolsContextCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)

	a := NewAgentEngine(nil, nil)
	toolCalls := []models.ToolCall{{
		ID:      "call-1",
		Type:    "function",
		Function: models.ToolFunction{Name: "test_tool", Arguments: "{}"},
	}}
	_, err := a.ExecuteTools(ctx, toolCalls)
	if err == nil {
		t.Fatal("expected context deadline exceeded error")
	}
}

func strPtr(s string) *string {
	return &s
}
