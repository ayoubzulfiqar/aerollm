package agent

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// approvalStoreMock is a mock ApprovalStore for testing.
type approvalStoreMock struct {
	records map[string]ApprovalRecord
}

func newApprovalStoreMock() *approvalStoreMock {
	return &approvalStoreMock{records: make(map[string]ApprovalRecord)}
}

func (m *approvalStoreMock) Save(ctx context.Context, record ApprovalRecord) error {
	m.records[record.ApprovalID] = record
	return nil
}

func (m *approvalStoreMock) Get(ctx context.Context, approvalID string) (*ApprovalRecord, error) {
	r, ok := m.records[approvalID]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (m *approvalStoreMock) Update(ctx context.Context, record ApprovalRecord) error {
	m.records[record.ApprovalID] = record
	return nil
}

// mockToolProviderWithApproval returns tool calls that require approval.
type mockToolProviderWithApproval struct {
	response *models.LLMResponse
	err      error
}

func (m *mockToolProviderWithApproval) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

// approvalTool is a tool that requires approval.
type approvalTool struct{}

func (a *approvalTool) Name() string        { return "approval_tool" }
func (a *approvalTool) Description() string { return "requires approval" }
func (a *approvalTool) Parameters() map[string]interface{} {
	return map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
}
func (a *approvalTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	return "done", nil
}
func (a *approvalTool) RequiresApproval() bool { return true }

func TestNewAdvancedAgentEngine(t *testing.T) {
	store := newApprovalStoreMock()
	a := NewAdvancedAgentEngine(nil, nil, store)
	if a == nil {
		t.Fatal("expected non-nil advanced agent engine")
	}
	if a.Store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestRunToolExecutionLoopWithHITL(t *testing.T) {
	resp := &models.LLMResponse{
		Choices: []models.Choice{{
			Message: models.Message{
				Role: models.RoleAssistant,
				ToolCalls: []models.ToolCall{{
					ID:   "call-1",
					Type: "function",
					Function: models.ToolFunction{
						Name:      "approval_tool",
						Arguments: "{}",
					},
				}},
			},
		}},
	}

	store := newApprovalStoreMock()
	registry := NewToolRegistry()
	registry.Register(&approvalTool{})
	a := NewAdvancedAgentEngine(&mockToolProviderWithApproval{response: resp}, registry, store)

	_, approvalID, err := a.RunToolExecutionLoopWithHITL(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if approvalID == "" {
		t.Fatal("expected approval ID")
	}

	if _, ok := store.records[approvalID]; !ok {
		t.Fatal("expected approval record to be saved")
	}
}

func TestResumeApprovalApproved(t *testing.T) {
	store := newApprovalStoreMock()
	store.Save(context.Background(), ApprovalRecord{
		ApprovalID: "approval-1",
		Status:     "pending",
		ToolCall:   models.ToolCall{ID: "call-1", Function: models.ToolFunction{Name: "approval_tool", Arguments: "{}"}},
	})

	resp := &models.LLMResponse{
		Choices: []models.Choice{{
			Message: models.Message{Role: models.RoleAssistant, Content: strPtrAdv("ok")},
		}},
	}

	registry := NewToolRegistry()
	registry.Register(&approvalTool{})
	a := NewAdvancedAgentEngine(&mockToolProviderWithApproval{response: resp}, registry, store)

	result, err := a.ResumeApproval(context.Background(), "approval-1", true, &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestResumeApprovalRejected(t *testing.T) {
	store := newApprovalStoreMock()
	store.Save(context.Background(), ApprovalRecord{
		ApprovalID: "approval-1",
		Status:     "pending",
	})

	a := NewAdvancedAgentEngine(nil, nil, store)
	_, err := a.ResumeApproval(context.Background(), "approval-1", false, &models.LLMRequest{})
	if err == nil {
		t.Fatal("expected error when approval is rejected")
	}
}

func TestResumeApprovalNotFound(t *testing.T) {
	store := newApprovalStoreMock()
	a := NewAdvancedAgentEngine(nil, nil, store)
	_, err := a.ResumeApproval(context.Background(), "missing", true, &models.LLMRequest{})
	if err == nil {
		t.Fatal("expected error when approval not found")
	}
}

func strPtrAdv(s string) *string {
	return &s
}
