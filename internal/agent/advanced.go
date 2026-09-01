package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ApprovalRecord represents a pending human approval for a tool call.
type ApprovalRecord struct {
	ApprovalID   string
	RequestID    string
	ToolCall     models.ToolCall
	Arguments    string
	Status       string
	CreatedAt    time.Time
	ExpiresAt    time.Time
	Result       interface{}
	Error        error
}

// ApprovalStore persists approval records.
type ApprovalStore interface {
	Save(ctx context.Context, record ApprovalRecord) error
	Get(ctx context.Context, approvalID string) (*ApprovalRecord, error)
	Update(ctx context.Context, record ApprovalRecord) error
}

// RedisApprovalStore stores approvals in Redis.
type RedisApprovalStore struct {
	client cache.RedisClient
	prefix string
	ttl    time.Duration
}

// NewRedisApprovalStore creates a new approval store.
func NewRedisApprovalStore(client cache.RedisClient, prefix string, ttl time.Duration) *RedisApprovalStore {
	return &RedisApprovalStore{client: client, prefix: prefix, ttl: ttl}
}

func (r *RedisApprovalStore) key(approvalID string) string {
	return fmt.Sprintf("%s%s", r.prefix, approvalID)
}

// Save stores an approval record.
func (r *RedisApprovalStore) Save(ctx context.Context, record ApprovalRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, r.key(record.ApprovalID), string(data), r.ttl).Err()
}

// Get retrieves an approval record.
func (r *RedisApprovalStore) Get(ctx context.Context, approvalID string) (*ApprovalRecord, error) {
	val, err := r.client.Get(ctx, r.key(approvalID)).Result()
	if err != nil {
		return nil, err
	}
	var record ApprovalRecord
	if err := json.Unmarshal([]byte(val), &record); err != nil {
		return nil, err
	}
	return &record, nil
}

// Update updates an approval record.
func (r *RedisApprovalStore) Update(ctx context.Context, record ApprovalRecord) error {
	return r.Save(ctx, record)
}

// AdvancedAgentEngine extends AgentEngine with HITL support.
type AdvancedAgentEngine struct {
	AgentEngine
	Store ApprovalStore
	mu   sync.RWMutex
}

// NewAdvancedAgentEngine creates a new advanced agent engine.
func NewAdvancedAgentEngine(provider ToolProvider, registry *ToolRegistry, store ApprovalStore) *AdvancedAgentEngine {
	if registry == nil {
		registry = NewToolRegistry()
	}
	return &AdvancedAgentEngine{
		AgentEngine: *NewAgentEngine(provider, registry),
		Store:       store,
	}
}

// RunToolExecutionLoopWithHITL runs the agentic loop with human-in-the-loop support.
func (e *AdvancedAgentEngine) RunToolExecutionLoopWithHITL(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, string, error) {
	iteration := 0
	for {
		if iteration >= e.MaxIterations {
			return nil, "", &MaxIterationsError{Iteration: iteration}
		}
		iteration++

		resp, err := e.Provider.CallLLM(ctx, req)
		if err != nil {
			return nil, "", err
		}

		toolCalls := extractToolCalls(resp)
		if len(toolCalls) == 0 {
			return resp, "", nil
		}

		// Check if any tool requires approval
		requiresApproval := false
		for _, tc := range toolCalls {
			if tool, ok := e.Registry.Get(tc.Function.Name); ok {
				if toolWithApproval, ok := tool.(interface{ RequiresApproval() bool }); ok {
					if toolWithApproval.RequiresApproval() {
						requiresApproval = true
						break
					}
				}
			}
		}

		if requiresApproval && e.Store != nil {
			approvalID, err := e.requestApproval(ctx, req, toolCalls)
			if err != nil {
				return nil, "", err
			}
			return nil, approvalID, nil
		}

		results, err := e.ExecuteTools(ctx, toolCalls)
		if err != nil {
			return nil, "", err
		}

		toolMessages := buildToolMessages(toolCalls, results)
		req.Messages = append(req.Messages, toolMessages...)
	}
}

// requestApproval creates an approval record and returns the approval ID.
func (e *AdvancedAgentEngine) requestApproval(ctx context.Context, req *models.LLMRequest, toolCalls []models.ToolCall) (string, error) {
	approvalID, _ := generateApprovalID()
	record := ApprovalRecord{
		ApprovalID: approvalID,
		RequestID:  fmt.Sprintf("req-%d", time.Now().UnixNano()),
		ToolCall:   toolCalls[0],
		Arguments:  toolCalls[0].Function.Arguments,
		Status:     "pending",
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(24 * time.Hour),
	}
	if err := e.Store.Save(ctx, record); err != nil {
		return "", err
	}
	return approvalID, nil
}

// ResumeApproval resumes an agent execution from an approval.
func (e *AdvancedAgentEngine) ResumeApproval(ctx context.Context, approvalID string, approved bool, req *models.LLMRequest) (*models.LLMResponse, error) {
	record, err := e.Store.Get(ctx, approvalID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("approval not found")
	}
	if record.Status != "pending" {
		return nil, fmt.Errorf("approval already processed")
	}

	if !approved {
		record.Status = "rejected"
		record.Error = fmt.Errorf("user rejected approval")
		_ = e.Store.Update(ctx, *record)
		return nil, record.Error
	}

	record.Status = "approved"
	record.Result = "approved"
	_ = e.Store.Update(ctx, *record)

	resp, err := e.Provider.CallLLM(ctx, req)
	if err != nil {
		return nil, err
	}
	toolCalls := extractToolCalls(resp)
	if len(toolCalls) == 0 {
		return resp, nil
	}
	results, err := e.ExecuteTools(ctx, toolCalls)
	if err != nil {
		return nil, err
	}
	toolMessages := buildToolMessages(toolCalls, results)
	req.Messages = append(req.Messages, toolMessages...)
	return resp, nil
}

// generateApprovalID generates a unique approval ID.
func generateApprovalID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// ToolWithApproval wraps a tool with approval requirement.
type ToolWithApproval struct {
	Tool
}

// RequiresApproval returns true for tools wrapped with approval.
func (t *ToolWithApproval) RequiresApproval() bool {
	return true
}

// NewApprovalTool wraps a tool to require human approval.
func NewApprovalTool(t Tool) Tool {
	return &ToolWithApproval{Tool: t}
}
