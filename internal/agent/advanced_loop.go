package agent

import (
	"context"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// LoopHook is called at agent loop checkpoints.
type LoopHook interface {
	BeforeLLM(ctx context.Context, req *models.LLMRequest)
	AfterLLM(ctx context.Context, resp *models.LLMResponse)
	BeforeTools(ctx context.Context, calls []models.ToolCall)
	AfterTools(ctx context.Context, results []*ToolResult)
	OnToolDeficit(ctx context.Context, deficit ToolDeficitSignal)
}

// ToolDeficitSignal mirrors synthesis deficit signals for agent loop handling.
type ToolDeficitSignal struct {
	RequestID   string
	MissingTool string
	Reason      string
}

// NoopLoopHook is a zero-value hook implementation.
type NoopLoopHook struct{}

func (n *NoopLoopHook) BeforeLLM(_ context.Context, _ *models.LLMRequest) {}
func (n *NoopLoopHook) AfterLLM(_ context.Context, _ *models.LLMResponse) {}
func (n *NoopLoopHook) BeforeTools(_ context.Context, _ []models.ToolCall) {}
func (n *NoopLoopHook) AfterTools(_ context.Context, _ []*ToolResult)     {}
func (n *NoopLoopHook) OnToolDeficit(_ context.Context, _ ToolDeficitSignal) {}

// AdvancedLoopOptions configures the advanced agent loop.
type AdvancedLoopOptions struct {
	Hooks            []LoopHook
	RetryToolErrors  bool
	MaxToolRetries   int
	ToolRetryDelay   time.Duration
}

// RunAdvancedExecutionLoop runs the agentic loop with hooks, retry, and deficit handling.
func (e *AgentEngine) RunAdvancedExecutionLoop(ctx context.Context, req *models.LLMRequest, opts AdvancedLoopOptions) (*models.LLMResponse, error) {
	if opts.MaxToolRetries <= 0 {
		opts.MaxToolRetries = 1
	}
	if opts.ToolRetryDelay <= 0 {
		opts.ToolRetryDelay = 200 * time.Millisecond
	}

	iteration := 0
	for {
		if iteration >= e.MaxIterations {
			return nil, &MaxIterationsError{Iteration: iteration}
		}
		iteration++

		for _, hook := range opts.Hooks {
			hook.BeforeLLM(ctx, req)
		}

		resp, err := e.Provider.CallLLM(ctx, req)
		if err != nil {
			return nil, err
		}

		for _, hook := range opts.Hooks {
			hook.AfterLLM(ctx, resp)
		}

		toolCalls := extractToolCalls(resp)
		if len(toolCalls) == 0 {
			return resp, nil
		}

		for _, hook := range opts.Hooks {
			hook.BeforeTools(ctx, toolCalls)
		}

		results, err := e.ExecuteToolsWithRetry(ctx, toolCalls, opts)
		if err != nil {
			return nil, err
		}

		for _, hook := range opts.Hooks {
			hook.AfterTools(ctx, results)
		}

		toolMessages := buildToolMessages(toolCalls, results)
		req.Messages = append(req.Messages, toolMessages...)
	}
}

// ExecuteToolsWithRetry executes tool calls with optional retry on transient errors.
func (e *AgentEngine) ExecuteToolsWithRetry(ctx context.Context, toolCalls []models.ToolCall, opts AdvancedLoopOptions) ([]*ToolResult, error) {
	results, err := e.ExecuteTools(ctx, toolCalls)
	if err == nil || !opts.RetryToolErrors {
		return results, err
	}

	var lastErr error
	for attempt := 0; attempt < opts.MaxToolRetries; attempt++ {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		case <-time.After(opts.ToolRetryDelay):
		}

		failed := make([]models.ToolCall, 0, len(results))
		for i, r := range results {
			if r != nil && r.Error != nil {
				failed = append(failed, toolCalls[i])
			}
		}
		if len(failed) == 0 {
			return results, nil
		}

		retryResults, retryErr := e.ExecuteTools(ctx, failed)
		if retryErr != nil {
			lastErr = retryErr
			continue
		}
		j := 0
		for i, r := range results {
			if r != nil && r.Error != nil {
				if j < len(retryResults) {
					results[i] = retryResults[j]
					j++
				}
			}
		}
		return results, nil
	}

	if lastErr != nil {
		return results, lastErr
	}
	return results, err
}

// ToolDeficitHandler converts synthesis-style deficit signals into agent loop events.
type ToolDeficitHandler struct {
	mu       sync.RWMutex
	deficits map[string]ToolDeficitSignal
}

// NewToolDeficitHandler creates a new handler.
func NewToolDeficitHandler() *ToolDeficitHandler {
	return &ToolDeficitHandler{deficits: make(map[string]ToolDeficitSignal)}
}

// Record stores a deficit signal by request ID.
func (h *ToolDeficitHandler) Record(reqID string, signal ToolDeficitSignal) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deficits[reqID] = signal
}

// Get retrieves a deficit signal by request ID.
func (h *ToolDeficitHandler) Get(reqID string) (ToolDeficitSignal, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s, ok := h.deficits[reqID]
	return s, ok
}
