package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ToolResult holds the result of executing a tool call.
type ToolResult struct {
	ToolCallID string
	Name       string
	Content    interface{}
	Error      error
	Duration   time.Duration
	Cached     bool
}

// ContentToString returns the tool result content as a string.
func (t *ToolResult) ContentToString() string {
	if t.Content == nil {
		return ""
	}
	switch v := t.Content.(type) {
	case string:
		return v
	default:
		return ""
	}
}

// ToolProvider is the interface for calling the LLM during the agent loop.
type ToolProvider interface {
	CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error)
}

// AgentEngine executes tool calls from LLM responses concurrently.
type AgentEngine struct {
	Provider      ToolProvider
	MaxIterations int
	ToolTimeout   time.Duration
	MaxConcurrent int
	ToolCache     map[string]*ToolResult
	mu            sync.RWMutex
}

// NewAgentEngine creates a new AgentEngine.
func NewAgentEngine(provider ToolProvider) *AgentEngine {
	return &AgentEngine{
		Provider:      provider,
		MaxIterations: 10,
		ToolTimeout:   30 * time.Second,
		MaxConcurrent: 10,
		ToolCache:     make(map[string]*ToolResult),
	}
}

// RunToolExecutionLoop runs the agentic loop: parse tool calls, execute concurrently,
// feed results back to LLM, and repeat until a text response is generated.
func (e *AgentEngine) RunToolExecutionLoop(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	iteration := 0
	for {
		if iteration >= e.MaxIterations {
			return nil, &MaxIterationsError{Iteration: iteration}
		}
		iteration++

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
	}
}

// ExecuteTools runs multiple tool calls concurrently using errgroup.
func (e *AgentEngine) ExecuteTools(ctx context.Context, toolCalls []models.ToolCall) ([]*ToolResult, error) {
	timeout := e.ToolTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(e.getMaxConcurrent())

	results := make([]*ToolResult, len(toolCalls))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, tc := range toolCalls {
		i := i
		tc := tc
		g.Go(func() error {
			defer wg.Done()
			start := time.Now()
			result, err := e.executeSingleTool(ctx, tc)
			result.Duration = time.Since(start)
			mu.Lock()
			results[i] = result
			mu.Unlock()
			return err
		})
		wg.Add(1)
	}

	if err := g.Wait(); err != nil {
		return results, err
	}

	return results, nil
}

// executeSingleTool executes a single tool call.
func (e *AgentEngine) executeSingleTool(ctx context.Context, tc models.ToolCall) (*ToolResult, error) {
	select {
	case <-ctx.Done():
		return &ToolResult{
			ToolCallID: tc.ID,
			Name:       tc.Function.Name,
			Error:      ctx.Err(),
		}, ctx.Err()
	default:
		return &ToolResult{
			ToolCallID: tc.ID,
			Name:       tc.Function.Name,
			Content:    nil,
			Error:      nil,
		}, nil
	}
}

func (e *AgentEngine) getMaxConcurrent() int {
	if e.MaxConcurrent > 0 {
		return e.MaxConcurrent
	}
	return 10
}

// MaxIterationsError is returned when the agent exceeds max iterations.
type MaxIterationsError struct {
	Iteration int
}

func (e *MaxIterationsError) Error() string {
	return fmt.Sprintf("max iterations (%d) reached", e.Iteration)
}

// extractToolCalls parses tool_calls from the LLM response.
func extractToolCalls(resp *models.LLMResponse) []models.ToolCall {
	var calls []models.ToolCall
	for _, choice := range resp.Choices {
		calls = append(calls, choice.Message.ToolCalls...)
	}
	return calls
}

// buildToolMessages formats tool results back into the message history.
func buildToolMessages(calls []models.ToolCall, results []*ToolResult) []models.Message {
	messages := make([]models.Message, len(calls))
	for i, tc := range calls {
		content := ""
		if i < len(results) && results[i] != nil {
			content = results[i].ContentToString()
			if results[i].Error != nil {
				content = "error: " + results[i].Error.Error()
			}
		}
		messages[i] = models.Message{
			Role:      models.RoleTool,
			ToolCallID: &tc.ID,
			ToolResult: &content,
		}
	}
	return messages
}

// ToolCallCacheKey generates a cache key for a tool call.
func ToolCallCacheKey(tc models.ToolCall) string {
	return fmt.Sprintf("tool:%s:%s", tc.Function.Name, tc.ID)
}
