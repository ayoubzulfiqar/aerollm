package agent

import (
	"context"
	"encoding/json"
	"errors"
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

// Tool is the interface that tool implementations must satisfy.
type Tool interface {
	Name() string
	Description() string
	Parameters() map[string]interface{}
	Execute(ctx context.Context, args map[string]interface{}) (interface{}, error)
}

// ToolRegistry stores available tools for the agent.
type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// NewToolRegistry creates a new ToolRegistry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: make(map[string]Tool)}
}

// Register adds a tool to the registry.
func (r *ToolRegistry) Register(t Tool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t == nil {
		return errors.New("tool cannot be nil")
	}
	if _, exists := r.tools[t.Name()]; exists {
		return fmt.Errorf("tool %q already registered", t.Name())
	}
	r.tools[t.Name()] = t
	return nil
}

// Get returns a tool by name.
func (r *ToolRegistry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tools[name]
	return t, ok
}

// All returns all registered tool names.
func (r *ToolRegistry) All() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	return names
}

// Execute runs a tool by name with JSON arguments.
func (r *ToolRegistry) Execute(ctx context.Context, name string, argsJSON string) (interface{}, error) {
	r.mu.RLock()
	t, ok := r.tools[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown tool: %s", name)
	}

	var args map[string]interface{}
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return nil, fmt.Errorf("invalid tool arguments: %w", err)
	}

	result, err := t.Execute(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("tool %q failed: %w", name, err)
	}
	return result, nil
}

// AgentEngine executes tool calls from LLM responses concurrently.
type AgentEngine struct {
	Provider      ToolProvider
	MaxIterations int
	ToolTimeout   time.Duration
	MaxConcurrent int
	ToolCache     map[string]*ToolResult
	mu            sync.RWMutex
	Registry      *ToolRegistry
	ToolBilling   ToolCallBiller
}

// ToolCallBiller bills tool executions for the economy layer.
type ToolCallBiller interface {
	BillToolCall(ctx context.Context, toolName string) error
}

// NewAgentEngine creates a new AgentEngine.
func NewAgentEngine(provider ToolProvider, registry *ToolRegistry) *AgentEngine {
	if registry == nil {
		registry = NewToolRegistry()
	}
	return &AgentEngine{
		Provider:      provider,
		MaxIterations: 10,
		ToolTimeout:   30 * time.Second,
		MaxConcurrent: 10,
		ToolCache:     make(map[string]*ToolResult),
		Registry:      registry,
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

	for i, tc := range toolCalls {
		i := i
		tc := tc
		g.Go(func() error {
			start := time.Now()
			result, err := e.executeSingleTool(ctx, tc)
			result.Duration = time.Since(start)
			mu.Lock()
			results[i] = result
			mu.Unlock()
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return results, err
	}

	return results, nil
}

// executeSingleTool executes a single tool call from the registry.
func (e *AgentEngine) executeSingleTool(ctx context.Context, tc models.ToolCall) (*ToolResult, error) {
	select {
	case <-ctx.Done():
		return &ToolResult{
			ToolCallID: tc.ID,
			Name:       tc.Function.Name,
			Error:      ctx.Err(),
		}, ctx.Err()
	default:
	}

	if e.Registry == nil {
		return &ToolResult{
			ToolCallID: tc.ID,
			Name:       tc.Function.Name,
			Content:    nil,
			Error:      nil,
		}, nil
	}

	content, err := e.Registry.Execute(ctx, tc.Function.Name, tc.Function.Arguments)
	if err != nil {
		return &ToolResult{
			ToolCallID: tc.ID,
			Name:       tc.Function.Name,
			Error:      err,
		}, err
	}

	result := &ToolResult{
		ToolCallID: tc.ID,
		Name:       tc.Function.Name,
		Content:    content,
		Error:      nil,
	}

	if e.ToolBilling != nil {
		_ = e.ToolBilling.BillToolCall(ctx, tc.Function.Name)
	}

	return result, nil
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
			Role:       models.RoleTool,
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
