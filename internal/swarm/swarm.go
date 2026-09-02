package swarm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/state"
)

type noopToolProvider struct{}

func (n *noopToolProvider) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	_ = ctx
	_ = req
	return &models.LLMResponse{
		ID:      "noop",
		Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: strPtr("ok")}}},
	}, nil
}

// SwarmOrchestrator manages the lifecycle of spawned sub-agents.
type SwarmOrchestrator struct {
	mu        sync.Mutex
	agents    map[string]*SubAgent
	stateStore state.StateStore
	toolRegistry *agent.ToolRegistry
	Provider agent.ToolProvider
}

// NewSwarmOrchestrator creates a new swarm orchestrator.
func NewSwarmOrchestrator(store state.StateStore, registry *agent.ToolRegistry) *SwarmOrchestrator {
	return &SwarmOrchestrator{
		agents:     make(map[string]*SubAgent),
		stateStore: store,
		toolRegistry: registry,
	}
}

// SubAgent represents a spawned agent with its own execution context.
type SubAgent struct {
	ID          string
	ParentID    string
	Task        string
	Engine      *agent.AgentEngine
	Ctx         context.Context
	Cancel      context.CancelFunc
	Result      *SwarmResult
	mu          sync.RWMutex
}

// SwarmResult holds the result of a sub-agent execution.
type SwarmResult struct {
	SubAgentID string
	ParentID   string
	Response   *models.LLMResponse
	Error      error
	Duration   time.Duration
	Memories   []state.Vector
}

func strPtr(s string) *string { return &s }

// SpawnSubAgentRequest is the input for spawning a sub-agent.
type SpawnSubAgentRequest struct {
	Task     string
	ParentID string
	Timeout  time.Duration
}

// Spawn creates a new sub-agent and runs it concurrently.
func (o *SwarmOrchestrator) Spawn(ctx context.Context, req SpawnSubAgentRequest) (*SubAgent, <-chan *SwarmResult) {
	if o == nil || o.toolRegistry == nil {
		return nil, nil
	}
	subCtx, cancel := context.WithCancel(ctx)
	timeout := req.Timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	subCtx, cancel = context.WithTimeout(subCtx, timeout)

	sub := &SubAgent{
		ID:       fmt.Sprintf("sub-%d", time.Now().UnixNano()),
		ParentID: req.ParentID,
		Task:     req.Task,
	}
	if o.Provider == nil {
		sub.Engine = agent.NewAgentEngine(&noopToolProvider{}, o.toolRegistry)
	} else {
		sub.Engine = agent.NewAgentEngine(o.Provider, o.toolRegistry)
	}
	sub.Ctx = subCtx
	sub.Cancel = cancel

	o.mu.Lock()
	o.agents[sub.ID] = sub
	o.mu.Unlock()

	resultCh := make(chan *SwarmResult, 1)
	go func() {
		defer close(resultCh)
		start := time.Now()
		resp, err := sub.Engine.RunToolExecutionLoop(subCtx, &models.LLMRequest{
			Messages: []models.Message{
				{Role: models.RoleUser, Content: strPtr(req.Task)},
			},
		})
		dur := time.Since(start)
		result := &SwarmResult{
			SubAgentID: sub.ID,
			ParentID:   req.ParentID,
			Response:   resp,
			Error:      err,
			Duration:   dur,
		}
		sub.mu.Lock()
		sub.Result = result
		sub.mu.Unlock()

		if o.stateStore != nil {
			_ = o.stateStore.StoreShortTermMemory(ctx, sub.ID, []state.Vector{
				{
					ID:   fmt.Sprintf("memory-%d", time.Now().UnixNano()),
					Data: []float64{1},
					Meta: map[string]string{"task": req.Task, "error": fmt.Sprint(err)},
				},
			})
		}

		select {
		case resultCh <- result:
		case <-ctx.Done():
		}
	}()

	return sub, resultCh
}

// Cancel terminates a sub-agent by ID.
func (o *SwarmOrchestrator) Cancel(id string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if sub, ok := o.agents[id]; ok {
		if sub.Cancel != nil {
			sub.Cancel()
		}
	}
}

// ActiveCount returns the number of tracked sub-agents.
func (o *SwarmOrchestrator) ActiveCount() int {
	if o == nil {
		return 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.agents)
}

// SpawnSubAgentTool is a tool that triggers sub-agent spawning.
type SpawnSubAgentTool struct {
	orchestrator *SwarmOrchestrator
}

// NewSpawnSubAgentTool creates a new spawn tool.
func NewSpawnSubAgentTool(orchestrator *SwarmOrchestrator) *SpawnSubAgentTool {
	return &SpawnSubAgentTool{orchestrator: orchestrator}
}

// Name returns the tool name.
func (t *SpawnSubAgentTool) Name() string {
	return "spawn_sub_agent"
}

// Description returns the tool description.
func (t *SpawnSubAgentTool) Description() string {
	return "Spawns a sub-agent to execute a task concurrently in the swarm"
}

// Parameters returns the tool parameters schema.
func (t *SpawnSubAgentTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"task": map[string]interface{}{
				"type":        "string",
				"description": "The task for the sub-agent to execute",
			},
		},
		"required": []string{"task"},
	}
}

// Execute spawns a sub-agent and waits for its result.
func (t *SpawnSubAgentTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	if t.orchestrator == nil {
		return nil, fmt.Errorf("orchestrator not configured")
	}
	task, _ := args["task"].(string)
	if task == "" {
		return nil, fmt.Errorf("task is required")
	}
	parentID, _ := ctx.Value("swarm_parent_id").(string)
	sub, ch := t.orchestrator.Spawn(ctx, SpawnSubAgentRequest{
		Task:     task,
		ParentID: parentID,
		Timeout:  60 * time.Second,
	})
	if sub == nil || ch == nil {
		return nil, fmt.Errorf("failed to spawn sub-agent")
	}

	select {
	case res := <-ch:
		if res.Error != nil {
			return nil, fmt.Errorf("sub-agent error: %w", res.Error)
		}
		if res.Response == nil || len(res.Response.Choices) == 0 {
			return map[string]interface{}{"sub_agent_id": sub.ID, "content": ""}, nil
		}
		return map[string]interface{}{
			"sub_agent_id": sub.ID,
			"content":      res.Response.Choices[0].Message.Content,
		}, nil
	case <-ctx.Done():
		t.orchestrator.Cancel(sub.ID)
		return nil, ctx.Err()
	}
}
