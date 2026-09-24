package swarm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/state"
)

// ErrNoProvider is reported when a sub-agent is spawned without an LLM provider.
var ErrNoProvider = errors.New("swarm: no LLM provider configured")

// ErrMaxDepth is returned when spawn_sub_agent would exceed MaxSpawnDepth.
var ErrMaxDepth = errors.New("swarm: maximum sub-agent spawn depth exceeded")

// MaxSpawnDepth bounds recursive sub-agent spawning (a sub-agent may itself
// call spawn_sub_agent). Depth 0 is the top-level request.
const MaxSpawnDepth = 3

// SwarmOrchestrator manages the lifecycle of spawned sub-agents.
type SwarmOrchestrator struct {
	mu           sync.Mutex
	agents       map[string]*SubAgent
	stateStore   state.StateStore
	toolRegistry *agent.ToolRegistry
	// Provider is the LLM used by sub-agents. When nil, spawned sub-agents
	// fail with ErrNoProvider.
	Provider agent.ToolProvider
}

// NewSwarmOrchestrator creates a new swarm orchestrator.
func NewSwarmOrchestrator(store state.StateStore, registry *agent.ToolRegistry) *SwarmOrchestrator {
	return &SwarmOrchestrator{
		agents:       make(map[string]*SubAgent),
		stateStore:   store,
		toolRegistry: registry,
	}
}

// SubAgent represents a spawned agent with its own execution context.
type SubAgent struct {
	ID       string
	ParentID string
	Task     string
	Engine   *agent.AgentEngine
	Ctx      context.Context
	Cancel   context.CancelFunc
	Result   *SwarmResult
	mu       sync.RWMutex
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

type ctxKey int

const (
	parentIDKey ctxKey = iota
	depthKey
)

// legacyParentIDKey is the untyped key older callers used.
const legacyParentIDKey = "swarm_parent_id"

// WithParentID returns a context carrying the swarm parent ID used by
// spawn_sub_agent to link sub-agents to their parent.
func WithParentID(ctx context.Context, parentID string) context.Context {
	return context.WithValue(ctx, parentIDKey, parentID)
}

// ParentIDFromContext returns the swarm parent ID, honoring the legacy
// "swarm_parent_id" string key for compatibility.
func ParentIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(parentIDKey).(string); ok && v != "" {
		return v
	}
	// Legacy untyped key kept for backward compatibility.
	if v, ok := ctx.Value(legacyParentIDKey).(string); ok {
		return v
	}
	return ""
}

func depthFromContext(ctx context.Context) int {
	d, _ := ctx.Value(depthKey).(int)
	return d
}

func newID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

// Spawn creates a new sub-agent and runs it concurrently. The returned channel
// is buffered and receives exactly one result before being closed. The
// sub-agent is removed from the active set once it finishes.
func (o *SwarmOrchestrator) Spawn(ctx context.Context, req SpawnSubAgentRequest) (*SubAgent, <-chan *SwarmResult) {
	if o == nil || o.toolRegistry == nil {
		return nil, nil
	}
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	depth := depthFromContext(ctx) + 1
	subCtx, cancel := context.WithTimeout(context.WithValue(ctx, depthKey, depth), timeout)

	sub := &SubAgent{
		ID:       newID("sub"),
		ParentID: req.ParentID,
		Task:     req.Task,
		Ctx:      subCtx,
		Cancel:   cancel,
	}
	if o.Provider != nil {
		sub.Engine = agent.NewAgentEngine(o.Provider, o.toolRegistry)
	}
	subCtx = WithParentID(subCtx, sub.ID)

	o.mu.Lock()
	o.agents[sub.ID] = sub
	o.mu.Unlock()

	resultCh := make(chan *SwarmResult, 1)
	go func() {
		defer close(resultCh)
		defer cancel()
		defer func() {
			o.mu.Lock()
			delete(o.agents, sub.ID)
			o.mu.Unlock()
		}()
		start := time.Now()
		var (
			resp *models.LLMResponse
			err  error
		)
		if sub.Engine == nil {
			err = ErrNoProvider
		} else {
			resp, err = sub.Engine.RunToolExecutionLoop(subCtx, &models.LLMRequest{
				Messages: []models.Message{
					{Role: models.RoleUser, Content: strPtr(req.Task)},
				},
				Tools: o.subAgentTools(depth),
			})
		}
		result := &SwarmResult{
			SubAgentID: sub.ID,
			ParentID:   req.ParentID,
			Response:   resp,
			Error:      err,
			Duration:   time.Since(start),
		}
		if o.stateStore != nil {
			mem := state.Vector{
				ID:   newID("memory"),
				Data: []float64{1},
				Meta: map[string]string{"task": req.Task, "parent_id": req.ParentID},
			}
			if err != nil {
				mem.Meta["error"] = err.Error()
			}
			// Use a fresh short-lived context: the sub-agent context may be
			// cancelled/expired, but the memory record should still be kept.
			storeCtx, storeCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			if storeErr := o.stateStore.StoreShortTermMemory(storeCtx, sub.ID, []state.Vector{mem}); storeErr == nil {
				result.Memories = []state.Vector{mem}
			}
			storeCancel()
		}
		sub.mu.Lock()
		sub.Result = result
		sub.mu.Unlock()
		resultCh <- result // buffered: never blocks
	}()

	return sub, resultCh
}

// subAgentTools returns the tool definitions offered to a sub-agent: every
// registry tool except those requiring human approval, and without
// spawn_sub_agent once the depth limit is reached.
func (o *SwarmOrchestrator) subAgentTools(depth int) []models.ToolDefinition {
	var defs []models.ToolDefinition
	for _, t := range o.toolRegistry.List() {
		if agent.RequiresApproval(t) {
			continue
		}
		if t.Name() == spawnToolName && depth >= MaxSpawnDepth {
			continue
		}
		defs = append(defs, agent.ToolDefinitionOf(t))
	}
	return defs
}

// Cancel terminates a sub-agent by ID.
func (o *SwarmOrchestrator) Cancel(id string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	sub, ok := o.agents[id]
	o.mu.Unlock()
	if ok && sub.Cancel != nil {
		sub.Cancel()
	}
}

// ActiveCount returns the number of currently running sub-agents.
func (o *SwarmOrchestrator) ActiveCount() int {
	if o == nil {
		return 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.agents)
}

const spawnToolName = "spawn_sub_agent"

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
	return spawnToolName
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

// maxTaskLen bounds the task text a model can hand to a sub-agent.
const maxTaskLen = 16 * 1024

// Execute spawns a sub-agent and waits for its result.
func (t *SpawnSubAgentTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	if t == nil || t.orchestrator == nil {
		return nil, fmt.Errorf("orchestrator not configured")
	}
	task, _ := args["task"].(string)
	if task == "" {
		return nil, fmt.Errorf("task is required")
	}
	if len(task) > maxTaskLen {
		return nil, fmt.Errorf("task exceeds %d bytes", maxTaskLen)
	}
	if depthFromContext(ctx) >= MaxSpawnDepth {
		return nil, ErrMaxDepth
	}
	sub, ch := t.orchestrator.Spawn(ctx, SpawnSubAgentRequest{
		Task:     task,
		ParentID: ParentIDFromContext(ctx),
		Timeout:  60 * time.Second,
	})
	if sub == nil || ch == nil {
		return nil, fmt.Errorf("failed to spawn sub-agent")
	}

	select {
	case res, ok := <-ch:
		if !ok || res == nil {
			return nil, fmt.Errorf("sub-agent %s produced no result", sub.ID)
		}
		if res.Error != nil {
			return nil, fmt.Errorf("sub-agent error: %w", res.Error)
		}
		content := ""
		if res.Response != nil && len(res.Response.Choices) > 0 && res.Response.Choices[0].Message.Content != nil {
			content = *res.Response.Choices[0].Message.Content
		}
		return map[string]interface{}{
			"sub_agent_id": sub.ID,
			"content":      content,
		}, nil
	case <-ctx.Done():
		t.orchestrator.Cancel(sub.ID)
		return nil, ctx.Err()
	}
}
