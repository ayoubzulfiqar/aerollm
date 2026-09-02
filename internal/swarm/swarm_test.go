package swarm

import (
	"context"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/state"
)

type fakeToolProvider struct{}

func (f *fakeToolProvider) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	_ = ctx
	_ = req
	return &models.LLMResponse{
		ID:      "test",
		Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: strPtr("done")}}},
	}, nil
}

func newSwarmEngine(registry *agent.ToolRegistry) *agent.AgentEngine {
	return agent.NewAgentEngine(&fakeToolProvider{}, registry)
}

type fakeProvider struct {
	resp *models.LLMResponse
}

func (f *fakeProvider) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	_ = ctx
	_ = req
	return f.resp, nil
}

func TestSpawnSubAgent(t *testing.T) {
	store, err := state.OpenBboltStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("open state store failed: %v", err)
	}
	defer store.Close()

	registry := agent.NewToolRegistry()
	orch := NewSwarmOrchestrator(store, registry)
	if orch.ActiveCount() != 0 {
		t.Fatalf("expected 0 agents, got %d", orch.ActiveCount())
	}

	ctx := context.Background()
	_, ch := orch.Spawn(ctx, SpawnSubAgentRequest{
		Task:     "do something",
		ParentID: "p1",
		Timeout:  5 * time.Second,
	})
	if orch.ActiveCount() != 1 {
		t.Fatalf("expected 1 agent, got %d", orch.ActiveCount())
	}
	res := <-ch
	if res.Error != nil {
		t.Fatalf("spawn error: %v", res.Error)
	}
	if res.ParentID != "p1" {
		t.Fatalf("expected parent p1, got %s", res.ParentID)
	}
}

func TestCancelSubAgent(t *testing.T) {
	registry := agent.NewToolRegistry()
	orch := NewSwarmOrchestrator(nil, registry)

	ctx := context.Background()
	sub, ch := orch.Spawn(ctx, SpawnSubAgentRequest{Task: "long", Timeout: 30 * time.Second})
	if sub == nil || ch == nil {
		t.Fatal("expected sub and channel")
	}
	orch.Cancel(sub.ID)
	select {
	case <-ch:
		// ok canceled
	case <-time.After(time.Second):
		t.Fatal("expected cancel to return result quickly")
	}
}

func TestSpawnSubAgentTool(t *testing.T) {
	orch := NewSwarmOrchestrator(nil, agent.NewToolRegistry())
	tool := NewSpawnSubAgentTool(orch)
	ctx := context.Background()
	res, err := tool.Execute(ctx, map[string]interface{}{"task": "run task"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m, ok := res.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map result, got %T", res)
	}
	if _, ok := m["content"]; !ok {
		t.Fatalf("expected content in result, got %+v", m)
	}
}
