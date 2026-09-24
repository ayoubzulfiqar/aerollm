package swarm

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// blockingProvider blocks until released or the context is cancelled.
type blockingProvider struct {
	release chan struct{}
	mu      sync.Mutex
	lastReq *models.LLMRequest
}

func (b *blockingProvider) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	b.mu.Lock()
	b.lastReq = req
	b.mu.Unlock()
	select {
	case <-b.release:
		return &models.LLMResponse{Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: strPtr("released")}}}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type namedTool struct{ name string }

func (n *namedTool) Name() string        { return n.name }
func (n *namedTool) Description() string { return "test tool" }
func (n *namedTool) Parameters() map[string]interface{} {
	return map[string]interface{}{"type": "object"}
}
func (n *namedTool) Execute(context.Context, map[string]interface{}) (interface{}, error) {
	return "ok", nil
}

func waitResult(t *testing.T, ch <-chan *SwarmResult) *SwarmResult {
	t.Helper()
	select {
	case res := <-ch:
		return res
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for sub-agent result")
		return nil
	}
}

func waitActive(t *testing.T, orch *SwarmOrchestrator, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for orch.ActiveCount() != want {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d active agents, got %d", want, orch.ActiveCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSpawnSubAgent(t *testing.T) {
	store, err := state.OpenBboltStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("open state store failed: %v", err)
	}
	defer store.Close()

	orch := NewSwarmOrchestrator(store, agent.NewToolRegistry())
	orch.Provider = &fakeToolProvider{}
	if orch.ActiveCount() != 0 {
		t.Fatalf("expected 0 agents, got %d", orch.ActiveCount())
	}
	sub, ch := orch.Spawn(context.Background(), SpawnSubAgentRequest{Task: "do something", ParentID: "p1", Timeout: 5 * time.Second})
	if sub == nil || !strings.HasPrefix(sub.ID, "sub-") || len(sub.ID) < 20 {
		t.Fatalf("expected random sub-agent id, got %+v", sub)
	}
	res := waitResult(t, ch)
	if res.Error != nil {
		t.Fatalf("spawn error: %v", res.Error)
	}
	if res.ParentID != "p1" {
		t.Fatalf("expected parent p1, got %s", res.ParentID)
	}
	if len(res.Memories) != 1 {
		t.Fatalf("expected memory to be recorded, got %+v", res.Memories)
	}
	waitActive(t, orch, 0)
}

func TestActiveCountTracksRunningAgents(t *testing.T) {
	prov := &blockingProvider{release: make(chan struct{})}
	orch := NewSwarmOrchestrator(nil, agent.NewToolRegistry())
	orch.Provider = prov
	_, ch := orch.Spawn(context.Background(), SpawnSubAgentRequest{Task: "long", Timeout: 5 * time.Second})
	if orch.ActiveCount() != 1 {
		t.Fatalf("expected 1 running agent, got %d", orch.ActiveCount())
	}
	close(prov.release)
	if res := waitResult(t, ch); res.Error != nil {
		t.Fatalf("unexpected error: %v", res.Error)
	}
	waitActive(t, orch, 0)
}

func TestSpawnWithoutProviderFailsHonestly(t *testing.T) {
	orch := NewSwarmOrchestrator(nil, agent.NewToolRegistry())
	_, ch := orch.Spawn(context.Background(), SpawnSubAgentRequest{Task: "x"})
	res := waitResult(t, ch)
	if !errors.Is(res.Error, ErrNoProvider) {
		t.Fatalf("expected ErrNoProvider, got %v", res.Error)
	}
}

func TestCancelSubAgent(t *testing.T) {
	orch := NewSwarmOrchestrator(nil, agent.NewToolRegistry())
	orch.Provider = &blockingProvider{release: make(chan struct{})}
	sub, ch := orch.Spawn(context.Background(), SpawnSubAgentRequest{Task: "long", Timeout: 30 * time.Second})
	if sub == nil || ch == nil {
		t.Fatal("expected sub and channel")
	}
	orch.Cancel(sub.ID)
	select {
	case res := <-ch:
		if res == nil || !errors.Is(res.Error, context.Canceled) {
			t.Fatalf("expected cancellation error, got %+v", res)
		}
	case <-time.After(time.Second):
		t.Fatal("expected cancel to return result quickly")
	}
}

func TestSubAgentOffersRegistryToolsExceptApproval(t *testing.T) {
	reg := agent.NewToolRegistry()
	_ = reg.Register(&namedTool{name: "calc"})
	_ = reg.Register(agent.NewApprovalTool(&namedTool{name: "dangerous"}))
	prov := &blockingProvider{release: make(chan struct{})}
	close(prov.release)
	orch := NewSwarmOrchestrator(nil, reg)
	orch.Provider = prov
	_, ch := orch.Spawn(context.Background(), SpawnSubAgentRequest{Task: "t"})
	waitResult(t, ch)
	prov.mu.Lock()
	defer prov.mu.Unlock()
	if prov.lastReq == nil {
		t.Fatal("provider not called")
	}
	names := map[string]bool{}
	for _, d := range prov.lastReq.Tools {
		names[d.Name] = true
	}
	if !names["calc"] || names["dangerous"] {
		t.Fatalf("unexpected offered tools: %+v", prov.lastReq.Tools)
	}
}

func TestSpawnSubAgentTool(t *testing.T) {
	orch := NewSwarmOrchestrator(nil, agent.NewToolRegistry())
	orch.Provider = &fakeToolProvider{}
	tool := NewSpawnSubAgentTool(orch)
	res, err := tool.Execute(context.Background(), map[string]interface{}{"task": "run task"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m, ok := res.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map result, got %T", res)
	}
	if c, ok := m["content"].(string); !ok || c != "done" {
		t.Fatalf("expected plain string content, got %#v", m["content"])
	}
}

func TestSpawnSubAgentToolDepthLimit(t *testing.T) {
	orch := NewSwarmOrchestrator(nil, agent.NewToolRegistry())
	orch.Provider = &fakeToolProvider{}
	tool := NewSpawnSubAgentTool(orch)
	ctx := context.WithValue(context.Background(), depthKey, MaxSpawnDepth)
	if _, err := tool.Execute(ctx, map[string]interface{}{"task": "x"}); !errors.Is(err, ErrMaxDepth) {
		t.Fatalf("expected ErrMaxDepth, got %v", err)
	}
	if _, err := tool.Execute(context.Background(), map[string]interface{}{"task": strings.Repeat("a", maxTaskLen+1)}); err == nil {
		t.Fatal("expected error for oversized task")
	}
}

func TestParentIDFromContext(t *testing.T) {
	if got := ParentIDFromContext(WithParentID(context.Background(), "p")); got != "p" {
		t.Fatalf("typed key: got %q", got)
	}
	//nolint:staticcheck // exercising legacy untyped key.
	legacy := context.WithValue(context.Background(), legacyParentIDKey, "legacy")
	if got := ParentIDFromContext(legacy); got != "legacy" {
		t.Fatalf("legacy key: got %q", got)
	}
}

func voter(choice string, err error) Voter {
	return func(ctx context.Context, topic string, options []string) (string, error) {
		return choice, err
	}
}

func TestRunVoteTalliesVotes(t *testing.T) {
	c := NewConsensusProtocol()
	res, err := c.RunVote(context.Background(), VoteRequest{
		Topic:   "pick",
		Options: []string{"a", "b"},
		Voters:  []Voter{voter("b", nil), voter("b", nil), voter("a", nil), voter("zzz", nil), voter("", errors.New("x"))},
	})
	if err != nil {
		t.Fatalf("vote failed: %v", err)
	}
	if res.Winner != "b" || res.Votes["b"] != 2 || res.Votes["a"] != 1 || res.Quorum != 3 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(c.History()) != 1 {
		t.Fatalf("expected history entry")
	}
}

func TestRunVoteTieBreakAndQuorum(t *testing.T) {
	c := NewConsensusProtocol()
	res, err := c.RunVote(context.Background(), VoteRequest{Options: []string{"x", "y"}, Voters: []Voter{voter("y", nil), voter("x", nil)}})
	if err != nil || res.Winner != "x" {
		t.Fatalf("expected tie broken by option order, got %+v err=%v", res, err)
	}
	if _, err := c.RunVote(context.Background(), VoteRequest{Options: []string{"x"}, MinVoters: 2, Voters: []Voter{voter("x", nil)}}); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("expected ErrNoQuorum, got %v", err)
	}
	if _, err := c.RunVote(context.Background(), VoteRequest{Options: []string{"x"}}); !errors.Is(err, ErrNoVoters) {
		t.Fatalf("expected ErrNoVoters, got %v", err)
	}
	if _, err := c.RunVote(context.Background(), VoteRequest{Voters: []Voter{voter("x", nil)}}); err == nil {
		t.Fatal("expected error for no options")
	}
}

func TestRunVoteTimeoutIgnoresSlowVoters(t *testing.T) {
	c := NewConsensusProtocol()
	slow := func(ctx context.Context, topic string, options []string) (string, error) {
		<-ctx.Done()
		return "b", nil
	}
	panicky := func(ctx context.Context, topic string, options []string) (string, error) { panic("boom") }
	start := time.Now()
	res, err := c.RunVote(context.Background(), VoteRequest{
		Options: []string{"a", "b"},
		Timeout: 50 * time.Millisecond,
		Voters:  []Voter{voter("a", nil), slow, panicky},
	})
	if err != nil {
		t.Fatalf("vote failed: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("vote did not honor timeout")
	}
	if res.Winner != "a" {
		t.Fatalf("expected a to win, got %+v", res)
	}
}

func TestFederatedShareAndExport(t *testing.T) {
	store, err := state.OpenBboltStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	f := NewFederatedLearning(store)
	ctx := context.Background()
	if err := f.ShareKnowledge(ctx, KnowledgeFragment{Topic: "t", Content: "c", Embedding: []float64{1, 0}}); err != nil {
		t.Fatalf("share: %v", err)
	}
	if err := f.ShareKnowledge(ctx, KnowledgeFragment{ID: "bad", Embedding: []float64{math.NaN()}}); err == nil {
		t.Fatal("expected NaN embedding to be rejected")
	}
	path := filepath.Join(t.TempDir(), "ckpt", "knowledge.jsonl")
	if err := f.ExportCheckpoint(ctx, path); err != nil {
		t.Fatalf("export: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected 0600 perms, got %v", info.Mode().Perm())
	}
	b, _ := os.ReadFile(path)
	if lines := strings.Count(string(b), "\n"); lines != 1 {
		t.Fatalf("expected 1 fragment, got %d: %s", lines, b)
	}
	if err := f.ExportCheckpoint(ctx, ""); err == nil {
		t.Fatal("expected error for empty path")
	}
}
