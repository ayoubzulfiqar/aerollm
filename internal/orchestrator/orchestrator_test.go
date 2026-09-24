package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExecuteLinearDAG(t *testing.T) {
	graph := Graph{
		Nodes: []Node{
			{ID: "a", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return map[string]interface{}{"value": 1}, nil
			}},
			{ID: "b", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				v, _ := input["value"].(int)
				return map[string]interface{}{"value": v + 1}, nil
			}, DependsOn: []string{"a"}},
		},
		Edges: []Edge{{From: "a", To: "b", Source: "value", Target: "value"}},
	}
	res, err := Execute(context.Background(), graph, ExecutionOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	out, ok := res.Get("b")
	if !ok || out["value"] != 2 {
		t.Fatalf("expected b output 2, got %v", out)
	}
}

func TestExecuteFanOut(t *testing.T) {
	graph := Graph{
		Nodes: []Node{
			{ID: "a", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return map[string]interface{}{"value": 10}, nil
			}},
			{ID: "b", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				v, _ := input["value"].(int)
				return map[string]interface{}{"value": v + 1}, nil
			}, DependsOn: []string{"a"}},
			{ID: "c", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				v, _ := input["value"].(int)
				return map[string]interface{}{"value": v + 2}, nil
			}, DependsOn: []string{"a"}},
		},
	}
	_, err := Execute(context.Background(), graph, ExecutionOptions{MaxConcurrency: 4})
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
}

func TestValidateGraphDetectsMissingNode(t *testing.T) {
	graph := Graph{
		Nodes: []Node{{ID: "a", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
			return nil, nil
		}, DependsOn: []string{"missing"}}},
	}
	err := ValidateGraph(graph)
	if err == nil {
		t.Fatal("expected error for missing dependency")
	}
}

func TestValidateGraphDetectsCycle(t *testing.T) {
	graph := Graph{
		Nodes: []Node{
			{ID: "a", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			}, DependsOn: []string{"b"}},
			{ID: "b", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			}, DependsOn: []string{"a"}},
		},
	}
	err := ValidateGraph(graph)
	if err == nil {
		t.Fatal("expected cycle detection error")
	}
}

func TestExecutePropagatesUpstreamError(t *testing.T) {
	graph := Graph{
		Nodes: []Node{
			{ID: "a", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return nil, errors.New("fail a")
			}},
			{ID: "b", Kind: NodeKindFunction, Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			}, DependsOn: []string{"a"}},
		},
	}
	_, err := Execute(context.Background(), graph, ExecutionOptions{MaxConcurrency: 2})
	if err == nil {
		t.Fatalf("expected upstream error, got: %v", err)
	}
}

func fn(out map[string]interface{}) func(context.Context, map[string]interface{}) (map[string]interface{}, error) {
	return func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
		return out, nil
	}
}

func runWithTimeout(t *testing.T, graph Graph, opts ExecutionOptions) (*ExecutionResult, error) {
	t.Helper()
	type res struct {
		r   *ExecutionResult
		err error
	}
	ch := make(chan res, 1)
	go func() {
		r, err := Execute(context.Background(), graph, opts)
		ch <- res{r, err}
	}()
	select {
	case r := <-ch:
		return r.r, r.err
	case <-time.After(3 * time.Second):
		t.Fatal("Execute deadlocked")
		return nil, nil
	}
}

func TestExecuteNoDeadlockWithConcurrencyOneReverseOrder(t *testing.T) {
	graph := Graph{Nodes: []Node{
		{ID: "c", Run: fn(map[string]interface{}{"c": 3}), DependsOn: []string{"b"}},
		{ID: "b", Run: fn(map[string]interface{}{"b": 2}), DependsOn: []string{"a"}},
		{ID: "a", Run: fn(map[string]interface{}{"a": 1})},
	}}
	res, err := runWithTimeout(t, graph, ExecutionOptions{MaxConcurrency: 1})
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if _, ok := res.Get(id); !ok {
			t.Fatalf("missing output for %s", id)
		}
	}
}

func TestExecuteCycleReturnsErrorWithoutHanging(t *testing.T) {
	graph := Graph{Nodes: []Node{
		{ID: "a", Run: fn(nil), DependsOn: []string{"b"}},
		{ID: "b", Run: fn(nil), DependsOn: []string{"a"}},
	}}
	_, err := runWithTimeout(t, graph, ExecutionOptions{})
	if !errors.Is(err, ErrCycle) {
		t.Fatalf("expected ErrCycle, got %v", err)
	}
	selfLoop := Graph{Nodes: []Node{{ID: "a", Run: fn(nil)}}, Edges: []Edge{{From: "a", To: "a"}}}
	if _, err := runWithTimeout(t, selfLoop, ExecutionOptions{}); !errors.Is(err, ErrCycle) {
		t.Fatalf("expected ErrCycle for self-loop edge, got %v", err)
	}
}

func TestExecuteRejectsInvalidNodes(t *testing.T) {
	dup := Graph{Nodes: []Node{{ID: "a", Run: fn(nil)}, {ID: "a", Run: fn(nil)}}}
	if _, err := runWithTimeout(t, dup, ExecutionOptions{}); err == nil {
		t.Fatal("expected duplicate id error")
	}
	nilRun := Graph{Nodes: []Node{{ID: "a"}}}
	if _, err := runWithTimeout(t, nilRun, ExecutionOptions{}); err == nil {
		t.Fatal("expected nil Run error")
	}
	emptyID := Graph{Nodes: []Node{{Run: fn(nil)}}}
	if _, err := runWithTimeout(t, emptyID, ExecutionOptions{}); err == nil {
		t.Fatal("expected empty id error")
	}
}

func TestExecuteRecoversPanics(t *testing.T) {
	graph := Graph{Nodes: []Node{{ID: "p", Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
		panic("boom")
	}}}}
	_, err := runWithTimeout(t, graph, ExecutionOptions{})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected panic converted to error, got %v", err)
	}
}

func TestExecuteEdgesImplyDependencies(t *testing.T) {
	var mu sync.Mutex
	var order []string
	rec := func(id string, out map[string]interface{}) func(context.Context, map[string]interface{}) (map[string]interface{}, error) {
		return func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			if id == "b" {
				return map[string]interface{}{"got": input["x"]}, nil
			}
			return out, nil
		}
	}
	graph := Graph{
		Nodes: []Node{{ID: "b", Run: rec("b", nil)}, {ID: "a", Run: rec("a", map[string]interface{}{"v": 7})}},
		Edges: []Edge{{From: "a", To: "b", Source: "v", Target: "x"}},
	}
	res, err := runWithTimeout(t, graph, ExecutionOptions{MaxConcurrency: 4})
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	if len(order) != 2 || order[0] != "a" {
		t.Fatalf("edge did not imply ordering: %v", order)
	}
	if out, _ := res.Get("b"); out["got"] != 7 {
		t.Fatalf("edge input not resolved: %v", out)
	}
}

func TestExecuteRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	graph := Graph{Nodes: []Node{
		{ID: "slow", Run: func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}},
		{ID: "after", Run: fn(nil), DependsOn: []string{"slow"}},
	}}
	errCh := make(chan error, 1)
	go func() {
		_, err := Execute(ctx, graph, ExecutionOptions{})
		errCh <- err
	}()
	<-started
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Execute did not return after cancellation")
	}
}

func TestExecuteEmptyGraph(t *testing.T) {
	if _, err := Execute(context.Background(), Graph{}, ExecutionOptions{}); err != nil {
		t.Fatalf("unexpected error for empty graph: %v", err)
	}
}
