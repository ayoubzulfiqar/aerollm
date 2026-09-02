package orchestrator

import (
	"context"
	"errors"
	"testing"
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
