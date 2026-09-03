package k8s

import (
	"context"
	"testing"
)

func TestManifestReconcilerAppliesAeroRoute(t *testing.T) {
	m := &ManifestReconciler{
		RouterApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
			return ApplyResult{Kind: kind, Name: name, Applied: true, Message: "router ok"}, nil
		},
	}
	obj := map[string]interface{}{
		"kind": string(KindAeroRoute),
		"metadata": map[string]interface{}{"name": "r1"},
		"spec": map[string]interface{}{"strategy": "cost"},
	}
	if err := m.Reconcile(context.Background(), obj); err != nil { t.Fatalf("unexpected error: %v", err) }
	status, _ := obj["status"].(map[string]interface{})
	if status == nil || status["message"] != "router ok" || status["applied"] != true { t.Fatalf("unexpected status: %v", status) }
}

func TestManifestReconcilerAppliesAeroBudget(t *testing.T) {
	m := &ManifestReconciler{
		BudgetApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
			return ApplyResult{Kind: kind, Name: name, Applied: true, Message: "budget ok"}, nil
		},
	}
	obj := map[string]interface{}{
		"kind": string(KindAeroBudget),
		"metadata": map[string]interface{}{"name": "b1"},
		"spec": map[string]interface{}{"max_usd": 100},
	}
	if err := m.Reconcile(context.Background(), obj); err != nil { t.Fatalf("unexpected error: %v", err) }
	status, _ := obj["status"].(map[string]interface{})
	if status == nil || status["message"] != "budget ok" || status["applied"] != true { t.Fatalf("unexpected status: %v", status) }
}

func TestManifestReconcilerAppliesPipeline(t *testing.T) {
	m := &ManifestReconciler{
		PipelineApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
			return ApplyResult{Kind: kind, Name: name, Applied: true, Message: "pipeline ok"}, nil
		},
	}
	obj := map[string]interface{}{
		"kind": string(KindAeroAgentPipeline),
		"metadata": map[string]interface{}{"name": "p1"},
		"spec": map[string]interface{}{"nodes": []string{"a", "b"}},
	}
	if err := m.Reconcile(context.Background(), obj); err != nil { t.Fatalf("unexpected error: %v", err) }
	status, _ := obj["status"].(map[string]interface{})
	if status == nil || status["message"] != "pipeline ok" || status["applied"] != true { t.Fatalf("unexpected status: %v", status) }
}

func TestManifestReconcilerUnsupportedKind(t *testing.T) {
	m := &ManifestReconciler{}
	obj := map[string]interface{}{
		"kind": "Unknown",
		"metadata": map[string]interface{}{"name": "x"},
	}
	if err := m.Reconcile(context.Background(), obj); err == nil { t.Fatal("expected error for unsupported kind") }
}

func TestManifestReconcilerUnsupportedType(t *testing.T) {
	m := &ManifestReconciler{}
	if err := m.Reconcile(context.Background(), "not-a-map"); err == nil { t.Fatal("expected error for unsupported type") }
}
