package k8s

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestManifestReconcilerAppliesAeroRoute(t *testing.T) {
	m := &ManifestReconciler{
		RouterApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
			return ApplyResult{Kind: kind, Name: name, Applied: true, Message: "router ok"}, nil
		},
	}
	obj := map[string]interface{}{
		"kind":     string(KindAeroRoute),
		"metadata": map[string]interface{}{"name": "r1"},
		"spec":     map[string]interface{}{"strategy": "cost"},
	}
	if err := m.Reconcile(context.Background(), obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	status, _ := obj["status"].(map[string]interface{})
	if status == nil || status["message"] != "router ok" || status["applied"] != true {
		t.Fatalf("unexpected status: %v", status)
	}
}

func TestManifestReconcilerAppliesAeroBudget(t *testing.T) {
	m := &ManifestReconciler{
		BudgetApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
			return ApplyResult{Kind: kind, Name: name, Applied: true, Message: "budget ok"}, nil
		},
	}
	obj := map[string]interface{}{
		"kind":     string(KindAeroBudget),
		"metadata": map[string]interface{}{"name": "b1"},
		"spec":     map[string]interface{}{"max_usd": 100},
	}
	if err := m.Reconcile(context.Background(), obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	status, _ := obj["status"].(map[string]interface{})
	if status == nil || status["message"] != "budget ok" || status["applied"] != true {
		t.Fatalf("unexpected status: %v", status)
	}
}

func TestManifestReconcilerAppliesPipeline(t *testing.T) {
	m := &ManifestReconciler{
		PipelineApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
			return ApplyResult{Kind: kind, Name: name, Applied: true, Message: "pipeline ok"}, nil
		},
	}
	obj := map[string]interface{}{
		"kind":     string(KindAeroAgentPipeline),
		"metadata": map[string]interface{}{"name": "p1"},
		"spec":     map[string]interface{}{"nodes": []string{"a", "b"}},
	}
	if err := m.Reconcile(context.Background(), obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	status, _ := obj["status"].(map[string]interface{})
	if status == nil || status["message"] != "pipeline ok" || status["applied"] != true {
		t.Fatalf("unexpected status: %v", status)
	}
}

func TestManifestReconcilerUnsupportedKind(t *testing.T) {
	m := &ManifestReconciler{}
	obj := map[string]interface{}{
		"kind":     "Unknown",
		"metadata": map[string]interface{}{"name": "x"},
	}
	if err := m.Reconcile(context.Background(), obj); err == nil {
		t.Fatal("expected error for unsupported kind")
	}
}

func TestManifestReconcilerUnsupportedType(t *testing.T) {
	m := &ManifestReconciler{}
	if err := m.Reconcile(context.Background(), "not-a-map"); err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestManifestReconcilerValidation(t *testing.T) {
	called := false
	apply := func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
		called = true
		return ApplyResult{Kind: kind, Name: name, Applied: true}, nil
	}
	m := &ManifestReconciler{RouterApply: apply, BudgetApply: apply, PipelineApply: apply}
	cases := []struct {
		name string
		obj  map[string]interface{}
		want string
	}{
		{"missing kind", map[string]interface{}{"metadata": map[string]interface{}{"name": "x"}}, "missing kind"},
		{"missing metadata", map[string]interface{}{"kind": "AeroRoute"}, "metadata.name"},
		{"bad name", map[string]interface{}{"kind": "AeroRoute", "metadata": map[string]interface{}{"name": "Bad_Name"}}, "RFC 1123"},
		{"metadata not object", map[string]interface{}{"kind": "AeroRoute", "metadata": "x"}, "metadata must be an object"},
		{"spec not object", map[string]interface{}{"kind": "AeroRoute", "metadata": map[string]interface{}{"name": "r"}, "spec": "x"}, "spec must be an object"},
		{"negative budget", map[string]interface{}{"kind": "AeroBudget", "metadata": map[string]interface{}{"name": "b"}, "spec": map[string]interface{}{"max_usd": -5.0}}, "max_usd"},
		{"unknown field typo", map[string]interface{}{"kind": "AeroBudget", "metadata": map[string]interface{}{"name": "b"}, "spec": map[string]interface{}{"maxUSD": 5.0}}, "unknown field"},
		{"pipeline cycle", map[string]interface{}{"kind": "AeroAgentPipeline", "metadata": map[string]interface{}{"name": "p"}, "spec": map[string]interface{}{"nodes": []interface{}{"a", "b"}, "edges": []interface{}{"a->b", "b->a"}}}, "cycle"},
		{"bad weights", map[string]interface{}{"kind": "AeroRoute", "metadata": map[string]interface{}{"name": "r"}, "spec": map[string]interface{}{"strategy": "weighted", "providers": []interface{}{"a"}, "weights": map[string]interface{}{"b": 1.0}}}, "weights"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called = false
			err := m.Reconcile(context.Background(), tc.obj)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
			if called {
				t.Fatal("apply must not run for invalid manifests")
			}
			if tc.name != "missing kind" {
				status, _ := tc.obj["status"].(map[string]interface{})
				if status == nil || status["applied"] != false {
					t.Fatalf("expected failed status, got %v", tc.obj["status"])
				}
			}
		})
	}
}

func TestManifestReconcilerIdempotent(t *testing.T) {
	var applies int
	m := &ManifestReconciler{BudgetApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
		applies++
		return ApplyResult{Kind: kind, Name: name, Applied: true, Message: "budget ok"}, nil
	}}
	mk := func(max float64) map[string]interface{} {
		return map[string]interface{}{"kind": "AeroBudget", "metadata": map[string]interface{}{"name": "b1"}, "spec": map[string]interface{}{"max_usd": max}}
	}
	for i := 0; i < 3; i++ {
		if err := m.Reconcile(context.Background(), mk(100)); err != nil {
			t.Fatal(err)
		}
	}
	if applies != 1 {
		t.Fatalf("expected 1 apply for an unchanged spec, got %d", applies)
	}
	obj := mk(100)
	_ = m.Reconcile(context.Background(), obj)
	if st := obj["status"].(map[string]interface{}); st["message"] != "unchanged" || st["applied"] != true {
		t.Fatalf("unexpected status for unchanged spec: %v", st)
	}
	if err := m.Reconcile(context.Background(), mk(200)); err != nil || applies != 2 {
		t.Fatalf("changed spec must be applied: err=%v applies=%d", err, applies)
	}
	m.Forget(KindAeroBudget, "b1")
	if err := m.Reconcile(context.Background(), mk(200)); err != nil || applies != 3 {
		t.Fatalf("forgotten spec must be re-applied: err=%v applies=%d", err, applies)
	}
}

func TestManifestReconcilerApplyErrorNotRecorded(t *testing.T) {
	fail := true
	var applies int
	m := &ManifestReconciler{RouterApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
		applies++
		if fail {
			return ApplyResult{}, errors.New("control plane down")
		}
		return ApplyResult{Applied: true}, nil
	}}
	obj := map[string]interface{}{"kind": "AeroRoute", "metadata": map[string]interface{}{"name": "r1"}}
	if err := m.Reconcile(context.Background(), obj); err == nil {
		t.Fatal("expected apply error")
	}
	if st := obj["status"].(map[string]interface{}); st["applied"] != false || st["message"] != "control plane down" {
		t.Fatalf("unexpected status: %v", st)
	}
	fail = false
	if err := m.Reconcile(context.Background(), obj); err != nil || applies != 2 {
		t.Fatalf("failed apply must be retried: err=%v applies=%d", err, applies)
	}
}

func TestManifestReconcilerNoApplyFuncIsHonest(t *testing.T) {
	m := &ManifestReconciler{}
	obj := map[string]interface{}{"kind": "AeroRoute", "metadata": map[string]interface{}{"name": "r1"}}
	if err := m.Reconcile(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	st := obj["status"].(map[string]interface{})
	if st["applied"] != false || st["message"] != msgNoApplyFunc {
		t.Fatalf("expected honest not-applied status, got %v", st)
	}
}

func TestManifestReconcilerCancelledContext(t *testing.T) {
	called := false
	m := &ManifestReconciler{RouterApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
		called = true
		return ApplyResult{Applied: true}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	obj := map[string]interface{}{"kind": "AeroRoute", "metadata": map[string]interface{}{"name": "r1"}}
	if err := m.Reconcile(ctx, obj); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("expected context.Canceled without apply, err=%v called=%v", err, called)
	}
}

func TestManifestReconcilerAcceptsJSONBytes(t *testing.T) {
	m := &ManifestReconciler{}
	if err := m.Reconcile(context.Background(), []byte(`{"kind":"AeroRoute","metadata":{"name":"r1"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := m.Reconcile(context.Background(), []byte(`{`)); err == nil {
		t.Fatal("expected JSON error")
	}
	if err := m.Reconcile(context.Background(), []byte(`null`)); err == nil {
		t.Fatal("expected null object error")
	}
}
