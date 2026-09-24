package k8s

import (
	"context"
	"testing"
)

func TestReconcilerStructReconcile(t *testing.T) {
	applied := false
	r := &Reconciler{
		Apply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
			_ = ctx
			_ = spec
			if kind != KindAeroRoute || name != "r1" {
				return ApplyResult{}, nil
			}
			applied = true
			return ApplyResult{Kind: kind, Name: name, Applied: true, Message: "ok"}, nil
		},
	}
	res, err := r.Reconcile(context.Background(), KindAeroRoute, "r1", map[string]interface{}{"key": "value"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied || !res.Applied || res.Message != "ok" {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestReconcilerNilApply(t *testing.T) {
	r := &Reconciler{Apply: nil}
	res, err := r.Reconcile(context.Background(), KindAeroRoute, "r1", nil)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if res.Applied || res.Message != msgNoApplyFunc || res.Kind != KindAeroRoute || res.Name != "r1" {
		t.Fatalf("nil apply must report not-applied honestly, got %+v", res)
	}
	var nilR *Reconciler
	if res, err := nilR.Reconcile(context.Background(), KindAeroRoute, "r1", nil); err != nil || res.Applied {
		t.Fatalf("nil reconciler: res=%+v err=%v", res, err)
	}
}

func TestReconcilerCancelledContext(t *testing.T) {
	called := false
	r := &Reconciler{Apply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
		called = true
		return ApplyResult{Applied: true}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Reconcile(ctx, KindAeroRoute, "r1", nil); err == nil || called {
		t.Fatalf("expected cancelled error without apply, err=%v called=%v", err, called)
	}
}
