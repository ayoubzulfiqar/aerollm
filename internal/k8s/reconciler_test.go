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
	_, err := r.Reconcile(context.Background(), KindAeroRoute, "r1", nil)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
