package k8s

import (
	"context"
	"errors"
	"testing"
)

func TestReconcileNilReconciler(t *testing.T) {
	var r *Reconciler
	_, err := r.Reconcile(context.Background(), KindAeroRoute, "r1", nil)
	if err == nil {
		t.Fatal("expected error when reconciler is nil")
	}
}

func TestReconcileNilApply(t *testing.T) {
	r := &Reconciler{}
	_, err := r.Reconcile(context.Background(), KindAeroRoute, "r1", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error when apply function is nil")
	}
}

func TestReconcileSuccess(t *testing.T) {
	applied := false
	r := &Reconciler{Apply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
		applied = true
		if kind != KindAeroRoute || name != "r1" {
			return ApplyResult{}, errors.New("unexpected kind/name")
		}
		return ApplyResult{Kind: kind, Name: name, Applied: true, Message: "ok"}, nil
	}}
	res, err := r.Reconcile(context.Background(), KindAeroRoute, "r1", map[string]interface{}{"key": "value"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied || !res.Applied || res.Message != "ok" {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestSetupManagerNoOp(t *testing.T) {
	if err := SetupManager(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
