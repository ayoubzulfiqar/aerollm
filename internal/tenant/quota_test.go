package tenant

import (
	"context"
	"testing"
)

func TestQuotaUpsertAndGet(t *testing.T) {
	store := NewInMemoryQuotaStore()
	q := &Quota{ID: "q1", Scope: ScopeTenant, TargetID: "t1", Limit: 10}
	if err := store.Upsert(context.Background(), q); err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	got, err := store.Get(context.Background(), "q1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got.Limit != 10 {
		t.Fatalf("expected limit 10, got %d", got.Limit)
	}
}

func TestEnforceReturnsErrorWhenExceeded(t *testing.T) {
	store := NewInMemoryQuotaStore()
	q := &Quota{ID: "q1", Scope: ScopeTenant, TargetID: "t1", Limit: 5}
	_, err := store.Enforce(context.Background(), q, 6)
	if err == nil {
		t.Fatalf("expected quota exceeded error")
	}
	if _, ok := err.(*QuotaEnforcedError); !ok {
		t.Fatalf("expected QuotaEnforcedError, got %T", err)
	}
}

func TestEnforceIncrementsUsage(t *testing.T) {
	store := NewInMemoryQuotaStore()
	q := &Quota{ID: "q1", Scope: ScopeTenant, TargetID: "t1", Limit: 10}
	got, err := store.Enforce(context.Background(), q, 3)
	if err != nil {
		t.Fatalf("enforce failed: %v", err)
	}
	if got.Used != 3 {
		t.Fatalf("expected used 3, got %d", got.Used)
	}
	got, err = store.Enforce(context.Background(), q, 2)
	if err != nil {
		t.Fatalf("second enforce failed: %v", err)
	}
	if got.Used != 5 {
		t.Fatalf("expected used 5, got %d", got.Used)
	}
}
