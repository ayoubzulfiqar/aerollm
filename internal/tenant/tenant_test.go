package tenant

import (
	"context"
	"testing"
)

func TestContextHelpers(t *testing.T) {
	key := &APIKey{ID: "k1", TenantID: "t1"}
	ctx := WithTenantContext(context.Background(), key)

	got, ok := APIKeyFromContext(ctx)
	if !ok || got == nil || got.ID != "k1" {
		t.Fatalf("APIKeyFromContext failed: %v %v", ok, got)
	}

	tenantID, ok := TenantIDFromContext(ctx)
	if !ok || tenantID != "t1" {
		t.Fatalf("TenantIDFromContext failed: %v %v", ok, tenantID)
	}

	got2, ok := APIKeyFromContext(nil)
	if ok || got2 != nil {
		t.Fatalf("nil context should not return key")
	}
}
