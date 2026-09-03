package marketplace_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/tenant"
)

func TestRequesterMiddlewareRejectsMissingAPIKey(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mw := marketplace.RequesterMiddleware(tenant.NewInMemoryTenantResolver(), next)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/marketplace/plugins", nil)
	mw.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestRequesterMiddlewarePassesWithValidAPIKey(t *testing.T) {
	resolver := tenant.NewInMemoryTenantResolver()
	resolver.Add(&tenant.APIKey{HashedKey: "token", TenantID: "t1", Active: true})
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mw := marketplace.RequesterMiddleware(resolver, next)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/marketplace/plugins", nil)
	req.Header.Set("Authorization", "token")
	mw.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
