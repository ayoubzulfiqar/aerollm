package marketplace_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/tenant"
)

func serveWithAuth(resolver tenant.Resolver, auth string) *httptest.ResponseRecorder {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mw := marketplace.RequesterMiddleware(resolver, next)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/marketplace/plugins", nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	mw.ServeHTTP(rec, req)
	return rec
}

func TestRequesterMiddlewareRejectsMissingAPIKey(t *testing.T) {
	rec := serveWithAuth(tenant.NewInMemoryTenantResolver(), "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected JSON error, got %q", rec.Header().Get("Content-Type"))
	}
}

func TestRequesterMiddlewarePassesWithValidAPIKey(t *testing.T) {
	resolver := tenant.NewInMemoryTenantResolver()
	resolver.Add(&tenant.APIKey{HashedKey: "token", TenantID: "t1", Active: true})
	if rec := serveWithAuth(resolver, "token"); rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec := serveWithAuth(resolver, "Bearer token"); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with bearer scheme, got %d", rec.Code)
	}
	if rec := serveWithAuth(resolver, "bearer token"); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with lowercase bearer scheme, got %d", rec.Code)
	}
}

func TestRequesterMiddlewareRejectsInactiveAndOversizedKeys(t *testing.T) {
	resolver := tenant.NewInMemoryTenantResolver()
	resolver.Add(&tenant.APIKey{HashedKey: "disabled", TenantID: "t1", Active: false})
	if rec := serveWithAuth(resolver, "Bearer disabled"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for inactive key, got %d", rec.Code)
	}
	if rec := serveWithAuth(resolver, "Bearer "+strings.Repeat("k", 4096)); rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for oversized key, got %d", rec.Code)
	}
}

func TestRequesterMiddlewareFailsClosedWithoutResolver(t *testing.T) {
	if rec := serveWithAuth(nil, "Bearer anything"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when auth is not configured, got %d", rec.Code)
	}
}
