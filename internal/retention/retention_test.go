package retention

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRetentionStore(t *testing.T) {
	store := NewRetentionStore()
	store.Upsert(RetentionPolicy{ID: "r1", Resource: "logs", TTL: 24, MaxItems: 1000})
	if _, ok := store.Get("r1"); !ok {
		t.Fatalf("expected policy r1")
	}
	if len(store.List()) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(store.List()))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/retention", WebhookHandler(store))
	req := httptest.NewRequest(http.MethodGet, "/v1/retention?id=r1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"resource":"logs"`) {
		t.Fatalf("expected resource logs in body, got: %s", rec.Body.String())
	}
}
