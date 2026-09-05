package compliance

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPPolicyStore(t *testing.T) {
	store := NewHTTPPolicyStore()
	store.UpsertRule(HTTPPolicyRule{ID: "allow", Name: "allow", Expression: "allow", Severity: "low"})
	if _, ok := store.GetRule("allow"); !ok {
		t.Fatalf("expected rule allow")
	}
	if len(store.ListRules()) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(store.ListRules()))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/policy", HTTPPolicyHandler(store))
	req := httptest.NewRequest(http.MethodGet, "/policy?id=allow", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"severity":"low"`) {
		t.Fatalf("expected severity low in body, got: %s", rec.Body.String())
	}
}

func TestHTTPBlockHandler(t *testing.T) {
	store := NewHTTPPolicyStore()
	store.UpsertRule(HTTPPolicyRule{ID: "deny-post", Name: "deny-post", Expression: "deny-post", Severity: "high"})
	handler := HTTPBlockHandler(store)
	mux := http.NewServeMux()
	mux.Handle("/block", handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))

	req := httptest.NewRequest(http.MethodPost, "/block", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnavailableForLegalReasons {
		t.Fatalf("expected 451, got %d", rec.Code)
	}
}
