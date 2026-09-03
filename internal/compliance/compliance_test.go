package compliance

import (
	"context"
	"testing"

	"net/http"
	"net/http/httptest"
)

func TestSimplePolicyEngineDefaultAllow(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	res, err := e.Evaluate(context.Background(), map[string]interface{}{"method": "GET", "path": "/health"})
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if !res.Allowed { t.Fatalf("expected allow by default, got %v", res) }
}

func TestSimplePolicyEngineExplicitAllow(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	e.AddRule(Rule{ID: "allow-admin", Allow: true, Path: "/admin", Methods: []string{"GET"}})
	res, err := e.Evaluate(context.Background(), map[string]interface{}{"method": "GET", "path": "/admin/me"})
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if !res.Allowed { t.Fatalf("expected allow by rule, got %v", res) }
}

func TestSimplePolicyEngineExplicitDeny(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	e.AddRule(Rule{ID: "deny-admin", Allow: false, Path: "/admin", Methods: []string{"DELETE"}})
	res, err := e.Evaluate(context.Background(), map[string]interface{}{"method": "DELETE", "path": "/admin/me"})
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if res.Allowed { t.Fatalf("expected deny by rule") }
	if res.Policy != "deny-admin" { t.Fatalf("expected policy deny-admin, got %s", res.Policy) }
}

func TestSimplePolicyEngineHeaderMatch(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	e.AddRule(Rule{ID: "allow-secure", Allow: true, Headers: map[string]string{"Authorization": "Bearer"}})
	res, err := e.Evaluate(context.Background(), map[string]interface{}{
		"method": "GET",
		"path":   "/secure",
		"header": map[string]interface{}{"Authorization": []string{"Bearer abc"}},
	})
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if !res.Allowed { t.Fatalf("expected allow by header rule, got %v", res) }
}

func TestComplianceMiddlewareAllows(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	e.AddRule(Rule{ID: "allow-health", Allow: true, Path: "/health"})
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := ComplianceMiddleware(e)(next)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK { t.Fatalf("expected 200, got %d", w.Code) }
}

func TestComplianceMiddlewareDenies(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	e.AddRule(Rule{ID: "deny-bad", Allow: false, Path: "/bad"})
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := ComplianceMiddleware(e)(next)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bad", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnavailableForLegalReasons { t.Fatalf("expected 451, got %d", w.Code) }
}
