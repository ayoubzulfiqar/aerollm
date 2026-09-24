package compliance

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestComplianceMiddlewareRestoresBody(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	e.AddRule(Rule{ID: "deny-secret", Allow: false, Body: "TOP-SECRET"})
	var seen string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen = string(b)
		w.WriteHeader(http.StatusOK)
	})
	h := ComplianceMiddleware(e)(next)

	body := `{"prompt":"hello"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || seen != body {
		t.Fatalf("body not restored: %d %q", w.Code, seen)
	}

	// Chunked body (unknown length) must not panic and is still evaluated.
	r = httptest.NewRequest(http.MethodPost, "/v1/chat", io.NopCloser(strings.NewReader("this is TOP-SECRET")))
	r.ContentLength = -1
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnavailableForLegalReasons || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected 451 json, got %d", w.Code)
	}

	// A lying huge Content-Length is rejected without allocating it.
	r = httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader("x"))
	r.ContentLength = 1 << 40
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
}

type errEngine struct{}

func (errEngine) Evaluate(context.Context, map[string]interface{}) (PolicyResult, error) {
	return PolicyResult{}, errors.New("boom")
}

func TestComplianceMiddlewareFailsClosed(t *testing.T) {
	h := ComplianceMiddleware(errEngine{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusInternalServerError || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected 500 json, got %d", w.Code)
	}
}

func TestPathMatchingIsSegmentAwareAndCleaned(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	e.AddRule(Rule{ID: "deny-admin", Allow: false, Path: "/admin"})
	for p, denied := range map[string]bool{
		"/admin":         true,
		"/admin/users":   true,
		"//admin":        true,
		"/x/../admin":    true,
		"/administrator": false,
		"/public":        false,
	} {
		res, _ := e.Evaluate(context.Background(), map[string]interface{}{"method": "GET", "path": p})
		if res.Allowed == denied {
			t.Errorf("path %q: allowed=%v", p, res.Allowed)
		}
	}
}

func TestHeaderRulesAreCaseInsensitive(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	e.AddRule(Rule{ID: "deny-debug", Allow: false, Headers: map[string]string{"x-debug": "1"}})
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Debug", "1")
	in, _ := RequestInput(r)
	if res, _ := e.Evaluate(context.Background(), in); res.Allowed {
		t.Fatal("lowercase rule header must match canonical request header")
	}
}

func TestConcurrentEngineAndRegistry(t *testing.T) {
	e := NewSimplePolicyEngine("default")
	reg := NewPolicyRegistry()
	logger := NewMemoryAuditLoggerWithCapacity(50)
	audited := WithAudit(e, logger)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); e.AddRule(Rule{ID: "r", Allow: true, Path: "/x"}) }()
		go func() { defer wg.Done(); reg.AddRule(Rule{ID: "p", Allow: false}); _, _ = reg.Rule("p") }()
		go func() {
			defer wg.Done()
			_, _ = audited.Evaluate(context.Background(), map[string]interface{}{"method": "GET", "path": "/x"})
			_ = logger.Events()
		}()
	}
	wg.Wait()
	if n := len(logger.Events()); n != 50 {
		t.Fatalf("expected 50 retained events, got %d", n)
	}
}

func TestAuditLoggerBoundedAndRedacted(t *testing.T) {
	l := NewMemoryAuditLoggerWithCapacity(3)
	for i := 0; i < 5; i++ {
		l.Log(&AuditEvent{Policy: "p", Decision: "allow", Input: map[string]interface{}{
			"header": map[string]interface{}{"Authorization": "Bearer sk-secret", "Accept": "json"},
			"body":   "my password is hunter2",
		}})
	}
	evs := l.Events()
	if len(evs) != 3 || l.Dropped() != 2 {
		t.Fatalf("expected 3 events and 2 dropped, got %d/%d", len(evs), l.Dropped())
	}
	s := evs[0].String()
	if strings.Contains(s, "sk-secret") || strings.Contains(s, "hunter2") {
		t.Fatalf("audit output leaks secrets: %s", s)
	}
	if !strings.Contains(s, "[REDACTED]") || !strings.Contains(s, "body_bytes") {
		t.Fatalf("expected redaction markers: %s", s)
	}
	l.Clear()
	if len(l.Events()) != 0 {
		t.Fatal("clear failed")
	}
}

func TestEvaluateWithRegistry(t *testing.T) {
	reg := NewPolicyRegistry()
	reg.AddRule(Rule{ID: "no-delete", Allow: false, Methods: []string{"DELETE"}})
	res, err := EvaluateWithRegistry(reg, context.Background(), map[string]interface{}{"policy_id": "no-delete", "method": "delete"})
	if err != nil || res.Allowed {
		t.Fatalf("expected deny, got %+v %v", res, err)
	}
	if _, err := EvaluateWithRegistry(reg, context.Background(), map[string]interface{}{"policy_id": "missing"}); err == nil {
		t.Fatal("unknown policy must error")
	}
	if _, err := EvaluateWithRegistry(nil, context.Background(), map[string]interface{}{"policy_id": "x"}); err == nil {
		t.Fatal("nil registry must error")
	}
}

func TestHTTPPolicyValidationAndExpressions(t *testing.T) {
	s := NewHTTPPolicyStore()
	for _, bad := range []HTTPPolicyRule{
		{ID: "", Expression: "deny"},
		{ID: "x", Expression: "rm -rf /"},
		{ID: "x", Expression: "deny-path:admin"},
		{ID: "x", Expression: "deny", Severity: "critical"},
		{ID: "bad id!", Expression: "deny"},
	} {
		if err := s.UpsertRule(bad); err == nil {
			t.Errorf("expected rejection of %+v", bad)
		}
	}
	must := func(r HTTPPolicyRule) {
		t.Helper()
		if err := s.UpsertRule(r); err != nil {
			t.Fatal(err)
		}
	}
	must(HTTPPolicyRule{ID: "a", Expression: "deny-path:/Admin", Severity: "HIGH"})
	must(HTTPPolicyRule{ID: "b", Expression: "require-header:X-Tenant-ID", Severity: "block"})
	must(HTTPPolicyRule{ID: "c", Expression: "deny-delete", Severity: "low"})
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := HTTPBlockHandler(s)(next)
	run := func(method, path string, tenant bool) int {
		r := httptest.NewRequest(method, path, nil)
		if tenant {
			r.Header.Set("X-Tenant-ID", "t1")
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if c := run(http.MethodGet, "/Admin/x", true); c != http.StatusUnavailableForLegalReasons {
		t.Fatalf("deny-path (case preserved): %d", c)
	}
	if c := run(http.MethodGet, "/public", false); c != http.StatusUnavailableForLegalReasons {
		t.Fatalf("require-header: %d", c)
	}
	if c := run(http.MethodDelete, "/public", true); c != http.StatusOK {
		t.Fatalf("low severity is advisory: %d", c)
	}
	if got, _ := s.GetRule("a"); got.Severity != "high" {
		t.Fatalf("severity not normalised: %q", got.Severity)
	}
	if !s.DeleteRule("a") || s.DeleteRule("a") {
		t.Fatal("delete semantics")
	}
}

func TestHTTPPolicyHandlerErrors(t *testing.T) {
	s := NewHTTPPolicyStore()
	h := HTTPPolicyHandler(s)
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"id":"x","expression":"bogus"}`)))
	if w.Code != http.StatusBadRequest || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("invalid rule: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodPatch, "/", nil))
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") == "" {
		t.Fatalf("expected 405 with Allow, got %d", w.Code)
	}
	w = httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"id":"x","name":"`+strings.Repeat("a", 70<<10)+`"}`)))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
	w = httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodDelete, "/?id=nope", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}
