package chaos

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestShouldFaultRespectsPercent(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 0})
	if inj.ShouldFault() {
		t.Fatalf("expected no fault at 0%%")
	}
}

func TestApplyErrorWritesJSON(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 100, StatusCode: http.StatusBadGateway, Message: "boom"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	err := inj.Apply(w, r)
	if err == nil {
		t.Fatalf("expected error")
	}
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", w.Code)
	}
	if w.Body.String() != `{"error":"boom"}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestHandlerUpdatesInjector(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 0})
	h := Handler(inj)

	req := httptest.NewRequest(http.MethodPost, "/v1/chaos/fault", strings.NewReader(`{"type":"error","percent":100}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	if !inj.ShouldFault() {
		t.Fatalf("expected injector to update")
	}
}
