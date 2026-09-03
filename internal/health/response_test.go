package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServeHTTPReadyz(t *testing.T) {
	reg := NewRegistry()
	mux := http.NewServeMux()
	mux.Handle("/readyz", reg)
	reg.Register(&fakeChecker{name: "fake", healthy: true})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"ready"`) {
		t.Fatalf("expected ready status, got: %s", rec.Body.String())
	}
}

func TestServeHTTPNotReady(t *testing.T) {
	reg := NewRegistry()
	mux := http.NewServeMux()
	mux.Handle("/readyz", reg)
	reg.Register(&fakeChecker{name: "db", healthy: false})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"not_ready"`) {
		t.Fatalf("expected not_ready status, got: %s", rec.Body.String())
	}
}

func TestPrintChecks(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&fakeChecker{name: "a", healthy: true})
	reg.Register(&fakeChecker{name: "b", healthy: false})
	checks := reg.Checks(context.Background())
	PrintChecks(checks)
}
