package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/resilience"
)

func TestResilienceStatusRouteOK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/resilience/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":"ok"}`))
	})

	req := httptest.NewRequest(http.MethodGet, "/resilience/status", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"state":"ok"`) {
		t.Fatalf("expected state ok, got: %s", rec.Body.String())
	}
}

func TestResilienceStatusRouteDegraded(t *testing.T) {
	cb := resilience.NewCircuitBreaker(1, 5*time.Minute)
	cb.RecordFailure()
	handler := resilience.Handler(resilience.DefaultConfig(), cb)

	mux := http.NewServeMux()
	mux.HandleFunc("/resilience/status", handler)

	req := httptest.NewRequest(http.MethodGet, "/resilience/status", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"state":"degraded"`) {
		t.Fatalf("expected degraded state, got: %s", rec.Body.String())
	}
}
