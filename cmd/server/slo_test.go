package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/slo"
)

func TestSLOBudgetRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/slo/budget", slo.Handler(slo.NewErrorBudget(0), "latency"))

	req := httptest.NewRequest(http.MethodGet, "/v1/slo/budget", nil)
	req.Header.Set("x-slo-target", "latency")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
}

func TestSLOMiddlewareBlocksExhaustedBudget(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mw := slo.Middleware(slo.NewErrorBudget(0))
	handler := mw(mux)

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
}
