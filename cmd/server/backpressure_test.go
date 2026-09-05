package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/backpressure"
)

func TestBackpressureStatusRoute(t *testing.T) {
	backpressureController := backpressure.NewBackpressureController(backpressure.Config{MaxInflight: 1000, Window: time.Minute})
	mux := http.NewServeMux()
	mux.HandleFunc("/backpressure/status", backpressureController.Handler())

	req := httptest.NewRequest(http.MethodGet, "/backpressure/status", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "inflight") {
		t.Fatalf("expected inflight in response, got: %s", rec.Body.String())
	}
}

func TestBackpressureMiddlewareDropsWhenSaturated(t *testing.T) {
	backpressureController := backpressure.NewBackpressureController(backpressure.Config{MaxInflight: 0, Window: time.Minute})
	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := backpressureController.Middleware(mux)

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
