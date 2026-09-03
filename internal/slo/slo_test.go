package slo

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestErrorBudgetConsume(t *testing.T) {
	b := NewErrorBudget(2)
	if b.Remaining() != 2 {
		t.Fatalf("expected 2, got %f", b.Remaining())
	}
	b.Consume(1)
	if b.Remaining() != 1 {
		t.Fatalf("expected 1, got %f", b.Remaining())
	}
	b.Consume(2)
	if b.Remaining() != 0 {
		t.Fatalf("expected 0, got %f", b.Remaining())
	}
}

func TestHandlerBudgetExhausted(t *testing.T) {
	b := NewErrorBudget(0)
	h := Handler(b, "latency")

	req := httptest.NewRequest(http.MethodGet, "/v1/slo/budget", nil)
	req.Header.Set("x-slo-target", "latency")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
}

func TestHandlerBudgetAvailable(t *testing.T) {
	b := NewErrorBudget(10)
	h := Handler(b, "latency")

	req := httptest.NewRequest(http.MethodGet, "/v1/slo/budget", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestMiddlewareBlocksWhenBudgetExhausted(t *testing.T) {
	b := NewErrorBudget(0)
	mw := Middleware(b)

	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := mw(mux)

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
}
