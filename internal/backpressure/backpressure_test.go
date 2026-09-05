package backpressure

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAllowWhenUnderLimit(t *testing.T) {
	bp := NewBackpressureController(Config{MaxInflight: 2, Window: time.Minute})
	if !bp.Allow() {
		t.Fatalf("expected allow under limit")
	}
	bp.Record(true)
	if !bp.Allow() {
		t.Fatalf("expected second allow")
	}
	bp.Record(true)
}

func TestDropWhenOverLimit(t *testing.T) {
	bp := NewBackpressureController(Config{MaxInflight: 1, Window: time.Minute})
	bp.Allow()
	if bp.Allow() {
		t.Fatalf("expected drop when over limit")
	}
	bp.Record(true)
}

func TestMetricsWindowReset(t *testing.T) {
	bp := NewBackpressureController(Config{MaxInflight: 1, Window: time.Second})
	bp.Allow()
	bp.Record(true)
	bp.Allow()
	time.Sleep(2 * time.Second)
	metrics := bp.Metrics()
	if metrics.Dropped != 0 || metrics.Inflight != 0 {
		t.Fatalf("expected window reset")
	}
}

func TestHandlerReturnsMetrics(t *testing.T) {
	bp := NewBackpressureController(DefaultConfig())
	mux := http.NewServeMux()
	mux.HandleFunc("/backpressure/status", bp.Handler())
	server := httptest.NewServer(mux)
	defer server.Close()
	
	resp, err := http.Get(server.URL + "/backpressure/status")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestMiddlewareDropsWhenOverLimit(t *testing.T) {
	bp := NewBackpressureController(Config{MaxInflight: 0, Window: time.Minute})
	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := bp.Middleware(mux)
	
	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
