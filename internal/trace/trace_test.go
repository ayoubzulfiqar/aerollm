package trace

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTraceMiddlewareInstrumentsRequest(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	handler := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if p.RequestCount() != 1 {
		t.Fatalf("expected 1 request, got %d", p.RequestCount())
	}
	if p.ErrorCount() != 0 {
		t.Fatalf("expected 0 errors, got %d", p.ErrorCount())
	}
	if p.AvgLatency() <= 0 {
		t.Fatalf("expected positive avg latency, got %f", p.AvgLatency())
	}
}

func TestTraceMiddlewareRecordsErrors(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	handler := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if p.ErrorCount() != 1 {
		t.Fatalf("expected 1 error, got %d", p.ErrorCount())
	}
}

func TestMetricsHandler(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	_, span := p.StartSpan(nil, "op")
	p.End(nil, span, 10*time.Millisecond, false)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	p.MetricsHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected json content type, got %s", rec.Header().Get("Content-Type"))
	}
}

func TestAvgLatencyZeroWhenNoData(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	if p.AvgLatency() != 0 {
		t.Fatalf("expected 0 avg latency with no data, got %f", p.AvgLatency())
	}
}
