package trace

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTraceMiddlewareInstrumentsRequest(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	handler := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if p.AvgLatency() < 0 {
		t.Fatalf("expected non-negative avg latency, got %f", p.AvgLatency())
	}
}

func TestTraceMiddlewareRecordsNonZeroLatency(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	handler := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

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

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/trace/metrics", p.MetricsHandler())
	req := httptest.NewRequest(http.MethodGet, "/v1/trace/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var snap MetricsSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if snap.Service != "svc" {
		t.Fatalf("expected service=svc, got %s", snap.Service)
	}
	if snap.Requests != 1 {
		t.Fatalf("expected 1 request, got %d", snap.Requests)
	}
}

func TestAvgLatencyZeroWhenNoData(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	if p.AvgLatency() != 0 {
		t.Fatalf("expected 0 avg latency with no data, got %f", p.AvgLatency())
	}
}
