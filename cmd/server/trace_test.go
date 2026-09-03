package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/trace"
)

func TestTraceMetricsEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	p := trace.NewProvider(trace.Config{ServiceName: "svc"})
	_, span := p.StartSpan(nil, "op")
	p.End(nil, span, 5*time.Millisecond, false)
	mux.HandleFunc("/v1/trace/metrics", p.MetricsHandler())

	req := httptest.NewRequest(http.MethodGet, "/v1/trace/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"service":"svc"`) {
		t.Fatalf("expected service in metrics response, got: %s", rec.Body.String())
	}
}

func TestTraceMiddlewareAddsTraceHeaders(t *testing.T) {
	p := trace.NewProvider(trace.Config{ServiceName: "svc"})
	handler := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Header().Get("X-Trace-Id") == "" {
		t.Fatalf("expected X-Trace-Id header, got: %v", rec.Header())
	}
	if rec.Header().Get("X-Span-Id") == "" {
		t.Fatalf("expected X-Span-Id header, got: %v", rec.Header())
	}
}

func TestTraceMetricsSnapshotStructure(t *testing.T) {
	p := trace.NewProvider(trace.Config{ServiceName: "svc"})
	_, span := p.StartSpan(nil, "op")
	p.End(nil, span, 15*time.Millisecond, false)

	req := httptest.NewRequest(http.MethodGet, "/v1/trace/metrics", nil)
	rec := httptest.NewRecorder()
	p.MetricsHandler().ServeHTTP(rec, req)

	var snap trace.MetricsSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if snap.Service != "svc" {
		t.Fatalf("expected service=svc, got %s", snap.Service)
	}
	if snap.Requests != 1 {
		t.Fatalf("expected 1 request, got %d", snap.Requests)
	}
	if snap.Errors != 0 {
		t.Fatalf("expected 0 errors, got %d", snap.Errors)
	}
	if snap.AvgLatencyMs <= 0 {
		t.Fatalf("expected positive avg latency, got %f", snap.AvgLatencyMs)
	}
}
