package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
	"github.com/ayoubzulfiqar/aerollm/internal/trace"
)

func TestPQCKeysRoute(t *testing.T) {
	mux := http.NewServeMux()
	km := pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridEd25519MLDSA65)
	mux.HandleFunc("/v1/pqc/keys", pqc.HandshakeHandler(km))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/pqc/keys", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "algorithm") {
		t.Fatalf("expected algorithm in response: %s", w.Body.String())
	}
}

func TestSpatialStreamRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/spatial/stream", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		spatial.NewVideo3DStreamHandler().StreamResponse(w, r, r.Body)
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/spatial/stream", strings.NewReader("hello"))
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "hello") {
		t.Fatalf("expected streamed body, got: %s", w.Body.String())
	}
}

func TestTraceMetricsRoute(t *testing.T) {
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
