package main

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
	"github.com/ayoubzulfiqar/aerollm/internal/trace"
	"github.com/ayoubzulfiqar/aerollm/internal/federated"
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

func TestFederatedAggregateRoute(t *testing.T) {
	mux := http.NewServeMux()
	fedAgg := federated.NewFedAvgAggregator()
	mux.HandleFunc("/v1/federated/aggregate", func(w http.ResponseWriter, r *http.Request) {
		var updates []*federated.LoRAMatrix
		if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		out, err := fedAgg.Aggregate(r.Context(), updates)
		if err != nil {
			http.Error(w, `{"error":"aggregate failed"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	payload := `[{"Rows":1,"Cols":2,"Data":[1,2],"Owner":"e1"},{"Rows":1,"Cols":2,"Data":[3,4],"Owner":"e2"}]`
	req := httptest.NewRequest(http.MethodPost, "/v1/federated/aggregate", strings.NewReader(payload))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"Owner":""`) {
		t.Fatalf("expected aggregated matrix response, got: %s", rec.Body.String())
	}
}

func TestFederatedVerifyEdgeCases(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil { t.Fatalf("keygen failed: %v", err) }
	agg := federated.NewFedAvgAggregatorWithVerify(priv)
	m := &federated.LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "e1"}
	payload := []byte(fmt.Sprintf("%s:%d:%s", m.Owner, m.Rows, m.Checksum()))
	sig := ed25519.Sign(priv, payload)

	if err := agg.Verify(nil, m, sig); err != nil {
		t.Fatalf("expected valid signature, got error: %v", err)
	}
	if err := agg.Verify(nil, m, []byte("bad")); err == nil {
		t.Fatalf("expected invalid signature error")
	}
	if err := agg.Verify(nil, nil, sig); err == nil {
		t.Fatalf("expected error for nil update")
	}
}
