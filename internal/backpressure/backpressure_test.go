package backpressure

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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
	now := time.Now()
	bp.now = func() time.Time { return now }
	bp.Allow()
	bp.Record(true)
	bp.Allow()
	bp.Allow() // dropped
	now = now.Add(2 * time.Second)
	metrics := bp.Metrics()
	if metrics.Dropped != 0 || metrics.Total != 0 {
		t.Fatalf("expected window counters reset, got %+v", metrics)
	}
	if metrics.Inflight != 1 {
		t.Fatalf("in-flight requests must survive a window reset, got %d", metrics.Inflight)
	}
}

func TestWindowResetDoesNotExceedLimit(t *testing.T) {
	bp := NewBackpressureController(Config{MaxInflight: 2, Window: time.Second})
	now := time.Now()
	bp.now = func() time.Time { return now }
	if !bp.Allow() || !bp.Allow() {
		t.Fatal("expected two admissions")
	}
	now = now.Add(5 * time.Second)
	if bp.Allow() {
		t.Fatal("window reset must not free in-flight slots")
	}
	bp.Record(true)
	if !bp.Allow() {
		t.Fatal("slot should be free after Record")
	}
}

func TestConcurrentAllowRecordAndMetrics(t *testing.T) {
	bp := NewBackpressureController(Config{MaxInflight: 5, Window: time.Millisecond})
	var wg sync.WaitGroup
	var maxSeen atomic.Int64
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if bp.Allow() {
					if m := bp.Metrics(); m.Inflight > maxSeen.Load() {
						maxSeen.Store(m.Inflight)
					}
					bp.Record(true)
				}
			}
		}()
	}
	wg.Wait()
	if maxSeen.Load() > 5 {
		t.Fatalf("in-flight exceeded limit: %d", maxSeen.Load())
	}
	if m := bp.Metrics(); m.Inflight != 0 {
		t.Fatalf("in-flight leak: %d", m.Inflight)
	}
}

func TestRecordWithoutAllowDoesNotGoNegative(t *testing.T) {
	bp := NewBackpressureController(DefaultConfig())
	bp.Record(false)
	if m := bp.Metrics(); m.Inflight != 0 {
		t.Fatalf("inflight went negative: %d", m.Inflight)
	}
}

func TestHandlerMethodsAndHealth(t *testing.T) {
	bp := NewBackpressureController(Config{MaxInflight: 0, Window: time.Minute, MaxDropRate: 0.5})
	bp.Allow()
	rec := httptest.NewRecorder()
	bp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	var m Metrics
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("bad JSON: %v %s", err, rec.Body.String())
	}
	if m.Healthy || m.Dropped != 1 || m.Config.MaxDropRate != 0.5 {
		t.Fatalf("unexpected metrics %+v", m)
	}
	rec = httptest.NewRecorder()
	bp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST: %d allow=%q", rec.Code, rec.Header().Get("Allow"))
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
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("expected Retry-After header, got %q", rec.Header().Get("Retry-After"))
	}
}
