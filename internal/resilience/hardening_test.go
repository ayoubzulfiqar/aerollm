package resilience

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitBreakerGuardsAndHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(0, 5*time.Millisecond) // threshold<=0 -> 1
	if cb.Threshold() != 1 {
		t.Fatalf("expected threshold 1, got %d", cb.Threshold())
	}
	cb.RecordFailure()
	if rawState(cb) != StateDegraded {
		t.Fatal("expected degraded after one failure")
	}
	time.Sleep(10 * time.Millisecond)
	if cb.State() != StateRecovering {
		t.Fatalf("expected recovering, got %v", cb.State())
	}
	// A failure while recovering re-trips immediately, even below threshold.
	cb2 := NewCircuitBreaker(3, 5*time.Millisecond)
	for i := 0; i < 3; i++ {
		cb2.RecordFailure()
	}
	time.Sleep(10 * time.Millisecond)
	if cb2.State() != StateRecovering {
		t.Fatalf("expected recovering, got %v", cb2.State())
	}
	cb2.RecordFailure()
	if rawState(cb2) != StateDegraded {
		t.Fatalf("expected re-trip to degraded, got %v", rawState(cb2))
	}
	cb2.RecordSuccess()
	if cb2.State() != StateNormal || cb2.Failures() != 0 {
		t.Fatalf("expected normal after success, got %v/%d", cb2.State(), cb2.Failures())
	}
	if NewCircuitBreaker(1, 0).resetTimeout != defaultResetTimeout {
		t.Fatal("expected default reset timeout for non-positive input")
	}
}

// rawState reads the state without advancing it (avoids timing flakes).
func rawState(c *CircuitBreaker) State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func TestCircuitBreakerAllow(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Hour)
	if !cb.Allow() {
		t.Fatal("expected allow when normal")
	}
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("expected reject when degraded")
	}
}

func TestCircuitBreakerConcurrent(t *testing.T) {
	cb := NewCircuitBreaker(5, time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				switch (i + j) % 3 {
				case 0:
					cb.RecordFailure()
				case 1:
					cb.RecordSuccess()
				default:
					_ = cb.State()
					_ = cb.Failures()
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestHandlerMethodsAndStates(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Hour)
	h := Handler(DefaultConfig(), cb)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/resilience/status", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("expected 405 with Allow, got %d %q", rec.Code, rec.Header().Get("Allow"))
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatal("405 must be JSON")
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/resilience/status", nil))
	var body StatusResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if rec.Code != http.StatusOK || body.State != "ok" {
		t.Fatalf("expected ok, got %d %s", rec.Code, rec.Body.String())
	}

	cb.RecordFailure()
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/resilience/status", nil))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("expected 503 with Retry-After 2, got %d %q", rec.Code, rec.Header().Get("Retry-After"))
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/resilience/status", nil))
	if rec.Code != http.StatusServiceUnavailable || rec.Body.Len() != 0 {
		t.Fatalf("HEAD must have no body, got %d %q", rec.Code, rec.Body.String())
	}

	// Recovering is reported honestly.
	rc := NewCircuitBreaker(1, time.Millisecond)
	rc.RecordFailure()
	time.Sleep(5 * time.Millisecond)
	rec = httptest.NewRecorder()
	Handler(DefaultConfig(), rc).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if rec.Code != http.StatusOK || body.State != "recovering" {
		t.Fatalf("expected recovering, got %d %s", rec.Code, rec.Body.String())
	}

	// Nil breaker is safe; forced degraded mode still returns 503.
	cfg := DefaultConfig()
	cfg.Mode = StateDegraded
	rec = httptest.NewRecorder()
	Handler(cfg, nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for forced degraded mode, got %d", rec.Code)
	}
}

func TestBulkheadGuards(t *testing.T) {
	b := NewBulkhead(0)
	if b.Capacity() != 1 {
		t.Fatalf("expected capacity 1, got %d", b.Capacity())
	}
	b.Release() // unbalanced release must not block
	if !b.TryAcquire() {
		t.Fatal("expected TryAcquire to succeed")
	}
	if b.TryAcquire() {
		t.Fatal("expected TryAcquire to fail when full")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b.Release()
	if b.Acquire(ctx) {
		t.Fatal("cancelled context must not acquire")
	}
	if !b.Acquire(nil) { //nolint:staticcheck // nil ctx is tolerated
		t.Fatal("nil ctx should acquire a free slot")
	}
	b.Release()
	if b.InUse() != 0 {
		t.Fatalf("expected 0 in use, got %d", b.InUse())
	}
}

func TestMiddlewareRejectsQuicklyWhenFull(t *testing.T) {
	b := NewBulkhead(1)
	release := make(chan struct{})
	var entered atomic.Int32
	h := MiddlewareWithWait(b, 20*time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered.Add(1)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))

	done := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		done <- rec.Code
	}()
	for entered.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	start := time.Now()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	if time.Since(start) > time.Second {
		t.Fatal("rejection took too long")
	}
	if rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("expected JSON 503 with Retry-After, got %v", rec.Header())
	}
	close(release)
	if code := <-done; code != http.StatusNoContent {
		t.Fatalf("first request should succeed, got %d", code)
	}
	if b.InUse() != 0 {
		t.Fatalf("slot leaked: %d in use", b.InUse())
	}

	// nil bulkhead disables limiting.
	rec = httptest.NewRecorder()
	Middleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) })).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("expected passthrough, got %d", rec.Code)
	}
}
