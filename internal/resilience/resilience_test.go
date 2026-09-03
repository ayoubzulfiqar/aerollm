package resilience

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCircuitBreakerTripsAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(2, 5*time.Minute)
	if cb.State() != StateNormal {
		t.Fatalf("expected normal, got %v", cb.State())
	}
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateDegraded {
		t.Fatalf("expected degraded after threshold, got %v", cb.State())
	}
}

func TestCircuitBreakerRecoversAfterReset(t *testing.T) {
	cb := NewCircuitBreaker(2, 1*time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure()
	time.Sleep(2 * time.Millisecond)
	if cb.State() != StateRecovering && cb.State() != StateNormal {
		t.Fatalf("expected recovering/normal after reset timeout, got %v", cb.State())
	}
}

func TestHandlerDegradedResponse(t *testing.T) {
	cb := NewCircuitBreaker(1, 5*time.Minute)
	cb.RecordFailure()
	handler := Handler(DefaultConfig(), cb)

	req := httptest.NewRequest(http.MethodGet, "/resilience/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected json content type")
	}
}

func TestBulkheadLimitsConcurrency(t *testing.T) {
	b := NewBulkhead(1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if !b.Acquire(ctx) {
		t.Fatalf("expected first acquire")
	}
	if b.Acquire(ctx) {
		t.Fatalf("expected second acquire to fail")
	}
	b.Release()
}
