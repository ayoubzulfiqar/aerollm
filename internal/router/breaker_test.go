package router

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

func TestBreakerOpensAfterRetryableFailures(t *testing.T) {
	p := &fnProvider{name: "p", fn: failWith(upstreamErr(503))}
	cb := NewCircuitBreaker(p, 3, time.Minute)
	for i := 0; i < 3; i++ {
		if _, err := cb.ChatCompletions(context.Background(), &models.LLMRequest{}); err == nil {
			t.Fatal("expected error")
		}
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected open, got %v", cb.State())
	}
	_, err := cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	if !errors.Is(err, providers.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if !providers.IsRetryable(err) {
		t.Fatal("circuit open error must be retryable")
	}
	var coe *CircuitBreakerOpenError
	if !errors.As(err, &coe) || coe.RetryAfter <= 0 || coe.Provider != "p" {
		t.Fatalf("expected CircuitBreakerOpenError with RetryAfter, got %#v", err)
	}
	if got := p.calls.Load(); got != 3 {
		t.Fatalf("open breaker must not call provider; calls=%d", got)
	}
	h := cb.Health()
	if !h.CircuitOpen || h.Healthy || h.Failures != 3 {
		t.Fatalf("unexpected health: %+v", h)
	}
}

func TestBreakerIgnoresNonRetryableAndCanceled(t *testing.T) {
	p := &fnProvider{name: "p", fn: failWith(upstreamErr(400))}
	cb := NewCircuitBreaker(p, 2, time.Minute)
	for i := 0; i < 10; i++ {
		_, _ = cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	}
	if cb.State() != StateClosed {
		t.Fatalf("400s must not open the breaker, got %v", cb.State())
	}

	p.fn = failWith(context.Canceled)
	for i := 0; i < 10; i++ {
		_, _ = cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	}
	// Caller's ctx already done: even a retryable error is not the provider's fault.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.fn = failWith(upstreamErr(500))
	for i := 0; i < 10; i++ {
		_, _ = cb.ChatCompletions(ctx, &models.LLMRequest{})
	}
	if cb.State() != StateClosed {
		t.Fatalf("cancellation must not open the breaker, got %v", cb.State())
	}
}

func TestBreakerNilResponseIsFailure(t *testing.T) {
	p := &fnProvider{name: "p", fn: func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) { return nil, nil }}
	cb := NewCircuitBreaker(p, 1, time.Minute)
	_, err := cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	if providers.StatusCode(err) != 502 {
		t.Fatalf("expected 502 bad response error, got %v", err)
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected open, got %v", cb.State())
	}
}

func openBreaker(t *testing.T, cb *CircuitBreaker, p *fnProvider) {
	t.Helper()
	prev := p.fn
	p.fn = failWith(upstreamErr(500))
	for cb.State() == StateClosed {
		_, _ = cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	}
	p.fn = prev
}

func TestBreakerHalfOpenMaxCalls(t *testing.T) {
	release := make(chan struct{})
	var entered sync.WaitGroup
	p := &fnProvider{name: "p"}
	cb := NewCircuitBreakerWithConfig(p, CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: 10 * time.Millisecond, HalfOpenMaxCalls: 2})
	openBreaker(t, cb, p)
	time.Sleep(15 * time.Millisecond)

	p.fn = func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
		entered.Done()
		<-release
		return &models.LLMResponse{}, nil
	}
	entered.Add(2)
	before := p.calls.Load()

	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cb.ChatCompletions(context.Background(), &models.LLMRequest{})
			errs <- err
		}()
	}
	entered.Wait() // exactly two probes reached the provider
	// The other three must be rejected without reaching the provider.
	waitFor(t, 2*time.Second, func() bool { return len(errs) == 3 }, "3 rejections")
	close(release)
	wg.Wait()
	close(errs)

	var rejected, ok int
	for err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, providers.ErrCircuitOpen):
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 2 || rejected != 3 {
		t.Fatalf("expected 2 probes and 3 rejections, got ok=%d rejected=%d", ok, rejected)
	}
	if got := p.calls.Load() - before; got != 2 {
		t.Fatalf("expected 2 provider calls, got %d", got)
	}
	if cb.State() != StateClosed {
		t.Fatalf("successful probe must close breaker, got %v", cb.State())
	}
}

func TestBreakerHalfOpenProbeFailureReopens(t *testing.T) {
	p := &fnProvider{name: "p"}
	cb := NewCircuitBreakerWithConfig(p, CircuitBreakerConfig{MaxFailures: 3, ResetTimeout: 10 * time.Millisecond})
	openBreaker(t, cb, p)
	time.Sleep(15 * time.Millisecond)
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected half-open after timeout, got %v", cb.State())
	}
	p.fn = failWith(upstreamErr(502))
	_, _ = cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	if cb.State() != StateOpen {
		t.Fatalf("a single failed probe must re-open, got %v", cb.State())
	}
	_, err := cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	if !errors.Is(err, providers.ErrCircuitOpen) {
		t.Fatalf("expected open with fresh timeout, got %v", err)
	}
}

func TestBreakerHalfOpenProbe4xxCloses(t *testing.T) {
	p := &fnProvider{name: "p"}
	cb := NewCircuitBreakerWithConfig(p, CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: 10 * time.Millisecond})
	openBreaker(t, cb, p)
	time.Sleep(15 * time.Millisecond)
	p.fn = failWith(upstreamErr(404))
	_, _ = cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	if cb.State() != StateClosed {
		t.Fatalf("4xx probe proves liveness; expected closed, got %v", cb.State())
	}
}

func TestBreakerHalfOpenProbeCanceledReleasesSlot(t *testing.T) {
	p := &fnProvider{name: "p"}
	cb := NewCircuitBreakerWithConfig(p, CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: 10 * time.Millisecond})
	openBreaker(t, cb, p)
	time.Sleep(15 * time.Millisecond)
	p.fn = failWith(context.Canceled)
	_, _ = cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	if cb.State() != StateHalfOpen {
		t.Fatalf("cancelled probe must leave half-open, got %v", cb.State())
	}
	p.fn = nil
	if _, err := cb.ChatCompletions(context.Background(), &models.LLMRequest{}); err != nil {
		t.Fatalf("probe slot must have been released: %v", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected closed, got %v", cb.State())
	}
}

func TestBreakerStaleGenerationIgnored(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	p := &fnProvider{name: "p"}
	cb := NewCircuitBreakerWithConfig(p, CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: time.Minute})

	p.fn = func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
		close(started)
		<-release
		return &models.LLMResponse{}, nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	}()
	<-started
	p.fn = failWith(upstreamErr(500))
	_, _ = cb.ChatCompletions(context.Background(), &models.LLMRequest{})
	if cb.State() != StateOpen {
		t.Fatalf("expected open, got %v", cb.State())
	}
	close(release)
	<-done
	if cb.State() != StateOpen {
		t.Fatalf("a slow success admitted before opening must not close the breaker, got %v", cb.State())
	}
}

func TestNewCircuitBreakerDefaults(t *testing.T) {
	cb := NewCircuitBreaker(&fnProvider{name: "p"}, 0, -1)
	if cb.maxFailures != DefaultMaxFailures || cb.resetTimeout != DefaultResetTimeout || cb.halfOpenMaxCalls != DefaultHalfOpenMaxCalls {
		t.Fatalf("defaults not applied: %d %v %d", cb.maxFailures, cb.resetTimeout, cb.halfOpenMaxCalls)
	}
}

func TestBreakerHealthLatencyEWMA(t *testing.T) {
	p := &fnProvider{name: "p", fn: func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
		time.Sleep(5 * time.Millisecond)
		return &models.LLMResponse{}, nil
	}}
	cb := NewCircuitBreaker(p, 3, time.Minute)
	if _, err := cb.ChatCompletions(context.Background(), &models.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	h := cb.Health()
	if h.LatencyMs < 4 || !h.Healthy || h.CircuitOpen || h.Name != "p" {
		t.Fatalf("unexpected health: %+v", h)
	}
}

func TestBreakerForwardsOptionalInterfaces(t *testing.T) {
	mp := &modelProvider{fnProvider: &fnProvider{name: "m"}, models: []string{"a"}, def: "a"}
	cb := NewCircuitBreaker(mp, 1, time.Minute)
	if !cb.SupportsModel("a") || cb.SupportsModel("b") || cb.DefaultModel() != "a" {
		t.Fatal("optional interfaces not forwarded")
	}
	if cb.Unwrap() != providers.Provider(mp) {
		t.Fatal("Unwrap must return inner provider")
	}
	plain := NewCircuitBreaker(&fnProvider{name: "x"}, 1, time.Minute)
	if !plain.SupportsModel("anything") || plain.DefaultModel() != "" {
		t.Fatal("plain provider must support everything and have no default")
	}
}

func TestRateLimitAccounting(t *testing.T) {
	limits := map[string]ProviderRateLimits{"p1": {TPM: 1000, RPM: 10}, "p2": {TPM: 1000, RPM: 10}}
	r := New(Config{Strategy: "usage_based", ProviderRateLimits: limits})
	r.RegisterProvider(&fnProvider{name: "p1"})
	r.RegisterProvider(&fnProvider{name: "p2"})
	for i := 0; i < 20; i++ {
		if _, err := r.Route(context.Background(), &models.LLMRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	cbs := r.Providers()
	for _, cb := range cbs {
		if u := cb.Utilization(limits[cb.Name()]); u != 0 {
			t.Fatalf("routing must not consume rate limit; %s utilization=%v", cb.Name(), u)
		}
	}
	if _, err := cbs[0].ChatCompletions(context.Background(), &models.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	cbs[0].usage.mu.Lock()
	reqs, toks := cbs[0].usage.RequestCountMinute, cbs[0].usage.TokenCountMinute
	cbs[0].usage.mu.Unlock()
	if reqs != 1 || toks != 10 {
		t.Fatalf("expected 1 request / 10 tokens, got %d / %d", reqs, toks)
	}
	// After the window elapses counters reset.
	cbs[0].usage.mu.Lock()
	cbs[0].usage.LastReset = time.Now().Add(-2 * time.Minute)
	cbs[0].usage.mu.Unlock()
	if u := cbs[0].CheckRateLimit(limits["p1"]); u != 0 {
		t.Fatalf("expected reset window, utilization=%v", u)
	}
}
