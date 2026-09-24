package router

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

func retryAfterErr(status int, d time.Duration) error {
	return &providers.UpstreamError{Provider: "test", StatusCode: status, Message: "slow down", RetryAfter: d}
}

// fakeSleep records requested waits without sleeping.
type fakeSleep struct {
	mu    sync.Mutex
	waits []time.Duration
	err   error
}

func (f *fakeSleep) sleep(ctx context.Context, d time.Duration) error {
	f.mu.Lock()
	f.waits = append(f.waits, d)
	f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	return ctx.Err()
}

func (f *fakeSleep) calls() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.waits...)
}

func TestRetryAfterRetriesLastProviderOnce(t *testing.T) {
	var n atomic.Int32
	p := &fnProvider{name: "only", fn: func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
		if n.Add(1) == 1 {
			return nil, retryAfterErr(429, 2*time.Second)
		}
		return &models.LLMResponse{Model: "m"}, nil
	}}
	r := New(Config{Strategy: "fallback", MaxRetryWait: 5 * time.Second})
	fs := &fakeSleep{}
	r.sleep = fs.sleep
	r.RegisterProvider(p)
	resp, served, err := r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{Model: "m"})
	if err != nil || resp == nil || served.Name() != "only" {
		t.Fatalf("retry should succeed: %v", err)
	}
	if w := fs.calls(); len(w) != 1 || w[0] != 2*time.Second || p.calls.Load() != 2 {
		t.Fatalf("waits=%v calls=%d", w, p.calls.Load())
	}
}

func TestRetryAfterDisabledOrOutOfBounds(t *testing.T) {
	cases := []struct {
		name    string
		maxWait time.Duration
		err     error
		ctx     func() (context.Context, context.CancelFunc)
	}{
		{"default off", 0, retryAfterErr(429, time.Second), nil},
		{"retry-after too long", time.Second, retryAfterErr(429, 2*time.Second), nil},
		{"no retry-after", time.Second, retryAfterErr(503, 0), nil},
		{"not retryable", time.Minute, retryAfterErr(400, time.Second), nil},
		{"transport error", time.Minute, &providers.TransportError{Provider: "x", Err: errors.New("reset")}, nil},
		{"deadline too close", time.Minute, retryAfterErr(429, 10*time.Second), func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 5*time.Second)
		}},
	}
	for _, c := range cases {
		p := &fnProvider{name: "only", fn: failWith(c.err)}
		r := New(Config{Strategy: "fallback", MaxRetryWait: c.maxWait})
		fs := &fakeSleep{}
		r.sleep = fs.sleep
		r.RegisterProvider(p)
		ctx, cancel := context.Background(), context.CancelFunc(func() {})
		if c.ctx != nil {
			ctx, cancel = c.ctx()
		}
		_, _, err := r.ChatCompletionsWithFallback(ctx, &models.LLMRequest{})
		cancel()
		if err == nil || p.calls.Load() != 1 || len(fs.calls()) != 0 {
			t.Fatalf("%s: must not wait/retry (calls=%d waits=%v err=%v)", c.name, p.calls.Load(), fs.calls(), err)
		}
		if !errors.Is(err, c.err) {
			t.Fatalf("%s: original error must be returned: %v", c.name, err)
		}
	}
}

func TestRetryAfterOnlyWhenNoCandidateRemains(t *testing.T) {
	// A second candidate exists: fall back to it instead of waiting.
	p1 := &fnProvider{name: "p1", fn: failWith(retryAfterErr(429, time.Second))}
	p2 := &fnProvider{name: "p2"}
	r := New(Config{Strategy: "fallback", MaxRetryWait: time.Minute})
	fs := &fakeSleep{}
	r.sleep = fs.sleep
	r.RegisterProvider(p1)
	r.RegisterProvider(p2)
	if _, served, err := r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{}); err != nil || served.Name() != "p2" || len(fs.calls()) != 0 {
		t.Fatalf("expected fallback without waiting: %v %v", err, fs.calls())
	}

	// MaxAttempts stopped the loop while candidates remain: no wait.
	q1 := &fnProvider{name: "q1", fn: failWith(retryAfterErr(429, time.Second))}
	q2 := &fnProvider{name: "q2", fn: failWith(retryAfterErr(429, time.Second))}
	r = New(Config{Strategy: "fallback", MaxRetryWait: time.Minute, MaxAttempts: 1})
	r.sleep = fs.sleep
	r.RegisterProvider(q1)
	r.RegisterProvider(q2)
	if _, _, err := r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{}); err == nil || len(fs.calls()) != 0 || q2.calls.Load() != 0 {
		t.Fatalf("attempt bound must not trigger a retry wait: %v", fs.calls())
	}

	// Every candidate failed; the last one asked to retry after 1s.
	a := &fnProvider{name: "a", fn: failWith(upstreamErr(503))}
	var n atomic.Int32
	b := &fnProvider{name: "b", fn: func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
		if n.Add(1) == 1 {
			return nil, retryAfterErr(429, time.Second)
		}
		return nil, retryAfterErr(503, time.Second)
	}}
	r = New(Config{Strategy: "fallback", MaxRetryWait: time.Minute})
	fs = &fakeSleep{}
	r.sleep = fs.sleep
	r.RegisterProvider(a)
	r.RegisterProvider(b)
	_, last, err := r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{})
	if err == nil || last.Name() != "b" || b.calls.Load() != 2 || a.calls.Load() != 1 || len(fs.calls()) != 1 {
		t.Fatalf("retry once only: err=%v a=%d b=%d waits=%v", err, a.calls.Load(), b.calls.Load(), fs.calls())
	}
	if providers.StatusCode(err) != 503 {
		t.Fatalf("the retry's error must be reported: %v", err)
	}
}

func TestRetryAfterWaitIsContextAware(t *testing.T) {
	// Real timer, cancelled mid-wait.
	p := &fnProvider{name: "only", fn: failWith(retryAfterErr(429, 10*time.Second))}
	r := New(Config{Strategy: "fallback", MaxRetryWait: time.Minute})
	r.RegisterProvider(p)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)
	start := time.Now()
	_, _, err := r.ChatCompletionsWithFallback(ctx, &models.LLMRequest{})
	if !errors.Is(err, context.Canceled) || time.Since(start) > 2*time.Second || p.calls.Load() != 1 {
		t.Fatalf("wait must end on cancel: err=%v elapsed=%v calls=%d", err, time.Since(start), p.calls.Load())
	}

	// Real short wait, then success.
	var n atomic.Int32
	q := &fnProvider{name: "q", fn: func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
		if n.Add(1) == 1 {
			return nil, retryAfterErr(503, 20*time.Millisecond)
		}
		return &models.LLMResponse{}, nil
	}}
	r = New(Config{Strategy: "fallback"})
	r.SetMaxRetryWait(time.Second)
	r.RegisterProvider(q)
	start = time.Now()
	if _, _, err := r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 20*time.Millisecond {
		t.Fatalf("did not honour Retry-After: %v", el)
	}

	// The retry is refused by the breaker: the original failure is reported.
	c := &fnProvider{name: "c", fn: failWith(retryAfterErr(429, time.Millisecond))}
	r = New(Config{Strategy: "fallback", MaxRetryWait: time.Second, BreakerConfig: CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: time.Hour}})
	r.RegisterProvider(c)
	_, _, err = r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{})
	if providers.StatusCode(err) != 429 || c.calls.Load() != 1 {
		t.Fatalf("open breaker must block the retry: %v calls=%d", err, c.calls.Load())
	}
}

func TestRetryAfterAppliesToStreamStart(t *testing.T) {
	var n atomic.Int32
	sp := &streamProvider{fnProvider: &fnProvider{name: "s"}}
	sp.stream = func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
		if n.Add(1) == 1 {
			return nil, retryAfterErr(429, time.Millisecond)
		}
		return chunksStream(models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "hi"}}}})(ctx, req)
	}
	r := New(Config{Strategy: "fallback", MaxRetryWait: time.Second})
	r.RegisterProvider(sp)
	ch, _, err := r.StreamWithFallback(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}
	if sp.streamCalls.Load() != 2 {
		t.Fatalf("stream start must be retried once: %d", sp.streamCalls.Load())
	}
}

func TestForceOpenAndResetCircuit(t *testing.T) {
	p1 := &fnProvider{name: "p1"}
	p2 := &fnProvider{name: "p2"}
	r := New(Config{Strategy: "fallback"})
	r.RegisterProvider(p1)
	r.RegisterProvider(p2)
	req := &models.LLMRequest{}

	if err := r.ForceOpen("nope", time.Minute); !errors.Is(err, ErrProviderNotFound) {
		t.Fatalf("unknown provider: %v", err)
	}
	if err := r.ResetCircuit("nope"); !errors.Is(err, ErrProviderNotFound) {
		t.Fatalf("unknown provider: %v", err)
	}
	if err := r.ForceOpen("p1", 0); err == nil {
		t.Fatal("non-positive duration must be rejected")
	}

	if err := r.ForceOpen("p1", time.Hour); err != nil {
		t.Fatal(err)
	}
	cands, err := r.Candidates(context.Background(), req)
	if err != nil || len(cands) != 1 || cands[0].Name() != "p2" {
		t.Fatalf("forced-open provider must be excluded: %v %v", cands, err)
	}
	cb, _ := r.GetProvider("p1")
	if _, err := cb.ChatCompletions(context.Background(), req); !errors.Is(err, providers.ErrCircuitOpen) || p1.calls.Load() != 0 {
		t.Fatalf("direct calls must be rejected while forced open: %v", err)
	}
	var oe *CircuitBreakerOpenError
	if _, err := cb.ChatCompletions(context.Background(), req); !errors.As(err, &oe) || oe.RetryAfter <= 59*time.Minute {
		t.Fatalf("retry-after must reflect the forced duration: %v", err)
	}
	h := cb.Health()
	if h.Healthy || !h.CircuitOpen || r.Providers()[0].State() != StateOpen {
		t.Fatalf("health while forced open: %+v", h)
	}

	if err := r.ResetCircuit("p1"); err != nil {
		t.Fatal(err)
	}
	if cands, _ := r.Candidates(context.Background(), req); len(cands) != 2 || cands[0].Name() != "p1" {
		t.Fatalf("reset must restore the provider: %v", cands)
	}
	if _, err := cb.ChatCompletions(context.Background(), req); err != nil || p1.calls.Load() != 1 {
		t.Fatalf("calls must pass after reset: %v", err)
	}

	// A short forced period expires into half-open, which admits a trial
	// call that closes the circuit.
	if err := r.ForceOpen("p1", 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if cands, _ := r.Candidates(context.Background(), req); len(cands) != 1 {
		t.Fatal("excluded during the forced period")
	}
	waitFor(t, 2*time.Second, func() bool {
		c, _ := r.Candidates(context.Background(), req)
		return len(c) == 2
	}, "forced period to expire")
	if _, err := cb.ChatCompletions(context.Background(), req); err != nil || r.Providers()[0].State() != StateClosed {
		t.Fatalf("trial call after expiry: %v state=%v", err, r.Providers()[0].State())
	}

	// Reset clears consecutive failures of a failure-opened breaker.
	f := &fnProvider{name: "f", fn: failWith(upstreamErr(503))}
	r2 := New(Config{BreakerConfig: CircuitBreakerConfig{MaxFailures: 2, ResetTimeout: time.Hour}})
	r2.RegisterProvider(f)
	fcb := r2.Providers()[0]
	for i := 0; i < 2; i++ {
		_, _ = fcb.ChatCompletions(context.Background(), req)
	}
	if fcb.State() != StateOpen {
		t.Fatal("breaker should be open after failures")
	}
	if err := r2.ResetCircuit("f"); err != nil || fcb.State() != StateClosed {
		t.Fatalf("reset: %v %v", err, fcb.State())
	}
	_, _ = fcb.ChatCompletions(context.Background(), req)
	if fcb.State() != StateClosed {
		t.Fatal("one failure after reset must not reopen (failures were cleared)")
	}
}

func TestForceOpenConcurrentWithTraffic(t *testing.T) {
	var inflight atomic.Int32
	p := &fnProvider{name: "p", fn: func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
		inflight.Add(1)
		defer inflight.Add(-1)
		time.Sleep(time.Millisecond)
		return nil, upstreamErr(503)
	}}
	r := New(Config{BreakerConfig: CircuitBreakerConfig{MaxFailures: 3, ResetTimeout: time.Millisecond}})
	r.RegisterProvider(p)
	cb := r.Providers()[0]
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = cb.ChatCompletions(context.Background(), &models.LLMRequest{})
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		_ = r.ForceOpen("p", time.Hour)
		_ = r.ResetCircuit("p")
		_ = cb.Health()
		_, _ = r.Candidates(context.Background(), &models.LLMRequest{})
	}
	if err := r.ForceOpen("p", time.Hour); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	// Calls admitted before ForceOpen belong to an older generation and
	// cannot close or shorten the forced period.
	if cb.State() != StateOpen {
		t.Fatalf("forced open must survive in-flight failures: %v", cb.State())
	}
	if c, _ := r.Candidates(context.Background(), &models.LLMRequest{}); len(c) != 0 {
		t.Fatal("forced-open provider must stay excluded")
	}
}

func TestRouterProbeAll(t *testing.T) {
	r := New(Config{})
	r.RegisterProvider(&fnProvider{name: "plain"})
	r.RegisterProvider(&probingProvider{fnProvider: &fnProvider{name: "probed"}, err: upstreamErr(503)})
	res := r.ProbeAll(context.Background())
	if len(res) != 1 || providers.StatusCode(res["probed"]) != 503 {
		t.Fatalf("probe results: %v", res)
	}
	cb, _ := r.GetProvider("plain")
	if err := cb.(*CircuitBreaker).Probe(context.Background()); !errors.Is(err, providers.ErrProbeNotSupported) {
		t.Fatalf("non-probing provider: %v", err)
	}
}

type probingProvider struct {
	*fnProvider
	err error
}

func (p *probingProvider) Probe(context.Context) error { return p.err }
