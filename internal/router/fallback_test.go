package router

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

func newFallbackRouter(ps ...providers.Provider) *Router {
	r := New(Config{Strategy: "fallback"})
	for _, p := range ps {
		r.RegisterProvider(p)
	}
	return r
}

func TestExecuteWithFallbackRetriesRetryable(t *testing.T) {
	p1 := &fnProvider{name: "p1", fn: failWith(upstreamErr(500))}
	p2 := &fnProvider{name: "p2"}
	p3 := &fnProvider{name: "p3"}
	r := newFallbackRouter(p1, p2, p3)
	resp, p, err := r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "p2" || resp.Model != "m" {
		t.Fatalf("expected p2 to serve, got %s", p.Name())
	}
	if p1.calls.Load() != 1 || p2.calls.Load() != 1 || p3.calls.Load() != 0 {
		t.Fatalf("unexpected calls %d %d %d", p1.calls.Load(), p2.calls.Load(), p3.calls.Load())
	}
}

func TestExecuteWithFallbackStopsOnNonRetryable(t *testing.T) {
	p1 := &fnProvider{name: "p1", fn: failWith(upstreamErr(400))}
	p2 := &fnProvider{name: "p2"}
	r := newFallbackRouter(p1, p2)
	_, p, err := r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{})
	var ue *providers.UpstreamError
	if !errors.As(err, &ue) || ue.StatusCode != 400 {
		t.Fatalf("expected upstream 400, got %v", err)
	}
	if p == nil || p.Name() != "p1" || p2.calls.Load() != 0 {
		t.Fatal("must stop after non-retryable error")
	}
}

func TestExecuteWithFallbackMaxAttempts(t *testing.T) {
	r := New(Config{Strategy: "fallback", MaxAttempts: 2})
	var ps []*fnProvider
	for i := 0; i < 5; i++ {
		p := &fnProvider{name: fmt.Sprintf("p%d", i), fn: failWith(upstreamErr(503))}
		ps = append(ps, p)
		r.RegisterProvider(p)
	}
	_, _, err := r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{})
	if providers.StatusCode(err) != 503 {
		t.Fatalf("expected wrapped 503, got %v", err)
	}
	var total int64
	for _, p := range ps {
		total += p.calls.Load()
	}
	if total != 2 {
		t.Fatalf("expected 2 attempts, got %d", total)
	}

	// Default bound is 3.
	r.SetMaxAttempts(0)
	for _, p := range ps {
		p.calls.Store(0)
	}
	_, _, _ = r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{})
	total = 0
	for _, p := range ps {
		total += p.calls.Load()
	}
	if total != DefaultMaxAttempts {
		t.Fatalf("expected %d attempts, got %d", DefaultMaxAttempts, total)
	}
}

func TestExecuteWithFallbackCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p1 := &fnProvider{name: "p1", fn: func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
		cancel()
		return nil, upstreamErr(500)
	}}
	p2 := &fnProvider{name: "p2"}
	r := newFallbackRouter(p1, p2)
	_, _, err := r.ChatCompletionsWithFallback(ctx, &models.LLMRequest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	var ue *providers.UpstreamError
	if !errors.As(err, &ue) {
		t.Fatalf("last provider error must be preserved: %v", err)
	}
	if p2.calls.Load() != 0 {
		t.Fatal("must not try more providers after cancellation")
	}
	if cb, _ := r.GetProvider("p1"); cb.(*CircuitBreaker).Health().Failures != 0 {
		t.Fatal("failure after caller cancellation must not count against the provider")
	}
}

func TestExecuteWithFallbackSkipsOpenCircuit(t *testing.T) {
	r := New(Config{Strategy: "fallback", MaxAttempts: 1})
	r.RegisterProvider(&fnProvider{name: "p1"})
	r.RegisterProvider(&fnProvider{name: "p2"})
	got, err := r.ExecuteWithFallback(context.Background(), &models.LLMRequest{}, func(p providers.Provider) error {
		if p.Name() == "p1" {
			return &CircuitBreakerOpenError{Provider: "p1"}
		}
		return nil
	})
	if err != nil || got.Name() != "p2" {
		t.Fatalf("open circuit must be skipped without consuming attempts: %v %v", got, err)
	}
	if _, err := r.ExecuteWithFallback(context.Background(), nil, nil); err == nil {
		t.Fatal("nil fn must error")
	}
}

func TestFallbackRecordsBreakerFailures(t *testing.T) {
	p1 := &fnProvider{name: "p1", fn: failWith(upstreamErr(500))}
	p2 := &fnProvider{name: "p2"}
	r := New(Config{Strategy: "fallback", BreakerConfig: CircuitBreakerConfig{MaxFailures: 2, ResetTimeout: time.Minute}})
	r.RegisterProvider(p1)
	r.RegisterProvider(p2)
	for i := 0; i < 5; i++ {
		if _, p, err := r.ChatCompletionsWithFallback(context.Background(), &models.LLMRequest{}); err != nil || p.Name() != "p2" {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
	if p1.calls.Load() != 2 {
		t.Fatalf("p1 breaker should open after 2 failures; calls=%d", p1.calls.Load())
	}
	cb, _ := r.GetProvider("p1")
	if !cb.Health().CircuitOpen {
		t.Fatal("expected p1 circuit open")
	}
}

func TestStreamWithFallback(t *testing.T) {
	plain := &fnProvider{name: "plain"}
	broken := &streamProvider{fnProvider: &fnProvider{name: "broken"}, stream: func(context.Context, *models.LLMRequest) (<-chan models.StreamChunk, error) {
		return nil, upstreamErr(502)
	}}
	good := &streamProvider{fnProvider: &fnProvider{name: "good"}, stream: chunksStream(models.StreamChunk{ID: "a"})}
	r := newFallbackRouter(plain, broken, good)
	ch, p, err := r.StreamWithFallback(context.Background(), &models.LLMRequest{})
	if err != nil || p.Name() != "good" {
		t.Fatalf("expected good stream, got %v %v", p, err)
	}
	var n int
	for range ch {
		n++
	}
	if n != 1 || broken.streamCalls.Load() != 1 {
		t.Fatalf("unexpected stream result n=%d brokenCalls=%d", n, broken.streamCalls.Load())
	}

	onlyPlain := newFallbackRouter(&fnProvider{name: "a"}, &fnProvider{name: "b"})
	if _, _, err := onlyPlain.StreamWithFallback(context.Background(), &models.LLMRequest{}); !errors.Is(err, providers.ErrStreamingNotSupported) {
		t.Fatalf("expected ErrStreamingNotSupported, got %v", err)
	}
}

func TestModelAwareFiltering(t *testing.T) {
	gpt := &modelProvider{fnProvider: &fnProvider{name: "gpt"}, models: []string{"gpt-4o"}}
	claude := &modelProvider{fnProvider: &fnProvider{name: "claude"}, models: []string{"claude-3"}}
	r := newFallbackRouter(gpt, claude)
	p, err := r.Route(context.Background(), &models.LLMRequest{Model: "claude-3"})
	if err != nil || p.Name() != "claude" {
		t.Fatalf("expected claude, got %v %v", p, err)
	}
	cands, _ := r.Candidates(context.Background(), &models.LLMRequest{Model: "gpt-4o"})
	if len(cands) != 1 || cands[0].Name() != "gpt" {
		t.Fatalf("expected only gpt, got %v", cands)
	}
	_, err = r.Route(context.Background(), &models.LLMRequest{Model: "llama"})
	var npe *NoProviderError
	if !errors.As(err, &npe) || npe.Model != "llama" || !strings.Contains(err.Error(), `"llama"`) {
		t.Fatalf("expected NoProviderError for llama, got %v", err)
	}
	// Providers that do not declare models remain candidates.
	r.RegisterProvider(&fnProvider{name: "any"})
	if p, err := r.Route(context.Background(), &models.LLMRequest{Model: "llama"}); err != nil || p.Name() != "any" {
		t.Fatalf("expected undeclared provider, got %v %v", p, err)
	}
	if (&NoProviderError{}).Error() != "no available provider" {
		t.Fatal("unexpected default message")
	}
}

func TestCostStrategyPerProviderModel(t *testing.T) {
	cm := intelligence.NewModelCostMap()
	if err := cm.LoadFromBytes([]byte(`{
		"pricey-model": {"input_cost_per_1m_tokens": 30, "output_cost_per_1m_tokens": 60},
		"cheap-model": {"input_cost_per_1m_tokens": 0.1, "output_cost_per_1m_tokens": 0.2}
	}`)); err != nil {
		t.Fatal(err)
	}
	r := New(Config{Strategy: "cost", CostMap: cm})
	r.RegisterProvider(&defaultOnly{fnProvider: &fnProvider{name: "pricey"}, def: "pricey-model"})
	r.RegisterProvider(&defaultOnly{fnProvider: &fnProvider{name: "cheap"}, def: "cheap-model"})
	req := &models.LLMRequest{Messages: []models.Message{{Role: models.RoleUser, Content: strp("hello there")}}}
	cands, err := r.Candidates(context.Background(), req)
	if err != nil || len(cands) != 2 || cands[0].Name() != "cheap" || cands[1].Name() != "pricey" {
		t.Fatalf("expected [cheap pricey], got %v %v", cands, err)
	}

	// Both serve the requested model: tie keeps registration order.
	r2 := New(Config{Strategy: "cost", CostMap: cm})
	r2.RegisterProvider(&modelProvider{fnProvider: &fnProvider{name: "a"}, models: []string{"cheap-model"}, def: "pricey-model"})
	r2.RegisterProvider(&modelProvider{fnProvider: &fnProvider{name: "b"}, models: []string{"cheap-model"}, def: "cheap-model"})
	p, err := r2.Route(context.Background(), &models.LLMRequest{Model: "cheap-model"})
	if err != nil || p.Name() != "a" {
		t.Fatalf("expected a (tie, registration order), got %v %v", p, err)
	}
}

func TestLatencyStrategyNaNSafe(t *testing.T) {
	r := New(Config{Strategy: "latency"})
	r.RegisterProvider(&fnProvider{name: "nan", latency: math.NaN()})
	r.RegisterProvider(&fnProvider{name: "ok", latency: 100})
	r.RegisterProvider(&fnProvider{name: "inf", latency: math.Inf(1)})
	p, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil || p.Name() != "ok" {
		t.Fatalf("expected ok, got %v %v", p, err)
	}
	r2 := New(Config{Strategy: "latency"})
	r2.RegisterProvider(&fnProvider{name: "a", latency: math.NaN()})
	r2.RegisterProvider(&fnProvider{name: "b", latency: math.Inf(1)})
	if p, err := r2.Route(context.Background(), &models.LLMRequest{}); err != nil || p == nil {
		t.Fatalf("latency routing must never return nil provider: %v %v", p, err)
	}
}

func TestLatencyStrategyUsesObservedLatency(t *testing.T) {
	slow := &fnProvider{name: "slow", latency: 1, fn: func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return &models.LLMResponse{}, nil
	}}
	fast := &fnProvider{name: "fast", latency: 5}
	r := New(Config{Strategy: "latency"})
	r.RegisterProvider(slow)
	r.RegisterProvider(fast)
	if p, _ := r.Route(context.Background(), &models.LLMRequest{}); p.Name() != "slow" {
		t.Fatalf("before observations the reported latency is used; got %s", p.Name())
	}
	cb, _ := r.GetProvider("slow")
	if _, err := cb.ChatCompletions(context.Background(), &models.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	if p, _ := r.Route(context.Background(), &models.LLMRequest{}); p.Name() != "fast" {
		t.Fatalf("observed latency must win; got %s", p.Name())
	}
}

func TestRoundRobinRotates(t *testing.T) {
	r := New(Config{Strategy: "round_robin"})
	for _, n := range []string{"a", "b", "c"} {
		r.RegisterProvider(&fnProvider{name: n})
	}
	counts := map[string]int{}
	for i := 0; i < 6; i++ {
		p, err := r.Route(context.Background(), &models.LLMRequest{})
		if err != nil {
			t.Fatal(err)
		}
		counts[p.Name()]++
	}
	if counts["a"] != 2 || counts["b"] != 2 || counts["c"] != 2 {
		t.Fatalf("uneven rotation: %v", counts)
	}
	cands, _ := r.Candidates(context.Background(), &models.LLMRequest{})
	if len(cands) != 3 {
		t.Fatalf("expected all candidates, got %d", len(cands))
	}
	// Unknown strategy falls back to round robin.
	r.SetStrategy("bogus")
	if p, err := r.Route(context.Background(), nil); err != nil || p == nil {
		t.Fatalf("unknown strategy / nil request: %v %v", p, err)
	}
}

func TestRegisterReplaceAndRemove(t *testing.T) {
	r := newFallbackRouter(&fnProvider{name: "a"}, &fnProvider{name: "b"})
	replacement := &fnProvider{name: "a"}
	r.RegisterProvider(replacement)
	if got := len(r.Providers()); got != 2 {
		t.Fatalf("re-registering must replace, got %d providers", got)
	}
	if cb := r.Providers()[0]; cb.Unwrap() != providers.Provider(replacement) {
		t.Fatal("replacement must keep position and wrap new provider")
	}
	if !r.RemoveProvider("a") || r.RemoveProvider("a") {
		t.Fatal("RemoveProvider must report removal once")
	}
	if p, _ := r.Route(context.Background(), &models.LLMRequest{}); p.Name() != "b" {
		t.Fatalf("expected b, got %s", p.Name())
	}
	r.RegisterProvider(nil) // must not panic
}

func TestRouterRaceStress(t *testing.T) {
	cm := intelligence.NewModelCostMap()
	r := New(Config{Strategy: "round_robin", BreakerConfig: CircuitBreakerConfig{MaxFailures: 2, ResetTimeout: time.Millisecond, HalfOpenMaxCalls: 2}})
	for i := 0; i < 4; i++ {
		i := i
		p := &streamProvider{fnProvider: &fnProvider{name: fmt.Sprintf("p%d", i), latency: float64(i)}, stream: chunksStream(models.StreamChunk{Usage: &models.Usage{TotalTokens: 3}})}
		if i%2 == 0 {
			var n int64
			var mu sync.Mutex
			p.fn = func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
				mu.Lock()
				n++
				fail := n%3 == 0
				mu.Unlock()
				if fail {
					return nil, upstreamErr(500)
				}
				return &models.LLMResponse{Usage: &models.Usage{TotalTokens: 5}}, nil
			}
		}
		r.RegisterProvider(p)
	}
	strategies := []string{"round_robin", "least_busy", "usage_based", "latency", "cost", "fallback"}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				req := &models.LLMRequest{Model: "gpt-4o", Messages: []models.Message{{Role: models.RoleUser, Content: strp("hi")}}}
				switch (g + i) % 7 {
				case 0:
					r.SetStrategy(strategies[i%len(strategies)])
				case 1:
					r.SetRateLimits(map[string]ProviderRateLimits{"p0": {TPM: 100, RPM: 10}, "p1": {RPM: 5}})
				case 2:
					if ch, _, err := r.StreamWithFallback(context.Background(), req); err == nil {
						for range ch {
						}
					}
				case 3:
					for _, cb := range r.Providers() {
						_ = cb.Health()
						_ = cb.State()
					}
				case 4:
					r.SetCostMap(cm)
					r.SetMaxAttempts(i % 4)
				default:
					_, _, _ = r.ChatCompletionsWithFallback(context.Background(), req)
					_, _ = r.Route(context.Background(), req)
				}
			}
		}(g)
	}
	wg.Wait()
	for _, cb := range r.Providers() {
		if cb.Inflight() != 0 {
			t.Fatalf("%s: inflight leak %d", cb.Name(), cb.Inflight())
		}
	}
}
