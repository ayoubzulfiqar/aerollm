package intelligence

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestBanditConcurrentRouteUpdate(t *testing.T) {
	b := NewBanditRouter()
	cands := []ModelOption{{Provider: "a", Model: "m1"}, {Provider: "b", Model: "m2"}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if _, err := b.Route(context.Background(), cands); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				b.Update("a", "m1", float64(j), 0.01, i%2 == 0)
				_ = b.Snapshot()
			}
		}(i)
	}
	wg.Wait()
}

func TestBanditEmptyAndCanceled(t *testing.T) {
	b := NewBanditRouter()
	if _, err := b.Route(context.Background(), nil); !errors.Is(err, ErrNoCandidates) {
		t.Fatalf("expected ErrNoCandidates, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Route(ctx, []ModelOption{{Provider: "a"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestBanditUpdateSanitizesInputs(t *testing.T) {
	b := NewBanditRouter()
	b.Update("p", "m", math.NaN(), math.Inf(1), true)
	b.Update("p", "m", -5, -1, false)
	b.Update("p", "m", math.Inf(1), 0, true)
	s := b.Snapshot()["p/m"]
	for _, v := range []float64{s.Alpha, s.Beta} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 1 {
			t.Fatalf("invalid posterior after sanitized updates: %+v", s)
		}
	}
	// Alpha+Beta grows by exactly 1 per update (fractional Bernoulli).
	if got := s.Alpha + s.Beta; math.Abs(got-5) > 1e-9 {
		t.Fatalf("expected alpha+beta=5, got %v", got)
	}
}

func TestBanditPrefersRewardedArm(t *testing.T) {
	b := NewBanditRouter()
	for i := 0; i < 200; i++ {
		b.Update("good", "m", 10, 0, true)
		b.Update("bad", "m", 10000, 5, false)
	}
	cands := []ModelOption{{Provider: "bad", Model: "m"}, {Provider: "good", Model: "m"}}
	wins := 0
	for i := 0; i < 200; i++ {
		c, err := b.Route(context.Background(), cands)
		if err != nil {
			t.Fatal(err)
		}
		if c.Provider == "good" {
			wins++
		}
	}
	if wins < 190 {
		t.Fatalf("expected good arm to dominate, won %d/200", wins)
	}
}

func TestBanditScoreNaNSafe(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for _, s := range []BanditState{{Alpha: math.NaN(), Beta: 1}, {Alpha: 0, Beta: -1}, {Alpha: 0.3, Beta: 0.2}, {Alpha: 1e6, Beta: 1}} {
		v := s.Score(r)
		if math.IsNaN(v) || v < 0 || v > 1 {
			t.Fatalf("score out of range for %+v: %v", s, v)
		}
	}
}

func TestCostMapNormalizedLookups(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()
	cases := []struct {
		model string
		in    float64
	}{
		{"gpt-4o-2024-08-06", 2.5},
		{"GPT-4o", 2.5},
		{"openai/gpt-4o-mini-2024-07-18", 0.15},
		{"gpt-4-0613", 30.0},
		{"gpt-4-1106-preview", 10.0},
		{"gpt-4o-audio-preview", 2.5},
		{"gpt-4.1-mini-2025-04-14", 0.4},
		{"claude-3-opus-20240229", 15.0},
		{"claude-3-5-sonnet-latest", 3.0},
		{"claude-3-5-sonnet@20240620", 3.0},
		{"anthropic.claude-3-haiku-20240307-v1:0", 0.25},
		{"us.anthropic.claude-3-5-sonnet-20241022-v2:0", 3.0},
		{"claude-sonnet-4-20250514", 3.0},
		{"gemini-2.0-flash-001", 0.075},
		{"gemini-exp-1206", 0.35},
		{"gpt-3.5-turbo-0125", 0.5},
	}
	for _, c := range cases {
		cost, ok := m.LookupKnown(c.model)
		if !ok {
			t.Errorf("%s: expected known pricing", c.model)
			continue
		}
		if cost.InputCostPer1M != c.in {
			t.Errorf("%s: expected input %.3f, got %.3f (%s)", c.model, c.in, cost.InputCostPer1M, cost.ModelName)
		}
	}
	if _, ok := m.LookupKnown("totally-unknown"); ok {
		t.Fatal("unknown model must not be known")
	}
	if _, ok := m.LookupKnown("gpt-4oops"); ok {
		t.Fatal("gpt-4 must only match on a '-' boundary")
	}
}

func TestCostMapLoadFromDefaultIdempotent(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()
	n := len(m.prefixes)
	m.LoadFromDefault()
	if len(m.prefixes) != n {
		t.Fatalf("prefixes duplicated: %d -> %d", n, len(m.prefixes))
	}
}

func TestCostMapLoadFromBytesDefaultAndValidation(t *testing.T) {
	m := NewModelCostMap()
	err := m.LoadFromBytes([]byte(`{"default":{"input_cost_per_1m_tokens":2,"output_cost_per_1m_tokens":4},"My-Model":{"input_cost_per_1m_tokens":1,"output_cost_per_1m_tokens":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := m.Lookup("unknown"); c.InputCostPer1M != 2 {
		t.Fatalf("expected default key to apply, got %+v", c)
	}
	if c, ok := m.LookupKnown("my-model"); !ok || c.InputCostPer1M != 1 {
		t.Fatalf("expected case-insensitive match, got %+v %v", c, ok)
	}
	if err := m.LoadFromBytes([]byte(`{"bad":{"input_cost_per_1m_tokens":-1}}`)); err == nil {
		t.Fatal("expected negative price to be rejected")
	}
	// Rejected load leaves previous data intact.
	if _, ok := m.LookupKnown("my-model"); !ok {
		t.Fatal("rejected load must not clobber existing data")
	}
	if err := m.LoadFromBytes([]byte(`not json`)); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestCostFor(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()
	usd, ok := m.CostFor("gpt-4o-2024-08-06", &models.Usage{PromptTokens: 1_000_000, CompletionTokens: 1_000_000})
	if !ok || math.Abs(usd-12.5) > 1e-9 {
		t.Fatalf("expected 12.5 USD, got %v %v", usd, ok)
	}
	if _, ok := m.CostFor("unknown-model", &models.Usage{PromptTokens: 10}); ok {
		t.Fatal("unknown model must report ok=false")
	}
	if _, ok := m.CostFor("gpt-4o", nil); ok {
		t.Fatal("nil usage must report ok=false")
	}
	usd, ok = m.CostFor("gpt-4o", &models.Usage{PromptTokens: -100, CompletionTokens: -5})
	if !ok || usd != 0 {
		t.Fatalf("negative tokens must clamp to 0, got %v", usd)
	}
	if got := m.CalculateCost("gpt-4o", &models.Usage{PromptTokens: -1_000_000}); got != 0 {
		t.Fatalf("CalculateCost must clamp negatives, got %v", got)
	}
	var nilMap *ModelCostMap
	if _, ok := nilMap.CostFor("gpt-4o", &models.Usage{}); ok {
		t.Fatal("nil map must report ok=false")
	}
}

func TestRefreshPeriodicallyContext(t *testing.T) {
	m := NewModelCostMap()
	if err := m.RefreshPeriodicallyContext(context.Background(), 0, func() ([]byte, error) { return nil, nil }); err == nil {
		t.Fatal("expected error for zero interval")
	}
	if err := m.RefreshPeriodicallyContext(context.Background(), time.Second, nil); err == nil {
		t.Fatal("expected error for nil refresh fn")
	}
	// Old API must not panic or block on an invalid interval.
	m.RefreshPeriodically(-1, nil)

	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.RefreshPeriodicallyContext(ctx, time.Millisecond, func() ([]byte, error) {
			calls.Add(1)
			return []byte(`{"fresh-model":{"input_cost_per_1m_tokens":9,"output_cost_per_1m_tokens":9}}`), nil
		})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh loop did not stop on cancel")
	}
	if c, ok := m.LookupKnown("fresh-model"); !ok || c.InputCostPer1M != 9 {
		t.Fatalf("expected refreshed data, got %+v %v", c, ok)
	}
}

func TestHeuristicSelectorNaNAndEmpty(t *testing.T) {
	s := NewHeuristicSelector()
	if _, err := s.Select(context.Background(), nil, Policy{}); !errors.Is(err, ErrNoCandidates) {
		t.Fatalf("expected ErrNoCandidates, got %v", err)
	}
	opts := []ModelOption{
		{Model: "nan", Cost: math.NaN(), Quality: math.NaN()},
		{Model: "ok", Cost: 1, Quality: 0.8},
		{Model: "cheap", Cost: 0.5, Quality: 0.6},
	}
	got, err := s.Select(context.Background(), opts, Policy{MinQuality: 0.5, PreferCheapest: true})
	if err != nil || got.Model != "cheap" {
		t.Fatalf("expected cheap, got %+v %v", got, err)
	}
	got, _ = s.Select(context.Background(), opts, Policy{})
	if got.Model != "ok" {
		t.Fatalf("expected highest quality 'ok', got %+v", got)
	}
	// Soft policy: nothing qualifies -> best-ranked overall, not an error.
	got, err = s.Select(context.Background(), opts, Policy{MinQuality: 0.99})
	if err != nil || got.Model != "ok" {
		t.Fatalf("expected soft fallback to 'ok', got %+v %v", got, err)
	}
}

func TestSLASelectorHardRequirements(t *testing.T) {
	s := NewSLASelector()
	if _, err := s.Select(context.Background(), nil, SLA{}); !errors.Is(err, ErrNoCandidates) {
		t.Fatalf("expected ErrNoCandidates, got %v", err)
	}
	opts := []ModelOptionWithSLA{
		{ModelOption: ModelOption{Model: "slow", Latency: 5000, Quality: 0.9}, Available: true},
		{ModelOption: ModelOption{Model: "down", Latency: 10, Quality: 0.9}, Available: false},
		{ModelOption: ModelOption{Model: "nan", Latency: math.NaN(), Quality: 0.9}, Available: true},
	}
	if _, err := s.Select(context.Background(), opts, SLA{MaxLatencyMs: 100}); !errors.Is(err, ErrNoCandidateMeetsSLA) {
		t.Fatalf("expected ErrNoCandidateMeetsSLA, got %v", err)
	}
	got, err := s.Select(context.Background(), opts, SLA{MaxLatencyMs: 6000})
	if err != nil || got.Model != "slow" {
		t.Fatalf("expected slow, got %+v %v", got, err)
	}
}
