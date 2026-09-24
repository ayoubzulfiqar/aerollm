package finops

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
	"github.com/redis/go-redis/v9"
)

type fakeDispatcher struct {
	dispatched []webhooks.Event
	targeted   []webhooks.WebhookConfig
	mu         sync.Mutex
}

func (f *fakeDispatcher) DispatchAsync(_ context.Context, event webhooks.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatched = append(f.dispatched, event)
}

func (f *fakeDispatcher) events() []webhooks.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]webhooks.Event(nil), f.dispatched...)
}

type fakeTargetedDispatcher struct{ fakeDispatcher }

func (f *fakeTargetedDispatcher) DispatchToAsync(_ context.Context, cfg webhooks.WebhookConfig, event webhooks.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targeted = append(f.targeted, cfg)
	f.dispatched = append(f.dispatched, event)
}

func newTracker() *CostTracker {
	cm := intelligence.NewModelCostMap()
	cm.LoadFromDefault()
	return NewCostTracker(nil, NewPricingMap(), cm)
}

func TestPricingMapDefaults(t *testing.T) {
	p := NewPricingMap()
	pr, ok := p.Get("gpt-4")
	if !ok || pr.PromptPrice != 0.03 {
		t.Fatalf("unexpected pricing: %+v", pr)
	}
}

func TestCalculateCost(t *testing.T) {
	c := newTracker()
	mc, ok := c.costMap.Lookup("gpt-4o")
	if !ok {
		t.Fatal("gpt-4o missing from default cost map")
	}
	cost := c.CalculateCost("gpt-4o", &models.Usage{PromptTokens: 1000000, CompletionTokens: 500000})
	want := mc.InputCostPer1M + 0.5*mc.OutputCostPer1M
	if !approxEqual(cost, want, 0.0001) {
		t.Fatalf("expected %.4f, got %.4f", want, cost)
	}
}

// TestCostUnitsPinned pins the unit conversion against list prices
// (gpt-4o: $2.50 / 1M input, $10 / 1M output) so a per-1K vs per-token
// mix-up can never silently overcharge by 1000x again.
func TestCostUnitsPinned(t *testing.T) {
	cm := intelligence.NewModelCostMap()
	if err := cm.LoadFromBytes([]byte(`{"gpt-4o":{"input_cost_per_1m_tokens":2.5,"output_cost_per_1m_tokens":10}}`)); err != nil {
		t.Fatal(err)
	}
	c := NewCostTracker(nil, NewPricingMap(), cm)
	cases := []struct {
		model   string
		in, out int
		wantUSD float64
	}{
		{"gpt-4o", 1_000_000, 1_000_000, 12.5},
		{"gpt-4o", 1000, 500, 0.0075},
		{"gpt-4o-2024-08-06", 1000, 0, 0.0025},
		{"gpt-4o", 1, 1, 0.0000125},
	}
	for _, tc := range cases {
		got := c.CalculateCost(tc.model, &models.Usage{PromptTokens: tc.in, CompletionTokens: tc.out})
		if !approxEqual(got, tc.wantUSD, 1e-12) {
			t.Errorf("%s %d/%d: got $%.10f, want $%.10f", tc.model, tc.in, tc.out, got, tc.wantUSD)
		}
	}
}

func approxEqual(a, b, epsilon float64) bool {
	return a-b < epsilon && b-a < epsilon
}

func TestCalculateCostNilUsage(t *testing.T) {
	c := NewCostTracker(nil, NewPricingMap(), intelligence.NewModelCostMap())
	if got := c.CalculateCost("gpt-4", nil); got != 0 {
		t.Fatalf("expected 0 for nil usage, got %f", got)
	}
}

func TestDateSuffixedAndPrefixedModelNames(t *testing.T) {
	c := newTracker()
	usage := &models.Usage{PromptTokens: 1_000_000, CompletionTokens: 1_000_000}
	// model -> base model whose price must be used.
	cases := map[string]string{
		"gpt-4o-2024-08-06":          "gpt-4o", // not the shorter gpt-4 prefix
		"openai/gpt-4o":              "gpt-4o",
		"GPT-4o":                     "gpt-4o",
		"gpt-4-0613":                 "gpt-4",
		"claude-3-5-sonnet-20241022": "claude-3-5-sonnet",
		"anthropic.claude-3-sonnet-20240229-v1:0": "claude-3-sonnet",
		"gpt-3.5-turbo-0125":                      "gpt-3.5-turbo",
		"gemini-1.5-pro-latest":                   "gemini-1.5-pro",
		"together_ai/meta/llama-3-70b":            "llama-3-70b",
	}
	for model, base := range cases {
		mc, ok := c.costMap.Lookup(base)
		if !ok {
			t.Fatalf("base model %s missing", base)
		}
		want := mc.InputCostPer1M + mc.OutputCostPer1M
		b := c.CostBreakdown(model, usage)
		if !approxEqual(b.TotalCostUSD, want, 1e-9) || !b.KnownModel {
			t.Errorf("%s: got %.4f (%s via %s), want %.4f (%s)", model, b.TotalCostUSD, b.PricedAs, b.PricingSource, want, base)
		}
	}
}

func TestUnknownModelUsesConservativeFallback(t *testing.T) {
	c := newTracker()
	b := c.CostBreakdown("totally-new-model-x", &models.Usage{PromptTokens: 1_000_000, CompletionTokens: 1_000_000})
	if b.KnownModel || b.PricingSource != SourceFallback {
		t.Fatalf("expected fallback pricing, got %+v", b)
	}
	if want := DefaultUnknownModelPrice.InputPer1M + DefaultUnknownModelPrice.OutputPer1M; !approxEqual(b.TotalCostUSD, want, 1e-9) {
		t.Fatalf("expected %.2f, got %.4f", want, b.TotalCostUSD)
	}
	before := len(c.prices.Models())
	for i := 0; i < 100; i++ {
		c.prices.Ensure(fmt.Sprintf("attacker-model-%d", i))
	}
	if len(c.prices.Models()) != before {
		t.Fatal("unknown model names must not grow the pricing map")
	}
}

func TestLegacyPricingMapIsPer1KTokens(t *testing.T) {
	c := NewCostTracker(nil, NewPricingMap(), nil)
	got := c.CalculateCost("gpt-4", &models.Usage{PromptTokens: 1000, CompletionTokens: 1000})
	if !approxEqual(got, 0.09, 1e-12) {
		t.Fatalf("gpt-4 1K+1K tokens should cost $0.09, got %v", got)
	}
}

func TestOverridesAndInvalidPrices(t *testing.T) {
	c := newTracker()
	if !c.SetModelPrice("gpt-4o", 1, 2) {
		t.Fatal("valid override rejected")
	}
	if c.SetModelPrice("x", -1, 2) || c.SetModelPrice("x", math.NaN(), 2) || c.SetUnknownModelPrice(math.Inf(1), 0) {
		t.Fatal("invalid prices must be rejected")
	}
	got := c.EstimateCost("gpt-4o-2024-08-06", &models.Usage{PromptTokens: 1_000_000, CompletionTokens: 1_000_000})
	if !approxEqual(got, 3, 1e-9) {
		t.Fatalf("override not applied: %v", got)
	}
	neg := c.EstimateCost("gpt-4o", &models.Usage{PromptTokens: -100, CompletionTokens: -5})
	if neg != 0 {
		t.Fatalf("negative token counts must not produce negative cost: %v", neg)
	}
	if est := c.EstimateRequestCost("gpt-4o", 1_000_000, 0); !approxEqual(est, 1, 1e-9) {
		t.Fatalf("EstimateRequestCost = %v", est)
	}
}

func TestNormalizeModelName(t *testing.T) {
	cases := map[string]string{
		"gpt-4o-2024-08-06":      "gpt-4o",
		"gpt-4-1106-preview":     "gpt-4",
		"claude-3-opus@20240229": "claude-3-opus",
		"mistral-large-2402":     "mistral-large",
		"command-r-08-2024":      "command-r",
		"azure/gpt-4o":           "gpt-4o",
		"llama-3-8b":             "llama-3-8b",
		"2024-01-01":             "2024-01-01",
		"us.anthropic.claude-3-haiku-20240307-v1:0": "claude-3-haiku",
	}
	for in, want := range cases {
		if got := NormalizeModelName(in); got != want {
			t.Errorf("NormalizeModelName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRecordUsageNoRedisNoOp(t *testing.T) {
	c := NewCostTracker(nil, NewPricingMap(), intelligence.NewModelCostMap())
	err := c.RecordUsage(context.Background(), CostRequest{
		APIKey: "sk-nil-redis",
		Model:  "gpt-4",
		Usage:  &models.Usage{PromptTokens: 10, CompletionTokens: 10},
	})
	if err != nil {
		t.Fatalf("expected nil error without redis, got %v", err)
	}
}

func TestSetBudgetWebhookConfigStoresDispatcher(t *testing.T) {
	c := newTracker()
	fd := &fakeDispatcher{}
	c.SetBudgetWebhookConfig(fd, webhooks.BudgetWebhookConfig{URL: "http://example.com/budget", Timeout: time.Second})
	ctx := context.Background()
	if err := c.SetBudget(ctx, "sk-test-123456", 0.001); err != nil {
		t.Fatal(err)
	}
	_ = c.RecordUsage(ctx, CostRequest{APIKey: "sk-test-123456", Model: "gpt-4", Usage: &models.Usage{PromptTokens: 100, CompletionTokens: 100}})
	if len(fd.events()) == 0 {
		t.Fatal("non-*WebhookDispatcher dispatchers must receive budget events")
	}
}

func runBudgetLifecycle(t *testing.T, c *CostTracker) {
	t.Helper()
	ctx := context.Background()
	fd := &fakeDispatcher{}
	c.SetBudgetWebhookConfig(fd, webhooks.BudgetWebhookConfig{})
	const key = "sk-live-abcdef123456"

	if rem, err := c.CheckBudget(ctx, key, 1); err != nil || rem != 0 {
		t.Fatalf("no budget configured must allow: %v %v", rem, err)
	}
	if err := c.SetBudget(ctx, key, 1.0); err != nil {
		t.Fatal(err)
	}
	if err := c.SetBudget(ctx, key, -1); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("negative budget must be rejected: %v", err)
	}
	// $0.10 per request.
	req := CostRequest{APIKey: key, Model: "m", CostUSD: 0.10}
	for i := 0; i < 7; i++ {
		if err := c.RecordUsage(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	if len(fd.events()) != 0 {
		t.Fatalf("no threshold crossed yet, got %d events", len(fd.events()))
	}
	rem, err := c.CheckBudget(ctx, "Bearer "+key, 0.2)
	if err != nil || !approxEqual(rem, 0.3, 1e-9) {
		t.Fatalf("expected 0.3 remaining, got %v %v", rem, err)
	}
	if _, err := c.CheckBudget(ctx, key, 0.5); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("estimate above remaining must fail: %v", err)
	}
	_ = c.RecordUsage(ctx, req) // 0.8 -> threshold
	evs := fd.events()
	if len(evs) != 1 || evs[0].Type != webhooks.EventBudgetThreshold {
		t.Fatalf("expected one threshold event, got %+v", evs)
	}
	_ = c.RecordUsage(ctx, req)
	_ = c.RecordUsage(ctx, req) // 1.0 -> exceeded
	_ = c.RecordUsage(ctx, req) // 1.1 -> no new event
	_ = c.RecordUsage(ctx, req)
	evs = fd.events()
	if len(evs) != 2 || evs[1].Type != webhooks.EventBudgetExceeded {
		t.Fatalf("expected exactly one exceeded event, got %d: %+v", len(evs), evs)
	}
	for _, ev := range evs {
		for k, v := range ev.Payload {
			if s, ok := v.(string); ok && strings.Contains(s, key) {
				t.Fatalf("payload field %s leaks the raw API key", k)
			}
		}
	}
	if _, err := c.CheckBudget(ctx, key, 0); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("exhausted budget must fail: %v", err)
	}
	st, _ := c.GetBudget(ctx, key)
	if !approxEqual(st.SpendUSD, 1.2, 1e-9) || !st.Exceeded {
		t.Fatalf("unexpected status %+v", st)
	}
	if err := c.DeductBudget(ctx, key, 0.01); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("DeductBudget over limit must report exceeded: %v", err)
	}
	// Raising the limit re-arms thresholds.
	_ = c.SetBudget(ctx, key, 10)
	if _, err := c.CheckBudget(ctx, key, 1); err != nil {
		t.Fatalf("raised budget must allow: %v", err)
	}
	_ = c.ResetSpend(ctx, key)
	st, _ = c.GetBudget(ctx, key)
	if st.SpendUSD != 0 {
		t.Fatalf("spend not reset: %+v", st)
	}
	if err := c.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: math.NaN(), Model: "gpt-4o", Usage: &models.Usage{PromptTokens: 1}}); err != nil {
		t.Fatalf("NaN caller cost must fall back to pricing usage: %v", err)
	}
	if err := c.DeductBudget(ctx, key, -1); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("negative deduction must be rejected: %v", err)
	}
	_ = c.RemoveBudget(ctx, key)
	if _, err := c.CheckBudget(ctx, key, 1e6); err != nil {
		t.Fatalf("removed budget must allow: %v", err)
	}
}

func TestBudgetLifecycleMemory(t *testing.T) {
	runBudgetLifecycle(t, newTracker())
}

func TestBudgetLifecycleRedis(t *testing.T) {
	cm := intelligence.NewModelCostMap()
	cm.LoadFromDefault()
	runBudgetLifecycle(t, NewCostTrackerWithStore(NewRedisBudgetStore(newFakeRedis(), ""), NewPricingMap(), cm))
}

func concurrentExceeded(t *testing.T, c *CostTracker) {
	ctx := context.Background()
	fd := &fakeDispatcher{}
	c.SetBudgetWebhookConfig(fd, webhooks.BudgetWebhookConfig{})
	c.SetAlertThresholds() // only the exceeded event
	const key = "sk-concurrent-000000"
	_ = c.SetBudget(ctx, key, 5)
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = c.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.01})
			}
		}()
	}
	wg.Wait()
	st, err := c.GetBudget(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !approxEqual(st.SpendUSD, 10, 1e-9) {
		t.Fatalf("lost updates: spend %.9f, want 10", st.SpendUSD)
	}
	if n := len(fd.events()); n != 1 {
		t.Fatalf("exceeded webhook must fire exactly once, fired %d times", n)
	}
}

func TestConcurrentSpendIsExactMemory(t *testing.T) { concurrentExceeded(t, newTracker()) }
func TestConcurrentSpendIsExactRedis(t *testing.T) {
	concurrentExceeded(t, NewCostTrackerWithStore(NewRedisBudgetStore(newFakeRedis(), ""), nil, nil))
}

func TestBudgetPeriodRollover(t *testing.T) {
	c := newTracker()
	now := time.Date(2026, 9, 24, 23, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	c.SetBudgetPeriod(PeriodDaily)
	ctx := context.Background()
	_ = c.SetBudget(ctx, "sk-period-key-1", 1)
	_ = c.RecordUsage(ctx, CostRequest{APIKey: "sk-period-key-1", CostUSD: 1})
	if _, err := c.CheckBudget(ctx, "sk-period-key-1", 0); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal("expected exhausted budget")
	}
	now = now.Add(2 * time.Hour) // next UTC day
	if _, err := c.CheckBudget(ctx, "sk-period-key-1", 0.5); err != nil {
		t.Fatalf("budget must reset on a new day: %v", err)
	}
}

func TestTargetedBudgetWebhook(t *testing.T) {
	c := newTracker()
	td := &fakeTargetedDispatcher{}
	c.SetBudgetWebhookConfig(td, webhooks.BudgetWebhookConfig{URL: "https://hooks.example/budget", Secret: "s"})
	var hooked []BudgetEvent
	c.OnBudgetEvent(func(ev BudgetEvent) { hooked = append(hooked, ev) })
	ctx := context.Background()
	_ = c.SetBudget(ctx, "sk-targeted-123456", 0)
	_ = c.RecordUsage(ctx, CostRequest{APIKey: "sk-targeted-123456", CostUSD: 0.5})
	if len(td.targeted) != 1 || td.targeted[0].URL != "https://hooks.example/budget" || td.targeted[0].Secret != "s" {
		t.Fatalf("expected targeted delivery, got %+v", td.targeted)
	}
	if len(hooked) != 1 || !hooked[0].Exceeded || hooked[0].KeyHint != "sk-...3456" {
		t.Fatalf("unexpected hook events %+v", hooked)
	}
}

func TestAppendAndDrainUsage(t *testing.T) {
	for name, c := range map[string]*CostTracker{
		"memory": newTracker(),
		"redis":  NewCostTrackerWithStore(NewRedisBudgetStore(newFakeRedis(), ""), nil, nil),
	} {
		ctx := context.Background()
		if err := c.AppendUsage(ctx, billing.MeterEntry{CustomerID: "c", EventName: "tokens", Value: -1}); !errors.Is(err, billing.ErrInvalidMeterEntry) {
			t.Fatalf("%s: invalid entry accepted: %v", name, err)
		}
		if err := c.AppendUsage(ctx, billing.MeterEntry{CustomerID: "c", EventName: "tokens", Value: 3}, billing.MeterEntry{CustomerID: "c", EventName: "tokens", Value: 4}); err != nil {
			t.Fatal(err)
		}
		got, err := c.DrainUsage(ctx, "c", 10)
		if err != nil || len(got) != 2 || got[0].Value != 3 || got[0].Timestamp.IsZero() {
			t.Fatalf("%s: drain returned %+v %v", name, got, err)
		}
		if again, _ := c.DrainUsage(ctx, "c", 10); len(again) != 0 {
			t.Fatalf("%s: drain must remove entries", name)
		}
	}
}

func TestKeyHintAndBudgetID(t *testing.T) {
	if KeyHint("sk-abcdefghijkl") != "sk-...ijkl" || KeyHint("short") != "***" {
		t.Fatal("unexpected key hints")
	}
	if BudgetID("Bearer sk-x-123456789") != BudgetID("sk-x-123456789") {
		t.Fatal("Bearer prefix must be ignored")
	}
	if strings.Contains(BudgetID("sk-x-123456789"), "sk-") {
		t.Fatal("budget id must not contain the key")
	}
}

// ---------------------------------------------------------------------------
// fakeRedis emulates the handful of commands used by redisBudgetStore,
// including the addSpend Lua script.
// ---------------------------------------------------------------------------

type fakeRedis struct {
	mu    sync.Mutex
	kv    map[string]string
	lists map[string][]string
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{kv: map[string]string{}, lists: map[string][]string{}}
}

func (f *fakeRedis) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	if script != addSpendScript {
		cmd.SetErr(errors.New("unknown script"))
		return cmd
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, _ := strconv.ParseInt(f.kv[keys[0]], 10, 64)
	delta := args[0].(int64)
	cur += delta
	f.kv[keys[0]] = strconv.FormatInt(cur, 10)
	var limit interface{}
	if v, ok := f.kv[keys[1]]; ok {
		limit = v
	}
	cmd.SetVal([]interface{}{cur, limit})
	return cmd
}

func (f *fakeRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewStringCmd(ctx)
	if v, ok := f.kv[key]; ok {
		cmd.SetVal(v)
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}

func (f *fakeRedis) Set(ctx context.Context, key string, value interface{}, _ time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kv[key] = fmt.Sprint(value)
	return redis.NewStatusCmd(ctx)
}

func (f *fakeRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.kv, k)
		delete(f.lists, k)
	}
	return redis.NewIntCmd(ctx)
}

func (f *fakeRedis) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, v := range values {
		switch b := v.(type) {
		case []byte:
			f.lists[key] = append(f.lists[key], string(b))
		default:
			f.lists[key] = append(f.lists[key], fmt.Sprint(b))
		}
	}
	return redis.NewIntCmd(ctx)
}

func (f *fakeRedis) Expire(ctx context.Context, key string, _ time.Duration) *redis.BoolCmd {
	return redis.NewBoolCmd(ctx)
}

func (f *fakeRedis) LPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewStringSliceCmd(ctx)
	l := f.lists[key]
	if len(l) == 0 {
		cmd.SetErr(redis.Nil)
		return cmd
	}
	if count > len(l) {
		count = len(l)
	}
	cmd.SetVal(append([]string(nil), l[:count]...))
	f.lists[key] = l[count:]
	return cmd
}
