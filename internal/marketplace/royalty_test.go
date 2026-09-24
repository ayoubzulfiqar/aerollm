package marketplace

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
)

type recordingDispatcher struct {
	mu         sync.Mutex
	events     []webhooks.Event
	registered map[webhooks.EventType][]webhooks.WebhookConfig
	ctxErr     []error
}

func (d *recordingDispatcher) DispatchAsync(ctx context.Context, ev webhooks.Event) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, ev)
	d.ctxErr = append(d.ctxErr, ctx.Err())
}

func (d *recordingDispatcher) Register(t webhooks.EventType, cfg webhooks.WebhookConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.registered == nil {
		d.registered = map[webhooks.EventType][]webhooks.WebhookConfig{}
	}
	d.registered[t] = append(d.registered[t], cfg)
}

func TestUSDToMicros(t *testing.T) {
	cases := []struct {
		in   float64
		want int64
		ok   bool
	}{
		{0, 0, true},
		{0.1 + 0.2, 300_000, true}, // float noise is rounded away
		{1.5, 1_500_000, true},
		{0.0000004, 0, true},
		{0.0000006, 1, true},
		{-0.01, 0, false},
		{math.NaN(), 0, false},
		{math.Inf(1), 0, false},
		{1e13, 0, false},
	}
	for _, c := range cases {
		got, err := USDToMicros(c.in)
		if (err == nil) != c.ok || (c.ok && got != c.want) {
			t.Errorf("USDToMicros(%v)=%d,%v want %d ok=%v", c.in, got, err, c.want, c.ok)
		}
	}
}

func TestRoyaltyRecorderPrecisionIdempotencyAndValidation(t *testing.T) {
	d := &recordingDispatcher{}
	r := NewRoyaltyRecorder(d, webhooks.BudgetWebhookConfig{URL: "https://hooks.example/royalty"})
	if len(d.registered[RoyaltyEventType]) != 1 {
		t.Fatalf("expected webhook URL to be registered with the dispatcher")
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Summing 0.1 ten times in float64 is not 1.0; micro-units are exact.
	for i := 0; i < 10; i++ {
		r.RecordUsage(ctx, finops.CostRequest{RequestID: "req-" + string(rune('a'+i)), Model: "plugin-x", APIKey: "sk-secret-123", CostUSD: 0.1}, "creator-1")
	}
	cancel()
	if got := r.CreatorTotalMicros("creator-1"); got != 1_000_000 {
		t.Fatalf("expected exactly 1_000_000 micros, got %d", got)
	}
	if got := r.PluginTotalMicros("plugin-x"); got != 1_000_000 {
		t.Fatalf("plugin total %d", got)
	}

	// Replaying the same request is a no-op.
	r.RecordUsage(context.Background(), finops.CostRequest{RequestID: "req-a", Model: "plugin-x", CostUSD: 5}, "creator-1")
	if err := r.Record(context.Background(), RoyaltyEvent{PluginID: "plugin-x", CreatorID: "creator-1", RequestID: "req-a", CostMicros: 5}); !errors.Is(err, ErrDuplicateRoyalty) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
	if got := r.CreatorTotalMicros("creator-1"); got != 1_000_000 {
		t.Fatalf("duplicate changed total: %d", got)
	}

	for name, ev := range map[string]RoyaltyEvent{
		"negative micros": {PluginID: "p", CreatorID: "c", RequestID: "r1", CostMicros: -1},
		"negative usd":    {PluginID: "p", CreatorID: "c", RequestID: "r2", CostUSD: -1},
		"nan usd":         {PluginID: "p", CreatorID: "c", RequestID: "r3", CostUSD: math.NaN()},
		"missing request": {PluginID: "p", CreatorID: "c"},
		"missing creator": {PluginID: "p", RequestID: "r4"},
	} {
		if err := r.Record(context.Background(), ev); !errors.Is(err, ErrInvalidRoyalty) {
			t.Errorf("%s: expected ErrInvalidRoyalty, got %v", name, err)
		}
	}

	snap := r.Snapshot()
	if len(snap) != 10 {
		t.Fatalf("expected 10 events, got %d", len(snap))
	}
	for _, ev := range snap {
		if strings.Contains(ev.APIKey, "secret") || !strings.HasPrefix(ev.APIKey, "key_") {
			t.Fatalf("raw API key retained: %q", ev.APIKey)
		}
		if ev.CostMicros != 100_000 || ev.CostUSD != 0.1 {
			t.Fatalf("unexpected amounts: %+v", ev)
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.events) != 10 {
		t.Fatalf("expected 10 webhook dispatches, got %d", len(d.events))
	}
	for _, err := range d.ctxErr {
		if err != nil {
			t.Fatalf("webhook dispatched with a cancelled context: %v", err)
		}
	}
	if d.events[0].ID == d.events[1].ID || !strings.HasPrefix(d.events[0].ID, "royalty-") {
		t.Fatalf("unexpected event ids %q %q", d.events[0].ID, d.events[1].ID)
	}
}

func TestRoyaltyRecorderOverflowAndEviction(t *testing.T) {
	r := NewRoyaltyRecorder(nil, webhooks.BudgetWebhookConfig{})
	if err := r.Record(context.Background(), RoyaltyEvent{PluginID: "p", CreatorID: "c", RequestID: "r1", CostMicros: math.MaxInt64}); err != nil {
		t.Fatal(err)
	}
	if err := r.Record(context.Background(), RoyaltyEvent{PluginID: "p", CreatorID: "c", RequestID: "r2", CostMicros: 1}); !errors.Is(err, ErrRoyaltyOverflow) {
		t.Fatalf("expected overflow error, got %v", err)
	}

	r2 := NewRoyaltyRecorder(nil, webhooks.BudgetWebhookConfig{})
	r2.SetMaxEvents(3)
	for i := 0; i < 10; i++ {
		if err := r2.Record(context.Background(), RoyaltyEvent{PluginID: "p", CreatorID: "c", RequestID: string(rune('a' + i)), CostMicros: 7}); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(r2.Snapshot()); n != 3 {
		t.Fatalf("expected 3 retained events, got %d", n)
	}
	if got := r2.CreatorTotalMicros("c"); got != 70 {
		t.Fatalf("totals must survive eviction: %d", got)
	}
}

func TestRoyaltyRecorderConcurrent(t *testing.T) {
	r := NewRoyaltyRecorder(nil, webhooks.BudgetWebhookConfig{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				// Every worker records the same 200 request IDs: exactly one wins each.
				_ = r.Record(context.Background(), RoyaltyEvent{PluginID: "p", CreatorID: "c", RequestID: string(rune(0x100 + i)), CostMicros: 1})
				_ = r.Snapshot()
			}
		}()
	}
	wg.Wait()
	if got := r.CreatorTotalMicros("c"); got != 200 {
		t.Fatalf("expected 200, got %d", got)
	}
}
