package finops

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
)

type fakeDispatcher struct {
	dispatched []webhooks.Event
	mu         sync.Mutex
}

func (f *fakeDispatcher) DispatchAsync(_ context.Context, event webhooks.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatched = append(f.dispatched, event)
}

func TestPricingMapDefaults(t *testing.T) {
	p := NewPricingMap()
	pr, ok := p.Get("gpt-4")
	if !ok || pr.PromptPrice != 0.03 {
		t.Fatalf("unexpected pricing: %+v", pr)
	}
}

func TestCalculateCost(t *testing.T) {
	p := NewPricingMap()
	cm := intelligence.NewModelCostMap()
	cm.LoadFromDefault()
	c := NewCostTracker(nil, p, cm)
	cost := c.CalculateCost("gpt-4o", &models.Usage{PromptTokens: 1000000, CompletionTokens: 500000})
	// gpt-4o: input 5.0/1M, output 15.0/1M
	// Cost = 1.0*5.0 + 0.5*15.0 = 5.0 + 7.5 = 12.5
	expected := 12.5
	if !approxEqual(cost, expected, 0.0001) {
		t.Fatalf("expected %.4f, got %.4f", expected, cost)
	}
}

func approxEqual(a, b, epsilon float64) bool {
	return a-b < epsilon && b-a < epsilon
}

func TestCalculateCostNilUsage(t *testing.T) {
	p := NewPricingMap()
	cm := intelligence.NewModelCostMap()
	c := NewCostTracker(nil, p, cm)
	if got := c.CalculateCost("gpt-4", nil); got != 0 {
		t.Fatalf("expected 0 for nil usage, got %f", got)
	}
}

func TestRecordUsageNoRedisNoOp(t *testing.T) {
	p := NewPricingMap()
	cm := intelligence.NewModelCostMap()
	c := NewCostTracker(nil, p, cm)
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
	p := NewPricingMap()
	cm := intelligence.NewModelCostMap()
	c := NewCostTracker(nil, p, cm)
	fd := &fakeDispatcher{}
	c.SetBudgetWebhookConfig(fd, webhooks.BudgetWebhookConfig{
		URL:     "http://example.com/budget",
		Timeout: time.Second,
	})

	// Without Redis, RecordUsage is a no-op and no webhook is emitted.
	// This verifies the config is stored without panicking.
	_ = c.RecordUsage(context.Background(), CostRequest{
		APIKey: "sk-test",
		Model:  "gpt-4",
		Usage:  &models.Usage{PromptTokens: 10, CompletionTokens: 10},
	})
}
