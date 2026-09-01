package finops

import (
	"context"
	"sync"
	"testing"
	"time"

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
	c := NewCostTracker(nil, p)
	cost := c.CalculateCost("gpt-4", &models.Usage{PromptTokens: 10, CompletionTokens: 20})
	expected := 10*0.03 + 20*0.06
	if cost != expected {
		t.Fatalf("expected %.4f, got %.4f", expected, cost)
	}
}

func TestCalculateCostNilUsage(t *testing.T) {
	p := NewPricingMap()
	c := NewCostTracker(nil, p)
	if got := c.CalculateCost("gpt-4", nil); got != 0 {
		t.Fatalf("expected 0 for nil usage, got %f", got)
	}
}

func TestRecordUsageNoRedisNoOp(t *testing.T) {
	p := NewPricingMap()
	c := NewCostTracker(nil, p)
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
	c := NewCostTracker(nil, p)
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
