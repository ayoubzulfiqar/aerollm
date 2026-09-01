package finops

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
	"github.com/redis/go-redis/v9"
)

// Pricing defines per-model token prices in USD.
type Pricing struct {
	PromptPrice     float64
	CompletionPrice float64
}

// PricingMap holds pricing for models.
type PricingMap struct {
	mu     sync.RWMutex
	prices map[string]Pricing
}

// NewPricingMap creates a new pricing map with defaults.
func NewPricingMap() *PricingMap {
	p := &PricingMap{prices: make(map[string]Pricing)}
	p.SetDefault("gpt-4", Pricing{PromptPrice: 0.03, CompletionPrice: 0.06})
	p.SetDefault("gpt-3.5-turbo", Pricing{PromptPrice: 0.0015, CompletionPrice: 0.002})
	p.SetDefault("claude-3-opus", Pricing{PromptPrice: 0.015, CompletionPrice: 0.075})
	p.SetDefault("claude-3-sonnet", Pricing{PromptPrice: 0.003, CompletionPrice: 0.015})
	return p
}

// SetDefault sets pricing if not already defined.
func (p *PricingMap) SetDefault(model string, pricing Pricing) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.prices[model]; !exists {
		p.prices[model] = pricing
	}
}

// Get returns pricing for a model.
func (p *PricingMap) Get(model string) (Pricing, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pr, ok := p.prices[model]
	return pr, ok
}

// Ensure ensures pricing exists for a model, using a fallback if needed.
func (p *PricingMap) Ensure(model string) Pricing {
	if pr, ok := p.Get(model); ok {
		return pr
	}
	fallback := Pricing{PromptPrice: 0.01, CompletionPrice: 0.02}
	p.SetDefault(model, fallback)
	return fallback
}

// CostRequest represents a billable request.
type CostRequest struct {
	RequestID      string
	APIKey         string
	Model          string
	Usage          *models.Usage
	Timestamp      time.Time
	CostUSD        float64
	BudgetRemaining float64
}

// CostTracker tracks costs and budgets in Redis.
type CostTracker struct {
	redis         *redis.Client
	prices        *PricingMap
	dispatcher    dispatcherInterface
	budgetWebhook webhooks.BudgetWebhookConfig
	webhookMu     sync.RWMutex
}

// NewCostTracker creates a new cost tracker.
func NewCostTracker(redisClient *redis.Client, prices *PricingMap) *CostTracker {
	return &CostTracker{redis: redisClient, prices: prices}
}

// CalculateCost computes cost in USD for a request.
func (c *CostTracker) CalculateCost(model string, usage *models.Usage) float64 {
	if usage == nil {
		return 0
	}
	pricing := c.prices.Ensure(model)
	return float64(usage.PromptTokens)*pricing.PromptPrice + float64(usage.CompletionTokens)*pricing.CompletionPrice
}

// BudgetKey returns the Redis key for API key budget state.
func (c *CostTracker) BudgetKey(apiKey string) string {
	return fmt.Sprintf("budget:%s", apiKey)
}

// CheckBudget checks whether the API key has remaining budget.
func (c *CostTracker) CheckBudget(ctx context.Context, apiKey string, estimatedCost float64) (float64, error) {
	if c.redis == nil {
		return 0, nil
	}
	key := c.BudgetKey(apiKey)
	val, err := c.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var remaining float64
	if _, err := fmt.Sscanf(val, "%f", &remaining); err != nil {
		return 0, err
	}
	if remaining < estimatedCost {
		return remaining, fmt.Errorf("budget exceeded: remaining %.4f USD, need %.4f USD", remaining, estimatedCost)
	}
	return remaining, nil
}

// DeductBudget subtracts cost from API key budget.
func (c *CostTracker) DeductBudget(ctx context.Context, apiKey string, cost float64) error {
	if c.redis == nil {
		return nil
	}
	key := c.BudgetKey(apiKey)
	_, err := c.redis.Eval(ctx, `
		local current = tonumber(redis.call("GET", KEYS[1]) or "999999")
		local cost = tonumber(ARGV[1])
		if current <= 0 then
			return -1
		end
		local new = math.max(0, current - cost)
		redis.call("SET", KEYS[1], tostring(new))
		return new
	`, []string{key}, cost).Result()
	if err != nil && strings.Contains(err.Error(), "budget exceeded") {
		return err
	}
	return nil
}

// RecordUsage persists usage and cost for an API key.
func (c *CostTracker) RecordUsage(ctx context.Context, req CostRequest) error {
	if c.redis == nil {
		return nil
	}
	cost := c.CalculateCost(req.Model, req.Usage)
	req.CostUSD = cost
	err := c.DeductBudget(ctx, req.APIKey, cost)
	if err != nil && strings.Contains(err.Error(), "budget exceeded") {
		remaining := float64(0)
		if req.Usage != nil {
			remaining = c.CalculateCost(req.Model, req.Usage)
		}
		c.webhookMu.RLock()
		cfg := c.budgetWebhook
		d := c.dispatcher
		c.webhookMu.RUnlock()
		if d != nil && cfg.URL != "" {
			d.DispatchAsync(ctx, webhooks.Event{
				ID:        fmt.Sprintf("budget-%s-%d", req.APIKey, time.Now().UnixNano()),
				Type:      webhooks.EventBudgetExceeded,
				Timestamp: time.Now(),
				Payload: map[string]interface{}{
					"api_key":       req.APIKey,
					"model":         req.Model,
					"remaining_usd": remaining,
				},
			})
		}
	}
	return err
}

type dispatcherInterface interface {
	DispatchAsync(ctx context.Context, event webhooks.Event)
}

// SetBudgetWebhookConfig configures the webhook target for budget exceeded events.
func (c *CostTracker) SetBudgetWebhookConfig(dispatcher dispatcherInterface, cfg webhooks.BudgetWebhookConfig) {
	c.webhookMu.Lock()
	defer c.webhookMu.Unlock()
	c.budgetWebhook = cfg
	if d, ok := dispatcher.(*webhooks.WebhookDispatcher); ok {
		c.dispatcher = d
	}
}
