package finops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
	"github.com/redis/go-redis/v9"
)

// Pricing defines per-model token prices in USD per 1K tokens.
type Pricing struct {
	PromptPrice     float64
	CompletionPrice float64
}

// PricingMap holds pricing for models (USD per 1K tokens).
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
	if !validPrice(pricing.PromptPrice, pricing.CompletionPrice) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.prices[model]; !exists {
		p.prices[model] = pricing
	}
}

// Set sets (or replaces) pricing for a model. Invalid prices are ignored.
func (p *PricingMap) Set(model string, pricing Pricing) bool {
	if model == "" || !validPrice(pricing.PromptPrice, pricing.CompletionPrice) {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prices[model] = pricing
	return true
}

// Get returns pricing for a model.
func (p *PricingMap) Get(model string) (Pricing, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pr, ok := p.prices[model]
	return pr, ok
}

// fallbackPricing is returned by Ensure for unknown models (USD per 1K).
var fallbackPricing = Pricing{PromptPrice: 0.01, CompletionPrice: 0.03}

// Ensure returns pricing for a model, or the fallback for unknown models.
// Unknown model names are no longer inserted into the map: model names come
// from client requests and would otherwise grow the map without bound.
func (p *PricingMap) Ensure(model string) Pricing {
	if pr, ok := p.Get(model); ok {
		return pr
	}
	return fallbackPricing
}

// Models returns all registered model pricing keys, sorted.
func (p *PricingMap) Models() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.prices))
	for model := range p.prices {
		out = append(out, model)
	}
	sort.Strings(out)
	return out
}

// CostRequest represents a billable request.
type CostRequest struct {
	RequestID string
	APIKey    string
	Model     string
	Usage     *models.Usage
	Timestamp time.Time
	// CostUSD, when > 0, is used as-is instead of pricing Usage.
	CostUSD         float64
	BudgetRemaining float64
}

// ErrBudgetExceeded is returned (wrapped) by CheckBudget and DeductBudget
// when the key has no remaining budget. Use errors.Is to detect it; other
// errors indicate a storage failure.
var ErrBudgetExceeded = errors.New("budget exceeded")

// EventDispatcher delivers webhook events (implemented by
// *webhooks.WebhookDispatcher and *webhooks.QueueDispatcher).
type EventDispatcher interface {
	DispatchAsync(ctx context.Context, event webhooks.Event)
}

// dispatcherInterface is kept as an alias for backward compatibility.
type dispatcherInterface = EventDispatcher

// targetedDispatcher can deliver to an explicit endpoint.
type targetedDispatcher interface {
	DispatchToAsync(ctx context.Context, cfg webhooks.WebhookConfig, event webhooks.Event)
}

// BudgetEvent describes a budget threshold crossing.
type BudgetEvent struct {
	BudgetID   string       `json:"budget_id"`
	KeyHint    string       `json:"api_key_hint"`
	Model      string       `json:"model,omitempty"`
	Threshold  float64      `json:"threshold"`
	SpendUSD   float64      `json:"spend_usd"`
	LimitUSD   float64      `json:"limit_usd"`
	Remaining  float64      `json:"remaining_usd"`
	Period     BudgetPeriod `json:"period,omitempty"`
	PeriodKey  string       `json:"period_key"`
	Exceeded   bool         `json:"exceeded"`
	OccurredAt time.Time    `json:"occurred_at"`
}

// BudgetStatus is the current state of an API key's budget.
type BudgetStatus struct {
	BudgetID     string       `json:"budget_id"`
	HasLimit     bool         `json:"has_limit"`
	LimitUSD     float64      `json:"limit_usd"`
	SpendUSD     float64      `json:"spend_usd"`
	RemainingUSD float64      `json:"remaining_usd"`
	Period       BudgetPeriod `json:"period,omitempty"`
	PeriodKey    string       `json:"period_key"`
	Exceeded     bool         `json:"exceeded"`
}

// UsageResult is returned by Record.
type UsageResult struct {
	Cost      CostBreakdown `json:"cost"`
	CostUSD   float64       `json:"cost_usd"`
	Budget    BudgetStatus  `json:"budget"`
	Crossings []BudgetEvent `json:"crossings,omitempty"`
}

// CostTracker prices requests and enforces per-API-key budgets. Budget
// state lives in a BudgetStore (Redis when available, otherwise memory);
// all spend accounting uses atomic integer nano-dollar increments.
type CostTracker struct {
	store   BudgetStore
	prices  *PricingMap
	costMap *intelligence.ModelCostMap

	priceMu      sync.RWMutex
	overrides    map[string]ModelPrice
	unknownPrice ModelPrice

	cfgMu      sync.RWMutex
	thresholds []float64
	period     BudgetPeriod

	webhookMu     sync.RWMutex
	dispatcher    EventDispatcher
	budgetWebhook webhooks.BudgetWebhookConfig
	onEvent       func(BudgetEvent)

	now func() time.Time
}

// NewCostTracker creates a cost tracker. With a nil Redis client budgets are
// tracked in process memory (single instance only).
func NewCostTracker(redisClient *redis.Client, prices *PricingMap, costMap *intelligence.ModelCostMap) *CostTracker {
	var store BudgetStore
	if redisClient != nil {
		store = NewRedisBudgetStore(redisClient, "")
	}
	return NewCostTrackerWithStore(store, prices, costMap)
}

// NewCostTrackerWithStore creates a cost tracker over an explicit store
// (nil = in-memory).
func NewCostTrackerWithStore(store BudgetStore, prices *PricingMap, costMap *intelligence.ModelCostMap) *CostTracker {
	if store == nil {
		store = NewMemoryBudgetStore()
	}
	if prices == nil {
		prices = NewPricingMap()
	}
	return &CostTracker{
		store:        store,
		prices:       prices,
		costMap:      costMap,
		overrides:    map[string]ModelPrice{},
		unknownPrice: DefaultUnknownModelPrice,
		thresholds:   []float64{0.8},
		now:          time.Now,
	}
}

// SetBudgetPeriod configures automatic spend resets (default lifetime).
func (c *CostTracker) SetBudgetPeriod(p BudgetPeriod) {
	c.cfgMu.Lock()
	defer c.cfgMu.Unlock()
	c.period = p
}

// SetAlertThresholds sets the fractions of the limit (0 < t < 1) at which a
// budget_threshold_reached event fires once per crossing. Exceeding the
// full limit always fires budget_exceeded.
func (c *CostTracker) SetAlertThresholds(fractions ...float64) {
	out := make([]float64, 0, len(fractions))
	for _, f := range fractions {
		if f > 0 && f < 1 && !math.IsNaN(f) {
			out = append(out, f)
		}
	}
	sort.Float64s(out)
	c.cfgMu.Lock()
	defer c.cfgMu.Unlock()
	c.thresholds = out
}

// OnBudgetEvent registers a callback invoked synchronously for every
// threshold crossing (in addition to the webhook).
func (c *CostTracker) OnBudgetEvent(fn func(BudgetEvent)) {
	c.webhookMu.Lock()
	defer c.webhookMu.Unlock()
	c.onEvent = fn
}

// normalizeAPIKey strips an "Authorization: Bearer" prefix.
func normalizeAPIKey(apiKey string) string {
	k := strings.TrimSpace(apiKey)
	if len(k) > 7 && strings.EqualFold(k[:7], "bearer ") {
		k = strings.TrimSpace(k[7:])
	}
	return k
}

// BudgetID returns the non-reversible identifier under which an API key's
// budget is stored. Raw API keys are never written to the store.
func BudgetID(apiKey string) string {
	sum := sha256.Sum256([]byte("aerollm-budget\x00" + normalizeAPIKey(apiKey)))
	return hex.EncodeToString(sum[:16])
}

// KeyHint returns a redacted form of an API key safe for logs/webhooks
// ("sk-...wxyz").
func KeyHint(apiKey string) string {
	k := normalizeAPIKey(apiKey)
	if len(k) <= 8 {
		return "***"
	}
	prefix := k[:3]
	return prefix + "..." + k[len(k)-4:]
}

// BudgetKey returns the storage key suffix identifying the API key's budget.
func (c *CostTracker) BudgetKey(apiKey string) string {
	return fmt.Sprintf("budget:%s", BudgetID(apiKey))
}

func (c *CostTracker) currentPeriod() (BudgetPeriod, string, time.Duration) {
	c.cfgMu.RLock()
	p := c.period
	c.cfgMu.RUnlock()
	key, retain := periodKey(p, c.now())
	return p, key, retain
}

// SetBudget sets the spend limit (USD) for an API key.
func (c *CostTracker) SetBudget(ctx context.Context, apiKey string, limitUSD float64) error {
	if normalizeAPIKey(apiKey) == "" {
		return errors.New("finops: empty api key")
	}
	n, err := toNanos(limitUSD)
	if err != nil {
		return err
	}
	return c.store.SetLimit(ctx, BudgetID(apiKey), n)
}

// RemoveBudget removes the limit for an API key (spend is kept).
func (c *CostTracker) RemoveBudget(ctx context.Context, apiKey string) error {
	return c.store.ClearLimit(ctx, BudgetID(apiKey))
}

// ResetSpend zeroes the current period's spend for an API key.
func (c *CostTracker) ResetSpend(ctx context.Context, apiKey string) error {
	_, pk, _ := c.currentPeriod()
	return c.store.ResetSpend(ctx, BudgetID(apiKey), pk)
}

// GetBudget returns the current budget status for an API key.
func (c *CostTracker) GetBudget(ctx context.Context, apiKey string) (BudgetStatus, error) {
	p, pk, _ := c.currentPeriod()
	id := BudgetID(apiKey)
	st, err := c.store.Get(ctx, id, pk)
	if err != nil {
		return BudgetStatus{}, err
	}
	return statusFrom(id, p, pk, st.SpendNanos, st.LimitNanos, st.HasLimit), nil
}

func statusFrom(id string, p BudgetPeriod, pk string, spend, limit int64, hasLimit bool) BudgetStatus {
	bs := BudgetStatus{BudgetID: id, HasLimit: hasLimit, SpendUSD: fromNanos(spend), Period: p, PeriodKey: pk}
	if hasLimit {
		bs.LimitUSD = fromNanos(limit)
		bs.RemainingUSD = fromNanos(limit - spend)
		bs.Exceeded = spend >= limit
	}
	return bs
}

// CheckBudget checks whether the API key has at least estimatedCost USD of
// budget left. It returns (0, nil) when no budget is configured, the
// remaining amount otherwise, and an error wrapping ErrBudgetExceeded when
// the budget is exhausted or insufficient. Storage failures are returned as
// other errors so callers can choose to fail open or closed.
func (c *CostTracker) CheckBudget(ctx context.Context, apiKey string, estimatedCost float64) (float64, error) {
	if normalizeAPIKey(apiKey) == "" {
		return 0, nil
	}
	if math.IsNaN(estimatedCost) || estimatedCost < 0 {
		estimatedCost = 0
	}
	bs, err := c.GetBudget(ctx, apiKey)
	if err != nil {
		return 0, err
	}
	if !bs.HasLimit {
		return 0, nil
	}
	if bs.RemainingUSD <= 0 || bs.RemainingUSD < estimatedCost {
		return math.Max(bs.RemainingUSD, 0), fmt.Errorf("%w: remaining %.6f USD, need %.6f USD", ErrBudgetExceeded, math.Max(bs.RemainingUSD, 0), estimatedCost)
	}
	return bs.RemainingUSD, nil
}

// DeductBudget adds cost (USD) to the API key's spend. It returns an error
// wrapping ErrBudgetExceeded when the resulting spend is at or over the
// limit (the spend is still recorded: the work has already happened).
func (c *CostTracker) DeductBudget(ctx context.Context, apiKey string, cost float64) error {
	res, err := c.recordCost(ctx, apiKey, "", cost, CostBreakdown{TotalCostUSD: cost})
	if err != nil {
		return err
	}
	if res.Budget.HasLimit && res.Budget.Exceeded {
		return fmt.Errorf("%w: spend %.6f USD of %.6f USD", ErrBudgetExceeded, res.Budget.SpendUSD, res.Budget.LimitUSD)
	}
	return nil
}

// RecordUsage prices and records one completed request against the API
// key's budget and fires budget webhooks for threshold crossings. It returns
// an error only when the cost is invalid or the store fails; exceeding the
// budget is reported through webhooks and subsequent CheckBudget calls.
func (c *CostTracker) RecordUsage(ctx context.Context, req CostRequest) error {
	_, err := c.Record(ctx, req)
	return err
}

// Record is RecordUsage returning the priced cost and resulting budget state.
func (c *CostTracker) Record(ctx context.Context, req CostRequest) (UsageResult, error) {
	var breakdown CostBreakdown
	cost := req.CostUSD
	if cost > 0 && !math.IsInf(cost, 0) {
		breakdown = CostBreakdown{Model: req.Model, TotalCostUSD: cost, PricingSource: "caller"}
	} else {
		breakdown = c.CostBreakdown(req.Model, req.Usage)
		cost = breakdown.TotalCostUSD
	}
	return c.recordCost(ctx, req.APIKey, req.Model, cost, breakdown)
}

func (c *CostTracker) recordCost(ctx context.Context, apiKey, model string, cost float64, breakdown CostBreakdown) (UsageResult, error) {
	res := UsageResult{Cost: breakdown, CostUSD: cost}
	nanos, err := toNanos(cost)
	if err != nil {
		return res, err
	}
	if normalizeAPIKey(apiKey) == "" {
		return res, nil
	}
	p, pk, retain := c.currentPeriod()
	id := BudgetID(apiKey)
	if nanos == 0 {
		st, err := c.store.Get(ctx, id, pk)
		if err != nil {
			return res, err
		}
		res.Budget = statusFrom(id, p, pk, st.SpendNanos, st.LimitNanos, st.HasLimit)
		return res, nil
	}
	newSpend, limit, hasLimit, err := c.store.AddSpend(ctx, id, pk, nanos, retain)
	if err != nil {
		return res, err
	}
	res.Budget = statusFrom(id, p, pk, newSpend, limit, hasLimit)
	if hasLimit {
		res.Crossings = c.crossings(id, KeyHint(apiKey), model, p, pk, newSpend-nanos, newSpend, limit)
		for _, ev := range res.Crossings {
			c.emit(ctx, ev)
		}
	}
	return res, nil
}

// crossings returns the thresholds crossed by moving spend from prev to cur.
// Because the store increment is atomic, each crossing is observed by
// exactly one caller, even across gateway instances.
func (c *CostTracker) crossings(id, hint, model string, p BudgetPeriod, pk string, prev, cur, limit int64) []BudgetEvent {
	c.cfgMu.RLock()
	fractions := append(append([]float64(nil), c.thresholds...), 1.0)
	c.cfgMu.RUnlock()
	var out []BudgetEvent
	now := c.now().UTC()
	for _, f := range fractions {
		mark := int64(math.Ceil(float64(limit) * f))
		if f == 1.0 {
			mark = limit
		}
		crossed := prev < mark && cur >= mark
		if limit == 0 {
			// A zero budget is "crossed" by the first spend.
			crossed = f == 1.0 && prev == 0 && cur > 0
		}
		if crossed {
			out = append(out, BudgetEvent{
				BudgetID:   id,
				KeyHint:    hint,
				Model:      model,
				Threshold:  f,
				SpendUSD:   fromNanos(cur),
				LimitUSD:   fromNanos(limit),
				Remaining:  fromNanos(limit - cur),
				Period:     p,
				PeriodKey:  pk,
				Exceeded:   f == 1.0,
				OccurredAt: now,
			})
		}
	}
	return out
}

func (c *CostTracker) emit(ctx context.Context, ev BudgetEvent) {
	c.webhookMu.RLock()
	d, cfg, hook := c.dispatcher, c.budgetWebhook, c.onEvent
	c.webhookMu.RUnlock()
	if hook != nil {
		hook(ev)
	}
	if d == nil {
		return
	}
	typ := webhooks.EventBudgetThreshold
	if ev.Exceeded {
		typ = webhooks.EventBudgetExceeded
	}
	event := webhooks.Event{
		// Deterministic ID lets receivers de-duplicate redeliveries.
		ID:        fmt.Sprintf("budget-%s-%s-%g-%d", ev.BudgetID, ev.PeriodKey, ev.Threshold, int64(math.Round(ev.LimitUSD*nanosPerUSD))),
		Type:      typ,
		Timestamp: ev.OccurredAt,
		Payload: map[string]interface{}{
			"budget_id":     ev.BudgetID,
			"api_key_hint":  ev.KeyHint,
			"model":         ev.Model,
			"threshold":     ev.Threshold,
			"spend_usd":     ev.SpendUSD,
			"limit_usd":     ev.LimitUSD,
			"remaining_usd": ev.Remaining,
			"period":        string(ev.Period),
			"period_key":    ev.PeriodKey,
		},
	}
	if td, ok := d.(targetedDispatcher); ok && cfg.URL != "" {
		td.DispatchToAsync(ctx, cfg.WebhookConfig(), event)
		return
	}
	d.DispatchAsync(ctx, event)
}

// SetBudgetWebhookConfig configures the dispatcher for budget events. When
// cfg.URL is set and the dispatcher supports targeted delivery
// (*webhooks.WebhookDispatcher does), events go to cfg.URL; otherwise they
// go to the webhooks registered for EventBudgetExceeded /
// EventBudgetThreshold.
func (c *CostTracker) SetBudgetWebhookConfig(dispatcher dispatcherInterface, cfg webhooks.BudgetWebhookConfig) {
	c.webhookMu.Lock()
	defer c.webhookMu.Unlock()
	c.budgetWebhook = cfg
	c.dispatcher = dispatcher
}

// AppendUsage stores usage entries for downstream billing consumers.
// Entries must have a customer ID, an event name and a finite,
// non-negative value.
func (c *CostTracker) AppendUsage(ctx context.Context, entries ...billing.MeterEntry) error {
	if c == nil || len(entries) == 0 {
		return nil
	}
	valid := make([]billing.MeterEntry, 0, len(entries))
	for _, e := range entries {
		if err := billing.ValidateMeterEntry(e); err != nil {
			return err
		}
		if e.Timestamp.IsZero() {
			e.Timestamp = c.now().UTC()
		}
		valid = append(valid, e)
	}
	return c.store.AppendUsage(ctx, valid)
}

// DrainUsage removes and returns up to max buffered usage entries for a
// customer (max <= 0 means a store-defined batch size).
func (c *CostTracker) DrainUsage(ctx context.Context, customerID string, max int) ([]billing.MeterEntry, error) {
	return c.store.DrainUsage(ctx, customerID, max)
}
