package marketplace

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
)

// RoyaltyRecorder tracks micro-royalty events for third-party plugins.
type RoyaltyRecorder struct {
	mu         sync.RWMutex
	events     []RoyaltyEvent
	webhook    webhooks.BudgetWebhookConfig
	dispatcher eventDispatcher
}

// RoyaltyEvent records plugin usage for royalty calculation.
type RoyaltyEvent struct {
	PluginID   string
	CreatorID  string
	RequestID  string
	APIKey     string
	CostUSD    float64
	Timestamp  time.Time
}

// NewRoyaltyRecorder creates a new royalty recorder.
func NewRoyaltyRecorder(dispatcher eventDispatcher, cfg webhooks.BudgetWebhookConfig) *RoyaltyRecorder {
	return &RoyaltyRecorder{
		dispatcher: dispatcher,
		webhook:    cfg,
		events:     make([]RoyaltyEvent, 0),
	}
}

// RecordUsage logs plugin usage when a creator_id is present.
func (r *RoyaltyRecorder) RecordUsage(ctx context.Context, req finops.CostRequest, creatorID string) {
	if creatorID == "" {
		return
	}
	event := RoyaltyEvent{
		PluginID:  req.Model,
		CreatorID: creatorID,
		RequestID: req.RequestID,
		APIKey:    req.APIKey,
		CostUSD:   req.CostUSD,
		Timestamp: time.Now(),
	}
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()

	if r.dispatcher != nil && r.webhook.URL != "" {
		r.dispatcher.DispatchAsync(ctx, webhooks.Event{
			ID:        fmt.Sprintf("royalty-%s-%d", req.RequestID, time.Now().UnixNano()),
			Type:      "plugin_royalty",
			Timestamp: time.Now(),
			Payload: map[string]interface{}{
				"plugin_id":  req.Model,
				"creator_id": creatorID,
				"request_id": req.RequestID,
				"cost_usd":   req.CostUSD,
			},
		})
	}
}

// Snapshot returns current royalty events.
func (r *RoyaltyRecorder) Snapshot() []RoyaltyEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RoyaltyEvent, len(r.events))
	copy(out, r.events)
	return out
}

type eventDispatcher interface {
	DispatchAsync(ctx context.Context, event webhooks.Event)
}
