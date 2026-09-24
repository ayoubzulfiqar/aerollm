package marketplace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
)

// MicrosPerUSD is the number of integer micro-units in one US dollar.
const MicrosPerUSD int64 = 1_000_000

// DefaultMaxRoyaltyEvents bounds the in-memory event log (and the
// idempotency window) of a RoyaltyRecorder.
const DefaultMaxRoyaltyEvents = 100_000

// RoyaltyEventType is the webhook event type emitted for royalty events.
const RoyaltyEventType webhooks.EventType = "plugin_royalty"

var (
	// ErrInvalidRoyalty reports a royalty event with missing or invalid fields.
	ErrInvalidRoyalty = errors.New("marketplace: invalid royalty event")
	// ErrDuplicateRoyalty reports a royalty event already recorded (same plugin and request).
	ErrDuplicateRoyalty = errors.New("marketplace: duplicate royalty event")
	// ErrRoyaltyOverflow reports that a running total would overflow int64 micro-units.
	ErrRoyaltyOverflow = errors.New("marketplace: royalty total overflow")
)

// RoyaltyRecorder tracks micro-royalty events for third-party plugins.
//
// Amounts are held as int64 micro-USD (CostMicros); CostUSD is derived for
// display. Events are idempotent on (PluginID, RequestID) within the retained
// window, negative/NaN/overflowing amounts are rejected, and per-creator and
// per-plugin totals are kept exactly even after old events are evicted from
// the bounded log.
type RoyaltyRecorder struct {
	mu            sync.RWMutex
	events        []RoyaltyEvent
	seen          map[string]struct{}
	creatorTotals map[string]int64
	pluginTotals  map[string]int64
	maxEvents     int
	webhook       webhooks.BudgetWebhookConfig
	dispatcher    eventDispatcher
	now           func() time.Time
}

// RoyaltyEvent records plugin usage for royalty calculation.
type RoyaltyEvent struct {
	PluginID  string
	CreatorID string
	RequestID string
	// APIKey holds a non-reversible fingerprint ("key_" + 12 hex chars of
	// SHA-256) of the caller's API key, never the key itself.
	APIKey string
	// CostMicros is the authoritative amount in micro-USD.
	CostMicros int64
	// CostUSD is CostMicros / 1e6, kept for display and backward compatibility.
	CostUSD   float64
	Timestamp time.Time
}

// NewRoyaltyRecorder creates a new royalty recorder. When cfg.URL is set and
// the dispatcher supports registration (as *webhooks.WebhookDispatcher does),
// the URL is registered for RoyaltyEventType so events are actually delivered.
func NewRoyaltyRecorder(dispatcher eventDispatcher, cfg webhooks.BudgetWebhookConfig) *RoyaltyRecorder {
	r := &RoyaltyRecorder{
		dispatcher:    dispatcher,
		webhook:       cfg,
		events:        make([]RoyaltyEvent, 0),
		seen:          make(map[string]struct{}),
		creatorTotals: make(map[string]int64),
		pluginTotals:  make(map[string]int64),
		maxEvents:     DefaultMaxRoyaltyEvents,
		now:           time.Now,
	}
	if reg, ok := dispatcher.(webhookRegistrar); ok && cfg.URL != "" {
		reg.Register(RoyaltyEventType, webhooks.WebhookConfig{
			URL:        cfg.URL,
			Secret:     cfg.Secret,
			Timeout:    cfg.Timeout,
			Retries:    cfg.Retries,
			RetryDelay: cfg.RetryDelay,
		})
	}
	return r
}

// SetMaxEvents changes the retained-event bound (minimum 1).
func (r *RoyaltyRecorder) SetMaxEvents(n int) {
	if n < 1 {
		n = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxEvents = n
	r.evictLocked()
}

// USDToMicros converts a dollar amount to integer micro-USD, rejecting NaN,
// infinities, negative values and values that do not fit in int64.
func USDToMicros(usd float64) (int64, error) {
	if math.IsNaN(usd) || math.IsInf(usd, 0) {
		return 0, fmt.Errorf("%w: amount is not finite", ErrInvalidRoyalty)
	}
	if usd < 0 {
		return 0, fmt.Errorf("%w: negative amount", ErrInvalidRoyalty)
	}
	micros := math.Round(usd * float64(MicrosPerUSD))
	if micros >= math.MaxInt64 {
		return 0, fmt.Errorf("%w: amount too large", ErrInvalidRoyalty)
	}
	return int64(micros), nil
}

// FingerprintAPIKey returns a short non-reversible identifier for an API key.
func FingerprintAPIKey(key string) string {
	if key == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(key))
	return "key_" + hex.EncodeToString(sum[:6])
}

func royaltyKey(pluginID, requestID string) string {
	return pluginID + "\x00" + requestID
}

// Record validates and stores a royalty event. It returns ErrDuplicateRoyalty
// (and records nothing) when the same (PluginID, RequestID) was already
// recorded within the retained window. CostMicros is authoritative; when it is
// zero and CostUSD is set, CostUSD is converted.
func (r *RoyaltyRecorder) Record(ctx context.Context, ev RoyaltyEvent) error {
	if ev.PluginID == "" || ev.CreatorID == "" || ev.RequestID == "" {
		return fmt.Errorf("%w: plugin_id, creator_id and request_id are required", ErrInvalidRoyalty)
	}
	if len(ev.PluginID) > 256 || len(ev.CreatorID) > 256 || len(ev.RequestID) > 256 {
		return fmt.Errorf("%w: identifier too long", ErrInvalidRoyalty)
	}
	if ev.CostMicros < 0 {
		return fmt.Errorf("%w: negative amount", ErrInvalidRoyalty)
	}
	if ev.CostMicros == 0 && ev.CostUSD != 0 {
		m, err := USDToMicros(ev.CostUSD)
		if err != nil {
			return err
		}
		ev.CostMicros = m
	}
	ev.CostUSD = float64(ev.CostMicros) / float64(MicrosPerUSD)
	if ev.Timestamp.IsZero() {
		ev.Timestamp = r.now().UTC()
	}

	key := royaltyKey(ev.PluginID, ev.RequestID)
	r.mu.Lock()
	if _, dup := r.seen[key]; dup {
		r.mu.Unlock()
		return ErrDuplicateRoyalty
	}
	ct, pt := r.creatorTotals[ev.CreatorID], r.pluginTotals[ev.PluginID]
	if ct > math.MaxInt64-ev.CostMicros || pt > math.MaxInt64-ev.CostMicros {
		r.mu.Unlock()
		return ErrRoyaltyOverflow
	}
	r.creatorTotals[ev.CreatorID] = ct + ev.CostMicros
	r.pluginTotals[ev.PluginID] = pt + ev.CostMicros
	r.seen[key] = struct{}{}
	r.events = append(r.events, ev)
	r.evictLocked()
	r.mu.Unlock()

	r.dispatch(ctx, ev)
	return nil
}

func (r *RoyaltyRecorder) evictLocked() {
	if len(r.events) <= r.maxEvents {
		return
	}
	drop := len(r.events) - r.maxEvents
	for _, old := range r.events[:drop] {
		delete(r.seen, royaltyKey(old.PluginID, old.RequestID))
	}
	kept := make([]RoyaltyEvent, r.maxEvents)
	copy(kept, r.events[drop:])
	r.events = kept
}

func (r *RoyaltyRecorder) dispatch(ctx context.Context, ev RoyaltyEvent) {
	if r.dispatcher == nil || r.webhook.URL == "" {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Delivery is asynchronous and must outlive the triggering request.
	ctx = context.WithoutCancel(ctx)
	sum := sha256.Sum256([]byte(royaltyKey(ev.PluginID, ev.RequestID)))
	r.dispatcher.DispatchAsync(ctx, webhooks.Event{
		// Deterministic ID so receivers can de-duplicate redeliveries.
		ID:        "royalty-" + hex.EncodeToString(sum[:16]),
		Type:      RoyaltyEventType,
		Timestamp: ev.Timestamp,
		Payload: map[string]interface{}{
			"plugin_id":   ev.PluginID,
			"creator_id":  ev.CreatorID,
			"request_id":  ev.RequestID,
			"cost_micros": ev.CostMicros,
			"cost_usd":    ev.CostUSD,
		},
	})
}

// RecordUsage logs plugin usage when a creator_id is present. It is a
// fire-and-forget wrapper around Record kept for API compatibility: invalid
// or duplicate events are dropped. Use Record to observe errors.
func (r *RoyaltyRecorder) RecordUsage(ctx context.Context, req finops.CostRequest, creatorID string) {
	if creatorID == "" {
		return
	}
	micros, err := USDToMicros(req.CostUSD)
	if err != nil {
		return
	}
	_ = r.Record(ctx, RoyaltyEvent{
		PluginID:   req.Model,
		CreatorID:  creatorID,
		RequestID:  req.RequestID,
		APIKey:     FingerprintAPIKey(req.APIKey),
		CostMicros: micros,
	})
}

// Snapshot returns the retained royalty events (oldest first).
func (r *RoyaltyRecorder) Snapshot() []RoyaltyEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RoyaltyEvent, len(r.events))
	copy(out, r.events)
	return out
}

// CreatorTotalMicros returns the exact lifetime total for a creator in micro-USD.
func (r *RoyaltyRecorder) CreatorTotalMicros(creatorID string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.creatorTotals[creatorID]
}

// PluginTotalMicros returns the exact lifetime total for a plugin in micro-USD.
func (r *RoyaltyRecorder) PluginTotalMicros(pluginID string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pluginTotals[pluginID]
}

type eventDispatcher interface {
	DispatchAsync(ctx context.Context, event webhooks.Event)
}

type webhookRegistrar interface {
	Register(eventType webhooks.EventType, cfg webhooks.WebhookConfig)
}
