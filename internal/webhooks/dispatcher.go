package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
)

// EventType defines webhook event types.
type EventType string

const (
	EventBudgetExceeded     EventType = "budget_exceeded"
	EventAgentRequiresApproval EventType = "agent_requires_approval"
	EventShadowTestCompleted EventType = "shadow_test_completed"
)

// Event represents a webhook event.
type Event struct {
	ID        string                 `json:"id"`
	Type      EventType              `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

// WebhookConfig holds webhook endpoint configuration.
type WebhookConfig struct {
	URL     string
	Secret  string
	Timeout time.Duration
}

// WebhookDispatcher dispatches webhook events asynchronously.
type WebhookDispatcher struct {
	mu      sync.RWMutex
	configs map[EventType][]WebhookConfig
}

// NewWebhookDispatcher creates a new webhook dispatcher.
func NewWebhookDispatcher() *WebhookDispatcher {
	return &WebhookDispatcher{configs: make(map[EventType][]WebhookConfig)}
}

// Register registers a webhook URL for an event type.
func (d *WebhookDispatcher) Register(eventType EventType, cfg WebhookConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.configs[eventType] = append(d.configs[eventType], cfg)
}

// DispatchAsync sends an event asynchronously to all registered webhooks.
func (d *WebhookDispatcher) DispatchAsync(ctx context.Context, event Event) {
	d.mu.RLock()
	configs := d.configs[event.Type]
	d.mu.RUnlock()

	for _, cfg := range configs {
		go func(cfg WebhookConfig) {
			_ = d.send(ctx, cfg, event)
		}(cfg)
	}
}

// send delivers the event payload to the webhook URL.
func (d *WebhookDispatcher) send(ctx context.Context, cfg WebhookConfig, event Event) error {
	data, _ := json.Marshal(event)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Secret", cfg.Secret)

	client := &http.Client{Timeout: cfg.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		telemetry.RecordError()
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		telemetry.RecordError()
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}
