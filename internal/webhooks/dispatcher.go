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
	"github.com/redis/go-redis/v9"
)

// EventType defines webhook event types.
type EventType string

const (
	EventBudgetExceeded       EventType = "budget_exceeded"
	EventAgentRequiresApproval EventType = "agent_requires_approval"
	EventShadowTestCompleted  EventType = "shadow_test_completed"
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
	URL        string
	Secret     string
	Timeout    time.Duration
	Retries    int
	RetryDelay time.Duration
}

// BudgetWebhookConfig holds budget webhook target configuration.
type BudgetWebhookConfig struct {
	URL        string
	Secret     string
	Timeout    time.Duration
	Retries    int
	RetryDelay time.Duration
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
			_ = d.sendWithRetry(ctx, cfg, event)
		}(cfg)
	}
}

// sendWithRetry delivers the event payload with retry logic.
func (d *WebhookDispatcher) sendWithRetry(ctx context.Context, cfg WebhookConfig, event Event) error {
	var lastErr error
	retries := cfg.Retries
	if retries <= 0 {
		retries = 1
	}
	delay := cfg.RetryDelay
	if delay <= 0 {
		delay = 200 * time.Millisecond
	}

	for attempt := 0; attempt < retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay = delay * 2
		}
		if err := d.send(ctx, cfg, event); err != nil {
			lastErr = err
			telemetry.RecordError()
			continue
		}
		return nil
	}
	return fmt.Errorf("webhook delivery failed after %d attempts: %w", retries, lastErr)
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
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}

// WebhookQueue is the interface for persistent webhook queues.
type WebhookQueue interface {
	Enqueue(ctx context.Context, event Event) error
	Dequeue(ctx context.Context) (Event, error)
}

// RedisWebhookQueue implements WebhookQueue using a Redis list.
type RedisWebhookQueue struct {
	client *redis.Client
	key    string
}

// NewRedisWebhookQueue creates a new Redis-backed webhook queue.
func NewRedisWebhookQueue(client *redis.Client, key string) *RedisWebhookQueue {
	return &RedisWebhookQueue{client: client, key: key}
}

// Enqueue pushes an event onto the queue.
func (q *RedisWebhookQueue) Enqueue(ctx context.Context, event Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return q.client.LPush(ctx, q.key, data).Err()
}

// Dequeue pops an event from the queue with blocking semantics.
func (q *RedisWebhookQueue) Dequeue(ctx context.Context) (Event, error) {
	result, err := q.client.BRPop(ctx, 0, q.key).Result()
	if err != nil {
		return Event{}, err
	}
	var event Event
	if err := json.Unmarshal([]byte(result[1]), &event); err != nil {
		return Event{}, err
	}
	return event, nil
}

// StartWorker begins a background worker that processes webhook events from the queue.
func (d *WebhookDispatcher) StartWorker(ctx context.Context, queue WebhookQueue) {
	go func() {
		for {
			event, err := queue.Dequeue(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}
			d.DispatchAsync(ctx, event)
		}
	}()
}
