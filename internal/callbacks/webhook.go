package callbacks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
)

// WebhookConfig holds the configuration for a WebhookCallback endpoint.
type WebhookConfig struct {
	URL        string
	Secret     string
	Timeout    time.Duration
	Retries    int
	RetryDelay time.Duration
}

// WebhookCallback sends callback events to a configurable webhook endpoint.
// It conforms to the CallbackHandler interface and is the generic integration
// point for external observability pipelines (e.g. data warehouses, SIEMs).
//
// When Secret is set, deliveries carry X-AeroLLM-Timestamp and
// X-AeroLLM-Signature ("sha256=" + HMAC-SHA256(secret, "<ts>.<body>")),
// verifiable with webhooks.Verify / webhooks.VerifyRequest.
type WebhookCallback struct {
	config     WebhookConfig
	httpClient *http.Client
}

// NewWebhookCallback creates a new webhook callback handler.
func NewWebhookCallback(cfg WebhookConfig) *WebhookCallback {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Retries <= 0 {
		cfg.Retries = 3
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 200 * time.Millisecond
	}
	return &WebhookCallback{
		config:     cfg,
		httpClient: newHTTPClient(cfg.Timeout),
	}
}

// Name implements CallbackHandler.
func (w *WebhookCallback) Name() string { return "webhook" }

// webhookPayload is the JSON body sent to the webhook endpoint.
type webhookPayload struct {
	ID        string                `json:"id"`
	EventType string                `json:"event_type"`
	Timestamp string                `json:"timestamp"`
	Request   *CallbackRequestData  `json:"request"`
	Response  *CallbackResponseData `json:"response,omitempty"`
	Error     string                `json:"error,omitempty"`
}

// OnSuccess sends a success event to the webhook.
func (w *WebhookCallback) OnSuccess(ctx context.Context, req *CallbackRequestData, resp *CallbackResponseData) error {
	return w.sendWithRetry(ctx, webhookPayload{
		ID:        randomID(),
		EventType: "success",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Request:   req,
		Response:  resp,
	})
}

// OnError sends an error event to the webhook.
func (w *WebhookCallback) OnError(ctx context.Context, req *CallbackRequestData, err error) error {
	msg := "unknown error"
	if err != nil {
		msg = err.Error()
	}
	return w.sendWithRetry(ctx, webhookPayload{
		ID:        randomID(),
		EventType: "error",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Request:   req,
		Error:     msg,
	})
}

// sendWithRetry dispatches the webhook payload with exponential backoff and
// jitter. Client errors (4xx other than 408/429) are not retried.
func (w *WebhookCallback) sendWithRetry(ctx context.Context, payload webhookPayload) error {
	if err := validateEndpoint(w.config.URL); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt < w.config.Retries; attempt++ {
		if attempt > 0 {
			wait := backoff(w.config.RetryDelay, attempt, 10*time.Second)
			var se *httpStatusError
			if errors.As(lastErr, &se) && se.RetryAfter > wait {
				wait = min(se.RetryAfter, 30*time.Second)
			}
			if err := sleepCtx(ctx, wait); err != nil {
				return err
			}
		}
		h := http.Header{}
		h.Set("Content-Type", "application/json")
		h.Set("User-Agent", "AeroLLM-Callbacks/1.0")
		h.Set(webhooks.EventHeader, "callback."+payload.EventType)
		h.Set(webhooks.DeliveryHeader, payload.ID)
		webhooks.SetSignatureHeaders(h, w.config.Secret, body, time.Now())

		_, _, err := doRequest(ctx, w.httpClient, http.MethodPost, w.config.URL, body, h)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable(err) || ctx.Err() != nil {
			break
		}
	}
	return fmt.Errorf("webhook callback failed: %w", lastErr)
}
