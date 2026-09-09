package callbacks

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
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
		httpClient: &http.Client{Timeout: cfg.Timeout},
	}
}

// Name implements CallbackHandler.
func (w *WebhookCallback) Name() string { return "webhook" }

// webhookPayload is the JSON body sent to the webhook endpoint.
type webhookPayload struct {
	EventType string                 `json:"event_type"`
	Timestamp string                 `json:"timestamp"`
	Request   *CallbackRequestData   `json:"request"`
	Response  *CallbackResponseData  `json:"response,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

// OnSuccess sends a success event to the webhook.
func (w *WebhookCallback) OnSuccess(ctx context.Context, req *CallbackRequestData, resp *CallbackResponseData) error {
	payload := webhookPayload{
		EventType: "success",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Request:   req,
		Response:  resp,
	}
	return w.sendWithRetry(ctx, payload)
}

// OnError sends an error event to the webhook.
func (w *WebhookCallback) OnError(ctx context.Context, req *CallbackRequestData, err error) error {
	payload := webhookPayload{
		EventType: "error",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Request:   req,
		Error:     err.Error(),
	}
	return w.sendWithRetry(ctx, payload)
}

// sendWithRetry dispatches the webhook payload with retry logic.
func (w *WebhookCallback) sendWithRetry(ctx context.Context, payload webhookPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt < w.config.Retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.config.RetryDelay):
			}
		}

		req, err := http.NewRequestWithContext(ctx, "POST", w.config.URL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		// Sign the payload with HMAC-SHA256 if a secret is configured.
		if w.config.Secret != "" {
			signature := w.sign(body)
			req.Header.Set("X-Webhook-Signature", signature)
		}

		resp, err := w.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			resp.Body.Close()
			return nil
		}

		resp.Body.Close()
		lastErr = fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return fmt.Errorf("webhook failed after %d retries: %w", w.config.Retries, lastErr)
}

// sign computes the HMAC-SHA256 signature of the payload.
func (w *WebhookCallback) sign(body []byte) string {
	mac := hmac.New(sha256.New, []byte(w.config.Secret))
	mac.Write(body)
	return fmt.Sprintf("sha256=%x", mac.Sum(nil))
}
