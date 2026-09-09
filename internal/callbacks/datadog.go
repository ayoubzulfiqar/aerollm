package callbacks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// DatadogCallback sends custom metrics and logs to Datadog.
// It sends metrics as Datadog statsd-style JSON payloads to the Datadog API
// and logs as structured events to the Datadog logs endpoint.
type DatadogCallback struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	site       string // e.g. "datadoghq.com"
}

// NewDatadogCallback creates a new Datadog callback handler.
func NewDatadogCallback(apiKey, baseURL, site string) *DatadogCallback {
	if site == "" {
		site = "datadoghq.com"
	}
	return &DatadogCallback{
		apiKey:    apiKey,
		baseURL:   baseURL,
		site:      site,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// Name implements CallbackHandler.
func (d *DatadogCallback) Name() string { return "datadog" }

// datadogMetric represents a single Datadog metric payload.
type datadogMetric struct {
	Metric string                 `json:"metric"`
	Points []map[string]interface{} `json:"points"`
	Type   string                 `json:"type,omitempty"`
	Tags   []string               `json:"tags,omitempty"`
	Host   string                 `json:"host,omitempty"`
}

// datadogEvent represents a Datadog log event.
type datadogEvent struct {
	Title     string                 `json:"title"`
	Text      string                 `json:"text"`
	AlertType string                 `json:"alert_type"`
	Tags      []string               `json:"tags"`
	SourceType string                 `json:"source_type_name"`
	Timestamp int64                  `json:"timestamp"`
}

// OnSuccess sends a metric for successful LLM calls and logs the result.
func (d *DatadogCallback) OnSuccess(ctx context.Context, req *CallbackRequestData, resp *CallbackResponseData) error {
	tags := []string{
		fmt.Sprintf("model:%s", req.Model),
		fmt.Sprintf("provider:%s", req.Provider),
		fmt.Sprintf("request_id:%s", req.RequestID),
	}

	// Send metric.
	metric := datadogMetric{
		Metric: "aerollm.llm.latency_ms",
		Points: []map[string]interface{}{
			{"timestamp": time.Now().Unix(), "value": resp.LatencyMs},
		},
		Type: "gauge",
		Tags: tags,
	}
	metricPayload := []datadogMetric{metric}
	now := time.Now()

	// Also send token usage metrics.
	for tokenType, count := range resp.TokenCount {
		tokMetric := datadogMetric{
			Metric: fmt.Sprintf("aerollm.llm.tokens.%s", tokenType),
			Points: []map[string]interface{}{
				{"timestamp": now.Unix(), "value": count},
			},
			Type: "count",
			Tags: tags,
		}
		metricPayload = append(metricPayload, tokMetric)
	}

	// Cost metric.
	if req.CostUSD > 0 {
		costMetric := datadogMetric{
			Metric: "aerollm.llm.cost_usd",
			Points: []map[string]interface{}{
				{"timestamp": now.Unix(), "value": req.CostUSD},
			},
			Type: "gauge",
			Tags: tags,
		}
		metricPayload = append(metricPayload, costMetric)
	}

	body, err := json.Marshal(metricPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal datadog metrics: %w", err)
	}

	metricURL := fmt.Sprintf("https://api.%s/api/v2/series", d.site)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", metricURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Authorization", fmt.Sprintf("API-Key %s", d.apiKey))
	httpReq.Header.Set("Content-Type", "application/json")

	metricResp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("datadog metric send failed: %w", err)
	}
	metricResp.Body.Close()

	// Send log event.
	event := datadogEvent{
		Title:      fmt.Sprintf("LLM Call Success: %s", req.Model),
		Text:       fmt.Sprintf("Provider: %s, Latency: %dms, Tokens: %v", req.Provider, resp.LatencyMs, resp.TokenCount),
		AlertType:  "info",
		Tags:        tags,
		SourceType: "aerollm",
		Timestamp:  now.Unix(),
	}
	eventBody, err := json.Marshal(struct {
		Events []datadogEvent `json:"events"`
	}{Events: []datadogEvent{event}})
	if err != nil {
		return fmt.Errorf("failed to marshal datadog event: %w", err)
	}
	logURL := fmt.Sprintf("https://http-intake.logs.%s/v1/input", d.site)
	logReq, err := http.NewRequestWithContext(ctx, "POST", logURL, bytes.NewReader(eventBody))
	if err != nil {
		return err
	}
	logReq.Header.Set("DD-API-KEY", d.apiKey)
	logReq.Header.Set("Content-Type", "application/json")

	logResp, err := d.httpClient.Do(logReq)
	if err != nil {
		return fmt.Errorf("datadog log send failed: %w", err)
	}
	logResp.Body.Close()

	return nil
}

// OnError sends an error metric and event to Datadog.
func (d *DatadogCallback) OnError(ctx context.Context, req *CallbackRequestData, err error) error {
	tags := []string{
		fmt.Sprintf("model:%s", req.Model),
		fmt.Sprintf("provider:%s", req.Provider),
		fmt.Sprintf("request_id:%s", req.RequestID),
		"error_type:llm_error",
	}

	// Send error metric.
	metric := datadogMetric{
		Metric: "aerollm.llm.errors",
		Points: []map[string]interface{}{
			{"timestamp": time.Now().Unix(), "value": 1},
		},
		Type: "count",
		Tags: tags,
	}
	body, _ := json.Marshal([]datadogMetric{metric})

	metricURL := fmt.Sprintf("https://api.%s/api/v2/series", d.site)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", metricURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Authorization", fmt.Sprintf("API-Key %s", d.apiKey))
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Send error event.
	event := datadogEvent{
		Title:      fmt.Sprintf("LLM Call Error: %s", req.Model),
		Text:       fmt.Sprintf("Provider: %s, Error: %s", req.Provider, err.Error()),
		AlertType:  "error",
		Tags:        tags,
		SourceType: "aerollm",
		Timestamp:  time.Now().Unix(),
	}
	eventBody, _ := json.Marshal(struct {
		Events []datadogEvent `json:"events"`
	}{Events: []datadogEvent{event}})

	logURL := fmt.Sprintf("https://http-intake.logs.%s/v1/input", d.site)
	logReq, err := http.NewRequestWithContext(ctx, "POST", logURL, bytes.NewReader(eventBody))
	if err != nil {
		return err
	}
	logReq.Header.Set("DD-API-KEY", d.apiKey)
	logReq.Header.Set("Content-Type", "application/json")

	logResp, err := d.httpClient.Do(logReq)
	if err != nil {
		return err
	}
	defer logResp.Body.Close()

	return nil
}
