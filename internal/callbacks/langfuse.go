package callbacks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// DefaultLangfuseURL is the Langfuse cloud endpoint.
const DefaultLangfuseURL = "https://cloud.langfuse.com"

// LangfuseCallback sends traces and generations to the Langfuse public
// ingestion API (POST /api/public/ingestion) using HTTP basic auth with the
// project's public key (username) and secret key (password).
type LangfuseCallback struct {
	publicKey  string
	secretKey  string
	baseURL    string
	httpClient *http.Client
}

// NewLangfuseCallback creates a Langfuse callback handler.
//
// For backward compatibility with the single-key configuration, apiKey may
// be "publicKey:secretKey"; otherwise apiKey is used as the secret key and
// projectID as the public key (Langfuse keys are "pk-lf-..." / "sk-lf-...").
// Prefer NewLangfuseCallbackWithKeys.
func NewLangfuseCallback(apiKey, baseURL, projectID string) *LangfuseCallback {
	public, secret := projectID, apiKey
	if pk, sk, ok := strings.Cut(apiKey, ":"); ok {
		public, secret = pk, sk
	}
	return NewLangfuseCallbackWithKeys(public, secret, baseURL)
}

// NewLangfuseCallbackWithKeys creates a Langfuse callback from explicit
// public/secret keys. An empty baseURL selects DefaultLangfuseURL.
func NewLangfuseCallbackWithKeys(publicKey, secretKey, baseURL string) *LangfuseCallback {
	if baseURL == "" {
		baseURL = DefaultLangfuseURL
	}
	return &LangfuseCallback{
		publicKey:  publicKey,
		secretKey:  secretKey,
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: newHTTPClient(10 * time.Second),
	}
}

// Name implements CallbackHandler.
func (l *LangfuseCallback) Name() string { return "langfuse" }

// langfuseEvent is one element of the ingestion batch.
type langfuseEvent struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Body      interface{} `json:"body"`
}

type langfuseTraceBody struct {
	ID        string                 `json:"id"`
	Timestamp string                 `json:"timestamp"`
	Name      string                 `json:"name"`
	UserID    string                 `json:"userId,omitempty"`
	Input     interface{}            `json:"input,omitempty"`
	Output    interface{}            `json:"output,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Tags      []string               `json:"tags,omitempty"`
}

type langfuseUsage struct {
	Input     int     `json:"input"`
	Output    int     `json:"output"`
	Total     int     `json:"total"`
	Unit      string  `json:"unit"`
	TotalCost float64 `json:"totalCost,omitempty"`
}

type langfuseGenerationBody struct {
	ID            string                 `json:"id"`
	TraceID       string                 `json:"traceId"`
	Name          string                 `json:"name"`
	StartTime     string                 `json:"startTime"`
	EndTime       string                 `json:"endTime,omitempty"`
	Model         string                 `json:"model,omitempty"`
	Input         interface{}            `json:"input,omitempty"`
	Output        interface{}            `json:"output,omitempty"`
	Usage         *langfuseUsage         `json:"usage,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
	Level         string                 `json:"level,omitempty"`
	StatusMessage string                 `json:"statusMessage,omitempty"`
}

// langfuseIngestionResponse is the 207 multi-status reply.
type langfuseIngestionResponse struct {
	Errors []struct {
		ID      string `json:"id"`
		Status  int    `json:"status"`
		Message string `json:"message"`
	} `json:"errors"`
}

func intFrom(m map[string]interface{}, keys ...string) int {
	for _, k := range keys {
		switch v := m[k].(type) {
		case int:
			return v
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
	}
	return 0
}

func traceIDFor(req *CallbackRequestData) string {
	if req != nil && req.RequestID != "" {
		return "aerollm-" + req.RequestID
	}
	return randomID()
}

func withMeta(base map[string]interface{}, extra map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		if v != nil && v != "" {
			out[k] = v
		}
	}
	return out
}

// OnSuccess sends a trace + generation to Langfuse.
func (l *LangfuseCallback) OnSuccess(ctx context.Context, req *CallbackRequestData, resp *CallbackResponseData) error {
	if req == nil {
		req = &CallbackRequestData{}
	}
	if resp == nil {
		resp = &CallbackResponseData{}
	}
	end := resp.Timestamp
	if end.IsZero() {
		end = time.Now()
	}
	start := req.Timestamp
	if start.IsZero() || start.After(end) {
		start = end.Add(-time.Duration(resp.LatencyMs) * time.Millisecond)
	}
	traceID := traceIDFor(req)
	model := resp.Model
	if model == "" {
		model = req.Model
	}
	usage := &langfuseUsage{
		Input:     intFrom(resp.Usage, "prompt_tokens", "input_tokens"),
		Output:    intFrom(resp.Usage, "completion_tokens", "output_tokens"),
		Unit:      "TOKENS",
		TotalCost: req.CostUSD,
	}
	if usage.Input == 0 && usage.Output == 0 {
		usage.Input, usage.Output = resp.TokenCount["input"], resp.TokenCount["output"]
	}
	usage.Total = usage.Input + usage.Output
	meta := withMeta(req.Metadata, map[string]interface{}{"provider": resp.Provider, "request_id": req.RequestID, "response_id": resp.ResponseID, "latency_ms": resp.LatencyMs, "finish_reason": resp.FinishReason})

	var output interface{}
	if resp.Output != "" {
		output = map[string]interface{}{"role": "assistant", "content": resp.Output}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	batch := []langfuseEvent{
		{ID: randomID(), Type: "trace-create", Timestamp: now, Body: langfuseTraceBody{
			ID: traceID, Timestamp: start.UTC().Format(time.RFC3339Nano), Name: "llm_call",
			UserID: req.User, Input: req.Messages, Output: output, Metadata: meta, Tags: append([]string{"aerollm"}, req.Tags...),
		}},
		{ID: randomID(), Type: "generation-create", Timestamp: now, Body: langfuseGenerationBody{
			ID: traceID + "-gen", TraceID: traceID, Name: "chat_completion",
			StartTime: start.UTC().Format(time.RFC3339Nano), EndTime: end.UTC().Format(time.RFC3339Nano),
			Model: model, Input: req.Messages, Output: output, Usage: usage, Metadata: meta, Level: "DEFAULT",
		}},
	}
	return l.ingest(ctx, batch)
}

// OnError sends an error trace + generation (level ERROR) to Langfuse.
func (l *LangfuseCallback) OnError(ctx context.Context, req *CallbackRequestData, callErr error) error {
	if req == nil {
		req = &CallbackRequestData{}
	}
	msg := "unknown error"
	if callErr != nil {
		msg = callErr.Error()
	}
	start := req.Timestamp
	if start.IsZero() {
		start = time.Now()
	}
	traceID := traceIDFor(req)
	// Copy metadata: the request data may be shared, never mutate it.
	meta := withMeta(req.Metadata, map[string]interface{}{"provider": req.Provider, "request_id": req.RequestID, "error": msg})
	now := time.Now().UTC().Format(time.RFC3339Nano)
	batch := []langfuseEvent{
		{ID: randomID(), Type: "trace-create", Timestamp: now, Body: langfuseTraceBody{
			ID: traceID, Timestamp: start.UTC().Format(time.RFC3339Nano), Name: "llm_call",
			UserID: req.User, Input: req.Messages, Metadata: meta, Tags: append([]string{"aerollm", "error"}, req.Tags...),
		}},
		{ID: randomID(), Type: "generation-create", Timestamp: now, Body: langfuseGenerationBody{
			ID: traceID + "-gen", TraceID: traceID, Name: "chat_completion",
			StartTime: start.UTC().Format(time.RFC3339Nano), EndTime: now,
			Model: req.Model, Input: req.Messages, Metadata: meta, Level: "ERROR", StatusMessage: msg,
		}},
	}
	return l.ingest(ctx, batch)
}

func (l *LangfuseCallback) ingest(ctx context.Context, batch []langfuseEvent) error {
	if l.publicKey == "" || l.secretKey == "" {
		return fmt.Errorf("langfuse: public and secret keys are required")
	}
	endpoint := l.baseURL + "/api/public/ingestion"
	if err := validateEndpoint(endpoint); err != nil {
		return err
	}
	body, err := json.Marshal(map[string]interface{}{"batch": batch})
	if err != nil {
		return fmt.Errorf("langfuse: marshal payload: %w", err)
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(l.publicKey+":"+l.secretKey)))
	h.Set("User-Agent", "AeroLLM-Callbacks/1.0")

	respBody, status, err := doRequest(ctx, l.httpClient, http.MethodPost, endpoint, body, h)
	if err != nil {
		return fmt.Errorf("langfuse ingestion failed: %w", err)
	}
	if status == http.StatusMultiStatus {
		var r langfuseIngestionResponse
		if json.Unmarshal(respBody, &r) == nil && len(r.Errors) > 0 {
			return fmt.Errorf("langfuse rejected %d of %d events: %s", len(r.Errors), len(batch), r.Errors[0].Message)
		}
	}
	return nil
}
