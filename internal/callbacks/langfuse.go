package callbacks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// LangfuseCallback sends traces, generations, and scores to the Langfuse API.
// It batches ingestions per request for efficiency and fires them asynchronously.
type LangfuseCallback struct {
	apiKey     string
	baseURL    string
	projectID  string
	httpClient *http.Client
	ingestion  *langfuseIngestion
}

// langfuseIngestion holds the trace and generation objects to send.
type langfuseIngestion struct {
	Traces     []langfuseTrace    `json:"traces,omitempty"`
	Generations []langfuseGeneration `json:"generations,omitempty"`
	mu         chan struct{} // semaphore for concurrent sends
}

// langfuseTrace represents a Langfuse trace object.
type langfuseTrace struct {
	ID        string                 `json:"id"`
	Timestamp string                 `json:"timestamp"`
	Name      string                 `json:"name"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	UserID    string                 `json:"user_id,omitempty"`
}

// langfuseGeneration represents a Langfuse generation object.
type langfuseGeneration struct {
	ID         string                 `json:"id"`
	TraceID    string                 `json:"trace_id"`
	Name       string                 `json:"name"`
	Model      string                 `json:"model"`
	StartTime  string                 `json:"start_time"`
	EndTime    string                 `json:"end_time"`
	// Input/Output captured for observability.
	Input      []map[string]interface{} `json:"input,omitempty"`
	Output     map[string]interface{}   `json:"output,omitempty"`
	Usage      map[string]interface{}   `json:"usage,omitempty"`
	Metadata   map[string]interface{}   `json:"metadata,omitempty"`
	Provider   string                   `json:"provider,omitempty"`
}

// NewLangfuseCallback creates a new Langfuse callback handler.
func NewLangfuseCallback(apiKey, baseURL, projectID string) *LangfuseCallback {
	return &LangfuseCallback{
		apiKey:    apiKey,
		baseURL:   baseURL,
		projectID: projectID,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		ingestion: &langfuseIngestion{
			Traces:     make([]langfuseTrace, 0),
			Generations: make([]langfuseGeneration, 0),
			mu:         make(chan struct{}, 10),
		},
	}
}

// Name implements CallbackHandler.
func (l *LangfuseCallback) Name() string { return "langfuse" }

// OnSuccess sends a trace + generation to Langfuse.
func (l *LangfuseCallback) OnSuccess(ctx context.Context, req *CallbackRequestData, resp *CallbackResponseData) error {
	traceID := fmt.Sprintf("trace_%s_%d", req.RequestID, time.Now().UnixNano())
	now := time.Now().UTC().Format(time.RFC3339Nano)

	trace := langfuseTrace{
		ID:        traceID,
		Timestamp: now,
		Name:      "llm_call",
		Metadata:  req.Metadata,
		UserID:    req.RequestID,
	}

	gen := langfuseGeneration{
		ID:        fmt.Sprintf("gen_%s", req.RequestID),
		TraceID:   traceID,
		Name:      req.Model,
		Model:     req.Model,
		StartTime: now,
		EndTime:   time.Now().UTC().Add(time.Duration(resp.LatencyMs) * time.Millisecond).Format(time.RFC3339Nano),
		Input:     req.Messages,
		Usage:     resp.Usage,
		Metadata:  req.Metadata,
		Provider:  req.Provider,
	}

	// Send ingestion batch.
	payload := map[string]interface{}{
		"traces":      []langfuseTrace{trace},
		"generations": []langfuseGeneration{gen},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal langfuse payload: %w", err)
	}

	ingestURL := fmt.Sprintf("%s/api/public/ingestion/trace", l.baseURL)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", ingestURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", l.apiKey))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Langfuse-Project-Id", l.projectID)

	resp2, err := l.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("langfuse ingestion failed: %w", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode >= 400 {
		return fmt.Errorf("langfuse returned status %d", resp2.StatusCode)
	}

	return nil
}

// OnError sends an error trace to Langfuse.
func (l *LangfuseCallback) OnError(ctx context.Context, req *CallbackRequestData, err error) error {
	traceID := fmt.Sprintf("trace_%s_%d", req.RequestID, time.Now().UnixNano())
	now := time.Now().UTC().Format(time.RFC3339Nano)

	trace := langfuseTrace{
		ID:        traceID,
		Timestamp: now,
		Name:      fmt.Sprintf("llm_error_%s", req.Model),
		Metadata:  req.Metadata,
		UserID:    req.RequestID,
	}

	// Attach error info to metadata.
	if trace.Metadata == nil {
		trace.Metadata = make(map[string]interface{})
	}
	trace.Metadata["error"] = err.Error()

	payload := map[string]interface{}{
		"traces": []langfuseTrace{trace},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal langfuse error payload: %w", err)
	}

	ingestURL := fmt.Sprintf("%s/api/public/ingestion/trace", l.baseURL)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", ingestURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", l.apiKey))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Langfuse-Project-Id", l.projectID)

	resp, err := l.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
