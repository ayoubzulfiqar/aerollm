package universal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// OpenAICompatibleAdapter is a reusable adapter for OpenAI-compatible APIs.
type OpenAICompatibleAdapter struct {
	name         string
	providerType string
	cfg          AdapterConfig
	http         *http.Client
}

// NewOpenAICompatibleAdapter creates a new OpenAI-compatible adapter.
func NewOpenAICompatibleAdapter(name, providerType, apiKey, baseURL string) *OpenAICompatibleAdapter {
	cfg := NewDefaultAdapterConfig(apiKey, baseURL)
	return &OpenAICompatibleAdapter{
		name:         name,
		providerType: providerType,
		cfg:          cfg,
		http:         cfg.HTTP,
	}
}

// Name returns the adapter name.
func (a *OpenAICompatibleAdapter) Name() string { return a.name }

// Type returns the adapter provider type as a string.
func (a *OpenAICompatibleAdapter) Type() string { return a.providerType }

// ProviderType returns the provider type.
func (a *OpenAICompatibleAdapter) ProviderType() providers.ProviderType {
	return providers.ProviderType(a.providerType)
}

// ChatCompletions sends a chat completion request to the OpenAI-compatible endpoint.
// OpenAI Structured Outputs: when req.ResponseFormat has type "json_schema",
// the request body includes response_format with the JSON schema, enabling
// guaranteed structured JSON responses from the provider.
func (a *OpenAICompatibleAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	payload := buildChatPayload(req)
	body, err := jsonMarshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.BaseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if a.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
	}
	resp, err := a.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("provider error: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	var llmResp models.LLMResponse
	if err := jsonUnmarshal(b, &llmResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &llmResp, nil
}

// Embeddings sends an embeddings request to the OpenAI-compatible endpoint.
func (a *OpenAICompatibleAdapter) Embeddings(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error) {
	payload := map[string]interface{}{
		"model": req.Model,
		"input": req.Input,
	}
	body, err := jsonMarshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.BaseURL+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if a.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
	}
	resp, err := a.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("provider error: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	var embResp models.EmbeddingResponse
	if err := jsonUnmarshal(b, &embResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &embResp, nil
}

// ImageGenerations sends an image generation request to the OpenAI-compatible endpoint.
func (a *OpenAICompatibleAdapter) ImageGenerations(ctx context.Context, req *models.ImageRequest) (*models.ImageResponse, error) {
	payload := map[string]interface{}{
		"model":  req.Model,
		"prompt": req.Prompt,
		"n":      req.N,
		"size":   req.Size,
	}
	body, err := jsonMarshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.BaseURL+"/v1/images/generations", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if a.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
	}
	resp, err := a.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("provider error: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	var imgResp models.ImageResponse
	if err := jsonUnmarshal(b, &imgResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &imgResp, nil
}

// AudioTranscriptions sends an audio transcription request to the OpenAI-compatible endpoint.
func (a *OpenAICompatibleAdapter) AudioTranscriptions(ctx context.Context, req *models.AudioRequest) (*models.AudioResponse, error) {
	// For audio endpoints, many OpenAI-compatible providers expect multipart form data.
	// We build a simple form payload with the supported fields.
	pr, pw := io.Pipe()
	bodyWriter := &bufferWriter{Writer: pw}
	boundary := "aero-audio-boundary"
	bodyWriter.WriteString("--" + boundary + "\r\n")
	bodyWriter.WriteString("Content-Disposition: form-data; name=\"model\"\r\n\r\n" + req.Model + "\r\n")
	bodyWriter.WriteString("--" + boundary + "\r\n")
	bodyWriter.WriteString("Content-Disposition: form-data; name=\"file\"\r\n\r\n" + req.File + "\r\n")
	if req.Prompt != "" {
		bodyWriter.WriteString("--" + boundary + "\r\n")
		bodyWriter.WriteString("Content-Disposition: form-data; name=\"prompt\"\r\n\r\n" + req.Prompt + "\r\n")
	}
	bodyWriter.WriteString("--" + boundary + "--\r\n")
	_ = bodyWriter.Close()
	pr.Close()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.BaseURL+"/v1/audio/transcriptions", pr)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	if a.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
	}
	resp, err := a.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("provider error: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	var audioResp models.AudioResponse
	if err := jsonUnmarshal(b, &audioResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &audioResp, nil
}

// Responses sends an OpenAI-compatible responses request.
func (a *OpenAICompatibleAdapter) Responses(ctx context.Context, req *models.ResponsesRequest) (*models.ResponsesResponse, error) {
	payload := map[string]interface{}{
		"model": req.Model,
		"input": req.Input,
	}
	if req.Previous != nil {
		payload["previous_response_id"] = *req.Previous
	}
	if len(req.Tools) > 0 {
		tools := make([]map[string]interface{}, 0, len(req.Tools))
		for _, tool := range req.Tools {
			tools = append(tools, map[string]interface{}{
				"type": "function",
				"function": map[string]interface{}{
					"name":        tool.Name,
					"description": tool.Description,
					"parameters":  tool.Parameters,
				},
			})
		}
		payload["tools"] = tools
	}
	body, err := jsonMarshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.BaseURL+"/v1/responses", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if a.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
	}
	resp, err := a.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("provider error: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	var responsesResp models.ResponsesResponse
	if err := jsonUnmarshal(b, &responsesResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &responsesResp, nil
}

// Stream sends a streaming chat completion request.
func (a *OpenAICompatibleAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	payload := buildChatPayload(req)
	payload.Stream = true
	body, err := jsonMarshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.BaseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if a.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
	}
	resp, err := a.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("provider error: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	ch := make(chan AeroStreamChunk)
	go func() {
		defer resp.Body.Close()
		defer close(ch)
		reader := NewEventStreamReader(resp.Body)
		for {
			event, err := reader.ReadEvent()
			if err != nil {
				if err != io.EOF {
					select {
					case ch <- AeroStreamChunk{Provider: a.name, Delta: "", Finish: false}:
					case <-ctx.Done():
					}
				}
				return
			}
			trimmed := strings.TrimSpace(event)
			if trimmed == "" || strings.HasPrefix(trimmed, ":") {
				continue
			}
			payload := trimmed
			if idx := strings.Index(trimmed, "data:"); idx >= 0 {
				payload = strings.TrimSpace(trimmed[idx+5:])
			}
			chunk, normErr := NewStreamNormalizer().Normalize(a.name, []byte(payload))
			if normErr != nil {
				select {
				case ch <- AeroStreamChunk{Provider: a.name, Delta: "", Finish: true}:
				case <-ctx.Done():
					return
				}
			}
			select {
			case ch <- chunk:
			case <-ctx.Done():
				return
			}
			if chunk.Finish {
				return
			}
		}
	}()
	return ch, nil
}

// Health returns the health status of the adapter.
func (a *OpenAICompatibleAdapter) Health() map[string]interface{} {
	return map[string]interface{}{"name": a.name, "type": a.providerType, "healthy": true}
}

// Close releases resources.
func (a *OpenAICompatibleAdapter) Close() error { return nil }

// bufferWriter is a simple write-flusher for multipart payloads.
type bufferWriter struct {
	io.Writer
}

// WriteString writes bytes from a string.
func (b *bufferWriter) WriteString(s string) (int, error) {
	return b.Writer.Write([]byte(s))
}

// Close is a no-op closer to satisfy request body building flow.
func (b *bufferWriter) Close() error { return nil }
