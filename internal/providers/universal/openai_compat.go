package universal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// OpenAICompatibleAdapter is a reusable adapter for OpenAI-compatible APIs.
type OpenAICompatibleAdapter struct {
	name         string
	providerType string
	cfg          AdapterConfig
	http         *http.Client
}

func NewOpenAICompatibleAdapter(name, providerType, apiKey, baseURL string) *OpenAICompatibleAdapter {
	cfg := NewDefaultAdapterConfig(apiKey, baseURL)
	return &OpenAICompatibleAdapter{
		name:         name,
		providerType: providerType,
		cfg:          cfg,
		http:         cfg.HTTP,
	}
}

func (a *OpenAICompatibleAdapter) Name() string { return a.name }
func (a *OpenAICompatibleAdapter) Type() string { return a.providerType }

func (a *OpenAICompatibleAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	body, err := jsonMarshal(req)
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

func (a *OpenAICompatibleAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	body, err := jsonMarshal(req)
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

func (a *OpenAICompatibleAdapter) Health() map[string]interface{} {
	return map[string]interface{}{"name": a.name, "type": a.providerType, "healthy": true}
}

func (a *OpenAICompatibleAdapter) Close() error { return nil }
