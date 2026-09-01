package providers

import (
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// LocalProvider implements the Provider interface for local/vLLM inference.
type LocalProvider struct {
	BaseURL string
	Model   string
	Client  *http.Client
}

// NewLocalProvider creates a new local/vLLM provider.
func NewLocalProvider(baseURL, model string) *LocalProvider {
	return &LocalProvider{
		BaseURL: baseURL,
		Model:   model,
		Client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// Name returns the provider name.
func (p *LocalProvider) Name() string { return "local" }

// Type returns the provider type.
func (p *LocalProvider) Type() ProviderType { return ProviderLocal }

// ChatCompletions sends a chat completion request to the local/vLLM endpoint.
func (p *LocalProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := p.Client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("provider returned error %d: %s", resp.StatusCode, string(respBody))
	}

	var llmResp models.LLMResponse
	if err := json.Unmarshal(respBody, &llmResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &llmResp, nil
}

// Health returns the current health status of the local provider.
func (p *LocalProvider) Health() ProviderHealth {
	return ProviderHealth{
		Name:        p.Name(),
		Type:        p.Type(),
		Healthy:     true,
		CircuitOpen: false,
	}
}

// Close releases any resources held by the local provider.
func (p *LocalProvider) Close() error { return nil }
