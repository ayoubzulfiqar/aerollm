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

// AnthropicProvider implements the Provider interface for Anthropic.
type AnthropicProvider struct {
	BaseURL string
	APIKey  string
	Model   string
	Client  *http.Client
}

// NewAnthropicProvider creates a new Anthropic provider.
func NewAnthropicProvider(baseURL, apiKey, model string) *AnthropicProvider {
	return &AnthropicProvider{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Model:   model,
		Client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Name returns the provider name.
func (p *AnthropicProvider) Name() string { return "anthropic" }

// Type returns the provider type.
func (p *AnthropicProvider) Type() ProviderType { return ProviderAnthropic }

// ChatCompletions sends a chat completion request to Anthropic.
func (p *AnthropicProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("x-api-key", p.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("anthropic-version", "2023-06-01")

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

// Health returns the current health status of the Anthropic provider.
func (p *AnthropicProvider) Health() ProviderHealth {
	return ProviderHealth{
		Name:        p.Name(),
		Type:        p.Type(),
		Healthy:     true,
		CircuitOpen: false,
	}
}

// Close releases any resources held by the Anthropic provider.
func (p *AnthropicProvider) Close() error { return nil }
