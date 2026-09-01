package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// OpenAIProvider calls OpenAI-compatible /v1/chat/completions.
type OpenAIProvider struct {
	name      string
	providerType ProviderType
	apiKey    string
	baseURL   string
	http      *http.Client
}

// NewOpenAIProvider creates a new OpenAIProvider.
func NewOpenAIProvider(name, apiKey, baseURL string) *OpenAIProvider {
	return &OpenAIProvider{
		name:       name,
		providerType: ProviderOpenAI,
		apiKey:     apiKey,
		baseURL:    baseURL,
		http:       &http.Client{Timeout: 60 * time.Second},
	}
}

// ChatCompletions sends a chat completion request.
func (p *OpenAIProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	_ = ctx
	url := p.baseURL + "/v1/chat/completions"
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var llmResp models.LLMResponse
	if err := json.Unmarshal(b, &llmResp); err != nil {
		return nil, fmt.Errorf("openai response unmarshal failed: %w", err)
	}
	return &llmResp, nil
}

// Name returns provider name.
func (p *OpenAIProvider) Name() string { return p.name }

// Type returns provider type.
func (p *OpenAIProvider) Type() ProviderType { return p.providerType }

// Health returns health status.
func (p *OpenAIProvider) Health() ProviderHealth {
	return ProviderHealth{Name: p.name, Type: p.providerType, Healthy: true}
}

// Close releases resources.
func (p *OpenAIProvider) Close() error { return nil }
