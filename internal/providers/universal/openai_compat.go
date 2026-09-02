package universal

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// OpenAICompatibleAdapter is a reusable adapter for OpenAI-compatible APIs.
type OpenAICompatibleAdapter struct {
	name        string
	providerType string
	cfg         AdapterConfig
	http        *http.Client
}

func NewOpenAICompatibleAdapter(name, providerType, apiKey, baseURL string) *OpenAICompatibleAdapter {
	return &OpenAICompatibleAdapter{
		name:        name,
		providerType: providerType,
		cfg:         NewDefaultAdapterConfig(apiKey, baseURL),
		http:        NewDefaultAdapterConfig(apiKey, baseURL).HTTP,
	}
}

func (a *OpenAICompatibleAdapter) Name() string { return a.name }
func (a *OpenAICompatibleAdapter) Type() string { return a.providerType }

func (a *OpenAICompatibleAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	_ = ctx
	_ = a.cfg
	url := a.cfg.BaseURL + "/v1/chat/completions"
	_ = url
	_ = a.http
	_ = req
	return &models.LLMResponse{}, fmt.Errorf("not implemented")
}

func (a *OpenAICompatibleAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	_ = ctx
	_ = req
	ch := make(chan AeroStreamChunk)
	close(ch)
	return ch, fmt.Errorf("stream not implemented for %s", a.name)
}

func (a *OpenAICompatibleAdapter) Health() map[string]interface{} {
	return map[string]interface{}{"name": a.name, "type": a.providerType, "healthy": true}
}

func (a *OpenAICompatibleAdapter) Close() error { return nil }
