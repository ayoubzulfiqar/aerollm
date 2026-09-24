package providers

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// DefaultOpenAIBaseURL is used when no base URL is configured.
const DefaultOpenAIBaseURL = "https://api.openai.com/v1"

// OpenAIProvider calls an OpenAI-compatible /chat/completions endpoint.
// The base URL may be given with or without the "/v1" suffix.
type OpenAIProvider struct {
	name         string
	providerType ProviderType
	apiKey       string
	baseURL      string
	base         *url.URL
	baseErr      error
	http         *http.Client
	models       ModelList
	health       HealthTracker
}

// NewOpenAIProvider creates a new OpenAIProvider. An empty baseURL means
// DefaultOpenAIBaseURL; a base URL without a path gets "/v1" appended.
func NewOpenAIProvider(name, apiKey, baseURL string) *OpenAIProvider {
	if baseURL == "" {
		baseURL = DefaultOpenAIBaseURL
	}
	if name == "" {
		name = "openai"
	}
	base, err := NormalizeBaseURL(baseURL, "/v1")
	return &OpenAIProvider{
		name:         name,
		providerType: ProviderOpenAI,
		apiKey:       apiKey,
		baseURL:      baseURL,
		base:         base,
		baseErr:      err,
		http:         &http.Client{Timeout: 120 * time.Second},
	}
}

// SetHTTPClient replaces the HTTP client (e.g. to change timeouts).
func (p *OpenAIProvider) SetHTTPClient(c *http.Client) {
	if c != nil {
		p.http = c
	}
}

// SetModels restricts the models this provider serves (patterns as in
// MatchModel). With no patterns the provider accepts every model.
func (p *OpenAIProvider) SetModels(patterns ...string) { p.models.Set(patterns...) }

// SupportsModel implements ModelSupporter.
func (p *OpenAIProvider) SupportsModel(model string) bool {
	if model == "" {
		return true
	}
	matched, ok := p.models.Match(model)
	return !ok || matched
}

func (p *OpenAIProvider) endpoint() (string, error) {
	if p.baseErr != nil {
		return "", &UpstreamError{Provider: p.name, StatusCode: http.StatusInternalServerError, Type: "configuration_error", Message: "provider base URL misconfigured: " + p.baseErr.Error()}
	}
	return JoinURL(p.base, "/chat/completions"), nil
}

func (p *OpenAIProvider) headers() http.Header {
	h := http.Header{}
	if p.apiKey != "" {
		h.Set("Authorization", "Bearer "+p.apiKey)
	}
	return h
}

// ChatCompletions sends a chat completion request.
func (p *OpenAIProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	if req == nil {
		return nil, invalidRequest(p.name, "request is nil")
	}
	endpoint, err := p.endpoint()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	resp, err := OpenAIChatCompletion(ctx, p.http, p.name, endpoint, p.headers(), NewOpenAIChatRequest(req, "", false, false))
	p.health.Observe(time.Since(start), err)
	return resp, err
}

// StreamChatCompletions implements StreamingProvider. It requests a final
// usage chunk via stream_options.include_usage.
func (p *OpenAIProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	if req == nil {
		return nil, invalidRequest(p.name, "request is nil")
	}
	endpoint, err := p.endpoint()
	if err != nil {
		return nil, err
	}
	return OpenAIChatStream(ctx, p.http, p.name, endpoint, p.headers(), NewOpenAIChatRequest(req, "", true, true), p.health.Observe)
}

// Name returns provider name.
func (p *OpenAIProvider) Name() string { return p.name }

// Type returns provider type.
func (p *OpenAIProvider) Type() ProviderType { return p.providerType }

// Health returns health derived from recent calls.
func (p *OpenAIProvider) Health() ProviderHealth {
	h := p.health.Snapshot(p.name, p.providerType)
	if p.baseErr != nil {
		h.Healthy = false
	}
	return h
}

// Close releases resources.
func (p *OpenAIProvider) Close() error {
	p.http.CloseIdleConnections()
	return nil
}
