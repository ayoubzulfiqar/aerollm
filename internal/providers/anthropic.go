package providers

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// DefaultAnthropicBaseURL is used when no base URL is configured.
const DefaultAnthropicBaseURL = "https://api.anthropic.com"

// AnthropicProvider implements the Provider interface for Anthropic's native
// Messages API, translating OpenAI-style requests and responses.
type AnthropicProvider struct {
	BaseURL string
	APIKey  string
	// Model is the default model, used when a request names none.
	Model  string
	Client *http.Client

	models ModelList
	health HealthTracker
}

// NewAnthropicProvider creates a new Anthropic provider.
func NewAnthropicProvider(baseURL, apiKey, model string) *AnthropicProvider {
	return &AnthropicProvider{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Model:   model,
		Client:  &http.Client{Timeout: 120 * time.Second},
	}
}

// AnthropicMessagesURL returns the /v1/messages URL for a base URL given
// with or without a trailing "/v1".
func AnthropicMessagesURL(baseURL string) (string, error) {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultAnthropicBaseURL
	}
	u, err := NormalizeBaseURL(baseURL, "")
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimSuffix(u.Path, "/v1")
	return JoinURL(u, "/v1/messages"), nil
}

// AnthropicModelsURL returns the /v1/models URL (used for health probes) for
// a base URL given with or without a trailing "/v1".
func AnthropicModelsURL(baseURL string) (string, error) {
	messages, err := AnthropicMessagesURL(baseURL)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(messages)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimSuffix(u.Path, "/messages") + "/models"
	u.RawPath = ""
	return u.String(), nil
}

// Probe implements Prober with GET /v1/models; the result is reflected in
// Health.
func (p *AnthropicProvider) Probe(ctx context.Context) error {
	return RunProbe(&p.health, func() error {
		endpoint, err := AnthropicModelsURL(p.BaseURL)
		if err != nil {
			return &UpstreamError{Provider: p.Name(), StatusCode: http.StatusInternalServerError, Type: "configuration_error", Message: "provider base URL misconfigured: " + err.Error()}
		}
		return ProbeEndpoint(ctx, p.client(), p.Name(), endpoint, AnthropicHeaders(p.APIKey))
	})
}

// Name returns the provider name.
func (p *AnthropicProvider) Name() string { return "anthropic" }

// Type returns the provider type.
func (p *AnthropicProvider) Type() ProviderType { return ProviderAnthropic }

// DefaultModel implements DefaultModeler.
func (p *AnthropicProvider) DefaultModel() string { return p.Model }

// SetModels overrides which models this provider serves (patterns as in
// MatchModel). By default it serves "claude*" models and its default model.
func (p *AnthropicProvider) SetModels(patterns ...string) { p.models.Set(patterns...) }

// SupportsModel implements ModelSupporter.
func (p *AnthropicProvider) SupportsModel(model string) bool {
	if model == "" {
		return p.Model != ""
	}
	if matched, ok := p.models.Match(model); ok {
		return matched
	}
	return MatchModel("claude*", model) || (p.Model != "" && strings.EqualFold(model, p.Model))
}

func (p *AnthropicProvider) client() *http.Client {
	if p.Client != nil {
		return p.Client
	}
	return http.DefaultClient
}

func (p *AnthropicProvider) prepare(req *models.LLMRequest, stream bool) (string, *AnthropicRequest, error) {
	endpoint, err := AnthropicMessagesURL(p.BaseURL)
	if err != nil {
		return "", nil, &UpstreamError{Provider: p.Name(), StatusCode: http.StatusInternalServerError, Type: "configuration_error", Message: "provider base URL misconfigured: " + err.Error()}
	}
	model := ""
	if req != nil && req.Model == "" {
		model = p.Model
	}
	body, err := BuildAnthropicRequest(p.Name(), req, model, stream)
	if err != nil {
		return "", nil, err
	}
	return endpoint, body, nil
}

// ChatCompletions sends a chat completion request to Anthropic.
func (p *AnthropicProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	endpoint, body, err := p.prepare(req, false)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	resp, err := AnthropicMessages(ctx, p.client(), p.Name(), endpoint, AnthropicHeaders(p.APIKey), body)
	p.health.Observe(time.Since(start), err)
	return resp, err
}

// StreamChatCompletions implements StreamingProvider.
func (p *AnthropicProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	endpoint, body, err := p.prepare(req, true)
	if err != nil {
		return nil, err
	}
	return AnthropicMessagesStream(ctx, p.client(), p.Name(), endpoint, AnthropicHeaders(p.APIKey), body, p.health.Observe)
}

// Health returns health derived from recent calls.
func (p *AnthropicProvider) Health() ProviderHealth {
	h := p.health.Snapshot(p.Name(), p.Type())
	if _, err := AnthropicMessagesURL(p.BaseURL); err != nil {
		h.Healthy = false
	}
	return h
}

// Close releases any resources held by the Anthropic provider.
func (p *AnthropicProvider) Close() error {
	if p.Client != nil {
		p.Client.CloseIdleConnections()
	}
	return nil
}
