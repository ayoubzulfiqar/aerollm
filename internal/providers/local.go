package providers

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// LocalProvider implements the Provider interface for local OpenAI-compatible
// inference servers (vLLM, Ollama, llama.cpp, TGI). The base URL may be given
// with or without "/v1".
type LocalProvider struct {
	BaseURL string
	// Model is the model served locally; requests without a model use it.
	Model  string
	Client *http.Client

	models ModelList
	health HealthTracker
}

// NewLocalProvider creates a new local/vLLM provider.
func NewLocalProvider(baseURL, model string) *LocalProvider {
	return &LocalProvider{
		BaseURL: baseURL,
		Model:   model,
		Client:  &http.Client{Timeout: 300 * time.Second},
	}
}

// Name returns the provider name.
func (p *LocalProvider) Name() string { return "local" }

// Type returns the provider type.
func (p *LocalProvider) Type() ProviderType { return ProviderLocal }

// DefaultModel implements DefaultModeler.
func (p *LocalProvider) DefaultModel() string { return p.Model }

// SetModels overrides which models this provider serves (patterns as in
// MatchModel). By default it serves only its configured Model (or anything
// when Model is empty).
func (p *LocalProvider) SetModels(patterns ...string) { p.models.Set(patterns...) }

// SupportsModel implements ModelSupporter.
func (p *LocalProvider) SupportsModel(model string) bool {
	if model == "" {
		return true
	}
	if matched, ok := p.models.Match(model); ok {
		return matched
	}
	return p.Model == "" || strings.EqualFold(model, p.Model)
}

func (p *LocalProvider) client() *http.Client {
	if p.Client != nil {
		return p.Client
	}
	return http.DefaultClient
}

func (p *LocalProvider) endpoint() (string, error) {
	base, err := NormalizeBaseURL(p.BaseURL, "/v1")
	if err != nil {
		return "", &UpstreamError{Provider: p.Name(), StatusCode: http.StatusInternalServerError, Type: "configuration_error", Message: "provider base URL misconfigured: " + err.Error()}
	}
	return JoinURL(base, "/chat/completions"), nil
}

// Probe implements Prober with GET {base}/models (vLLM, Ollama, llama.cpp
// and LM Studio all serve it); the result is reflected in Health.
func (p *LocalProvider) Probe(ctx context.Context) error {
	return RunProbe(&p.health, func() error {
		base, err := NormalizeBaseURL(p.BaseURL, "/v1")
		if err != nil {
			return &UpstreamError{Provider: p.Name(), StatusCode: http.StatusInternalServerError, Type: "configuration_error", Message: "provider base URL misconfigured: " + err.Error()}
		}
		return ProbeEndpoint(ctx, p.client(), p.Name(), JoinURL(base, "/models"), nil)
	})
}

func (p *LocalProvider) model(req *models.LLMRequest) string {
	if req.Model == "" {
		return p.Model
	}
	return req.Model
}

// ChatCompletions sends a chat completion request to the local endpoint.
func (p *LocalProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	if req == nil {
		return nil, invalidRequest(p.Name(), "request is nil")
	}
	endpoint, err := p.endpoint()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	resp, err := OpenAIChatCompletion(ctx, p.client(), p.Name(), endpoint, http.Header{}, NewOpenAIChatRequest(req, p.model(req), false, false))
	p.health.Observe(time.Since(start), err)
	return resp, err
}

// StreamChatCompletions implements StreamingProvider.
func (p *LocalProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	if req == nil {
		return nil, invalidRequest(p.Name(), "request is nil")
	}
	endpoint, err := p.endpoint()
	if err != nil {
		return nil, err
	}
	return OpenAIChatStream(ctx, p.client(), p.Name(), endpoint, http.Header{}, NewOpenAIChatRequest(req, p.model(req), true, true), p.health.Observe)
}

// Health returns health derived from recent calls.
func (p *LocalProvider) Health() ProviderHealth {
	h := p.health.Snapshot(p.Name(), p.Type())
	if _, err := NormalizeBaseURL(p.BaseURL, "/v1"); err != nil {
		h.Healthy = false
	}
	return h
}

// Close releases any resources held by the local provider.
func (p *LocalProvider) Close() error {
	if p.Client != nil {
		p.Client.CloseIdleConnections()
	}
	return nil
}
