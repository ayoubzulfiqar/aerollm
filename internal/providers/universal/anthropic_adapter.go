package universal

import (
	"context"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// AnthropicAdapter is a dedicated adapter for Anthropic's native Messages API
// (/v1/messages). It translates OpenAI-style requests (system prompts,
// multi-part content incl. images, tools, tool_choice, tool results) and
// responses, supports prompt caching via cache_control and streams
// Anthropic's SSE events as OpenAI-style chunks.
type AnthropicAdapter struct {
	name    string
	apiKey  string
	baseURL string
	model   string
	http    *http.Client

	endpoint    string
	endpointErr error
	modelsURL   string
	health      providers.HealthTracker
}

// NewAnthropicAdapterV2 creates a new Anthropic-specific adapter. baseURL may
// be empty (https://api.anthropic.com) and may include a trailing "/v1".
func NewAnthropicAdapterV2(apiKey, baseURL string) *AnthropicAdapter {
	a := &AnthropicAdapter{
		name:    "anthropic",
		apiKey:  apiKey,
		baseURL: baseURL,
		http:    &http.Client{Timeout: DefaultTimeout},
	}
	a.endpoint, a.endpointErr = providers.AnthropicMessagesURL(baseURL)
	if a.endpointErr == nil {
		a.modelsURL, a.endpointErr = providers.AnthropicModelsURL(baseURL)
	}
	return a
}

// Name returns the adapter name.
func (a *AnthropicAdapter) Name() string { return a.name }

// Type returns the adapter provider type as a string.
func (a *AnthropicAdapter) Type() string { return "anthropic" }

// ProviderType returns the provider type.
func (a *AnthropicAdapter) ProviderType() string { return "anthropic" }

// SetDefaultModel sets the model used when a request names none.
func (a *AnthropicAdapter) SetDefaultModel(model string) { a.model = model }

// SetHTTPClient replaces the HTTP client (e.g. to change timeouts).
func (a *AnthropicAdapter) SetHTTPClient(c *http.Client) {
	if c != nil {
		a.http = c
	}
}

func (a *AnthropicAdapter) prepare(req *models.LLMRequest, stream bool) (*providers.AnthropicRequest, error) {
	if a.endpointErr != nil {
		return nil, configError(a.name, a.endpointErr)
	}
	model := ""
	if req != nil && req.Model == "" {
		model = a.model
	}
	return providers.BuildAnthropicRequest(a.name, req, model, stream)
}

// ChatCompletions sends a chat completion request to Anthropic's
// /v1/messages endpoint and returns an OpenAI-style response. OpenAI
// response_format (json_object / json_schema) is emulated with a forced tool
// (see providers.BuildAnthropicRequest); seed and logprobs are dropped.
func (a *AnthropicAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	body, err := a.prepare(req, false)
	if err != nil {
		return nil, err
	}
	return timed(&a.health, func() (*models.LLMResponse, error) {
		return providers.AnthropicMessages(ctx, a.http, a.name, a.endpoint, providers.AnthropicHeaders(a.apiKey), body)
	})
}

// StreamChatCompletions implements providers.StreamingProvider:
// message_start / content_block_* / message_delta / message_stop events are
// converted into OpenAI-style chunks, including tool-call deltas and a final
// usage chunk.
func (a *AnthropicAdapter) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	body, err := a.prepare(req, true)
	if err != nil {
		return nil, err
	}
	return providers.AnthropicMessagesStream(ctx, a.http, a.name, a.endpoint, providers.AnthropicHeaders(a.apiKey), body, a.health.Observe)
}

// Stream sends a streaming chat completion request (legacy chunk format).
func (a *AnthropicAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	ch, err := a.StreamChatCompletions(ctx, req)
	if err != nil {
		return nil, err
	}
	return toAeroStream(ctx, a.name, ch), nil
}

// Probe implements providers.Prober with an authenticated GET /v1/models
// (bounded by providers.DefaultProbeTimeout). Its outcome is reflected in
// Health until a later probe or successful call. It is opt-in.
func (a *AnthropicAdapter) Probe(ctx context.Context) error {
	return providers.RunProbe(&a.health, func() error {
		if a.endpointErr != nil {
			return configError(a.name, a.endpointErr)
		}
		return providers.ProbeEndpoint(ctx, a.http, a.name, a.modelsURL, providers.AnthropicHeaders(a.apiKey))
	})
}

// Health returns the health status of the adapter.
func (a *AnthropicAdapter) Health() map[string]interface{} {
	return healthMap(a.name, "anthropic", &a.health, a.endpointErr)
}

// Close releases idle connections.
func (a *AnthropicAdapter) Close() error {
	if a.http != nil {
		a.http.CloseIdleConnections()
	}
	return nil
}

// safeDeref safely dereferences a *string.
func safeDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
