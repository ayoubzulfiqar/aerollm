package universal

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// AeroStreamChunk is the unified streaming chunk format.
type AeroStreamChunk struct {
	Delta       string
	Finish      bool
	Provider    string
	ContentType string
	Data        []byte
}

// StreamProvider extends Provider with streaming support.
type StreamProvider interface {
	StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error)
}

// ProviderAdapter is the unified provider contract for the universal registry.
type ProviderAdapter interface {
	Name() string
	Type() string
	ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error)
	Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error)
	Health() map[string]interface{}
	Close() error
}

// AdapterConfig carries common adapter settings.
type AdapterConfig struct {
	APIKey  string
	BaseURL string
	HTTP    *http.Client
}

// NewDefaultAdapterConfig creates a sane default HTTP client config.
func NewDefaultAdapterConfig(apiKey, baseURL string) AdapterConfig {
	return AdapterConfig{
		APIKey:  apiKey,
		BaseURL: baseURL,
		HTTP:    &http.Client{Timeout: 60 * time.Second},
	}
}

// ChatPayload is the common JSON body for OpenAI-compatible chat routes.
type ChatPayload struct {
	Model          string                   `json:"model"`
	Messages       []models.Message         `json:"messages"`
	Stream         bool                     `json:"stream"`
	MaxTokens      *int                     `json:"max_tokens,omitempty"`
	Temperature    *float64                 `json:"temperature,omitempty"`
	TopP           *float64                 `json:"top_p,omitempty"`
	Stop           []string                 `json:"stop,omitempty"`
	PresencePenalty *float64                `json:"presence_penalty,omitempty"`
	FrequencyPenalty *float64               `json:"frequency_penalty,omitempty"`
	Tools          []models.ToolDefinition  `json:"tools,omitempty"`
	ResponseFormat *models.ResponseFormat   `json:"response_format,omitempty"`
}

// ToChatPayload converts a universal request into an OpenAI-style payload.
func ToChatPayload(req *models.LLMRequest) ChatPayload {
	return ChatPayload{
		Model:            req.Model,
		Messages:         req.Messages,
		Stream:           false,
		MaxTokens:        req.MaxTokens,
		Temperature:      req.Temperature,
		TopP:             req.TopP,
		Stop:             req.Stop,
		PresencePenalty:  req.PresencePenalty,
		FrequencyPenalty: req.FrequencyPenalty,
		Tools:            req.Tools,
		ResponseFormat:   req.ResponseFormat,
	}
}

// MarshalChatPayload JSON-encodes a ChatPayload.
func MarshalChatPayload(p ChatPayload) ([]byte, error) {
	return json.Marshal(p)
}

// buildChatPayload converts a universal LLMRequest into an OpenAI-compatible
// chat payload, including response_format for Structured Outputs.
func buildChatPayload(req *models.LLMRequest) ChatPayload {
	return ToChatPayload(req)
}
