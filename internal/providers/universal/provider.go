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
	Model       string          `json:"model"`
	Messages    []models.Message `json:"messages"`
	Stream      bool            `json:"stream"`
	MaxTokens   *int            `json:"max_tokens,omitempty"`
	Temperature *float64        `json:"temperature,omitempty"`
}

// ToChatPayload converts a universal request into an OpenAI-style payload.
func ToChatPayload(req *models.LLMRequest) ChatPayload {
	return ChatPayload{
		Model:       req.Model,
		Messages:    req.Messages,
		Stream:      false,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
	}
}

// MarshalChatPayload JSON-encodes a ChatPayload.
func MarshalChatPayload(p ChatPayload) ([]byte, error) {
	return json.Marshal(p)
}
