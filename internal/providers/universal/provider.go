package universal

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// AeroStreamChunk is the legacy simplified streaming chunk format used by
// ProviderAdapter.Stream. New code should use StreamChatCompletions, which
// yields OpenAI-compatible models.StreamChunk values.
type AeroStreamChunk struct {
	Delta       string
	Finish      bool
	Provider    string
	ContentType string
	// Data is the JSON encoding of the underlying models.StreamChunk.
	Data []byte
	// Err reports a stream failure; it is set on the last chunk only.
	Err error
}

// StreamProvider is implemented by adapters that can stream OpenAI-style
// chunks. It has the same method set as providers.StreamingProvider and
// follows the same contract.
type StreamProvider interface {
	StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error)
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

// DefaultTimeout is the default HTTP timeout for non-streaming upstream calls
// (for streams it only bounds the wait for response headers).
const DefaultTimeout = 120 * time.Second

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
		HTTP:    &http.Client{Timeout: DefaultTimeout},
	}
}

// ChatPayload is a simplified OpenAI-style chat body.
//
// Deprecated: adapters send providers.OpenAIChatRequest, which carries all
// supported fields and strips gateway-only ones.
type ChatPayload struct {
	Model            string                  `json:"model"`
	Messages         []models.Message        `json:"messages"`
	Stream           bool                    `json:"stream"`
	MaxTokens        *int                    `json:"max_tokens,omitempty"`
	Temperature      *float64                `json:"temperature,omitempty"`
	TopP             *float64                `json:"top_p,omitempty"`
	Stop             []string                `json:"stop,omitempty"`
	PresencePenalty  *float64                `json:"presence_penalty,omitempty"`
	FrequencyPenalty *float64                `json:"frequency_penalty,omitempty"`
	Tools            []models.ToolDefinition `json:"tools,omitempty"`
	ResponseFormat   *models.ResponseFormat  `json:"response_format,omitempty"`
}

// ToChatPayload converts a universal request into a ChatPayload.
//
// Deprecated: use providers.NewOpenAIChatRequest.
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
