package universal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// AnthropicAdapter is a dedicated adapter for Anthropic's API.
// Unlike the generic OpenAICompatibleAdapter, this adapter uses Anthropic's
// native /v1/messages endpoint and supports prompt caching (cache_control).
type AnthropicAdapter struct {
	name     string
	apiKey   string
	baseURL  string
	model    string
	http     *http.Client
}

// NewAnthropicAdapterV2 creates a new Anthropic-specific adapter.
// This replaces the generic OpenAICompatibleAdapter for Anthropic.
func NewAnthropicAdapterV2(apiKey, baseURL string) *AnthropicAdapter {
	return &AnthropicAdapter{
		name:    "anthropic",
		apiKey:  apiKey,
		baseURL: strings.TrimSuffix(baseURL, "/"),
		http:    &http.Client{Timeout: 60 * time.Second},
	}
}

// Name returns the adapter name.
func (a *AnthropicAdapter) Name() string { return a.name }

// Type returns the adapter provider type as a string.
func (a *AnthropicAdapter) Type() string { return "anthropic" }

// ProviderType returns the provider type.
func (a *AnthropicAdapter) ProviderType() string { return "anthropic" }

// ChatCompletions sends a chat completion request to Anthropic's /v1/messages endpoint.
//
// Anthropic Prompt Caching: when messages contain CacheControl with type "ephemeral",
// the adapter inserts a cache_control block on the last user message before the
// breakpoint. This enables Anthropic's prompt caching feature, which can reduce
// costs by up to 90% for long, cacheable prompts.
//
// The request body follows Anthropic's native format with:
//   - model, max_tokens, temperature, top_p
//   - messages array with cache_control annotations
//   - tools array (for tool-use capable models)
//   - system message (if present)
func (a *AnthropicAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	anthropicReq, err := a.buildRequest(req)
	if err != nil {
		return nil, fmt.Errorf("build anthropic request: %w", err)
	}

	body, err := jsonMarshal(anthropicReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	endpoint := a.baseURL + "/v1/messages"
	if a.baseURL == "" {
		endpoint = "https://api.anthropic.com/v1/messages"
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build http request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	// Anthropic uses beta header for prompt caching.
	httpReq.Header.Set("anthropic-beta", "prompt-caching-2024-02-15")
	if a.apiKey != "" {
		httpReq.Header.Set("x-api-key", a.apiKey)
	}

	resp, err := a.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("anthropic error: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var anthropicResp anthropicResponse
	if err := jsonUnmarshal(b, &anthropicResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return a.normalizeResponse(anthropicResp, req.Model), nil
}

// anthropicMessage is a message in Anthropic's native format.
type anthropicMessage struct {
	Role    string             `json:"role"`
	Content []anthropicContent `json:"content"`
}

// anthropicContent represents a single content block in an Anthropic message.
// For text content, it may include cache_control to enable prompt caching.
type anthropicContent struct {
	Type          string         `json:"type"`          // "text" | "tool_use" | "tool_result"
	Text          string         `json:"text,omitempty"`
	CacheControl  *cacheControl  `json:"cache_control,omitempty"`
	ToolUse       *toolUseBlock  `json:"tool_use,omitempty"`
	ToolResult    *toolResultBlock `json:"tool_result,omitempty"`
}

// cacheControl enables Anthropic prompt caching for this content block.
type cacheControl struct {
	Type string `json:"type"` // "ephemeral"
}

// toolUseBlock represents a tool_use content block.
type toolUseBlock struct {
	Name string `json:"name"`
	Input map[string]interface{} `json:"input"`
}

// toolResultBlock represents a tool_result content block.
type toolResultBlock struct {
	ToolUseID string `json:"tool_use_id"`
	Content   string `json:"content"`
}

// anthropicRequest is the full request body for Anthropic's /v1/messages endpoint.
type anthropicRequest struct {
	Model         string            `json:"model"`
	MaxTokens     int               `json:"max_tokens"`
	Temperature   *float64          `json:"temperature,omitempty"`
	TopP          *float64          `json:"top_p,omitempty"`
	Messages      []anthropicMessage `json:"messages"`
	System        string            `json:"system,omitempty"`
	Tools         []anthropicTool   `json:"tools,omitempty"`
	Stream        bool              `json:"stream,omitempty"`
}

// anthropicTool represents a tool definition for Anthropic's API.
type anthropicTool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	InputSchema map[string]interface{} `json:"input_schema"`
}

// anthropicResponse is the response from Anthropic's API.
type anthropicResponse struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"`
	Role         string                 `json:"role"`
	Content      []anthropicContent     `json:"content"`
	Model        string                 `json:"model"`
	StopReason   string                 `json:"stop_reason"`
	Usage        anthropicUsage         `json:"usage"`
}

// anthropicUsage holds token usage from Anthropic's API.
type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// buildRequest converts a universal LLMRequest into Anthropic's native format.
func (a *AnthropicAdapter) buildRequest(req *models.LLMRequest) (anthropicRequest, error) {
	var messages []anthropicMessage
	var systemMsg string

	for _, msg := range req.Messages {
		content := anthropicContent{
			Type: "text",
		}
		if msg.Content != nil {
			content.Text = *msg.Content
		}

		// Handle prompt caching: if the message has CacheControl,
		// propagate it to the Anthropic content block.
		if msg.CacheControl != nil && msg.CacheControl.Type == "ephemeral" {
			content.CacheControl = &cacheControl{Type: "ephemeral"}
		}

		msgContent := []anthropicContent{content}

		// Handle tool calls on assistant messages.
		if len(msg.ToolCalls) > 0 {
			for _, tc := range msg.ToolCalls {
				msgContent = append(msgContent, anthropicContent{
					Type:     "tool_use",
					ToolUse: &toolUseBlock{
						Name:  tc.Function.Name,
						Input: parseToolInput(tc.Function.Arguments),
					},
				})
			}
		}

		// Handle tool results on tool messages.
		if msg.ToolResult != nil && msg.Role == models.RoleTool {
			msgContent = append(msgContent, anthropicContent{
				Type: "tool_result",
				ToolResult: &toolResultBlock{
					ToolUseID: safeDeref(msg.ToolCallID),
					Content:   *msg.ToolResult,
				},
			})
		}

		role := string(msg.Role)
		if role == "system" {
			systemMsg = content.Text
			continue
		}

		messages = append(messages, anthropicMessage{
			Role:    role,
			Content: msgContent,
		})
	}

	// Build tools list.
	var tools []anthropicTool
	for _, t := range req.Tools {
		tools = append(tools, anthropicTool{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: t.Parameters,
		})
	}

	// Determine max_tokens (default to 4096 if not specified).
	maxTokens := 4096
	if req.MaxTokens != nil {
		maxTokens = *req.MaxTokens
	}

	return anthropicRequest{
		Model:       req.Model,
		MaxTokens:   maxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		Messages:    messages,
		System:      systemMsg,
		Tools:       tools,
		Stream:      req.Stream,
	}, nil
}

// normalizeResponse converts Anthropic's response format to the unified LLMResponse.
func (a *AnthropicAdapter) normalizeResponse(resp anthropicResponse, model string) *models.LLMResponse {
	choices := make([]models.Choice, 0, len(resp.Content))
	for i, content := range resp.Content {
		choice := models.Choice{
			Index:        i,
			FinishReason: resp.StopReason,
		}
		msg := models.Message{
			Role: models.RoleAssistant,
		}
		if content.Type == "text" {
			msg.Content = &content.Text
		}
		choice.Message = msg
		choices = append(choices, choice)
	}

	return &models.LLMResponse{
		ID:      resp.ID,
		Model:   resp.Model,
		Choices: choices,
		Usage: &models.Usage{
			PromptTokens:     resp.Usage.InputTokens,
			CompletionTokens: resp.Usage.OutputTokens,
			TotalTokens:      resp.Usage.InputTokens + resp.Usage.OutputTokens,
		},
	}
}

// Health returns the health status of the adapter.
func (a *AnthropicAdapter) Health() map[string]interface{} {
	return map[string]interface{}{"name": a.name, "type": "anthropic", "healthy": true}
}

// Close releases resources.
func (a *AnthropicAdapter) Close() error { return nil }

// Stream sends a streaming chat completion request to Anthropic.
// Currently delegates to ChatCompletions and returns the full response as a single chunk.
// Full streaming support will be added in a future revision.
func (a *AnthropicAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	ch := make(chan AeroStreamChunk)
	go func() {
		defer close(ch)
		resp, err := a.ChatCompletions(ctx, req)
		if err != nil {
			ch <- AeroStreamChunk{Provider: a.name, Delta: "", Finish: true, Data: []byte(err.Error())}
			return
		}
		respBytes, _ := jsonMarshal(resp)
		ch <- AeroStreamChunk{Provider: a.name, Delta: string(respBytes), Finish: true}
	}()
	return ch, nil
}

// parseToolInput parses a JSON string into a map for tool input.
func parseToolInput(input string) map[string]interface{} {
	var result map[string]interface{}
	if err := jsonUnmarshal([]byte(input), &result); err != nil {
		return map[string]interface{}{"raw": input}
	}
	return result
}

// safeDeref safely dereferences a *string.
func safeDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
