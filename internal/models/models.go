package models

import (
	"time"
)

// MessageRole represents the role of a message in the conversation.
type MessageRole string

const (
	RoleSystem    MessageRole = "system"
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleTool      MessageRole = "tool"
)

// Message represents a single message in the LLM conversation history.
type Message struct {
	Role       MessageRole `json:"role"`
	Content    *string     `json:"content,omitempty"`
	Name       *string     `json:"name,omitempty"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
	ToolCallID *string     `json:"tool_call_id,omitempty"`
	ToolResult *string     `json:"tool_result,omitempty"`
}

// ToolCall represents a single tool call requested by the LLM.
type ToolCall struct {
	ID       string     `json:"id"`
	Type     string     `json:"type"`
	Function ToolFunction `json:"function"`
}

// ToolFunction represents the function definition within a tool call.
type ToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolDefinition represents a tool available to the agent.
type ToolDefinition struct {
	Name        string                  `json:"name"`
	Description string                  `json:"description"`
	Parameters  map[string]interface{}  `json:"parameters"`
}

// LLMRequest is the unified request structure sent to any LLM provider.
type LLMRequest struct {
	Model            string            `json:"model"`
	Messages         []Message         `json:"messages"`
	MaxTokens        *int              `json:"max_tokens,omitempty"`
	Temperature      *float64          `json:"temperature,omitempty"`
	TopP             *float64          `json:"top_p,omitempty"`
	Stream           bool              `json:"stream"`
	Stop             []string          `json:"stop,omitempty"`
	PresencePenalty  *float64          `json:"presence_penalty,omitempty"`
	FrequencyPenalty *float64          `json:"frequency_penalty,omitempty"`
	Tools            []ToolDefinition  `json:"tools,omitempty"`
	RagEnabled       bool              `json:"rag_enabled,omitempty"`
}

// LLMResponse is the unified response structure from any LLM provider.
type LLMResponse struct {
	ID             string     `json:"id"`
	Object         string     `json:"object"`
	Created        int64      `json:"created"`
	Model          string     `json:"model"`
	Choices        []Choice   `json:"choices"`
	Usage          *Usage     `json:"usage,omitempty"`
}

// Choice represents a single response choice.
type Choice struct {
	Index        int         `json:"index"`
	Message      Message     `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

// Usage represents token usage statistics.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ToolResult holds the result of executing a tool call.
type ToolResult struct {
	ToolCallID string
	Name       string
	Content    interface{}
	Error      error
	Duration   time.Duration
	Cached     bool
}

// ContentToString returns the tool result content as a string.
func (t *ToolResult) ContentToString() string {
	if t.Content == nil {
		return ""
	}
	switch v := t.Content.(type) {
	case string:
		return v
	default:
		return ""
	}
}

// TraceIDContextKey is the context key for trace IDs.
const TraceIDContextKey = "trace_id"

// GenerateTraceID creates a new trace ID.
func GenerateTraceID() string {
	return ""
}

// ChatRequest is the request body for chat completions endpoints.
type ChatRequest struct {
	Model            string            `json:"model"`
	Messages         []Message         `json:"messages"`
	MaxTokens        *int              `json:"max_tokens,omitempty"`
	Temperature      *float64          `json:"temperature,omitempty"`
	TopP             *float64          `json:"top_p,omitempty"`
	Stream           bool              `json:"stream"`
	Stop             []string          `json:"stop,omitempty"`
	PresencePenalty  *float64          `json:"presence_penalty,omitempty"`
	FrequencyPenalty *float64          `json:"frequency_penalty,omitempty"`
	Tools            []ToolDefinition  `json:"tools,omitempty"`
	RagEnabled       bool              `json:"rag_enabled,omitempty"`
}

// ChatResponse is the response body for chat completions endpoints.
type ChatResponse struct {
	ID             string     `json:"id"`
	Object         string     `json:"object"`
	Created        int64      `json:"created"`
	Model          string     `json:"model"`
	Choices        []Choice   `json:"choices"`
	Usage          *Usage     `json:"usage,omitempty"`
}

// EmbeddingRequest is the request body for embeddings endpoints.
type EmbeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

// EmbeddingResponse is the response body for embeddings endpoints.
type EmbeddingResponse struct {
	Object string      `json:"object"`
	Data   []Embedding `json:"data"`
	Model  string      `json:"model"`
	Usage  *Usage      `json:"usage,omitempty"`
}

// Embedding represents a single embedding vector.
type Embedding struct {
	Object    string    `json:"object"`
	Embedding []float64 `json:"embedding"`
	Index     int       `json:"index"`
}

// ImageRequest is the request body for image generation endpoints.
type ImageRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	N      int    `json:"n,omitempty"`
	Size   string `json:"size,omitempty"`
}

// ImageResponse is the response body for image generation endpoints.
type ImageResponse struct {
	Created int64     `json:"created"`
	Data    []Image   `json:"data"`
}

// Image represents a single generated image.
type Image struct {
	URL string `json:"url,omitempty"`
	B64 string `json:"b64_json,omitempty"`
}

// AudioRequest is the request body for audio transcription/speech endpoints.
type AudioRequest struct {
	Model  string `json:"model"`
	File   string `json:"file,omitempty"`
	Prompt string `json:"prompt,omitempty"`
}

// AudioResponse is the response body for audio transcription endpoints.
type AudioResponse struct {
	Text string `json:"text"`
}

// ResponsesRequest is the request body for OpenAI-compatible responses endpoints.
type ResponsesRequest struct {
	Model     string    `json:"model"`
	Input     string    `json:"input"`
	Previous  *string   `json:"previous_response_id,omitempty"`
	Tools     []ToolDefinition `json:"tools,omitempty"`
}

// ResponsesResponse is the response body for OpenAI-compatible responses endpoints.
type ResponsesResponse struct {
	ID             string     `json:"id"`
	Object         string     `json:"object"`
	CreatedAt      int64      `json:"created_at"`
	Model          string     `json:"model"`
	Output         []Message  `json:"output"`
	Usage          *Usage     `json:"usage,omitempty"`
}
