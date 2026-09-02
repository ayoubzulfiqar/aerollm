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
