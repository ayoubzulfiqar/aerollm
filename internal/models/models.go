package models

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// MessageRole represents the role of a message in the conversation.
type MessageRole string

const (
	RoleSystem    MessageRole = "system"
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleTool      MessageRole = "tool"
	// RoleDeveloper is OpenAI's replacement for "system" on newer models.
	RoleDeveloper MessageRole = "developer"
)

// Message represents a single message in the LLM conversation history.
//
// On the wire "content" may be a JSON string or an array of content parts
// (OpenAI multi-modal format). When an array is received, ContentParts holds
// the parts and Content holds the concatenated text of the text parts (nil
// when there are none), so text-only consumers keep working. When
// marshalling, ContentParts wins; if Content was modified after decoding
// (e.g. redaction), the text parts are replaced by a single text part holding
// the new Content and the non-text parts are kept.
type Message struct {
	Role    MessageRole `json:"role"`
	Content *string     `json:"content,omitempty"`
	// ContentParts holds array-form content (text, image_url, input_audio,
	// file). It is serialized as "content".
	ContentParts []ContentPart `json:"-"`
	Name         *string       `json:"name,omitempty"`
	ToolCalls    []ToolCall    `json:"tool_calls,omitempty"`
	ToolCallID   *string       `json:"tool_call_id,omitempty"`
	ToolResult   *string       `json:"tool_result,omitempty"`
	// CacheControl enables prompt caching (Anthropic beta feature).
	// When set to {"type":"ephemeral"}, Anthropic caches this message to reduce costs.
	CacheControl *CacheControl `json:"cache_control,omitempty"`
}

// Content part types.
const (
	ContentPartText       = "text"
	ContentPartImageURL   = "image_url"
	ContentPartInputAudio = "input_audio"
	ContentPartFile       = "file"
)

// ContentPart is one element of array-form message content.
type ContentPart struct {
	Type         string        `json:"type"`
	Text         string        `json:"text,omitempty"`
	ImageURL     *ImageURL     `json:"image_url,omitempty"`
	InputAudio   *InputAudio   `json:"input_audio,omitempty"`
	File         *FileContent  `json:"file,omitempty"`
	CacheControl *CacheControl `json:"cache_control,omitempty"`

	// raw keeps the original JSON of part types this package does not model
	// so they are passed through unchanged.
	raw json.RawMessage
}

// ImageURL is the payload of an "image_url" content part. URL is either an
// http(s) URL or a data URL ("data:image/png;base64,...").
type ImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

// InputAudio is the payload of an "input_audio" content part.
type InputAudio struct {
	Data   string `json:"data"`
	Format string `json:"format"`
}

// FileContent is the payload of a "file" content part.
type FileContent struct {
	FileID   string `json:"file_id,omitempty"`
	FileData string `json:"file_data,omitempty"`
	Filename string `json:"filename,omitempty"`
}

func isKnownPartType(t string) bool {
	switch t {
	case ContentPartText, ContentPartImageURL, ContentPartInputAudio, ContentPartFile:
		return true
	}
	return false
}

// UnmarshalJSON implements json.Unmarshaler.
func (p *ContentPart) UnmarshalJSON(data []byte) error {
	type alias ContentPart
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*p = ContentPart(a)
	p.raw = nil
	if !isKnownPartType(p.Type) {
		p.raw = append(json.RawMessage(nil), data...)
	}
	return nil
}

// MarshalJSON implements json.Marshaler.
func (p ContentPart) MarshalJSON() ([]byte, error) {
	if len(p.raw) > 0 && !isKnownPartType(p.Type) {
		return p.raw, nil
	}
	if p.Type == ContentPartText {
		// "text" is required for text parts even when empty.
		return json.Marshal(struct {
			Type         string        `json:"type"`
			Text         string        `json:"text"`
			CacheControl *CacheControl `json:"cache_control,omitempty"`
		}{p.Type, p.Text, p.CacheControl})
	}
	type alias ContentPart
	return json.Marshal(alias(p))
}

// TextContentParts returns a text-only content part slice for s.
func TextContentParts(s string) []ContentPart {
	return []ContentPart{{Type: ContentPartText, Text: s}}
}

// JoinTextParts concatenates the text of all text parts, separated by "\n".
func JoinTextParts(parts []ContentPart) string {
	var b strings.Builder
	first := true
	for _, p := range parts {
		if p.Type != ContentPartText {
			continue
		}
		if !first {
			b.WriteByte('\n')
		}
		b.WriteString(p.Text)
		first = false
	}
	return b.String()
}

func hasTextPart(parts []ContentPart) bool {
	for _, p := range parts {
		if p.Type == ContentPartText {
			return true
		}
	}
	return false
}

// TextContent returns the message text: Content when set, otherwise the
// concatenated text parts.
func (m Message) TextContent() string {
	if m.Content != nil {
		return *m.Content
	}
	return JoinTextParts(m.ContentParts)
}

// EffectiveContentParts returns the parts that represent this message's
// content, reconciling ContentParts with a modified Content (see Message).
// It returns nil when the message has string-only content.
func (m Message) EffectiveContentParts() []ContentPart {
	if len(m.ContentParts) == 0 {
		return nil
	}
	if m.Content == nil {
		return m.ContentParts
	}
	if hasTextPart(m.ContentParts) && *m.Content == JoinTextParts(m.ContentParts) {
		return m.ContentParts
	}
	if !hasTextPart(m.ContentParts) && *m.Content == "" {
		return m.ContentParts
	}
	// Content was changed after decoding: rebuild the text portion.
	out := make([]ContentPart, 0, len(m.ContentParts)+1)
	if *m.Content != "" {
		out = append(out, ContentPart{Type: ContentPartText, Text: *m.Content})
	}
	for _, p := range m.ContentParts {
		if p.Type != ContentPartText {
			out = append(out, p)
		}
	}
	return out
}

// UnmarshalJSON implements json.Unmarshaler, accepting string or array content.
func (m *Message) UnmarshalJSON(data []byte) error {
	type alias Message
	aux := struct {
		*alias
		Content json.RawMessage `json:"content,omitempty"`
	}{alias: (*alias)(m)}
	m.Content = nil
	m.ContentParts = nil
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	raw := bytes.TrimSpace(aux.Content)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		m.Content = &s
	case '[':
		var parts []ContentPart
		if err := json.Unmarshal(raw, &parts); err != nil {
			return fmt.Errorf("invalid message content parts: %w", err)
		}
		for i, p := range parts {
			if p.Type == "" {
				return fmt.Errorf("invalid message content part %d: missing type", i)
			}
		}
		m.ContentParts = parts
		if hasTextPart(parts) {
			s := JoinTextParts(parts)
			m.Content = &s
		}
	default:
		return errors.New("message content must be a string or an array of content parts")
	}
	return nil
}

// MarshalJSON implements json.Marshaler.
func (m Message) MarshalJSON() ([]byte, error) {
	type alias Message
	aux := struct {
		alias
		Content interface{} `json:"content,omitempty"`
	}{alias: alias(m)}
	if parts := m.EffectiveContentParts(); len(parts) > 0 {
		aux.Content = parts
	} else if m.Content != nil {
		aux.Content = *m.Content
	}
	return json.Marshal(aux)
}

// CacheControl enables prompt caching (Anthropic beta feature).
// When set to {"type":"ephemeral"}, Anthropic caches this message to reduce costs.
type CacheControl struct {
	Type string `json:"type"` // e.g. "ephemeral"
}

// ToolCall represents a single tool call requested by the LLM.
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

// ToolFunction represents the function definition within a tool call.
type ToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolDefinition represents a tool available to the model.
//
// It decodes both OpenAI's nested form
// {"type":"function","function":{"name","description","parameters","strict"}}
// and the flat form {"name","description","parameters"} (Anthropic's
// "input_schema" is accepted as an alias of "parameters"). It encodes to the
// OpenAI nested form. Non-function tool types are passed through verbatim.
type ToolDefinition struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Parameters  map[string]interface{} `json:"parameters"`
	// Strict enables OpenAI strict schema adherence.
	Strict *bool `json:"strict,omitempty"`
	// Type is the tool type; empty means "function".
	Type string `json:"-"`

	raw json.RawMessage
}

// IsFunction reports whether the tool is a function tool.
func (t ToolDefinition) IsFunction() bool { return t.Type == "" || t.Type == "function" }

type toolFunctionDef struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	Strict      *bool                  `json:"strict,omitempty"`
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *ToolDefinition) UnmarshalJSON(data []byte) error {
	var probe struct {
		Type        string                 `json:"type"`
		Function    json.RawMessage        `json:"function"`
		Name        string                 `json:"name"`
		Description string                 `json:"description"`
		Parameters  map[string]interface{} `json:"parameters"`
		InputSchema map[string]interface{} `json:"input_schema"`
		Strict      *bool                  `json:"strict"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}
	*t = ToolDefinition{}
	if probe.Type != "" && probe.Type != "function" {
		t.Type = probe.Type
		t.Name = probe.Name
		t.Description = probe.Description
		t.raw = append(json.RawMessage(nil), data...)
		return nil
	}
	if fn := bytes.TrimSpace(probe.Function); len(fn) > 0 && !bytes.Equal(fn, []byte("null")) {
		var f toolFunctionDef
		if err := json.Unmarshal(fn, &f); err != nil {
			return fmt.Errorf("invalid tool function: %w", err)
		}
		t.Name, t.Description, t.Parameters, t.Strict = f.Name, f.Description, f.Parameters, f.Strict
		return nil
	}
	t.Name = probe.Name
	t.Description = probe.Description
	t.Parameters = probe.Parameters
	if t.Parameters == nil {
		t.Parameters = probe.InputSchema
	}
	t.Strict = probe.Strict
	return nil
}

// MarshalJSON implements json.Marshaler (OpenAI nested form).
func (t ToolDefinition) MarshalJSON() ([]byte, error) {
	if !t.IsFunction() {
		if len(t.raw) > 0 {
			return t.raw, nil
		}
		return json.Marshal(struct {
			Type string `json:"type"`
			Name string `json:"name,omitempty"`
		}{t.Type, t.Name})
	}
	return json.Marshal(struct {
		Type     string          `json:"type"`
		Function toolFunctionDef `json:"function"`
	}{"function", toolFunctionDef{Name: t.Name, Description: t.Description, Parameters: t.Parameters, Strict: t.Strict}})
}

// Tool choice modes.
const (
	ToolChoiceNone     = "none"
	ToolChoiceAuto     = "auto"
	ToolChoiceRequired = "required"
)

// ToolChoice is OpenAI's tool_choice: a mode string ("none", "auto",
// "required") or {"type":"function","function":{"name":...}}. Anthropic-style
// {"type":"auto"|"any"|"none"|"tool","name":...} is also accepted on input.
type ToolChoice struct {
	// Mode is "none", "auto" or "required" when Function is empty.
	Mode string
	// Function forces a specific function tool by name.
	Function string
}

// UnmarshalJSON implements json.Unmarshaler.
func (c *ToolChoice) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	*c = ToolChoice{}
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		switch s {
		case ToolChoiceNone, ToolChoiceAuto, ToolChoiceRequired:
			c.Mode = s
			return nil
		}
		return fmt.Errorf("invalid tool_choice %q", s)
	}
	var obj struct {
		Type     string `json:"type"`
		Name     string `json:"name"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("invalid tool_choice: %w", err)
	}
	switch obj.Type {
	case "function":
		if obj.Function.Name == "" {
			return errors.New("invalid tool_choice: function.name is required")
		}
		c.Function = obj.Function.Name
	case "tool":
		if obj.Name == "" {
			return errors.New("invalid tool_choice: name is required")
		}
		c.Function = obj.Name
	case "any":
		c.Mode = ToolChoiceRequired
	case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired:
		c.Mode = obj.Type
	default:
		return fmt.Errorf("invalid tool_choice type %q", obj.Type)
	}
	return nil
}

// MarshalJSON implements json.Marshaler (OpenAI form).
func (c ToolChoice) MarshalJSON() ([]byte, error) {
	if c.Function != "" {
		return json.Marshal(struct {
			Type     string `json:"type"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		}{Type: "function", Function: struct {
			Name string `json:"name"`
		}{c.Function}})
	}
	mode := c.Mode
	if mode == "" {
		mode = ToolChoiceAuto
	}
	return json.Marshal(mode)
}

// StreamOptions configures streaming responses (OpenAI stream_options).
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// LLMRequest is the unified request structure sent to any LLM provider.
// @Description Unified chat completion request compatible with OpenAI, Anthropic, and 100+ providers.
type LLMRequest struct {
	// The model identifier (e.g. "gpt-4o", "claude-3-opus").
	Model string `json:"model" example:"gpt-4o"`
	// The conversation messages.
	Messages []Message `json:"messages"`
	// Maximum tokens to generate.
	MaxTokens *int `json:"max_tokens,omitempty" example:"4096"`
	// MaxCompletionTokens is the newer OpenAI name for MaxTokens.
	MaxCompletionTokens *int `json:"max_completion_tokens,omitempty"`
	// Sampling temperature (0-2).
	Temperature *float64 `json:"temperature,omitempty" example:"0.7"`
	// Nucleus sampling (0-1).
	TopP   *float64 `json:"top_p,omitempty" example:"1.0"`
	Stream bool     `json:"stream" example:"false"`
	// StreamOptions configures streaming (e.g. include_usage).
	StreamOptions *StreamOptions `json:"stream_options,omitempty"`
	// Stop sequences.
	Stop             []string         `json:"stop,omitempty"`
	PresencePenalty  *float64         `json:"presence_penalty,omitempty"`
	FrequencyPenalty *float64         `json:"frequency_penalty,omitempty"`
	Tools            []ToolDefinition `json:"tools,omitempty"`
	// ToolChoice controls which (if any) tool is called.
	ToolChoice *ToolChoice `json:"tool_choice,omitempty"`
	// ParallelToolCalls enables parallel function calling.
	ParallelToolCalls *bool `json:"parallel_tool_calls,omitempty"`
	// N is the number of choices to generate.
	N *int `json:"n,omitempty"`
	// Seed requests deterministic sampling.
	Seed *int64 `json:"seed,omitempty"`
	// User is an end-user identifier for abuse monitoring.
	User string `json:"user,omitempty"`
	// Logprobs requests log probabilities of output tokens.
	Logprobs *bool `json:"logprobs,omitempty"`
	// TopLogprobs is the number of most likely tokens to return (0-20).
	TopLogprobs *int `json:"top_logprobs,omitempty"`
	// LogitBias maps token IDs to bias values (-100..100).
	LogitBias  map[string]float64 `json:"logit_bias,omitempty"`
	RagEnabled bool               `json:"rag_enabled,omitempty"`
	// ResponseFormat controls structured output.
	ResponseFormat *ResponseFormat `json:"response_format,omitempty"`
}

// EffectiveMaxTokens returns MaxCompletionTokens if set, else MaxTokens.
func (r *LLMRequest) EffectiveMaxTokens() *int {
	if r == nil {
		return nil
	}
	if r.MaxCompletionTokens != nil {
		return r.MaxCompletionTokens
	}
	return r.MaxTokens
}

// ResponseFormat controls structured output formatting.
type ResponseFormat struct {
	Type       string      `json:"type"` // "json_schema" | "json_object" | "text"
	JSONSchema *JSONSchema `json:"json_schema,omitempty"`
}

// JSONSchema defines a JSON schema for structured output.
type JSONSchema struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Schema      map[string]any `json:"schema,omitempty"`
	Strict      bool           `json:"strict,omitempty"`
}

// LLMResponse is the unified response structure from any LLM provider.
// @Description Unified LLM response.
type LLMResponse struct {
	ID                string   `json:"id" example:"chatcmpl-123"`
	Object            string   `json:"object" example:"chat.completion"`
	Created           int64    `json:"created" example:"1700000000"`
	Model             string   `json:"model" example:"gpt-4o"`
	Choices           []Choice `json:"choices"`
	Usage             *Usage   `json:"usage,omitempty"`
	SystemFingerprint string   `json:"system_fingerprint,omitempty"`
}

// Choice represents a single response choice.
// @Description A single response choice.
type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
	// Logprobs is passed through verbatim when the upstream returns it.
	Logprobs json.RawMessage `json:"logprobs,omitempty" swaggertype:"object"`
}

// Usage represents token usage statistics.
// @Description Token usage statistics for a completion request.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens" example:"100"`
	CompletionTokens int `json:"completion_tokens" example:"50"`
	TotalTokens      int `json:"total_tokens" example:"150"`
	// PromptTokensDetails reports cached prompt tokens when known.
	PromptTokensDetails *PromptTokensDetails `json:"prompt_tokens_details,omitempty"`
}

// PromptTokensDetails breaks down prompt token usage.
type PromptTokensDetails struct {
	CachedTokens int `json:"cached_tokens"`
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

// ContentToString returns the tool result content as a string. Strings are
// returned as-is, byte slices as text, errors by message and any other value
// as JSON.
func (t *ToolResult) ContentToString() string {
	if t == nil || t.Content == nil {
		return ""
	}
	switch v := t.Content.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case json.RawMessage:
		return string(v)
	case error:
		return v.Error()
	case fmt.Stringer:
		return v.String()
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(b)
	}
}

// TraceIDContextKey is the context key for trace IDs.
const TraceIDContextKey = "trace_id"

// GenerateTraceID returns a new random W3C-compatible trace ID: 32 lowercase
// hex characters, never all zeros.
func GenerateTraceID() string {
	var b [16]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			// crypto/rand does not fail on supported platforms; fall back
			// to a time-derived ID rather than returning an empty one.
			binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
			binary.BigEndian.PutUint64(b[8:], uint64(time.Now().UnixNano())^0x9e3779b97f4a7c15)
		}
		if b != [16]byte{} {
			return hex.EncodeToString(b[:])
		}
	}
}

// ChatRequest is the request body for chat completions endpoints.
type ChatRequest struct {
	Model            string           `json:"model"`
	Messages         []Message        `json:"messages"`
	MaxTokens        *int             `json:"max_tokens,omitempty"`
	Temperature      *float64         `json:"temperature,omitempty"`
	TopP             *float64         `json:"top_p,omitempty"`
	Stream           bool             `json:"stream"`
	Stop             []string         `json:"stop,omitempty"`
	PresencePenalty  *float64         `json:"presence_penalty,omitempty"`
	FrequencyPenalty *float64         `json:"frequency_penalty,omitempty"`
	Tools            []ToolDefinition `json:"tools,omitempty"`
	RagEnabled       bool             `json:"rag_enabled,omitempty"`
}

// ChatResponse is the response body for chat completions endpoints.
type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

// EmbeddingRequest is the request body for embeddings endpoints.
//
// "input" may be a string, an array of strings, or (passed through
// verbatim) an array of token arrays. Input always holds the text: the single
// string, or all strings joined by "\n" (for guardrails and token counting);
// Inputs holds the array form.
type EmbeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
	// Inputs holds array-form input; it takes precedence over Input when
	// marshalling.
	Inputs         []string `json:"-"`
	EncodingFormat string   `json:"encoding_format,omitempty"`
	Dimensions     *int     `json:"dimensions,omitempty"`
	User           string   `json:"user,omitempty"`

	rawInput json.RawMessage
}

// InputList returns the inputs as a slice (nil when empty).
func (r *EmbeddingRequest) InputList() []string {
	if len(r.Inputs) > 0 {
		return r.Inputs
	}
	if r.Input != "" {
		return []string{r.Input}
	}
	return nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (r *EmbeddingRequest) UnmarshalJSON(data []byte) error {
	type alias EmbeddingRequest
	aux := struct {
		*alias
		Input json.RawMessage `json:"input"`
	}{alias: (*alias)(r)}
	r.Input, r.Inputs, r.rawInput = "", nil, nil
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	raw := bytes.TrimSpace(aux.Input)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	switch raw[0] {
	case '"':
		return json.Unmarshal(raw, &r.Input)
	case '[':
		var strs []string
		if err := json.Unmarshal(raw, &strs); err == nil {
			r.Inputs = strs
			r.Input = strings.Join(strs, "\n")
			return nil
		}
		var tokens []int64
		if err := json.Unmarshal(raw, &tokens); err == nil {
			r.rawInput = append(json.RawMessage(nil), raw...)
			return nil
		}
		var tokenLists [][]int64
		if err := json.Unmarshal(raw, &tokenLists); err == nil {
			r.rawInput = append(json.RawMessage(nil), raw...)
			return nil
		}
	}
	return errors.New("embedding input must be a string, an array of strings, or an array of token arrays")
}

// MarshalJSON implements json.Marshaler.
func (r EmbeddingRequest) MarshalJSON() ([]byte, error) {
	type alias EmbeddingRequest
	aux := struct {
		alias
		Input interface{} `json:"input"`
	}{alias: alias(r)}
	switch {
	case len(r.Inputs) > 0:
		aux.Input = r.Inputs
	case len(r.rawInput) > 0:
		aux.Input = r.rawInput
	default:
		aux.Input = r.Input
	}
	return json.Marshal(aux)
}

// EmbeddingResponse is the response body for embeddings endpoints.
type EmbeddingResponse struct {
	Object string      `json:"object"`
	Data   []Embedding `json:"data"`
	Model  string      `json:"model"`
	Usage  *Usage      `json:"usage,omitempty"`
}

// Embedding represents a single embedding vector. It decodes both float
// arrays and base64-encoded little-endian float32 vectors
// (encoding_format=base64) and always encodes as a float array.
type Embedding struct {
	Object    string    `json:"object"`
	Embedding []float64 `json:"embedding"`
	Index     int       `json:"index"`
}

// UnmarshalJSON implements json.Unmarshaler.
func (e *Embedding) UnmarshalJSON(data []byte) error {
	type alias Embedding
	aux := struct {
		*alias
		Embedding json.RawMessage `json:"embedding"`
	}{alias: (*alias)(e)}
	e.Embedding = nil
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	raw := bytes.TrimSpace(aux.Embedding)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	if raw[0] != '"' {
		return json.Unmarshal(raw, &e.Embedding)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("invalid base64 embedding: %w", err)
	}
	if len(b)%4 != 0 {
		return errors.New("invalid base64 embedding: length is not a multiple of 4")
	}
	e.Embedding = make([]float64, len(b)/4)
	for i := range e.Embedding {
		e.Embedding[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:])))
	}
	return nil
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
	Created int64   `json:"created"`
	Data    []Image `json:"data"`
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
//
// "input" may be a string or an array of input items; array input is kept
// verbatim in InputItems (and passed through upstream) while Input receives
// the concatenated text of any text content found in the items.
type ResponsesRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
	// InputItems holds array-form input verbatim.
	InputItems json.RawMessage  `json:"-"`
	Previous   *string          `json:"previous_response_id,omitempty"`
	Tools      []ToolDefinition `json:"tools,omitempty"`
}

// UnmarshalJSON implements json.Unmarshaler.
func (r *ResponsesRequest) UnmarshalJSON(data []byte) error {
	type alias ResponsesRequest
	aux := struct {
		*alias
		Input json.RawMessage `json:"input"`
	}{alias: (*alias)(r)}
	r.Input, r.InputItems = "", nil
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	raw := bytes.TrimSpace(aux.Input)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	switch raw[0] {
	case '"':
		return json.Unmarshal(raw, &r.Input)
	case '[':
		var items []struct {
			Content json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(raw, &items); err != nil {
			return fmt.Errorf("invalid responses input: %w", err)
		}
		r.InputItems = append(json.RawMessage(nil), raw...)
		var texts []string
		for _, it := range items {
			c := bytes.TrimSpace(it.Content)
			if len(c) == 0 {
				continue
			}
			if c[0] == '"' {
				var s string
				if json.Unmarshal(c, &s) == nil {
					texts = append(texts, s)
				}
				continue
			}
			var parts []struct {
				Text string `json:"text"`
			}
			if json.Unmarshal(c, &parts) == nil {
				for _, p := range parts {
					if p.Text != "" {
						texts = append(texts, p.Text)
					}
				}
			}
		}
		r.Input = strings.Join(texts, "\n")
		return nil
	}
	return errors.New("responses input must be a string or an array of input items")
}

// MarshalJSON implements json.Marshaler.
func (r ResponsesRequest) MarshalJSON() ([]byte, error) {
	type alias ResponsesRequest
	aux := struct {
		alias
		Input interface{} `json:"input"`
	}{alias: alias(r)}
	if len(r.InputItems) > 0 {
		aux.Input = r.InputItems
	} else {
		aux.Input = r.Input
	}
	return json.Marshal(aux)
}

// ResponsesResponse is the response body for OpenAI-compatible responses endpoints.
//
// When decoded from an upstream Responses API body, the typed fields are
// populated best-effort (message items become Messages with their
// output_text joined into Content; usage input/output tokens are mapped to
// prompt/completion tokens) and the original JSON is retained: marshalling
// such a value re-emits the upstream body verbatim so no item types
// (function_call, reasoning, ...) are lost. Values built in Go marshal from
// the typed fields.
type ResponsesResponse struct {
	ID        string    `json:"id"`
	Object    string    `json:"object"`
	CreatedAt int64     `json:"created_at"`
	Model     string    `json:"model"`
	Output    []Message `json:"output"`
	Usage     *Usage    `json:"usage,omitempty"`

	raw json.RawMessage
}

// UnmarshalJSON implements json.Unmarshaler.
func (r *ResponsesResponse) UnmarshalJSON(data []byte) error {
	type alias ResponsesResponse
	aux := struct {
		*alias
		Output []json.RawMessage `json:"output"`
		Usage  json.RawMessage   `json:"usage"`
	}{alias: (*alias)(r)}
	r.Output, r.Usage, r.raw = nil, nil, nil
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	for _, item := range aux.Output {
		var probe struct {
			Type    string          `json:"type"`
			Role    MessageRole     `json:"role"`
			Content json.RawMessage `json:"content"`
		}
		if json.Unmarshal(item, &probe) != nil || (probe.Type != "" && probe.Type != "message") {
			continue
		}
		msg := Message{Role: probe.Role}
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		c := bytes.TrimSpace(probe.Content)
		if len(c) > 0 && c[0] == '"' {
			var s string
			if json.Unmarshal(c, &s) == nil {
				msg.Content = &s
			}
		} else if json.Unmarshal(c, &parts) == nil {
			var texts []string
			for _, p := range parts {
				if p.Type == "output_text" || p.Type == "text" || p.Type == "input_text" {
					texts = append(texts, p.Text)
				}
			}
			if len(texts) > 0 {
				s := strings.Join(texts, "")
				msg.Content = &s
			}
		}
		r.Output = append(r.Output, msg)
	}
	if u := bytes.TrimSpace(aux.Usage); len(u) > 0 && !bytes.Equal(u, []byte("null")) {
		var usage struct {
			PromptTokens       int `json:"prompt_tokens"`
			CompletionTokens   int `json:"completion_tokens"`
			InputTokens        int `json:"input_tokens"`
			OutputTokens       int `json:"output_tokens"`
			TotalTokens        int `json:"total_tokens"`
			InputTokensDetails *struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"input_tokens_details"`
		}
		if err := json.Unmarshal(u, &usage); err == nil {
			out := &Usage{PromptTokens: usage.PromptTokens + usage.InputTokens, CompletionTokens: usage.CompletionTokens + usage.OutputTokens, TotalTokens: usage.TotalTokens}
			if out.TotalTokens == 0 {
				out.TotalTokens = out.PromptTokens + out.CompletionTokens
			}
			if usage.InputTokensDetails != nil && usage.InputTokensDetails.CachedTokens > 0 {
				out.PromptTokensDetails = &PromptTokensDetails{CachedTokens: usage.InputTokensDetails.CachedTokens}
			}
			r.Usage = out
		}
	}
	r.raw = append(json.RawMessage(nil), data...)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (r ResponsesResponse) MarshalJSON() ([]byte, error) {
	if len(r.raw) > 0 {
		return r.raw, nil
	}
	type alias ResponsesResponse
	return json.Marshal(alias(r))
}
