package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// AnthropicVersion is the anthropic-version header value sent upstream.
const AnthropicVersion = "2023-06-01"

// AnthropicDefaultMaxTokens is used when a request does not set max_tokens
// (Anthropic requires it).
const AnthropicDefaultMaxTokens = 4096

// AnthropicRequest is the /v1/messages request body.
type AnthropicRequest struct {
	Model         string                  `json:"model"`
	MaxTokens     int                     `json:"max_tokens"`
	System        []AnthropicContentBlock `json:"system,omitempty"`
	Messages      []AnthropicMessage      `json:"messages"`
	Temperature   *float64                `json:"temperature,omitempty"`
	TopP          *float64                `json:"top_p,omitempty"`
	StopSequences []string                `json:"stop_sequences,omitempty"`
	Stream        bool                    `json:"stream,omitempty"`
	Tools         []AnthropicTool         `json:"tools,omitempty"`
	ToolChoice    *AnthropicToolChoice    `json:"tool_choice,omitempty"`
	Metadata      *AnthropicMetadata      `json:"metadata,omitempty"`

	// jsonTool is the name of the forced tool that emulates OpenAI
	// response_format (see BuildAnthropicRequest); "" when not in use.
	jsonTool string
}

// AnthropicJSONToolName is the forced tool used to emulate OpenAI
// response_format ("json_schema" / "json_object") on Anthropic.
const AnthropicJSONToolName = "json_response"

// JSONResponseTool reports the name of the forced tool that emulates
// response_format for this request ("" when response_format is not used).
// Its tool_use input is returned to the client as the message content.
func (r *AnthropicRequest) JSONResponseTool() string {
	if r == nil {
		return ""
	}
	return r.jsonTool
}

// AnthropicMessage is a message in Anthropic's native format.
type AnthropicMessage struct {
	Role    string                  `json:"role"`
	Content []AnthropicContentBlock `json:"content"`
}

// AnthropicContentBlock is a content block (text, image, document, tool_use,
// tool_result) in requests and responses.
type AnthropicContentBlock struct {
	Type         string               `json:"type"`
	Text         string               `json:"text,omitempty"`
	Source       *AnthropicSource     `json:"source,omitempty"`
	ID           string               `json:"id,omitempty"`
	Name         string               `json:"name,omitempty"`
	Input        json.RawMessage      `json:"input,omitempty"`
	ToolUseID    string               `json:"tool_use_id,omitempty"`
	Content      string               `json:"content,omitempty"`
	CacheControl *models.CacheControl `json:"cache_control,omitempty"`
}

// AnthropicSource is an image/document source.
type AnthropicSource struct {
	Type      string `json:"type"` // "base64" | "url"
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

// AnthropicTool is a tool definition in Anthropic's format.
type AnthropicTool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	InputSchema map[string]interface{} `json:"input_schema"`
}

// AnthropicToolChoice is Anthropic's tool_choice.
type AnthropicToolChoice struct {
	Type                   string `json:"type"` // auto | any | tool | none
	Name                   string `json:"name,omitempty"`
	DisableParallelToolUse bool   `json:"disable_parallel_tool_use,omitempty"`
}

// AnthropicMetadata carries the end-user id.
type AnthropicMetadata struct {
	UserID string `json:"user_id,omitempty"`
}

// AnthropicResponse is the /v1/messages response body.
type AnthropicResponse struct {
	ID           string                  `json:"id"`
	Type         string                  `json:"type"`
	Role         string                  `json:"role"`
	Model        string                  `json:"model"`
	Content      []AnthropicContentBlock `json:"content"`
	StopReason   string                  `json:"stop_reason"`
	StopSequence *string                 `json:"stop_sequence"`
	Usage        AnthropicUsage          `json:"usage"`
}

// AnthropicUsage is Anthropic's token usage.
type AnthropicUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
}

// ToUsage converts to OpenAI-style usage (prompt tokens include cache reads
// and writes, as OpenAI's prompt_tokens includes cached tokens).
func (u AnthropicUsage) ToUsage() *models.Usage {
	prompt := u.InputTokens + u.CacheCreationInputTokens + u.CacheReadInputTokens
	out := &models.Usage{PromptTokens: prompt, CompletionTokens: u.OutputTokens, TotalTokens: prompt + u.OutputTokens}
	if u.CacheReadInputTokens > 0 {
		out.PromptTokensDetails = &models.PromptTokensDetails{CachedTokens: u.CacheReadInputTokens}
	}
	return out
}

func invalidRequest(provider, format string, args ...interface{}) *UpstreamError {
	return &UpstreamError{Provider: provider, StatusCode: http.StatusBadRequest, Type: "invalid_request_error", Message: fmt.Sprintf(format, args...)}
}

// ParseDataURL splits "data:<mime>;base64,<data>" into mime type and data.
func ParseDataURL(s string) (mediaType, data string, ok bool) {
	if !strings.HasPrefix(s, "data:") {
		return "", "", false
	}
	meta, payload, found := strings.Cut(s[len("data:"):], ",")
	if !found || !strings.HasSuffix(meta, ";base64") {
		return "", "", false
	}
	return strings.TrimSuffix(meta, ";base64"), payload, true
}

func toolInputJSON(args string) json.RawMessage {
	args = strings.TrimSpace(args)
	if args == "" {
		return json.RawMessage("{}")
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(args), &obj) == nil && obj != nil {
		return json.RawMessage(args)
	}
	// Anthropic requires an object; keep malformed arguments visible.
	b, _ := json.Marshal(map[string]string{"raw": args})
	return b
}

func anthropicPartBlocks(provider string, parts []models.ContentPart) ([]AnthropicContentBlock, error) {
	blocks := make([]AnthropicContentBlock, 0, len(parts))
	for _, p := range parts {
		switch p.Type {
		case models.ContentPartText:
			if p.Text == "" {
				continue
			}
			blocks = append(blocks, AnthropicContentBlock{Type: "text", Text: p.Text, CacheControl: p.CacheControl})
		case models.ContentPartImageURL:
			if p.ImageURL == nil || p.ImageURL.URL == "" {
				return nil, invalidRequest(provider, "image_url part has no url")
			}
			src := &AnthropicSource{Type: "url", URL: p.ImageURL.URL}
			if mt, data, ok := ParseDataURL(p.ImageURL.URL); ok {
				src = &AnthropicSource{Type: "base64", MediaType: mt, Data: data}
			} else if !strings.HasPrefix(p.ImageURL.URL, "https://") && !strings.HasPrefix(p.ImageURL.URL, "http://") {
				return nil, invalidRequest(provider, "image_url must be an http(s) URL or a base64 data URL")
			}
			blocks = append(blocks, AnthropicContentBlock{Type: "image", Source: src, CacheControl: p.CacheControl})
		case models.ContentPartFile:
			if p.File == nil || p.File.FileData == "" {
				return nil, invalidRequest(provider, "file parts must carry file_data (file_id is not supported)")
			}
			mt, data, ok := ParseDataURL(p.File.FileData)
			if !ok {
				return nil, invalidRequest(provider, "file_data must be a base64 data URL")
			}
			blocks = append(blocks, AnthropicContentBlock{Type: "document", Source: &AnthropicSource{Type: "base64", MediaType: mt, Data: data}, CacheControl: p.CacheControl})
		default:
			return nil, invalidRequest(provider, "content part type %q is not supported by this provider", p.Type)
		}
	}
	return blocks, nil
}

// anthropicJSONTool builds the forced tool that emulates OpenAI
// response_format. It returns (nil, nil) when no emulation is needed.
func anthropicJSONTool(provider string, rf *models.ResponseFormat) (*AnthropicTool, error) {
	if rf == nil {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimSpace(rf.Type)) {
	case "", "text":
		return nil, nil
	case "json_object":
		return &AnthropicTool{
			Name:        AnthropicJSONToolName,
			Description: "Respond with a single JSON object. Put the complete answer in this tool's input.",
			InputSchema: map[string]interface{}{"type": "object"},
		}, nil
	case "json_schema":
		js := rf.JSONSchema
		if js == nil || js.Schema == nil {
			return nil, invalidRequest(provider, "response_format json_schema requires json_schema.schema")
		}
		if t, ok := js.Schema["type"]; ok && t != "object" {
			return nil, invalidRequest(provider, "response_format json_schema must describe a JSON object (root \"type\": \"object\") for this provider")
		}
		schema := make(map[string]interface{}, len(js.Schema)+1)
		for k, v := range js.Schema {
			schema[k] = v
		}
		schema["type"] = "object"
		desc := "Respond with JSON that matches this tool's input schema. Put the complete answer in this tool's input."
		if js.Name != "" {
			desc += " Schema: " + js.Name + "."
		}
		if js.Description != "" {
			desc += " " + js.Description
		}
		return &AnthropicTool{Name: AnthropicJSONToolName, Description: desc, InputSchema: schema}, nil
	default:
		return nil, invalidRequest(provider, "response_format type %q is not supported", rf.Type)
	}
}

// BuildAnthropicRequest converts a gateway (OpenAI-style) request into
// Anthropic's /v1/messages format. model overrides req.Model when non-empty.
//
// OpenAI response_format "json_object" / "json_schema" is emulated by forcing
// a single tool (AnthropicJSONToolName) whose input schema is the requested
// schema; responses convert that tool call back into JSON message content
// with finish_reason "stop" (streams turn its input_json_delta events into
// content deltas). Combining response_format with tools is rejected with a
// 400. seed, logprobs/top_logprobs, logit_bias and presence/frequency
// penalties have no Anthropic equivalent and are dropped.
func BuildAnthropicRequest(provider string, req *models.LLMRequest, model string, stream bool) (*AnthropicRequest, error) {
	if req == nil {
		return nil, invalidRequest(provider, "request is nil")
	}
	if model == "" {
		model = req.Model
	}
	if model == "" {
		return nil, invalidRequest(provider, "model is required")
	}
	if req.N != nil && *req.N > 1 {
		return nil, invalidRequest(provider, "n > 1 is not supported by this provider")
	}
	out := &AnthropicRequest{Model: model, Stream: stream, TopP: req.TopP}
	out.MaxTokens = AnthropicDefaultMaxTokens
	if mt := req.EffectiveMaxTokens(); mt != nil {
		if *mt <= 0 {
			return nil, invalidRequest(provider, "max_tokens must be positive")
		}
		out.MaxTokens = *mt
	}
	if req.Temperature != nil {
		t := *req.Temperature
		// Anthropic accepts 0..1 while OpenAI accepts 0..2.
		if t > 1 {
			t = 1
		}
		if t < 0 {
			t = 0
		}
		out.Temperature = &t
	}
	if len(req.Stop) > 0 {
		out.StopSequences = req.Stop
	}
	if req.User != "" {
		out.Metadata = &AnthropicMetadata{UserID: req.User}
	}

	for _, m := range req.Messages {
		var blocks []AnthropicContentBlock
		switch m.Role {
		case models.RoleSystem, models.RoleDeveloper:
			if parts := m.EffectiveContentParts(); len(parts) > 0 {
				for _, p := range parts {
					if p.Type != models.ContentPartText {
						return nil, invalidRequest(provider, "system messages may only contain text")
					}
					if p.Text != "" {
						out.System = append(out.System, AnthropicContentBlock{Type: "text", Text: p.Text, CacheControl: p.CacheControl})
					}
				}
			} else if m.Content != nil && *m.Content != "" {
				out.System = append(out.System, AnthropicContentBlock{Type: "text", Text: *m.Content})
			}
			if m.CacheControl != nil && len(out.System) > 0 {
				out.System[len(out.System)-1].CacheControl = m.CacheControl
			}
			continue
		case models.RoleTool:
			if m.ToolCallID == nil || *m.ToolCallID == "" {
				return nil, invalidRequest(provider, "tool messages require tool_call_id")
			}
			content := m.TextContent()
			if content == "" && m.ToolResult != nil {
				content = *m.ToolResult
			}
			blocks = append(blocks, AnthropicContentBlock{Type: "tool_result", ToolUseID: *m.ToolCallID, Content: content})
		case models.RoleUser, models.RoleAssistant:
			if parts := m.EffectiveContentParts(); len(parts) > 0 {
				pb, err := anthropicPartBlocks(provider, parts)
				if err != nil {
					return nil, err
				}
				blocks = append(blocks, pb...)
			} else if m.Content != nil && *m.Content != "" {
				blocks = append(blocks, AnthropicContentBlock{Type: "text", Text: *m.Content})
			}
			for _, tc := range m.ToolCalls {
				if m.Role != models.RoleAssistant {
					return nil, invalidRequest(provider, "tool_calls are only valid on assistant messages")
				}
				blocks = append(blocks, AnthropicContentBlock{Type: "tool_use", ID: tc.ID, Name: tc.Function.Name, Input: toolInputJSON(tc.Function.Arguments)})
			}
		default:
			return nil, invalidRequest(provider, "unsupported message role %q", m.Role)
		}
		if len(blocks) == 0 {
			continue // Anthropic rejects empty content
		}
		if m.CacheControl != nil {
			blocks[len(blocks)-1].CacheControl = m.CacheControl
		}
		role := "user"
		if m.Role == models.RoleAssistant {
			role = "assistant"
		}
		// Anthropic expects alternating turns; merge consecutive same-role
		// messages (e.g. several tool results).
		if n := len(out.Messages); n > 0 && out.Messages[n-1].Role == role {
			out.Messages[n-1].Content = append(out.Messages[n-1].Content, blocks...)
			continue
		}
		out.Messages = append(out.Messages, AnthropicMessage{Role: role, Content: blocks})
	}
	if len(out.Messages) == 0 {
		return nil, invalidRequest(provider, "at least one non-system message is required")
	}

	jsonTool, err := anthropicJSONTool(provider, req.ResponseFormat)
	if err != nil {
		return nil, err
	}
	if jsonTool != nil {
		if len(req.Tools) > 0 {
			return nil, invalidRequest(provider, "response_format %q cannot be combined with tools for this provider (it is emulated with a forced tool call)", req.ResponseFormat.Type)
		}
		out.Tools = []AnthropicTool{*jsonTool}
		out.ToolChoice = &AnthropicToolChoice{Type: "tool", Name: jsonTool.Name}
		out.jsonTool = jsonTool.Name
		return out, nil
	}

	for _, t := range req.Tools {
		if !t.IsFunction() {
			return nil, invalidRequest(provider, "tool type %q is not supported by this provider", t.Type)
		}
		if t.Name == "" {
			return nil, invalidRequest(provider, "tool name is required")
		}
		schema := t.Parameters
		if schema == nil {
			schema = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
		}
		out.Tools = append(out.Tools, AnthropicTool{Name: t.Name, Description: t.Description, InputSchema: schema})
	}
	if len(out.Tools) > 0 {
		var tc *AnthropicToolChoice
		if req.ToolChoice != nil {
			switch {
			case req.ToolChoice.Function != "":
				tc = &AnthropicToolChoice{Type: "tool", Name: req.ToolChoice.Function}
			case req.ToolChoice.Mode == models.ToolChoiceRequired:
				tc = &AnthropicToolChoice{Type: "any"}
			case req.ToolChoice.Mode == models.ToolChoiceNone:
				tc = &AnthropicToolChoice{Type: "none"}
			default:
				tc = &AnthropicToolChoice{Type: "auto"}
			}
		}
		if req.ParallelToolCalls != nil && !*req.ParallelToolCalls {
			if tc == nil {
				tc = &AnthropicToolChoice{Type: "auto"}
			}
			if tc.Type != "none" {
				tc.DisableParallelToolUse = true
			}
		}
		out.ToolChoice = tc
	}
	return out, nil
}

// AnthropicFinishReason maps an Anthropic stop_reason to OpenAI's
// finish_reason.
func AnthropicFinishReason(stop string) string {
	switch stop {
	case "max_tokens", "model_context_window_exceeded":
		return "length"
	case "tool_use":
		return "tool_calls"
	case "refusal":
		return "content_filter"
	case "":
		return ""
	default: // end_turn, stop_sequence, pause_turn
		return "stop"
	}
}

// AnthropicToLLMResponse converts an Anthropic response into the gateway's
// OpenAI-style response (a single choice).
func AnthropicToLLMResponse(resp *AnthropicResponse) *models.LLMResponse {
	return AnthropicToLLMResponseJSON(resp, "")
}

// AnthropicToLLMResponseJSON is AnthropicToLLMResponse for requests that
// emulate response_format with the forced tool jsonTool (see
// AnthropicRequest.JSONResponseTool): that tool call's input becomes the
// message content (a JSON string) and a "tool_use" stop maps to
// finish_reason "stop". With jsonTool == "" it is AnthropicToLLMResponse.
func AnthropicToLLMResponseJSON(resp *AnthropicResponse, jsonTool string) *models.LLMResponse {
	msg := models.Message{Role: models.RoleAssistant}
	var text strings.Builder
	hasText := false
	var jsonContent *string
	for _, b := range resp.Content {
		switch b.Type {
		case "text":
			text.WriteString(b.Text)
			hasText = true
		case "tool_use":
			args := string(b.Input)
			if args == "" || args == "null" {
				args = "{}"
			}
			if jsonTool != "" && b.Name == jsonTool && jsonContent == nil {
				jsonContent = &args
				continue
			}
			msg.ToolCalls = append(msg.ToolCalls, models.ToolCall{ID: b.ID, Type: "function", Function: models.ToolFunction{Name: b.Name, Arguments: args}})
		}
	}
	finish := AnthropicFinishReason(resp.StopReason)
	switch {
	case jsonContent != nil:
		// The JSON is the answer; any stray preamble text would make the
		// content invalid JSON.
		msg.Content = jsonContent
		if finish == "tool_calls" && len(msg.ToolCalls) == 0 {
			finish = "stop"
		}
	case hasText || len(msg.ToolCalls) == 0:
		s := text.String()
		msg.Content = &s
	}
	return &models.LLMResponse{
		ID:      resp.ID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   resp.Model,
		Choices: []models.Choice{{Index: 0, Message: msg, FinishReason: finish}},
		Usage:   resp.Usage.ToUsage(),
	}
}

// AnthropicHeaders returns the standard request headers.
func AnthropicHeaders(apiKey string) http.Header {
	h := http.Header{}
	if apiKey != "" {
		h.Set("x-api-key", apiKey)
	}
	h.Set("anthropic-version", AnthropicVersion)
	return h
}

// AnthropicMessages performs a non-streaming /v1/messages call.
func AnthropicMessages(ctx context.Context, client *http.Client, provider, endpoint string, header http.Header, body *AnthropicRequest) (*models.LLMResponse, error) {
	var resp AnthropicResponse
	if err := DoJSON(ctx, client, provider, endpoint, header, body, &resp); err != nil {
		return nil, err
	}
	return AnthropicToLLMResponseJSON(&resp, body.JSONResponseTool()), nil
}

// AnthropicMessagesStream starts a streaming /v1/messages call and converts
// Anthropic's events into OpenAI-style chunks (role, text and tool-call
// deltas, finish reason, and a final usage chunk with empty choices).
func AnthropicMessagesStream(ctx context.Context, client *http.Client, provider, endpoint string, header http.Header, body *AnthropicRequest, observe func(time.Duration, error)) (<-chan models.StreamChunk, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("%s: marshal request: %w", provider, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("%s: build request: %w", provider, err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	start := time.Now()
	conn, err := openStream(ctx, client, provider, req)
	if err != nil {
		if observe != nil {
			observe(time.Since(start), err)
		}
		return nil, err
	}
	ttfb := time.Since(start)
	done := func(err error) {
		if observe != nil {
			observe(ttfb, err)
		}
	}
	if isJSONResponse(conn.resp) {
		return chunksFromJSONBody(ctx, provider, conn, func(b []byte) (*models.LLMResponse, error) {
			var ar AnthropicResponse
			if err := json.Unmarshal(b, &ar); err != nil {
				return nil, err
			}
			return AnthropicToLLMResponseJSON(&ar, body.JSONResponseTool()), nil
		}, done), nil
	}
	st := &anthropicStreamState{provider: provider, model: body.Model, created: time.Now().Unix(), toolIndex: map[int]int{}, jsonTool: body.JSONResponseTool(), jsonBlock: -1}
	return runSSE(ctx, provider, conn, st.decode, st.onEOF, done), nil
}

type anthropicStreamState struct {
	provider  string
	id        string
	model     string
	created   int64
	usage     AnthropicUsage
	toolIndex map[int]int // content block index -> tool call index
	nextTool  int
	finished  bool
	stopped   bool

	// jsonTool is the response_format emulation tool ("" when unused);
	// jsonBlock is the content block index streaming its input (-1 before
	// it starts) and jsonSent reports whether any of it was emitted.
	jsonTool  string
	jsonBlock int
	jsonSent  bool
}

func (s *anthropicStreamState) chunk(choices []models.StreamChoice) models.StreamChunk {
	if choices == nil {
		choices = []models.StreamChoice{}
	}
	return models.StreamChunk{ID: s.id, Object: "chat.completion.chunk", Created: s.created, Model: s.model, Choices: choices}
}

func (s *anthropicStreamState) delta(d models.MessageDelta) []models.StreamChunk {
	return []models.StreamChunk{s.chunk([]models.StreamChoice{{Index: 0, Delta: d}})}
}

func (s *anthropicStreamState) decode(ev SSEEvent) ([]models.StreamChunk, bool, error) {
	if strings.TrimSpace(ev.Data) == "" {
		return nil, false, nil
	}
	var e struct {
		Type    string          `json:"type"`
		Index   int             `json:"index"`
		Message json.RawMessage `json:"message"`
		Block   struct {
			Type string `json:"type"`
			Text string `json:"text"`
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"content_block"`
		Delta struct {
			Type        string `json:"type"`
			Text        string `json:"text"`
			PartialJSON string `json:"partial_json"`
			StopReason  string `json:"stop_reason"`
		} `json:"delta"`
		Usage *AnthropicUsage `json:"usage"`
		Error *struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(ev.Data), &e); err != nil {
		return nil, false, BadResponseError(s.provider, err)
	}
	typ := e.Type
	if typ == "" {
		typ = ev.Event
	}
	switch typ {
	case "message_start":
		var m struct {
			ID    string         `json:"id"`
			Model string         `json:"model"`
			Usage AnthropicUsage `json:"usage"`
		}
		if len(e.Message) > 0 {
			if err := json.Unmarshal(e.Message, &m); err != nil {
				return nil, false, BadResponseError(s.provider, err)
			}
		}
		if m.ID != "" {
			s.id = m.ID
		}
		if m.Model != "" {
			s.model = m.Model
		}
		s.usage = m.Usage
		return s.delta(models.MessageDelta{Role: models.RoleAssistant}), false, nil
	case "content_block_start":
		switch e.Block.Type {
		case "text":
			if e.Block.Text != "" {
				return s.delta(models.MessageDelta{Content: e.Block.Text}), false, nil
			}
		case "tool_use":
			if s.jsonTool != "" && e.Block.Name == s.jsonTool && s.jsonBlock < 0 {
				// response_format emulation: the tool input is the content.
				s.jsonBlock = e.Index
				return nil, false, nil
			}
			idx := s.nextTool
			s.nextTool++
			s.toolIndex[e.Index] = idx
			return s.delta(models.MessageDelta{ToolCalls: []models.ToolCallDelta{{
				Index: idx, ID: e.Block.ID, Type: "function", Function: models.ToolFunctionDelta{Name: e.Block.Name},
			}}}), false, nil
		}
		return nil, false, nil
	case "content_block_stop":
		if s.jsonBlock >= 0 && e.Index == s.jsonBlock && !s.jsonSent {
			s.jsonSent = true // the model produced an empty object
			return s.delta(models.MessageDelta{Content: "{}"}), false, nil
		}
		return nil, false, nil
	case "content_block_delta":
		switch e.Delta.Type {
		case "text_delta":
			if e.Delta.Text == "" || s.jsonBlock >= 0 {
				// Text after the JSON answer would make it invalid JSON.
				return nil, false, nil
			}
			return s.delta(models.MessageDelta{Content: e.Delta.Text}), false, nil
		case "input_json_delta":
			if s.jsonBlock >= 0 && e.Index == s.jsonBlock {
				if e.Delta.PartialJSON == "" {
					return nil, false, nil
				}
				s.jsonSent = true
				return s.delta(models.MessageDelta{Content: e.Delta.PartialJSON}), false, nil
			}
			idx, ok := s.toolIndex[e.Index]
			if !ok || e.Delta.PartialJSON == "" {
				return nil, false, nil
			}
			return s.delta(models.MessageDelta{ToolCalls: []models.ToolCallDelta{{
				Index: idx, Function: models.ToolFunctionDelta{Arguments: e.Delta.PartialJSON},
			}}}), false, nil
		}
		return nil, false, nil // thinking/signature deltas are not surfaced
	case "message_delta":
		if e.Usage != nil {
			if e.Usage.OutputTokens > 0 {
				s.usage.OutputTokens = e.Usage.OutputTokens
			}
			if e.Usage.InputTokens > 0 {
				s.usage.InputTokens = e.Usage.InputTokens
			}
			if e.Usage.CacheReadInputTokens > 0 {
				s.usage.CacheReadInputTokens = e.Usage.CacheReadInputTokens
			}
			if e.Usage.CacheCreationInputTokens > 0 {
				s.usage.CacheCreationInputTokens = e.Usage.CacheCreationInputTokens
			}
		}
		if e.Delta.StopReason == "" {
			return nil, false, nil
		}
		finish := AnthropicFinishReason(e.Delta.StopReason)
		if finish == "tool_calls" && s.jsonBlock >= 0 && s.nextTool == 0 {
			finish = "stop"
		}
		s.finished = true
		return []models.StreamChunk{s.chunk([]models.StreamChoice{{Index: 0, FinishReason: &finish}})}, false, nil
	case "message_stop":
		s.stopped = true
		var out []models.StreamChunk
		if !s.finished {
			finish := "stop"
			out = append(out, s.chunk([]models.StreamChoice{{Index: 0, FinishReason: &finish}}))
		}
		final := s.chunk(nil)
		final.Usage = s.usage.ToUsage()
		return append(out, final), true, nil
	case "error":
		ue := &UpstreamError{Provider: s.provider, StatusCode: http.StatusBadGateway, Message: "upstream stream error"}
		if e.Error != nil {
			ue.Type = e.Error.Type
			if e.Error.Message != "" {
				ue.Message = truncate(e.Error.Message, maxErrorMessageLen)
			}
			switch e.Error.Type {
			case "overloaded_error":
				ue.StatusCode = 529
			case "rate_limit_error":
				ue.StatusCode = http.StatusTooManyRequests
			case "api_error":
				ue.StatusCode = http.StatusInternalServerError
			case "invalid_request_error":
				ue.StatusCode = http.StatusBadRequest
			}
		}
		return nil, false, ue
	default: // ping, unknown future events
		return nil, false, nil
	}
}

func (s *anthropicStreamState) onEOF() error {
	if s.stopped {
		return nil
	}
	return &UpstreamError{Provider: s.provider, StatusCode: http.StatusBadGateway, Message: "upstream stream ended before message_stop"}
}
