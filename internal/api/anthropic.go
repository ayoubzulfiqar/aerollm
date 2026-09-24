package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// Anthropic Messages API compatibility (/v1/messages). Requests are
// translated to the gateway's OpenAI-style LLMRequest, served through the
// normal chat pipeline (auth, budgets, cache, fallback, accounting) and the
// result is translated back, including streaming events.

type anthropicRequest struct {
	Model         string             `json:"model"`
	MaxTokens     int                `json:"max_tokens"`
	System        json.RawMessage    `json:"system,omitempty"`
	Messages      []anthropicMessage `json:"messages"`
	Temperature   *float64           `json:"temperature,omitempty"`
	TopP          *float64           `json:"top_p,omitempty"`
	TopK          *int               `json:"top_k,omitempty"`
	StopSequences []string           `json:"stop_sequences,omitempty"`
	Stream        bool               `json:"stream,omitempty"`
	Tools         []anthropicTool    `json:"tools,omitempty"`
	ToolChoice    *struct {
		Type string `json:"type"`
		Name string `json:"name,omitempty"`
	} `json:"tool_choice,omitempty"`
	Metadata *struct {
		UserID string `json:"user_id,omitempty"`
	} `json:"metadata,omitempty"`
}

type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicBlock struct {
	Type         string               `json:"type"`
	Text         string               `json:"text,omitempty"`
	Source       *anthropicSource     `json:"source,omitempty"`
	ID           string               `json:"id,omitempty"`
	Name         string               `json:"name,omitempty"`
	Input        json.RawMessage      `json:"input,omitempty"`
	ToolUseID    string               `json:"tool_use_id,omitempty"`
	Content      json.RawMessage      `json:"content,omitempty"`
	IsError      bool                 `json:"is_error,omitempty"`
	CacheControl *models.CacheControl `json:"cache_control,omitempty"`
}

type anthropicSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

type anthropicTool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	InputSchema map[string]interface{} `json:"input_schema"`
}

type anthropicResponse struct {
	ID           string           `json:"id"`
	Type         string           `json:"type"`
	Role         string           `json:"role"`
	Model        string           `json:"model"`
	Content      []anthropicBlock `json:"content"`
	StopReason   *string          `json:"stop_reason"`
	StopSequence *string          `json:"stop_sequence"`
	Usage        anthropicUsage   `json:"usage"`
}

type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// parseBlocks decodes Anthropic content that is either a string or an array
// of blocks.
func parseBlocks(raw json.RawMessage) ([]anthropicBlock, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return []anthropicBlock{{Type: "text", Text: s}}, nil
	}
	var blocks []anthropicBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, err
	}
	return blocks, nil
}

func stringPtr(s string) *string { return &s }

// toLLMRequest translates an Anthropic Messages request.
func (a *anthropicRequest) toLLMRequest() (*models.LLMRequest, error) {
	if a.Model == "" {
		return nil, errors.New("model is required")
	}
	if a.MaxTokens <= 0 {
		return nil, errors.New("max_tokens is required and must be positive")
	}
	req := &models.LLMRequest{
		Model:       a.Model,
		Temperature: a.Temperature,
		TopP:        a.TopP,
		Stop:        a.StopSequences,
		Stream:      a.Stream,
	}
	mt := a.MaxTokens
	req.MaxTokens = &mt
	if a.Stream {
		req.StreamOptions = &models.StreamOptions{IncludeUsage: true}
	}
	if a.Metadata != nil {
		req.User = a.Metadata.UserID
	}

	if sys, err := parseBlocks(a.System); err != nil {
		return nil, fmt.Errorf("invalid system: %w", err)
	} else if len(sys) > 0 {
		var parts []models.ContentPart
		var cc *models.CacheControl
		for _, b := range sys {
			if b.Type == "text" {
				parts = append(parts, models.ContentPart{Type: models.ContentPartText, Text: b.Text, CacheControl: b.CacheControl})
				if b.CacheControl != nil {
					cc = b.CacheControl
				}
			}
		}
		text := models.JoinTextParts(parts)
		req.Messages = append(req.Messages, models.Message{Role: models.RoleSystem, Content: &text, CacheControl: cc})
	}

	for i, m := range a.Messages {
		if m.Role != "user" && m.Role != "assistant" {
			return nil, fmt.Errorf("messages[%d].role must be user or assistant", i)
		}
		blocks, err := parseBlocks(m.Content)
		if err != nil {
			return nil, fmt.Errorf("messages[%d].content: %w", i, err)
		}
		var parts []models.ContentPart
		var toolCalls []models.ToolCall
		var toolResults []models.Message
		for _, b := range blocks {
			switch b.Type {
			case "text":
				parts = append(parts, models.ContentPart{Type: models.ContentPartText, Text: b.Text, CacheControl: b.CacheControl})
			case "image":
				if b.Source == nil {
					return nil, fmt.Errorf("messages[%d]: image block without source", i)
				}
				url := b.Source.URL
				if b.Source.Type == "base64" {
					url = "data:" + b.Source.MediaType + ";base64," + b.Source.Data
				}
				parts = append(parts, models.ContentPart{Type: models.ContentPartImageURL, ImageURL: &models.ImageURL{URL: url}})
			case "tool_use":
				args := "{}"
				if len(b.Input) > 0 {
					args = string(b.Input)
				}
				toolCalls = append(toolCalls, models.ToolCall{ID: b.ID, Type: "function", Function: models.ToolFunction{Name: b.Name, Arguments: args}})
			case "tool_result":
				inner, err := parseBlocks(b.Content)
				if err != nil {
					return nil, fmt.Errorf("messages[%d]: tool_result content: %w", i, err)
				}
				var sb strings.Builder
				for _, ib := range inner {
					if ib.Type == "text" {
						sb.WriteString(ib.Text)
					}
				}
				text := sb.String()
				if b.IsError {
					text = "Error: " + text
				}
				toolResults = append(toolResults, models.Message{Role: models.RoleTool, ToolCallID: stringPtr(b.ToolUseID), Content: &text})
			}
		}
		// Tool results answer the previous assistant turn, so they precede
		// any user text in the same Anthropic message.
		req.Messages = append(req.Messages, toolResults...)
		if len(parts) == 0 && len(toolCalls) == 0 {
			continue
		}
		msg := models.Message{Role: models.MessageRole(m.Role), ToolCalls: toolCalls}
		if len(parts) > 0 {
			text := models.JoinTextParts(parts)
			msg.Content = &text
			hasNonText := false
			for _, p := range parts {
				if p.Type != models.ContentPartText || p.CacheControl != nil {
					hasNonText = true
				}
			}
			if hasNonText {
				msg.ContentParts = parts
			}
		}
		req.Messages = append(req.Messages, msg)
	}

	for _, t := range a.Tools {
		req.Tools = append(req.Tools, models.ToolDefinition{Name: t.Name, Description: t.Description, Parameters: t.InputSchema})
	}
	if tc := a.ToolChoice; tc != nil {
		switch tc.Type {
		case "auto":
			req.ToolChoice = &models.ToolChoice{Mode: models.ToolChoiceAuto}
		case "any":
			req.ToolChoice = &models.ToolChoice{Mode: models.ToolChoiceRequired}
		case "none":
			req.ToolChoice = &models.ToolChoice{Mode: models.ToolChoiceNone}
		case "tool":
			req.ToolChoice = &models.ToolChoice{Function: tc.Name}
		}
	}
	return req, nil
}

func anthropicStopReason(finish string) string {
	switch finish {
	case "length":
		return "max_tokens"
	case "tool_calls", "function_call":
		return "tool_use"
	case "stop_sequence":
		return "stop_sequence"
	default:
		return "end_turn"
	}
}

func anthropicID(id string) string {
	id = strings.TrimPrefix(id, "chatcmpl-")
	if strings.HasPrefix(id, "msg_") {
		return id
	}
	return "msg_" + id
}

func toolInput(args string) json.RawMessage {
	if json.Valid([]byte(args)) && strings.HasPrefix(strings.TrimSpace(args), "{") {
		return json.RawMessage(args)
	}
	return json.RawMessage("{}")
}

// fromLLMResponse translates a chat completion into an Anthropic message.
func fromLLMResponse(resp *models.LLMResponse) *anthropicResponse {
	out := &anthropicResponse{ID: anthropicID(resp.ID), Type: "message", Role: "assistant", Model: resp.Model, Content: []anthropicBlock{}}
	finish := "stop"
	if len(resp.Choices) > 0 {
		c := resp.Choices[0]
		finish = c.FinishReason
		if text := c.Message.TextContent(); text != "" {
			out.Content = append(out.Content, anthropicBlock{Type: "text", Text: text})
		}
		for _, tc := range c.Message.ToolCalls {
			out.Content = append(out.Content, anthropicBlock{Type: "tool_use", ID: tc.ID, Name: tc.Function.Name, Input: toolInput(tc.Function.Arguments)})
		}
		if len(c.Message.ToolCalls) > 0 && finish == "stop" {
			finish = "tool_calls"
		}
	}
	reason := anthropicStopReason(finish)
	out.StopReason = &reason
	if resp.Usage != nil {
		out.Usage = anthropicUsage{InputTokens: resp.Usage.PromptTokens, OutputTokens: resp.Usage.CompletionTokens}
	}
	return out
}

func anthropicErrorType(status int) string {
	switch {
	case status == http.StatusUnauthorized:
		return "authentication_error"
	case status == http.StatusForbidden:
		return "permission_error"
	case status == http.StatusNotFound:
		return "not_found_error"
	case status == http.StatusRequestEntityTooLarge:
		return "request_too_large"
	case status == http.StatusTooManyRequests:
		return "rate_limit_error"
	case status == 529 || status == http.StatusServiceUnavailable:
		return "overloaded_error"
	case status >= 500:
		return "api_error"
	default:
		return "invalid_request_error"
	}
}

func writeAnthropicError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"type":  "error",
		"error": map[string]string{"type": anthropicErrorType(status), "message": message},
	})
}

// openAIErrorMessage extracts the message from an OpenAI-style error body.
func openAIErrorMessage(body []byte) string {
	var e struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		return e.Error.Message
	}
	return strings.TrimSpace(string(body))
}

// Messages handles the /v1/messages endpoint (Anthropic compatibility).
// @Summary Anthropic-style messages
// @Description Anthropic Messages API compatible endpoint, served through the gateway's routing, caching and accounting. Supports stream=true.
// @Tags chat
// @Accept json
// @Produce json
// @Success 200 {object} anthropicResponse
// @Router /v1/messages [post]
func (h *Handler) Messages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnthropicError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var areq anthropicRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, defaultMaxBody))
	if err := dec.Decode(&areq); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeAnthropicError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeAnthropicError(w, http.StatusBadRequest, "invalid JSON: "+sanitizeDecodeError(err))
		return
	}
	req, err := areq.toLLMRequest()
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Stream {
		aw := &anthropicStreamWriter{w: w, rc: http.NewResponseController(w), model: req.Model}
		h.serveChat(aw, r, req, "/v1/messages")
		aw.finish()
		return
	}

	cw := &captureWriter{header: http.Header{}}
	h.serveChat(cw, r, req, "/v1/messages")
	for k, v := range cw.header {
		if k != "Content-Length" && k != "Content-Type" {
			w.Header()[k] = v
		}
	}
	status := cw.status
	if status == 0 {
		status = http.StatusOK
	}
	if status != http.StatusOK {
		writeAnthropicError(w, status, openAIErrorMessage(cw.body.Bytes()))
		return
	}
	var resp models.LLMResponse
	if err := json.Unmarshal(cw.body.Bytes(), &resp); err != nil {
		writeAnthropicError(w, http.StatusBadGateway, "invalid upstream response")
		return
	}
	writeJSON(w, http.StatusOK, fromLLMResponse(&resp))
}

// captureWriter buffers a response so it can be translated.
type captureWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (c *captureWriter) Header() http.Header { return c.header }
func (c *captureWriter) WriteHeader(code int) {
	if c.status == 0 {
		c.status = code
	}
}
func (c *captureWriter) Write(b []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	return c.body.Write(b)
}

// anthropicStreamWriter converts the OpenAI SSE stream produced by serveChat
// into Anthropic Messages streaming events on the fly.
type anthropicStreamWriter struct {
	w     http.ResponseWriter
	rc    *http.ResponseController
	model string

	status     int
	errBody    bytes.Buffer
	buf        []byte
	started    bool
	blockOpen  bool
	blockIndex int
	blockKind  string // "text" | "tool_use"
	toolIdx    int
	stopReason string
	usage      anthropicUsage
	finished   bool
}

func (a *anthropicStreamWriter) Header() http.Header { return a.w.Header() }

func (a *anthropicStreamWriter) WriteHeader(code int) {
	if a.status != 0 {
		return
	}
	a.status = code
	if code == http.StatusOK {
		h := a.w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		a.w.WriteHeader(code)
	}
}

// Unwrap lets http.ResponseController reach the real writer (deadlines).
func (a *anthropicStreamWriter) Unwrap() http.ResponseWriter { return a.w }

// Flush is a no-op: events are flushed as they are translated.
func (a *anthropicStreamWriter) Flush() {}

func (a *anthropicStreamWriter) Write(b []byte) (int, error) {
	if a.status == 0 {
		a.WriteHeader(http.StatusOK)
	}
	if a.status != http.StatusOK {
		return a.errBody.Write(b)
	}
	a.buf = append(a.buf, b...)
	for {
		i := bytes.Index(a.buf, []byte("\n\n"))
		if i < 0 {
			break
		}
		frame := string(a.buf[:i])
		a.buf = a.buf[i+2:]
		if err := a.handleFrame(frame); err != nil {
			return len(b), err
		}
	}
	return len(b), nil
}

func (a *anthropicStreamWriter) emit(event string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(a.w, "event: %s\ndata: %s\n\n", event, data); err != nil {
		return err
	}
	return a.rc.Flush()
}

func (a *anthropicStreamWriter) ensureStarted(id, model string) error {
	if a.started {
		return nil
	}
	a.started = true
	if model == "" {
		model = a.model
	}
	return a.emit("message_start", map[string]interface{}{
		"type": "message_start",
		"message": map[string]interface{}{
			"id": anthropicID(id), "type": "message", "role": "assistant", "model": model,
			"content": []interface{}{}, "stop_reason": nil, "stop_sequence": nil,
			"usage": anthropicUsage{},
		},
	})
}

func (a *anthropicStreamWriter) closeBlock() error {
	if !a.blockOpen {
		return nil
	}
	a.blockOpen = false
	err := a.emit("content_block_stop", map[string]interface{}{"type": "content_block_stop", "index": a.blockIndex})
	a.blockIndex++
	return err
}

func (a *anthropicStreamWriter) openBlock(kind string, block interface{}) error {
	if err := a.closeBlock(); err != nil {
		return err
	}
	a.blockOpen = true
	a.blockKind = kind
	return a.emit("content_block_start", map[string]interface{}{"type": "content_block_start", "index": a.blockIndex, "content_block": block})
}

func (a *anthropicStreamWriter) handleFrame(frame string) error {
	data := strings.TrimSpace(strings.TrimPrefix(frame, "data:"))
	if data == "[DONE]" {
		return a.finish()
	}
	var probe struct {
		Error *struct {
			Message string `json:"message"`
			Code    int    `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(data), &probe) == nil && probe.Error != nil {
		a.finished = true
		return a.emit("error", map[string]interface{}{"type": "error", "error": map[string]string{"type": anthropicErrorType(probe.Error.Code), "message": probe.Error.Message}})
	}
	var chunk models.StreamChunk
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return nil // ignore frames we cannot interpret
	}
	if err := a.ensureStarted(chunk.ID, chunk.Model); err != nil {
		return err
	}
	if chunk.Usage != nil {
		a.usage = anthropicUsage{InputTokens: chunk.Usage.PromptTokens, OutputTokens: chunk.Usage.CompletionTokens}
	}
	for _, c := range chunk.Choices {
		if c.Index != 0 {
			continue
		}
		if c.Delta.Content != "" {
			if !a.blockOpen || a.blockKind != "text" {
				if err := a.openBlock("text", map[string]string{"type": "text", "text": ""}); err != nil {
					return err
				}
			}
			if err := a.emit("content_block_delta", map[string]interface{}{"type": "content_block_delta", "index": a.blockIndex, "delta": map[string]string{"type": "text_delta", "text": c.Delta.Content}}); err != nil {
				return err
			}
		}
		for _, td := range c.Delta.ToolCalls {
			if !a.blockOpen || a.blockKind != "tool_use" || td.Index != a.toolIdx || td.ID != "" {
				a.toolIdx = td.Index
				if err := a.openBlock("tool_use", map[string]interface{}{"type": "tool_use", "id": td.ID, "name": td.Function.Name, "input": map[string]interface{}{}}); err != nil {
					return err
				}
			}
			if td.Function.Arguments != "" {
				if err := a.emit("content_block_delta", map[string]interface{}{"type": "content_block_delta", "index": a.blockIndex, "delta": map[string]string{"type": "input_json_delta", "partial_json": td.Function.Arguments}}); err != nil {
					return err
				}
			}
		}
		if c.FinishReason != nil {
			a.stopReason = anthropicStopReason(*c.FinishReason)
		}
	}
	return nil
}

// finish emits the closing events once (on [DONE] or when serveChat returns)
// or translates a pre-stream error response.
func (a *anthropicStreamWriter) finish() error {
	if a.finished {
		return nil
	}
	a.finished = true
	if a.status != 0 && a.status != http.StatusOK {
		writeAnthropicError(a.w, a.status, openAIErrorMessage(a.errBody.Bytes()))
		return nil
	}
	if !a.started {
		return nil
	}
	if err := a.closeBlock(); err != nil {
		return err
	}
	reason := a.stopReason
	if reason == "" {
		reason = "end_turn"
	}
	if err := a.emit("message_delta", map[string]interface{}{
		"type":  "message_delta",
		"delta": map[string]interface{}{"stop_reason": reason, "stop_sequence": nil},
		"usage": map[string]int{"input_tokens": a.usage.InputTokens, "output_tokens": a.usage.OutputTokens},
	}); err != nil {
		return err
	}
	return a.emit("message_stop", map[string]string{"type": "message_stop"})
}
