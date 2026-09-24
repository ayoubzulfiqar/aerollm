package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// OpenAIChatRequest is the wire body for OpenAI-compatible
// /chat/completions. It carries only fields OpenAI accepts (gateway-only
// fields such as rag_enabled or message-level cache_control are dropped).
type OpenAIChatRequest struct {
	Model               string                  `json:"model"`
	Messages            []OpenAIMessage         `json:"messages"`
	Stream              bool                    `json:"stream,omitempty"`
	StreamOptions       *models.StreamOptions   `json:"stream_options,omitempty"`
	MaxTokens           *int                    `json:"max_tokens,omitempty"`
	MaxCompletionTokens *int                    `json:"max_completion_tokens,omitempty"`
	Temperature         *float64                `json:"temperature,omitempty"`
	TopP                *float64                `json:"top_p,omitempty"`
	N                   *int                    `json:"n,omitempty"`
	Stop                []string                `json:"stop,omitempty"`
	PresencePenalty     *float64                `json:"presence_penalty,omitempty"`
	FrequencyPenalty    *float64                `json:"frequency_penalty,omitempty"`
	Seed                *int64                  `json:"seed,omitempty"`
	User                string                  `json:"user,omitempty"`
	Tools               []models.ToolDefinition `json:"tools,omitempty"`
	ToolChoice          *models.ToolChoice      `json:"tool_choice,omitempty"`
	ParallelToolCalls   *bool                   `json:"parallel_tool_calls,omitempty"`
	Logprobs            *bool                   `json:"logprobs,omitempty"`
	TopLogprobs         *int                    `json:"top_logprobs,omitempty"`
	LogitBias           map[string]float64      `json:"logit_bias,omitempty"`
	ResponseFormat      *models.ResponseFormat  `json:"response_format,omitempty"`
}

// OpenAIMessage is a chat message in OpenAI wire format.
type OpenAIMessage struct {
	Role       string            `json:"role"`
	Content    interface{}       `json:"content,omitempty"` // string | []models.ContentPart
	Name       *string           `json:"name,omitempty"`
	ToolCalls  []models.ToolCall `json:"tool_calls,omitempty"`
	ToolCallID *string           `json:"tool_call_id,omitempty"`
}

// NewOpenAIChatRequest converts a gateway request into the OpenAI wire
// format. When stream is true and includeUsage is set, it requests a final
// usage chunk via stream_options.include_usage.
func NewOpenAIChatRequest(req *models.LLMRequest, model string, stream, includeUsage bool) *OpenAIChatRequest {
	if model == "" {
		model = req.Model
	}
	out := &OpenAIChatRequest{
		Model:               model,
		Messages:            make([]OpenAIMessage, 0, len(req.Messages)),
		Stream:              stream,
		MaxTokens:           req.MaxTokens,
		MaxCompletionTokens: req.MaxCompletionTokens,
		Temperature:         req.Temperature,
		TopP:                req.TopP,
		N:                   req.N,
		Stop:                req.Stop,
		PresencePenalty:     req.PresencePenalty,
		FrequencyPenalty:    req.FrequencyPenalty,
		Seed:                req.Seed,
		User:                req.User,
		Tools:               req.Tools,
		Logprobs:            req.Logprobs,
		TopLogprobs:         req.TopLogprobs,
		LogitBias:           req.LogitBias,
		ResponseFormat:      req.ResponseFormat,
	}
	// OpenAI rejects tool_choice / parallel_tool_calls without tools.
	if len(req.Tools) > 0 {
		out.ToolChoice = req.ToolChoice
		out.ParallelToolCalls = req.ParallelToolCalls
	}
	if stream {
		switch {
		case req.StreamOptions != nil:
			so := *req.StreamOptions
			so.IncludeUsage = so.IncludeUsage || includeUsage
			out.StreamOptions = &so
		case includeUsage:
			out.StreamOptions = &models.StreamOptions{IncludeUsage: true}
		}
	}
	for _, m := range req.Messages {
		om := OpenAIMessage{Role: string(m.Role), Name: m.Name, ToolCalls: m.ToolCalls, ToolCallID: m.ToolCallID}
		if parts := m.EffectiveContentParts(); len(parts) > 0 {
			om.Content = parts
		} else if m.Content != nil {
			om.Content = *m.Content
		}
		if m.Role == models.RoleTool && om.Content == nil {
			result := ""
			if m.ToolResult != nil {
				result = *m.ToolResult
			}
			om.Content = result
		}
		out.Messages = append(out.Messages, om)
	}
	return out
}

// decodeOpenAIResponse parses a /chat/completions response body.
func decodeOpenAIResponse(b []byte) (*models.LLMResponse, error) {
	var resp models.LLMResponse
	if err := json.Unmarshal(b, &resp); err != nil {
		return nil, err
	}
	if resp.Object == "" {
		resp.Object = "chat.completion"
	}
	return &resp, nil
}

// OpenAIChatCompletion performs a non-streaming OpenAI-compatible chat
// completion against endpoint (the full /chat/completions URL).
func OpenAIChatCompletion(ctx context.Context, client *http.Client, provider, endpoint string, header http.Header, body *OpenAIChatRequest) (*models.LLMResponse, error) {
	var raw json.RawMessage
	if err := DoJSON(ctx, client, provider, endpoint, header, body, &raw); err != nil {
		return nil, err
	}
	resp, err := decodeOpenAIResponse(raw)
	if err != nil {
		return nil, BadResponseError(provider, err)
	}
	return resp, nil
}

// OpenAIChatStream starts a streaming OpenAI-compatible chat completion and
// returns a channel that follows the StreamingProvider contract. observe, if
// non-nil, receives the stream's start latency and final error.
func OpenAIChatStream(ctx context.Context, client *http.Client, provider, endpoint string, header http.Header, body *OpenAIChatRequest, observe func(time.Duration, error)) (<-chan models.StreamChunk, error) {
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
		return chunksFromJSONBody(ctx, provider, conn, decodeOpenAIResponse, done), nil
	}
	st := &openAIStreamState{provider: provider}
	return runSSE(ctx, provider, conn, st.decode, st.onEOF, done), nil
}

type openAIStreamState struct {
	provider  string
	id        string
	model     string
	created   int64
	sawFinish bool
	sawUsage  bool
}

func (s *openAIStreamState) decode(ev SSEEvent) ([]models.StreamChunk, bool, error) {
	data := bytes.TrimSpace([]byte(ev.Data))
	if len(data) == 0 {
		return nil, false, nil
	}
	if string(data) == "[DONE]" {
		return nil, true, nil
	}
	var payload struct {
		models.StreamChunk
		Error json.RawMessage `json:"error"`
		XGroq *struct {
			Usage *models.Usage `json:"usage"`
		} `json:"x_groq"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, false, BadResponseError(s.provider, err)
	}
	if ev.Event == "error" || (len(payload.Error) > 0 && string(payload.Error) != "null") {
		msg, typ := parseErrorBody(data)
		if msg == "" {
			msg = "upstream stream error"
		}
		return nil, false, &UpstreamError{Provider: s.provider, StatusCode: http.StatusBadGateway, Message: msg, Type: typ}
	}
	c := payload.StreamChunk
	if c.Usage == nil && payload.XGroq != nil && payload.XGroq.Usage != nil {
		c.Usage = payload.XGroq.Usage
	}
	if c.ID == "" {
		c.ID = s.id
	} else if s.id == "" {
		s.id = c.ID
	}
	if c.Model == "" {
		c.Model = s.model
	} else if s.model == "" {
		s.model = c.Model
	}
	if c.Created == 0 {
		if s.created == 0 {
			s.created = time.Now().Unix()
		}
		c.Created = s.created
	} else if s.created == 0 {
		s.created = c.Created
	}
	if c.Object == "" {
		c.Object = "chat.completion.chunk"
	}
	if c.Choices == nil {
		c.Choices = []models.StreamChoice{}
	}
	for _, ch := range c.Choices {
		if ch.FinishReason != nil && *ch.FinishReason != "" {
			s.sawFinish = true
		}
	}
	if c.Usage != nil {
		s.sawUsage = true
	}
	if len(c.Choices) == 0 && c.Usage == nil {
		return nil, false, nil
	}
	return []models.StreamChunk{c}, false, nil
}

func (s *openAIStreamState) onEOF() error {
	// Some servers omit [DONE]; a finish_reason is enough to call it complete.
	if s.sawFinish || s.sawUsage {
		return nil
	}
	return &UpstreamError{Provider: s.provider, StatusCode: http.StatusBadGateway, Message: "upstream stream ended unexpectedly"}
}
