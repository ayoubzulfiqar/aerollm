package universal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// StreamChatCompletions implements providers.StreamingProvider with the
// ConverseStream API (POST /model/{modelId}/converse-stream, signed like
// Converse). The AWS event-stream response is decoded natively and mapped to
// OpenAI-style chunks:
//
//   - messageStart → role delta
//   - contentBlockStart (toolUse) → tool call delta with id and name
//   - contentBlockDelta → text content delta or tool call arguments delta
//   - contentBlockStop → "{}" arguments for a tool call without input
//   - messageStop → finish_reason
//   - metadata → final usage chunk (empty choices)
//
// An exception (or error) message is reported as an *UpstreamError with an
// HTTP-equivalent status (throttlingException → 429, validationException →
// 400, ...). When it is the first message of the stream it is returned
// directly, so callers can still fall back to another provider; later it is
// sent as the final chunk's Err. Frame corruption (CRC mismatch, truncation,
// oversized frames) fails the stream with a 502.
func (a *BedrockAdapter) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	httpReq, err := a.newConverseRequest(ctx, req, "converse-stream", "application/vnd.amazon.eventstream")
	if err != nil {
		return nil, err
	}
	st := &bedrockStreamState{
		provider:  a.name,
		id:        "chatcmpl-" + models.GenerateTraceID()[:24],
		model:     req.Model,
		created:   a.now().Unix(),
		toolIndex: map[int]int{},
		toolArgs:  map[int]bool{},
	}
	return providers.StartStream(ctx, a.http, a.name, httpReq, a.health.Observe, st.start)
}

// bedrockStreamState maps ConverseStream events to chunks.
type bedrockStreamState struct {
	provider string
	id       string
	model    string
	created  int64

	toolIndex map[int]int  // contentBlockIndex -> tool call index
	toolArgs  map[int]bool // tool call index -> input emitted
	nextTool  int
	stopped   bool
}

// start reads the first message before the stream is handed to the caller:
// an exception there (e.g. throttling) is returned directly so the router
// can fall back.
func (s *bedrockStreamState) start(header http.Header, body io.Reader) (providers.StreamPump, error) {
	_ = header
	dec := newEventStreamDecoder(bufio.NewReaderSize(body, 64<<10))
	first, err := s.next(dec)
	if err != nil {
		return nil, err
	}
	pending, err := s.handle(first)
	if err != nil {
		return nil, err
	}
	return func(emit func(models.StreamChunk) bool) error {
		for {
			for _, c := range pending {
				if !emit(c) {
					return nil // consumer gone; StartStream reports ctx.Err()
				}
			}
			msg, err := s.next(dec)
			if err != nil {
				return err
			}
			if msg == nil {
				return nil // clean end after messageStop
			}
			if pending, err = s.handle(msg); err != nil {
				for _, c := range pending {
					if !emit(c) {
						return nil
					}
				}
				return err
			}
		}
	}, nil
}

// next decodes the next message. It returns (nil, nil) at a clean end of a
// completed stream.
func (s *bedrockStreamState) next(dec *eventStreamDecoder) (*esMessage, error) {
	msg, err := dec.Next()
	switch {
	case err == nil:
		return msg, nil
	case errors.Is(err, io.EOF):
		if s.stopped {
			return nil, nil
		}
		return nil, &providers.UpstreamError{Provider: s.provider, StatusCode: http.StatusBadGateway, Message: "upstream stream ended before messageStop"}
	case errors.Is(err, errEventStreamCRC), errors.Is(err, errEventStreamTruncated), errors.Is(err, errEventStreamFrame):
		return nil, providers.BadResponseError(s.provider, err)
	default:
		return nil, err // read failure: reported as a transport error
	}
}

func (s *bedrockStreamState) chunk(choices []models.StreamChoice) models.StreamChunk {
	if choices == nil {
		choices = []models.StreamChoice{}
	}
	return models.StreamChunk{ID: s.id, Object: "chat.completion.chunk", Created: s.created, Model: s.model, Choices: choices}
}

func (s *bedrockStreamState) delta(d models.MessageDelta) []models.StreamChunk {
	return []models.StreamChunk{s.chunk([]models.StreamChoice{{Index: 0, Delta: d}})}
}

// converseStreamEvent is the union of ConverseStream event payloads.
type converseStreamEvent struct {
	Role              string `json:"role"`
	ContentBlockIndex int    `json:"contentBlockIndex"`
	Start             *struct {
		ToolUse *struct {
			ToolUseID string `json:"toolUseId"`
			Name      string `json:"name"`
		} `json:"toolUse"`
	} `json:"start"`
	Delta *struct {
		Text    *string `json:"text"`
		ToolUse *struct {
			Input string `json:"input"`
		} `json:"toolUse"`
	} `json:"delta"`
	StopReason string         `json:"stopReason"`
	Usage      *converseUsage `json:"usage"`
}

// handle converts one message into chunks, or an error for exception and
// error messages.
func (s *bedrockStreamState) handle(m *esMessage) ([]models.StreamChunk, error) {
	switch m.header(":message-type") {
	case "exception":
		return nil, s.exception(m)
	case "error":
		msg := m.header(":error-message")
		if msg == "" {
			msg = "upstream stream error"
		}
		return nil, &providers.UpstreamError{Provider: s.provider, StatusCode: http.StatusBadGateway, Type: truncateStr(m.header(":error-code"), 100), Message: truncateStr(msg, 1024)}
	case "event", "":
	default:
		return nil, nil // unknown message types are ignored
	}
	eventType := m.header(":event-type")
	switch eventType {
	case "messageStart", "contentBlockStart", "contentBlockDelta", "contentBlockStop", "messageStop", "metadata":
	default:
		return nil, nil // future event types
	}
	var ev converseStreamEvent
	if len(m.Payload) > 0 {
		if err := json.Unmarshal(m.Payload, &ev); err != nil {
			return nil, providers.BadResponseError(s.provider, err)
		}
	}
	switch eventType {
	case "messageStart":
		return s.delta(models.MessageDelta{Role: models.RoleAssistant}), nil
	case "contentBlockStart":
		if ev.Start == nil || ev.Start.ToolUse == nil {
			return nil, nil
		}
		idx := s.nextTool
		s.nextTool++
		s.toolIndex[ev.ContentBlockIndex] = idx
		return s.delta(models.MessageDelta{ToolCalls: []models.ToolCallDelta{{
			Index: idx, ID: ev.Start.ToolUse.ToolUseID, Type: "function", Function: models.ToolFunctionDelta{Name: ev.Start.ToolUse.Name},
		}}}), nil
	case "contentBlockDelta":
		if ev.Delta == nil {
			return nil, nil
		}
		if ev.Delta.Text != nil && *ev.Delta.Text != "" {
			return s.delta(models.MessageDelta{Content: *ev.Delta.Text}), nil
		}
		if ev.Delta.ToolUse != nil && ev.Delta.ToolUse.Input != "" {
			idx, ok := s.toolIndex[ev.ContentBlockIndex]
			if !ok {
				return nil, nil
			}
			s.toolArgs[idx] = true
			return s.delta(models.MessageDelta{ToolCalls: []models.ToolCallDelta{{
				Index: idx, Function: models.ToolFunctionDelta{Arguments: ev.Delta.ToolUse.Input},
			}}}), nil
		}
		return nil, nil // reasoning content and other deltas are not surfaced
	case "contentBlockStop":
		if idx, ok := s.toolIndex[ev.ContentBlockIndex]; ok && !s.toolArgs[idx] {
			s.toolArgs[idx] = true
			return s.delta(models.MessageDelta{ToolCalls: []models.ToolCallDelta{{
				Index: idx, Function: models.ToolFunctionDelta{Arguments: "{}"},
			}}}), nil
		}
		return nil, nil
	case "messageStop":
		s.stopped = true
		finish := bedrockFinishReason(ev.StopReason)
		return []models.StreamChunk{s.chunk([]models.StreamChoice{{Index: 0, FinishReason: &finish}})}, nil
	case "metadata":
		if ev.Usage == nil {
			return nil, nil
		}
		c := s.chunk(nil)
		c.Usage = ev.Usage.toUsage()
		return []models.StreamChunk{c}, nil
	}
	return nil, nil
}

// exception converts an exception message into an *UpstreamError with the
// HTTP status Bedrock uses for that exception.
func (s *bedrockStreamState) exception(m *esMessage) error {
	typ := m.header(":exception-type")
	var body struct {
		Message            string `json:"message"`
		MessageUpper       string `json:"Message"`
		OriginalStatusCode int    `json:"originalStatusCode"`
	}
	_ = json.Unmarshal(m.Payload, &body)
	msg := body.Message
	if msg == "" {
		msg = body.MessageUpper
	}
	if msg == "" {
		msg = "upstream stream exception"
		if typ != "" {
			msg += ": " + typ
		}
	}
	status := bedrockExceptionStatus(typ)
	if strings.EqualFold(typ, "modelStreamErrorException") && body.OriginalStatusCode >= 400 && body.OriginalStatusCode <= 599 {
		status = body.OriginalStatusCode
	}
	return &providers.UpstreamError{Provider: s.provider, StatusCode: status, Type: truncateStr(typ, 100), Message: truncateStr(msg, 1024)}
}

// bedrockExceptionStatus maps a Bedrock exception name to its HTTP status.
func bedrockExceptionStatus(typ string) int {
	switch strings.ToLower(typ) {
	case "throttlingexception", "servicequotaexceededexception":
		return http.StatusTooManyRequests
	case "validationexception":
		return http.StatusBadRequest
	case "accessdeniedexception":
		return http.StatusForbidden
	case "resourcenotfoundexception":
		return http.StatusNotFound
	case "modeltimeoutexception":
		return http.StatusRequestTimeout
	case "internalserverexception":
		return http.StatusInternalServerError
	case "serviceunavailableexception", "modelnotreadyexception":
		return http.StatusServiceUnavailable
	default: // modelStreamErrorException, unknown exceptions
		return http.StatusBadGateway
	}
}

// truncateStr caps s at n bytes without splitting a UTF-8 sequence.
func truncateStr(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && (s[cut]&0xC0) == 0x80 {
		cut--
	}
	return s[:cut] + "..."
}
