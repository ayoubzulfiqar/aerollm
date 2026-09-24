package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// sseWriter writes OpenAI-style Server-Sent Events.
type sseWriter struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	started bool
}

func newSSEWriter(w http.ResponseWriter) *sseWriter {
	return &sseWriter{w: w, rc: http.NewResponseController(w)}
}

func (s *sseWriter) start() {
	if s.started {
		return
	}
	s.started = true
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	// Streams may outlive the server's WriteTimeout; lift it for this request.
	_ = s.rc.SetWriteDeadline(time.Time{})
	s.w.WriteHeader(http.StatusOK)
}

func (s *sseWriter) data(payload []byte) error {
	s.start()
	if _, err := s.w.Write([]byte("data: ")); err != nil {
		return err
	}
	if _, err := s.w.Write(payload); err != nil {
		return err
	}
	if _, err := s.w.Write([]byte("\n\n")); err != nil {
		return err
	}
	return s.rc.Flush()
}

func (s *sseWriter) event(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.data(b)
}

func (s *sseWriter) done() error { return s.data([]byte("[DONE]")) }

// streamError writes an in-band error event (the status is already 200).
func (s *sseWriter) streamError(err error) {
	status, msg, typ := upstreamStatus(err)
	_ = s.event(map[string]interface{}{"error": map[string]interface{}{"message": msg, "type": typ, "code": status}})
}

// streamChat serves stream=true. It streams natively when the provider
// supports it and otherwise converts a normal completion into chunks.
func (h *Handler) streamChat(ctx context.Context, w http.ResponseWriter, req *models.LLMRequest, m *requestMeta) {
	if h.usesServerTools(req) {
		h.streamFromCompletion(ctx, w, req, m)
		return
	}

	start := time.Now()
	ch, provider, err := h.openNativeStream(ctx, req)
	if errors.Is(err, providers.ErrStreamingNotSupported) {
		h.streamFromCompletion(ctx, w, req, m)
		return
	}
	if err == nil && ch == nil {
		err = providers.BadResponseError(providerNameOf(provider), errors.New("provider returned no stream"))
	}
	if err != nil {
		h.recordProvider(providerNameOf(provider), start, err)
		h.recordFailure(ctx, req, m, provider, err)
		writeUpstreamError(w, err)
		return
	}

	providerName := providerNameOf(provider)
	if providerName != "" {
		w.Header().Set("X-AeroLLM-Provider", providerName)
	}
	w.Header().Set("X-AeroLLM-Cache", "MISS")
	sse := newSSEWriter(w)
	sse.start()

	acc := newStreamAccumulator(req.Model)
	for {
		select {
		case <-ctx.Done():
			// Client disconnected; the provider observes ctx and closes ch.
			// Tokens generated so far were still consumed upstream, so they
			// are billed (estimated) but the partial answer is not cached.
			h.recordProvider(providerName, start, ctx.Err())
			h.recordPartial(ctx, req, m, acc, providerName)
			return
		case chunk, ok := <-ch:
			if !ok {
				if ctx.Err() != nil {
					// Closed because the client went away, not because the
					// upstream finished.
					h.recordProvider(providerName, start, ctx.Err())
					h.recordPartial(ctx, req, m, acc, providerName)
					return
				}
				h.recordProvider(providerName, start, nil)
				h.finishStream(ctx, sse, req, m, acc, providerName)
				return
			}
			if chunk.Err != nil {
				h.recordProvider(providerName, start, chunk.Err)
				h.recordFailure(ctx, req, m, provider, chunk.Err)
				sse.streamError(chunk.Err)
				h.recordPartial(ctx, req, m, acc, providerName)
				return
			}
			if chunk.Model == "" {
				chunk.Model = req.Model
			}
			if chunk.Object == "" {
				chunk.Object = "chat.completion.chunk"
			}
			if chunk.ID == "" {
				chunk.ID = "chatcmpl-" + m.requestID
			}
			if chunk.Created == 0 {
				chunk.Created = start.Unix()
			}
			acc.add(&chunk)
			if len(chunk.Choices) == 0 && chunk.Usage != nil && !wantsUsage(req) {
				continue // usage-only chunk the client did not ask for
			}
			if err := sse.event(chunk); err != nil {
				return // client write failed; stop reading
			}
		}
	}
}

// openNativeStream starts an upstream stream for req through the configured
// resolver or the router (with fallback). It returns
// providers.ErrStreamingNotSupported when no candidate can stream.
func (h *Handler) openNativeStream(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, providers.Provider, error) {
	if p, ok := h.ResolveModel(req.Model); ok {
		sp, canStream := p.(providers.StreamingProvider)
		if !canStream {
			return nil, p, providers.ErrStreamingNotSupported
		}
		ch, err := sp.StreamChatCompletions(ctx, req)
		return ch, p, err
	}
	if h.Router != nil {
		return h.Router.StreamWithFallback(ctx, req)
	}
	return nil, nil, providers.ErrStreamingNotSupported
}

// recordPartial bills an interrupted stream for what was generated so far.
func (h *Handler) recordPartial(ctx context.Context, req *models.LLMRequest, m *requestMeta, acc *streamAccumulator, providerName string) {
	if len(acc.choices) == 0 && acc.usage == nil {
		return
	}
	resp := acc.response()
	if resp.ID == "" {
		resp.ID = "chatcmpl-" + m.requestID
	}
	estimated := false
	if resp.Usage == nil {
		resp.Usage = estimateUsage(req, resp)
		estimated = true
	}
	m.noStore = true // never cache a partial answer
	respBytes, _ := json.Marshal(resp)
	h.afterCompletion(ctx, req, resp, respBytes, providerName, m, estimated, true)
}

func wantsUsage(req *models.LLMRequest) bool {
	return req.StreamOptions != nil && req.StreamOptions.IncludeUsage
}

// finishStream emits the usage chunk (when requested and the upstream did
// not send one), the [DONE] sentinel, and records the completed response.
func (h *Handler) finishStream(ctx context.Context, sse *sseWriter, req *models.LLMRequest, m *requestMeta, acc *streamAccumulator, providerName string) {
	resp := acc.response()
	if resp.ID == "" {
		resp.ID = "chatcmpl-" + m.requestID
	}
	if !acc.finished {
		m.noStore = true // upstream ended without a finish reason: incomplete
	}
	estimated := false
	if resp.Usage == nil {
		resp.Usage = estimateUsage(req, resp)
		estimated = true
		if wantsUsage(req) {
			_ = sse.event(models.StreamChunk{ID: resp.ID, Object: "chat.completion.chunk", Created: resp.Created, Model: resp.Model, Choices: []models.StreamChoice{}, Usage: resp.Usage})
		}
	}
	_ = sse.done()
	respBytes, _ := json.Marshal(resp)
	h.afterCompletion(ctx, req, resp, respBytes, providerName, m, estimated, true)
}

// streamFromCompletion runs a normal completion and replays it as chunks.
func (h *Handler) streamFromCompletion(ctx context.Context, w http.ResponseWriter, req *models.LLMRequest, m *requestMeta) {
	resp, provider, err := h.complete(ctx, req)
	if err != nil {
		h.recordFailure(ctx, req, m, provider, err)
		writeUpstreamError(w, err)
		return
	}
	if resp.Model == "" {
		resp.Model = req.Model
	}
	if resp.ID == "" {
		resp.ID = "chatcmpl-" + m.requestID
	}
	if resp.Created == 0 {
		resp.Created = time.Now().Unix()
	}
	resp.Object = "chat.completion"
	estimated := false
	if resp.Usage == nil {
		resp.Usage = estimateUsage(req, resp)
		estimated = true
	}
	if name := providerNameOf(provider); name != "" {
		w.Header().Set("X-AeroLLM-Provider", name)
	}
	w.Header().Set("X-AeroLLM-Cache", "MISS")
	h.writeStreamFromResponse(ctx, w, req, resp)
	respBytes, _ := json.Marshal(resp)
	h.afterCompletion(ctx, req, resp, respBytes, providerNameOf(provider), m, estimated, true)
}

// writeStreamFromResponse replays a complete response as SSE chunks.
func (h *Handler) writeStreamFromResponse(ctx context.Context, w http.ResponseWriter, req *models.LLMRequest, resp *models.LLMResponse) {
	sse := newSSEWriter(w)
	chunks := models.StreamChunksFromResponse(resp)
	for i := range chunks {
		if ctx.Err() != nil {
			return
		}
		if !wantsUsage(req) {
			chunks[i].Usage = nil
		}
		if err := sse.event(chunks[i]); err != nil {
			return
		}
	}
	if wantsUsage(req) && resp.Usage != nil && len(chunks) == 0 {
		_ = sse.event(models.StreamChunk{ID: resp.ID, Object: "chat.completion.chunk", Created: resp.Created, Model: resp.Model, Choices: []models.StreamChoice{}, Usage: resp.Usage})
	}
	_ = sse.done()
}

// streamAccumulator rebuilds a complete response from stream chunks so the
// stream can be cached, billed, logged and written to the ledger.
type streamAccumulator struct {
	id, model string
	created   int64
	usage     *models.Usage
	choices   map[int]*accChoice
	finished  bool // a finish_reason was received
}

type accChoice struct {
	role    models.MessageRole
	content strings.Builder
	finish  string
	tools   map[int]*models.ToolCall
}

func newStreamAccumulator(model string) *streamAccumulator {
	return &streamAccumulator{model: model, choices: map[int]*accChoice{}}
}

func (a *streamAccumulator) add(c *models.StreamChunk) {
	if a.id == "" {
		a.id = c.ID
	}
	if c.Model != "" {
		a.model = c.Model
	}
	if a.created == 0 {
		a.created = c.Created
	}
	if c.Usage != nil {
		a.usage = c.Usage
	}
	for _, ch := range c.Choices {
		acc, ok := a.choices[ch.Index]
		if !ok {
			acc = &accChoice{tools: map[int]*models.ToolCall{}}
			a.choices[ch.Index] = acc
		}
		if ch.Delta.Role != "" {
			acc.role = ch.Delta.Role
		}
		acc.content.WriteString(ch.Delta.Content)
		for _, td := range ch.Delta.ToolCalls {
			tc, ok := acc.tools[td.Index]
			if !ok {
				tc = &models.ToolCall{Type: "function"}
				acc.tools[td.Index] = tc
			}
			if td.ID != "" {
				tc.ID = td.ID
			}
			if td.Type != "" {
				tc.Type = td.Type
			}
			if td.Function.Name != "" {
				tc.Function.Name = td.Function.Name
			}
			tc.Function.Arguments += td.Function.Arguments
		}
		if ch.FinishReason != nil {
			acc.finish = *ch.FinishReason
			a.finished = true
		}
	}
}

func (a *streamAccumulator) response() *models.LLMResponse {
	resp := &models.LLMResponse{ID: a.id, Object: "chat.completion", Created: a.created, Model: a.model, Usage: a.usage}
	if resp.Created == 0 {
		resp.Created = time.Now().Unix()
	}
	idx := make([]int, 0, len(a.choices))
	for i := range a.choices {
		idx = append(idx, i)
	}
	sort.Ints(idx)
	for _, i := range idx {
		c := a.choices[i]
		role := c.role
		if role == "" {
			role = models.RoleAssistant
		}
		msg := models.Message{Role: role}
		if text := c.content.String(); text != "" || len(c.tools) == 0 {
			msg.Content = &text
		}
		if len(c.tools) > 0 {
			ti := make([]int, 0, len(c.tools))
			for k := range c.tools {
				ti = append(ti, k)
			}
			sort.Ints(ti)
			for _, k := range ti {
				msg.ToolCalls = append(msg.ToolCalls, *c.tools[k])
			}
		}
		finish := c.finish
		if finish == "" {
			finish = "stop"
		}
		resp.Choices = append(resp.Choices, models.Choice{Index: i, Message: msg, FinishReason: finish})
	}
	return resp
}

// Complete runs a non-streaming completion with the gateway's provider
// resolution, fallback and server-side tools, without HTTP concerns. It is
// used by internal consumers (evaluation judge, batch jobs, realtime).
func (h *Handler) Complete(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, providers.Provider, error) {
	if req == nil {
		return nil, nil, errors.New("nil request")
	}
	resp, p, err := h.complete(ctx, req)
	if err == nil && resp != nil && resp.Model == "" {
		resp.Model = req.Model
	}
	return resp, p, err
}

// OpenStream starts a streaming completion with the same provider resolution
// and fallback as /v1/chat/completions. Providers that cannot stream are
// served by replaying a complete response as chunks.
func (h *Handler) OpenStream(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, providers.Provider, error) {
	if req == nil {
		return nil, nil, errors.New("nil request")
	}
	if !h.usesServerTools(req) {
		ch, provider, err := h.openNativeStream(ctx, req)
		if err == nil && ch != nil {
			return ch, provider, nil
		}
		if err != nil && !errors.Is(err, providers.ErrStreamingNotSupported) {
			return nil, provider, err
		}
	}
	resp, p, err := h.Complete(ctx, req)
	if err != nil {
		return nil, p, err
	}
	chunks := models.StreamChunksFromResponse(resp)
	out := make(chan models.StreamChunk, len(chunks))
	for _, c := range chunks {
		out <- c
	}
	close(out)
	return out, p, nil
}
