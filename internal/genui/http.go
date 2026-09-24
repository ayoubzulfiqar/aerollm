package genui

import (
	"bytes"
	"context"
	"encoding/json"
	"mime"
	"net/http"
	"strings"
)

// DefaultMaxBufferBytes is the largest JSON response GenUI buffers for
// rewriting; larger responses are streamed through untouched.
const DefaultMaxBufferBytes = 4 << 20

// OptInHeader lets clients request GenUI rendering without a query parameter.
const OptInHeader = "X-AeroLLM-GenUI"

// NewGenUIHandler wraps a chat completions handler. For requests that opt in
// (see IsGenUIRequest) it inspects a successful, non-streaming JSON chat
// completion; if the first choice's message content embeds an aerollm_ui
// schema, the response is re-emitted as Server-Sent Events:
//
//	event: text       data: "<leading text>"
//	event: ui_schema  data: {"type":"aerollm_ui","components":[...]}
//	event: text       data: "<trailing text>"
//	event: done       data: {"id":...,"model":...,"usage":...,"finish_reason":...}
//
// Everything else passes through byte-for-byte with its original status code
// and headers: requests that did not opt in, non-2xx responses, non-JSON
// responses, SSE streams (which are forwarded live, preserving Flush), bodies
// larger than DefaultMaxBufferBytes, and completions without a UI schema.
func NewGenUIHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !IsGenUIRequest(r) {
			next(w, r)
			return
		}
		gw := &genUIWriter{ResponseWriter: w, limit: DefaultMaxBufferBytes}
		next(gw, WithGenUIRequest(r))
		gw.finish()
	}
}

// WithGenUIRequest returns r with the GenUI flag set on its context.
func WithGenUIRequest(r *http.Request) *http.Request {
	return r.WithContext(WithGenUI(r.Context()))
}

type writerMode int

const (
	modeUndecided writerMode = iota
	modeBuffer
	modePassthrough
)

// genUIWriter buffers a JSON chat completion so it can be rewritten, and
// switches to transparent pass-through for anything else.
type genUIWriter struct {
	http.ResponseWriter
	mode  writerMode
	code  int
	buf   bytes.Buffer
	limit int
}

func (g *genUIWriter) WriteHeader(code int) {
	if g.mode != modeUndecided {
		if g.mode == modePassthrough {
			// Let net/http report superfluous WriteHeader calls as usual.
			g.ResponseWriter.WriteHeader(code)
		}
		return
	}
	if code >= 100 && code < 200 {
		// Informational responses (e.g. 103 Early Hints) are forwarded as-is.
		g.ResponseWriter.WriteHeader(code)
		return
	}
	g.code = code
	if code == http.StatusOK && isJSON(g.Header().Get("Content-Type")) {
		g.mode = modeBuffer
		return
	}
	g.passthrough()
}

// passthrough commits the status code and any buffered bytes to the client.
func (g *genUIWriter) passthrough() {
	if g.mode == modePassthrough {
		return
	}
	g.mode = modePassthrough
	if g.code == 0 {
		g.code = http.StatusOK
	}
	g.ResponseWriter.WriteHeader(g.code)
	if g.buf.Len() > 0 {
		_, _ = g.ResponseWriter.Write(g.buf.Bytes())
		g.buf.Reset()
	}
}

func (g *genUIWriter) Write(b []byte) (int, error) {
	if g.mode == modeUndecided {
		g.WriteHeader(http.StatusOK)
	}
	if g.mode == modePassthrough {
		return g.ResponseWriter.Write(b)
	}
	if g.buf.Len()+len(b) > g.limit {
		g.passthrough()
		return g.ResponseWriter.Write(b)
	}
	return g.buf.Write(b)
}

// Flush forwards flushes. A flush while buffering means the handler is
// streaming, so the writer switches to pass-through first.
func (g *genUIWriter) Flush() {
	if g.mode != modePassthrough {
		if g.mode == modeUndecided && g.code == 0 {
			g.code = http.StatusOK
		}
		g.passthrough()
	}
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (g *genUIWriter) Unwrap() http.ResponseWriter { return g.ResponseWriter }

// finish emits the buffered response, rewritten as SSE when it carries a UI
// schema, or unchanged otherwise.
func (g *genUIWriter) finish() {
	switch g.mode {
	case modePassthrough:
		return
	case modeUndecided:
		// The handler wrote nothing; commit its (implicit) status.
		if g.code == 0 {
			g.code = http.StatusOK
		}
		g.passthrough()
		return
	}
	body := g.buf.Bytes()
	var resp struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content *string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage json.RawMessage `json:"usage,omitempty"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || len(resp.Choices) == 0 || resp.Choices[0].Message.Content == nil {
		g.passthrough()
		return
	}
	chunks := Intercept(*resp.Choices[0].Message.Content)
	if !HasUISchema(chunks) {
		g.passthrough()
		return
	}
	frames := make([][]byte, 0, len(chunks)+1)
	for _, c := range chunks {
		frame, err := EncodeSSE(c)
		if err != nil {
			g.passthrough()
			return
		}
		frames = append(frames, frame)
	}
	done := map[string]interface{}{"id": resp.ID, "model": resp.Model, "finish_reason": resp.Choices[0].FinishReason}
	if len(resp.Usage) > 0 && string(resp.Usage) != "null" {
		done["usage"] = resp.Usage
	}
	frame, err := EncodeSSE(SSEChunk{Event: "done", Data: done})
	if err != nil {
		g.passthrough()
		return
	}
	frames = append(frames, frame)

	h := g.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	h.Del("Content-Length")
	h.Del("Content-Encoding")
	h.Del("ETag")
	g.mode = modePassthrough
	g.ResponseWriter.WriteHeader(http.StatusOK)
	for _, f := range frames {
		if _, err := g.ResponseWriter.Write(f); err != nil {
			return
		}
	}
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func isJSON(contentType string) bool {
	if contentType == "" {
		return false
	}
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return mt == "application/json" || strings.HasSuffix(mt, "+json")
}

// WithGenUI returns a context flag indicating GenUI mode is enabled.
func WithGenUI(ctx context.Context) context.Context {
	return context.WithValue(ctx, genUIKey{}, true)
}

// GenUIFromContext reports whether the request opted into GenUI streaming.
func GenUIFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(genUIKey{}).(bool)
	return v
}

type genUIKey struct{}

// IsGenUIRequest reports whether the request explicitly asks for GenUI mode,
// via the query parameter genui=1|true or the X-AeroLLM-GenUI: 1|true header.
//
// "Accept: text/event-stream" alone is NOT treated as opt-in: OpenAI SDKs send
// it for ordinary stream=true requests, whose SSE must pass through untouched.
func IsGenUIRequest(r *http.Request) bool {
	if r == nil {
		return false
	}
	truthy := func(v string) bool {
		v = strings.ToLower(strings.TrimSpace(v))
		return v == "1" || v == "true" || v == "yes"
	}
	return truthy(r.URL.Query().Get("genui")) || truthy(r.Header.Get(OptInHeader))
}
