package rag

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultMaxBodyBytes is the largest request body the RAG middlewares
	// inspect. Larger bodies are passed through untouched (the downstream
	// handler applies its own limit).
	DefaultMaxBodyBytes int64 = 10 << 20
	// DefaultRetrievalTimeout bounds retrieval so a slow store cannot stall
	// chat requests; on timeout the request proceeds without context.
	DefaultRetrievalTimeout = 2 * time.Second
	// OptInHeader lets clients enable RAG without changing the JSON body.
	OptInHeader = "X-AeroLLM-RAG"
)

// MiddlewareOptions configures RAGHTTPMiddlewareWithOptions.
type MiddlewareOptions struct {
	// MaxBodyBytes caps the inspected body (default DefaultMaxBodyBytes).
	MaxBodyBytes int64
	// TopK is the number of documents injected (default 4).
	TopK int
	// Timeout bounds retrieval (default DefaultRetrievalTimeout).
	Timeout time.Duration
	// SystemPromptBuilder renders the documents (default DefaultSystemPrompt).
	SystemPromptBuilder func(query string, docs []Document) string
}

// RAGHTTPMiddleware injects retrieved context into chat requests that opt in
// with `"rag_enabled": true` in the JSON body (or the X-AeroLLM-RAG header).
// See RAGHTTPMiddlewareWithOptions.
func RAGHTTPMiddleware(retriever Retriever) func(http.HandlerFunc) http.HandlerFunc {
	return RAGHTTPMiddlewareWithOptions(retriever, MiddlewareOptions{})
}

// RAGHTTPMiddlewareWithOptions returns the RAG middleware. It:
//   - only inspects POST requests with a JSON (or unspecified) content type;
//   - reads at most MaxBodyBytes and always restores r.Body/ContentLength, so
//     the downstream handler sees either the original or the rewritten body;
//   - activates only when the request opts in and the retriever may hold
//     documents;
//   - edits the raw JSON, inserting one system message after the leading
//     system messages, so every other request field is forwarded verbatim;
//   - fails open: any parse/retrieval problem forwards the original request;
//   - never touches the ResponseWriter, so streaming (SSE) responses and
//     http.Flusher are unaffected.
func RAGHTTPMiddlewareWithOptions(retriever Retriever, opts MiddlewareOptions) func(http.HandlerFunc) http.HandlerFunc {
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultRetrievalTimeout
	}
	inj := NewRAGMiddleware(retriever)
	if opts.TopK > 0 {
		inj.TopK = opts.TopK
	}
	if opts.SystemPromptBuilder != nil {
		inj.SystemPromptBuilder = opts.SystemPromptBuilder
	}
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if retriever == nil || r.Method != http.MethodPost || !IsJSONRequest(r) || IsEmpty(retriever) {
				next(w, r)
				return
			}
			body, ok := PeekBody(r, opts.MaxBodyBytes)
			if !ok {
				next(w, r)
				return
			}
			payload, err := ParseChatPayload(body)
			if err != nil || !(payload.OptedIn("rag_enabled") || headerOptIn(r)) {
				next(w, r)
				return
			}
			query := strings.TrimSpace(payload.LastUserText())
			if query == "" {
				next(w, r)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), opts.Timeout)
			docs, err := retriever.Retrieve(ctx, clipRunes(query, maxQueryChars), inj.TopK)
			cancel()
			if err != nil || len(docs) == 0 {
				next(w, r)
				return
			}
			if err := payload.InsertSystemMessage(inj.SystemPromptBuilder(query, docs)); err != nil {
				next(w, r)
				return
			}
			out, err := payload.Marshal()
			if err != nil {
				next(w, r)
				return
			}
			SetBody(r, out)
			next(w, r)
		}
	}
}

func headerOptIn(r *http.Request) bool {
	v := strings.ToLower(strings.TrimSpace(r.Header.Get(OptInHeader)))
	return v == "1" || v == "true" || v == "yes"
}

// IsJSONRequest reports whether the request body is declared as JSON (or has
// no declared content type, as some clients omit it).
func IsJSONRequest(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		return true
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	return mt == "application/json" || strings.HasSuffix(mt, "+json")
}

// PeekBody reads the request body if it is at most max bytes. It always leaves
// r.Body readable from the start: when the body is complete it is replaced by
// an in-memory copy (ok=true); when it is larger than max or reading failed,
// the consumed prefix is stitched back in front of the remaining stream and
// ok is false.
func PeekBody(r *http.Request, max int64) ([]byte, bool) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, false
	}
	orig := r.Body
	buf, err := io.ReadAll(io.LimitReader(orig, max+1))
	if err != nil || int64(len(buf)) > max {
		r.Body = &stitchedBody{Reader: io.MultiReader(bytes.NewReader(buf), orig), closer: orig}
		return nil, false
	}
	r.Body = &stitchedBody{Reader: bytes.NewReader(buf), closer: orig}
	return buf, true
}

type stitchedBody struct {
	io.Reader
	closer io.Closer
}

func (s *stitchedBody) Close() error { return s.closer.Close() }

// SetBody replaces the request body with b and keeps ContentLength, the
// Content-Length header and GetBody consistent with it.
func SetBody(r *http.Request, b []byte) {
	r.Body = io.NopCloser(bytes.NewReader(b))
	r.ContentLength = int64(len(b))
	r.Header.Set("Content-Length", strconv.Itoa(len(b)))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
}

// ChatPayload is a lossless view of an OpenAI-style chat request body: unknown
// fields and message shapes (e.g. multimodal content arrays) are preserved.
type ChatPayload struct {
	fields   map[string]json.RawMessage
	messages []json.RawMessage
}

// ErrNotChatPayload is returned when a body is not a JSON object with a
// "messages" array.
var ErrNotChatPayload = errors.New("rag: body is not a chat request")

// ParseChatPayload parses a chat request body.
func ParseChatPayload(body []byte) (*ChatPayload, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return nil, ErrNotChatPayload
	}
	raw, ok := fields["messages"]
	if !ok {
		return nil, ErrNotChatPayload
	}
	var messages []json.RawMessage
	if err := json.Unmarshal(raw, &messages); err != nil {
		return nil, ErrNotChatPayload
	}
	return &ChatPayload{fields: fields, messages: messages}, nil
}

// OptedIn reports whether the boolean field flag is true.
func (p *ChatPayload) OptedIn(flag string) bool {
	raw, ok := p.fields[flag]
	if !ok {
		return false
	}
	var v bool
	return json.Unmarshal(raw, &v) == nil && v
}

type messageHead struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// LastUserText returns the text of the last user message; for multimodal
// content arrays the "text" parts are joined.
func (p *ChatPayload) LastUserText() string {
	for i := len(p.messages) - 1; i >= 0; i-- {
		var m messageHead
		if json.Unmarshal(p.messages[i], &m) != nil || m.Role != "user" {
			continue
		}
		return contentText(m.Content)
	}
	return ""
}

func contentText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return ""
	}
	var texts []string
	for _, part := range parts {
		if part.Type == "text" && part.Text != "" {
			texts = append(texts, part.Text)
		}
	}
	return strings.Join(texts, "\n")
}

// InsertSystemMessage inserts a system message with the given text after the
// leading system/developer messages.
func (p *ChatPayload) InsertSystemMessage(text string) error {
	msg, err := json.Marshal(map[string]string{"role": "system", "content": text})
	if err != nil {
		return err
	}
	at := 0
	for at < len(p.messages) {
		var m messageHead
		if json.Unmarshal(p.messages[at], &m) != nil || (m.Role != "system" && m.Role != "developer") {
			break
		}
		at++
	}
	msgs := make([]json.RawMessage, 0, len(p.messages)+1)
	msgs = append(msgs, p.messages[:at]...)
	msgs = append(msgs, msg)
	msgs = append(msgs, p.messages[at:]...)
	p.messages = msgs
	return nil
}

// Marshal renders the payload back to JSON.
func (p *ChatPayload) Marshal() ([]byte, error) {
	raw, err := json.Marshal(p.messages)
	if err != nil {
		return nil, err
	}
	p.fields["messages"] = raw
	return json.Marshal(p.fields)
}
