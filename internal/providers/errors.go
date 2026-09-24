package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// MaxResponseBytes caps how much of a successful upstream response body is
// read into memory. Anything larger is treated as an upstream failure.
const MaxResponseBytes int64 = 32 << 20 // 32 MiB

// maxErrorBodyBytes caps how much of an upstream error body is read.
const maxErrorBodyBytes int64 = 64 << 10

// maxErrorMessageLen caps the upstream message carried by UpstreamError.
const maxErrorMessageLen = 1024

// ErrCircuitOpen is matched (via errors.Is) by errors returned when a circuit
// breaker rejects a call without contacting the upstream.
var ErrCircuitOpen = errors.New("circuit breaker open")

// ErrStreamingNotSupported is returned by StreamChatCompletions when the
// underlying provider cannot stream. Callers should fall back to
// ChatCompletions (and models.StreamChunksFromResponse).
var ErrStreamingNotSupported = errors.New("streaming not supported by provider")

// ErrResponseTooLarge is returned when an upstream body exceeds the read cap.
var ErrResponseTooLarge = errors.New("upstream response too large")

// ModelSupporter is optionally implemented by providers that can declare which
// models they serve. Routers use it to filter candidates for a request.
type ModelSupporter interface {
	SupportsModel(model string) bool
}

// UpstreamError describes a non-2xx (or otherwise unusable) response from an
// upstream LLM provider. It never contains credentials.
type UpstreamError struct {
	// Provider is the name of the provider that failed.
	Provider string
	// StatusCode is the upstream HTTP status (502 for malformed responses).
	StatusCode int
	// Message is the upstream error message (truncated, credential-free).
	Message string
	// Type is the upstream error type/code when it could be parsed.
	Type string
	// RetryAfter is the upstream-requested back-off, if any.
	RetryAfter time.Duration
}

// Error implements error.
func (e *UpstreamError) Error() string {
	var b strings.Builder
	b.WriteString(e.Provider)
	if b.Len() == 0 {
		b.WriteString("upstream")
	}
	fmt.Fprintf(&b, ": upstream returned status %d", e.StatusCode)
	if e.Type != "" {
		fmt.Fprintf(&b, " (%s)", e.Type)
	}
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	return b.String()
}

// Retryable reports whether the same request may succeed on retry or on a
// different provider (408, 429 and 5xx).
func (e *UpstreamError) Retryable() bool {
	return e.StatusCode == http.StatusRequestTimeout ||
		e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode >= 500
}

// NewUpstreamError builds an UpstreamError from a non-2xx response. It reads
// (a bounded amount of) the body but does not close it.
func NewUpstreamError(provider string, resp *http.Response) *UpstreamError {
	e := &UpstreamError{Provider: provider, StatusCode: resp.StatusCode}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	e.Message, e.Type = parseErrorBody(body)
	if e.Message == "" {
		e.Message = http.StatusText(resp.StatusCode)
	}
	e.RetryAfter = parseRetryAfter(resp.Header, time.Now())
	return e
}

// BadResponseError reports an upstream response that could not be used
// (malformed JSON, oversized body, ...). It maps to 502 and is retryable.
func BadResponseError(provider string, cause error) *UpstreamError {
	msg := "invalid response from upstream"
	if errors.Is(cause, ErrResponseTooLarge) {
		msg = "upstream response too large"
	} else if cause != nil {
		msg = "invalid response from upstream: " + truncate(cause.Error(), 200)
	}
	return &UpstreamError{Provider: provider, StatusCode: http.StatusBadGateway, Message: msg}
}

// ReadResponseBody reads at most limit bytes from r, returning
// ErrResponseTooLarge when the body is larger.
func ReadResponseBody(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		limit = MaxResponseBytes
	}
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, ErrResponseTooLarge
	}
	return b, nil
}

// TransportError wraps a failure to reach the upstream so that it carries the
// provider name without leaking request details (URLs may embed keys).
type TransportError struct {
	Provider string
	Err      error
}

// Error implements error.
func (e *TransportError) Error() string {
	inner := e.Err
	// Strip *url.Error's `Post "https://..."` prefix: URLs can carry keys.
	var ue *url.Error
	if errors.As(inner, &ue) && ue.Err != nil {
		inner = ue.Err
	}
	return fmt.Sprintf("%s: request failed: %v", e.Provider, inner)
}

// Unwrap returns the underlying error.
func (e *TransportError) Unwrap() error { return e.Err }

// IsRetryable reports whether err is worth retrying on the same or another
// provider: 408/429/5xx upstream responses, timeouts, network errors and
// open circuit breakers are retryable; caller cancellation and other 4xx
// responses are not.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var ue *UpstreamError
	if errors.As(err, &ue) {
		return ue.Retryable()
	}
	if errors.Is(err, ErrCircuitOpen) {
		return true
	}
	var r interface{ Retryable() bool }
	if errors.As(err, &r) {
		return r.Retryable()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	var te *TransportError
	return errors.As(err, &te)
}

// StatusCode returns the upstream HTTP status carried by err, or 0.
func StatusCode(err error) int {
	var ue *UpstreamError
	if errors.As(err, &ue) {
		return ue.StatusCode
	}
	return 0
}

// parseErrorBody extracts a message and type from common error envelopes:
// OpenAI {"error":{"message","type","code"}}, Anthropic
// {"type":"error","error":{"type","message"}}, Gemini/Google
// [{"error":{...}}] / {"error":{"message","status"}}, Cohere {"message"}.
func parseErrorBody(body []byte) (msg, typ string) {
	body = []byte(strings.TrimSpace(string(body)))
	if len(body) == 0 {
		return "", ""
	}
	if body[0] == '[' {
		var arr []json.RawMessage
		if json.Unmarshal(body, &arr) == nil && len(arr) > 0 {
			body = arr[0]
		}
	}
	var env struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
		Type    string          `json:"type"`
	}
	if json.Unmarshal(body, &env) == nil {
		if len(env.Error) > 0 {
			var inner struct {
				Message string          `json:"message"`
				Type    string          `json:"type"`
				Status  string          `json:"status"`
				Code    json.RawMessage `json:"code"`
			}
			if json.Unmarshal(env.Error, &inner) == nil {
				typ = inner.Type
				if typ == "" {
					typ = inner.Status
				}
				if typ == "" && len(inner.Code) > 0 {
					var s string
					if json.Unmarshal(inner.Code, &s) == nil {
						typ = s
					}
				}
				return truncate(inner.Message, maxErrorMessageLen), truncate(typ, 100)
			}
			var s string
			if json.Unmarshal(env.Error, &s) == nil {
				return truncate(s, maxErrorMessageLen), ""
			}
		}
		if env.Message != "" {
			t := env.Type
			if t == "error" {
				t = ""
			}
			return truncate(env.Message, maxErrorMessageLen), truncate(t, 100)
		}
	}
	// Not JSON: only surface plain text, never HTML error pages.
	s := string(body)
	if strings.HasPrefix(s, "<") {
		return "", ""
	}
	return truncate(s, maxErrorMessageLen), ""
}

// parseRetryAfter reads Retry-After (seconds or HTTP date) and the
// OpenAI-style retry-after-ms header.
func parseRetryAfter(h http.Header, now time.Time) time.Duration {
	const maxRetryAfter = 10 * time.Minute
	clamp := func(d time.Duration) time.Duration {
		if d < 0 {
			return 0
		}
		if d > maxRetryAfter {
			return maxRetryAfter
		}
		return d
	}
	if v := strings.TrimSpace(h.Get("retry-after-ms")); v != "" {
		if ms, err := strconv.ParseFloat(v, 64); err == nil && ms >= 0 && ms < 1e9 {
			return clamp(time.Duration(ms * float64(time.Millisecond)))
		}
	}
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		if secs < 0 || secs > 1e6 {
			return clamp(maxRetryAfter)
		}
		return clamp(time.Duration(secs * float64(time.Second)))
	}
	if t, err := http.ParseTime(v); err == nil {
		return clamp(t.Sub(now))
	}
	return 0
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	// Avoid cutting a multi-byte rune in half.
	cut := n
	for cut > 0 && (s[cut]&0xC0) == 0x80 {
		cut--
	}
	return s[:cut] + "..."
}
