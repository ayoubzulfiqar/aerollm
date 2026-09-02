package genui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// NewGenUIHandler wraps an HTTP handler and intercepts responses containing
// an embedded UI schema. When detected, it emits SSE-style chunks; otherwise
// it falls back to the original response unchanged.
func NewGenUIHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Query().Get("Accept"), "text/event-stream") &&
			!strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			next(w, r)
			return
		}

		rec := &captureResponseWriter{ResponseWriter: w}
		next(rec, r)

		if rec.code == 0 {
			rec.code = http.StatusOK
		}

		body := rec.body.String()
		chunks := Intercept(body)
		if len(chunks) == 0 {
			w.WriteHeader(rec.code)
			_, _ = w.Write(rec.body.Bytes())
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		for _, chunk := range chunks {
			_ = json.NewEncoder(w).Encode(chunk)
		}
	}
}

type captureResponseWriter struct {
	http.ResponseWriter
	code int
	body bytes.Buffer
}

func (c *captureResponseWriter) WriteHeader(code int) {
	if c.code != 0 {
		return
	}
	c.code = code
}

func (c *captureResponseWriter) Write(b []byte) (int, error) {
	if c.code == 0 {
		c.code = http.StatusOK
	}
	return c.body.Write(b)
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

// IsGenUIRequest reports whether the request asks for GenUI mode.
func IsGenUIRequest(r *http.Request) bool {
	q := r.URL.Query().Get("genui")
	if q == "1" || q == "true" {
		return true
	}
	return strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}
