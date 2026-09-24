package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
)

// Middleware wraps an http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain applies middlewares so that the first one listed is the outermost
// (runs first on the way in, last on the way out).
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] != nil {
			h = mws[i](h)
		}
	}
	return h
}

// WriteJSONError writes an OpenAI-style error body:
// {"error":{"message":..,"type":..,"code":..}}.
func WriteJSONError(w http.ResponseWriter, status int, message, errType string) {
	if errType == "" {
		errType = errorTypeForStatus(status)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"error":{"message":%s,"type":%s,"code":%d}}`+"\n",
		strconv.Quote(message), strconv.Quote(errType), status)
}

func errorTypeForStatus(status int) string {
	switch {
	case status == http.StatusUnauthorized:
		return "authentication_error"
	case status == http.StatusForbidden:
		return "permission_error"
	case status == http.StatusNotFound:
		return "not_found_error"
	case status == http.StatusTooManyRequests:
		return "rate_limit_error"
	case status == http.StatusRequestEntityTooLarge:
		return "request_too_large"
	case status >= 500:
		return "api_error"
	default:
		return "invalid_request_error"
	}
}

// statusRecorder captures the status code and size while passing through
// http.Flusher so streaming responses keep working behind middleware.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wroteHeader {
		s.status = http.StatusOK
		s.wroteHeader = true
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += int64(n)
	return n, err
}

// Flush forwards to the underlying writer when it supports flushing.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

type requestIDKey struct{}

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// NewRequestID returns a random 128-bit hex identifier.
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

// RequestIDFromContext returns the request ID assigned by RequestID.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// WithRequestID stores a request ID in ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID propagates a well-formed inbound X-Request-ID or generates one,
// stores it in the context and echoes it on the response.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get("X-Request-ID")
			if !validRequestID.MatchString(id) {
				id = NewRequestID()
			}
			w.Header().Set("X-Request-ID", id)
			next.ServeHTTP(w, r.WithContext(WithRequestID(r.Context(), id)))
		})
	}
}

// Recover converts panics into a 500 response and logs the stack trace.
// http.ErrAbortHandler is re-panicked so net/http can abort the connection.
func Recover(logger LoggerInterface) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w}
			defer func() {
				if p := recover(); p != nil {
					if p == http.ErrAbortHandler {
						panic(p)
					}
					telemetry.RecordError()
					if logger != nil {
						logger.Error("panic recovered", "path", r.URL.Path, "request_id", RequestIDFromContext(r.Context()), "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
					}
					if !rec.wroteHeader {
						WriteJSONError(rec, http.StatusInternalServerError, "internal server error", "")
					}
				}
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

// AccessLog logs one line per request with method, path, status, size and
// latency. Query strings and headers are never logged (they can hold keys).
func AccessLog(logger LoggerInterface) Middleware {
	return func(next http.Handler) http.Handler {
		if logger == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)
			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}
			kv := []interface{}{
				"method", r.Method,
				"path", r.URL.Path,
				"status", status,
				"bytes", rec.bytes,
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", RequestIDFromContext(r.Context()),
			}
			if p, ok := PrincipalFromContext(r.Context()); ok {
				kv = append(kv, "key_id", p.KeyID)
			}
			if status >= 500 {
				logger.Error("request", kv...)
			} else {
				logger.Info("request", kv...)
			}
		})
	}
}

// BodyLimit caps request bodies at n bytes. Handlers that read past the
// limit get an error from the body reader (surfaced as 413 by DecodeJSON).
func BodyLimit(n int64) Middleware {
	return func(next http.Handler) http.Handler {
		if n <= 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > n {
				WriteJSONError(w, http.StatusRequestEntityTooLarge, "request body too large", "")
				return
			}
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, n)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// SecurityHeaders sets conservative defaults for an API server.
func SecurityHeaders() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := w.Header()
			h.Set("X-Content-Type-Options", "nosniff")
			h.Set("X-Frame-Options", "DENY")
			h.Set("Referrer-Policy", "no-referrer")
			if !strings.HasPrefix(r.URL.Path, "/swagger/") {
				h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
			}
			next.ServeHTTP(w, r)
		})
	}
}

// CORS allows browser access from the listed origins ("*" for any). It never
// allows credentials, so API keys must be sent explicitly in headers.
func CORS(allowed []string) Middleware {
	anyOrigin := false
	set := make(map[string]bool, len(allowed))
	for _, o := range allowed {
		o = strings.TrimRight(strings.TrimSpace(o), "/")
		if o == "*" {
			anyOrigin = true
		} else if o != "" {
			set[strings.ToLower(o)] = true
		}
	}
	return func(next http.Handler) http.Handler {
		if !anyOrigin && len(set) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" && (anyOrigin || set[strings.ToLower(origin)]) {
				h := w.Header()
				h.Add("Vary", "Origin")
				if anyOrigin {
					h.Set("Access-Control-Allow-Origin", "*")
				} else {
					h.Set("Access-Control-Allow-Origin", origin)
				}
				h.Set("Access-Control-Expose-Headers", "X-Request-ID, X-RateLimit-Limit-Requests, X-RateLimit-Remaining-Requests, X-RateLimit-Reset-Requests, Retry-After, X-AeroLLM-Cache, X-AeroLLM-Provider")
				if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
					h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
					h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key, X-Request-ID, Anthropic-Version, OpenAI-Organization")
					h.Set("Access-Control-Max-Age", "600")
					w.WriteHeader(http.StatusNoContent)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Inflight tracks concurrently served requests in telemetry.
func Inflight() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			telemetry.IncInflight()
			defer telemetry.DecInflight()
			next.ServeHTTP(w, r)
		})
	}
}

// Methods rejects requests whose method is not listed with 405 and an Allow
// header. OPTIONS is always passed through for CORS preflight.
func Methods(methods ...string) Middleware {
	allow := strings.Join(methods, ", ")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for _, m := range methods {
				if r.Method == m {
					next.ServeHTTP(w, r)
					return
				}
			}
			w.Header().Set("Allow", allow)
			WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
		})
	}
}
