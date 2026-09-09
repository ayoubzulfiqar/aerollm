package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
)

// RateLimitHeaders middleware injects standard OpenAI-compatible rate limit headers
// into every HTTP response (including 429s). These headers enable official SDKs
// (OpenAI Python/TS, etc.) to automatically handle backpressure.
//
// Headers injected:
//
//	X-RateLimit-Limit-Requests   – max requests allowed in the current window.
//	X-RateLimit-Limit-Tokens     – max tokens allowed in the current window.
//	X-RateLimit-Remaining-Requests – remaining requests for this window.
//	X-RateLimit-Remaining-Tokens   – remaining tokens for this window.
//	X-RateLimit-Reset-Requests    – seconds until the request window resets.
//	X-RateLimit-Reset-Tokens      – seconds until the token window resets.
//
// Rate limit values come from either:
//   - The validated VirtualKey's per-key RPS/token limits (if available in context).
//   - The global RateLimitConfig defaults (fallback).
//
type RateLimitHeadersMiddleware struct {
	Next       http.HandlerFunc
	RateLimiter ratelimit.RateLimiter
	DefaultRPS float64
	// DefaultTPM is the default token-per-minute limit applied when no per-key
	// or per-tenant limit is configured.
	DefaultTPM int
}

// NewRateLimitHeadersMiddleware creates a middleware that injects OpenAI-compatible
// rate limit headers into every response.
func NewRateLimitHeadersMiddleware(next http.HandlerFunc, rl ratelimit.RateLimiter, defaultRPS float64, defaultTPM int) *RateLimitHeadersMiddleware {
	if defaultRPS <= 0 {
		defaultRPS = 10.0
	}
	if defaultTPM <= 0 {
		defaultTPM = 60000 // 60k tokens/min is a reasonable default
	}
	return &RateLimitHeadersMiddleware{
		Next:       next,
		RateLimiter: rl,
		DefaultRPS:  defaultRPS,
		DefaultTPM:  defaultTPM,
	}
}

// ServeHTTP intercepts the response and appends rate limit headers before writing.
func (m *RateLimitHeadersMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Wrap the ResponseWriter to capture status code (for 429 detection).
	rw := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}

	// Determine the per-key rate limit limits.
	limits := m.getLimits(r)

	// Always set headers on the response — before the handler writes, and again
	// after (in case the handler changes the status to 429).
	m.setHeaders(w, limits)

	m.Next(rw, r)

	// On 429, recompute remaining (which drops to 0) and reset time.
	if rw.statusCode == http.StatusTooManyRequests {
		limits.RemainingRequests = 0
		limits.RemainingTokens = 0
		limits.ResetRequests = time.Until(time.Now().Add(time.Minute)).Round(time.Second).Seconds()
		limits.ResetTokens = limits.ResetRequests
		m.setHeaders(w, limits)
	}

	// Ensure headers are flushed on the actual ResponseWriter (the wrapper
	// forwards SetHeader calls, so they propagate).
}

// getLimits retrieves the rate limit limits for the requesting API key.
// Priority:
//  1. VirtualKey from context (per-key limits).
//  2. RateLimiter.GetLimits() if a real limiter is wired.
//  3. Global defaults.
func (m *RateLimitHeadersMiddleware) getLimits(r *http.Request) RateLimitInfo {
	apiKey := VirtualKeyOrAuth(r)

	// Check for virtual key in context (set by VirtualKeyAuthMiddleware).
	if vk, ok := VirtualKeyFromContext(r.Context()); ok {
		rps := m.DefaultRPS
		tpm := m.DefaultTPM

		// If the virtual key carries per-key limits in metadata, use them.
		if vk.Metadata != nil {
			if val, ok := vk.Metadata["rate_limit_rps"]; ok {
				if f, ok := val.(float64); ok && f > 0 {
					rps = f
				}
			}
			if val, ok := vk.Metadata["rate_limit_tpm"]; ok {
				if t, ok := val.(int); ok && t > 0 {
					tpm = t
				} else if f, ok := val.(float64); ok && f > 0 {
					tpm = int(f)
				}
			}
		}

		return RateLimitInfo{
			LimitRequests:  int(rps),
			LimitTokens:    tpm,
			RemainingRequests: int(rps), // Full window at request start.
			RemainingTokens:   tpm,
			ResetRequests:     time.Until(time.Now().Add(time.Minute)).Round(time.Second).Seconds(),
			ResetTokens:       time.Until(time.Now().Add(time.Minute)).Round(time.Second).Seconds(),
			APIKey:            apiKey,
		}
	}

	// Fall back to the rate limiter if one is wired.
	if m.RateLimiter != nil {
		record, err := m.RateLimiter.GetLimits(r.Context(), apiKey, "")
		if err == nil && record != nil {
			now := time.Now()
			return RateLimitInfo{
				LimitRequests:  int(m.DefaultRPS),
				LimitTokens:    m.DefaultTPM,
				RemainingRequests: record.Remaining,
				RemainingTokens:   m.DefaultTPM, // Token-level tracking requires per-request estimation.
				ResetRequests:     time.Until(time.Unix(record.ResetAt, 0)).Round(time.Second).Seconds(),
				ResetTokens:       time.Until(now.Add(time.Minute)).Round(time.Second).Seconds(),
				APIKey:            apiKey,
			}
		}
	}

	// Global defaults.
	return RateLimitInfo{
		LimitRequests:  int(m.DefaultRPS),
		LimitTokens:    m.DefaultTPM,
		RemainingRequests: int(m.DefaultRPS),
		RemainingTokens:   m.DefaultTPM,
		ResetRequests:     60,
		ResetTokens:       60,
		APIKey:            apiKey,
	}
}

// setHeaders writes the X-RateLimit-* headers onto the ResponseWriter.
func (m *RateLimitHeadersMiddleware) setHeaders(w http.ResponseWriter, info RateLimitInfo) {
	w.Header().Set("X-RateLimit-Limit-Requests", strconv.Itoa(info.LimitRequests))
	w.Header().Set("X-RateLimit-Limit-Tokens", strconv.Itoa(info.LimitTokens))
	w.Header().Set("X-RateLimit-Remaining-Requests", strconv.Itoa(info.RemainingRequests))
	w.Header().Set("X-RateLimit-Remaining-Tokens", strconv.Itoa(info.RemainingTokens))
	w.Header().Set("X-RateLimit-Reset-Requests", strconv.Itoa(int(info.ResetRequests)))
	w.Header().Set("X-RateLimit-Reset-Tokens", strconv.Itoa(int(info.ResetTokens)))
}

// RateLimitInfo holds the computed rate limit headers for the current request.
type RateLimitInfo struct {
	APIKey            string
	LimitRequests     int
	LimitTokens       int
	RemainingRequests int
	RemainingTokens   int
	ResetRequests     float64 // seconds until reset
	ResetTokens       float64 // seconds until reset
}

// VirtualKeyOrAuth extracts the API key from the request, preferring the
// Bearer token. This is used as a fallback when no VirtualKey is in context.
func VirtualKeyOrAuth(r *http.Request) string {
	return APIKeyFromRequest(r)
}

// responseWriterWrapper captures the status code so the middleware can react
// to 429 responses.
type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader intercepts the status code so the middleware can detect 429s.
func (rw *responseWriterWrapper) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Write captures the status code (defaults to 200 if WriteHeader wasn't called).
func (rw *responseWriterWrapper) Write(b []byte) (int, error) {
	if rw.statusCode == 0 {
		rw.statusCode = http.StatusOK
	}
	return rw.ResponseWriter.Write(b)
}

// Ensure the wrapper does not interfere with the real ResponseWriter's
// header map — Set/Add/Del on the wrapper are forwarded to the underlying writer.
var _ http.ResponseWriter = (*responseWriterWrapper)(nil)
