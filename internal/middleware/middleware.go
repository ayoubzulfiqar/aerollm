package middleware

import (
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
)

// LoggerInterface defines the logging methods used by middleware.
type LoggerInterface interface {
	Info(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// AuthMiddleware handles API key authentication.
//
// Deprecated: use Authenticator.RequireKey / RequireAdmin. Without a
// Validator, AuthMiddleware only checks that a key is present.
type AuthMiddleware struct {
	Next http.HandlerFunc
	// Validator, when set, must accept the key for the request to proceed.
	Validator func(key string) bool
}

// NewAuthMiddleware creates a new authentication middleware.
func NewAuthMiddleware(next http.HandlerFunc) *AuthMiddleware {
	return &AuthMiddleware{Next: next}
}

// ServeHTTP validates the API key from the Authorization header.
func (m *AuthMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	apiKey := APIKeyFromRequest(r)
	if apiKey == "" {
		WriteJSONError(w, http.StatusUnauthorized, "missing api key", "")
		return
	}
	if m.Validator != nil && !m.Validator(apiKey) {
		WriteJSONError(w, http.StatusUnauthorized, "invalid api key", "")
		return
	}
	m.Next(w, r)
}

// LoggingMiddleware logs incoming requests.
//
// Deprecated: use AccessLog.
type LoggingMiddleware struct {
	Next   http.HandlerFunc
	Logger LoggerInterface
}

// NewLoggingMiddleware creates a new logging middleware.
func NewLoggingMiddleware(next http.HandlerFunc, logger LoggerInterface) *LoggingMiddleware {
	return &LoggingMiddleware{Next: next, Logger: logger}
}

// ServeHTTP logs the request method, path, status and duration.
func (m *LoggingMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	AccessLog(m.Logger)(m.Next).ServeHTTP(w, r)
}

// RecoveryMiddleware recovers from panics.
//
// Deprecated: use Recover.
type RecoveryMiddleware struct {
	Next   http.HandlerFunc
	Logger LoggerInterface
}

// NewRecoveryMiddleware creates a new recovery middleware.
func NewRecoveryMiddleware(next http.HandlerFunc) *RecoveryMiddleware {
	return &RecoveryMiddleware{Next: next}
}

// ServeHTTP wraps the next handler with panic recovery.
func (m *RecoveryMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	Recover(m.Logger)(m.Next).ServeHTTP(w, r)
}

// RateLimitMiddleware enforces per-API-key rate limiting.
//
// Deprecated: use RateLimit.
type RateLimitMiddleware struct {
	Next        http.HandlerFunc
	RateLimiter interface{}
}

// NewRateLimitMiddleware creates a new rate limiting middleware.
func NewRateLimitMiddleware(next http.HandlerFunc, rl interface{}) *RateLimitMiddleware {
	return &RateLimitMiddleware{Next: next, RateLimiter: rl}
}

// ServeHTTP checks the rate limit for the requesting API key. Requests are
// passed through unchanged when no ratelimit.RateLimiter is configured.
func (m *RateLimitMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rl, ok := m.RateLimiter.(ratelimit.RateLimiter)
	if !ok || rl == nil {
		m.Next(w, r)
		return
	}
	RateLimit(RateLimitOptions{Limiter: rl})(m.Next).ServeHTTP(w, r)
}

// APIKeyFromRequest extracts the API key from the Authorization header
// ("Bearer <key>", scheme matched case-insensitively) or, for Anthropic SDK
// compatibility, from the X-API-Key header.
func APIKeyFromRequest(r *http.Request) string {
	if auth := strings.TrimSpace(r.Header.Get("Authorization")); auth != "" {
		if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
			return strings.TrimSpace(auth[7:])
		}
		return auth
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}
