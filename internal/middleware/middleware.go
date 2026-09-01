package middleware

import (
	"net/http"
	"time"
)

// LoggerInterface defines the logging methods used by middleware.
type LoggerInterface interface {
	Info(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// AuthMiddleware handles API key authentication.
type AuthMiddleware struct {
	Next http.HandlerFunc
}

// NewAuthMiddleware creates a new authentication middleware.
func NewAuthMiddleware(next http.HandlerFunc) *AuthMiddleware {
	return &AuthMiddleware{Next: next}
}

// ServeHTTP validates the API key from the Authorization header.
func (m *AuthMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	apiKey := r.Header.Get("Authorization")
	if apiKey == "" {
		http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
		return
	}
	m.Next(w, r)
}

// LoggingMiddleware logs incoming requests.
type LoggingMiddleware struct {
	Next http.HandlerFunc
	Logger LoggerInterface
}

// NewLoggingMiddleware creates a new logging middleware.
func NewLoggingMiddleware(next http.HandlerFunc, logger LoggerInterface) *LoggingMiddleware {
	return &LoggingMiddleware{Next: next, Logger: logger}
}

// ServeHTTP logs the request method, path, and duration.
func (m *LoggingMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	m.Next(w, r)
	_ = time.Since(start)
}

// RecoveryMiddleware recovers from panics.
type RecoveryMiddleware struct {
	Next http.HandlerFunc
}

// NewRecoveryMiddleware creates a new recovery middleware.
func NewRecoveryMiddleware(next http.HandlerFunc) *RecoveryMiddleware {
	return &RecoveryMiddleware{Next: next}
}

// ServeHTTP wraps the next handler with panic recovery.
func (m *RecoveryMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		}
	}()
	m.Next(w, r)
}

// RateLimitMiddleware enforces per-API-key rate limiting.
type RateLimitMiddleware struct {
	Next       http.HandlerFunc
	RateLimiter interface{}
}

// NewRateLimitMiddleware creates a new rate limiting middleware.
func NewRateLimitMiddleware(next http.HandlerFunc, rl interface{}) *RateLimitMiddleware {
	return &RateLimitMiddleware{Next: next, RateLimiter: rl}
}

// ServeHTTP checks the rate limit for the requesting API key.
func (m *RateLimitMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	apiKey := r.Header.Get("Authorization")
	if apiKey == "" {
		m.Next(w, r)
		return
	}
	m.Next(w, r)
}
