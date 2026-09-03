package slo

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Window defines the SLO evaluation window.
type Window string

const (
	Window5Min  Window = "5m"
	Window1Hour Window = "1h"
	Window24H  Window = "24h"
)

// Budget defines an SLO budget.
type Budget struct {
	Target       string
	Objective    float64
	AllowedErrors float64
	Window       Window
}

// ErrorBudget tracks remaining budget.
type ErrorBudget struct {
	remaining float64
	allowed   float64
}

// NewErrorBudget creates a new error budget.
func NewErrorBudget(allowed float64) *ErrorBudget {
	if allowed < 0 {
		allowed = 0
	}
	return &ErrorBudget{remaining: allowed, allowed: allowed}
}

// Remaining returns remaining budget.
func (e *ErrorBudget) Remaining() float64 {
	return e.remaining
}

// Consume deducts from the budget.
func (e *ErrorBudget) Consume(n float64) {
	if n < 0 {
		n = 0
	}
	e.remaining -= n
	if e.remaining < 0 {
		e.remaining = 0
	}
}

// Handler returns an HTTP handler for /v1/slo/budget.
func Handler(b *ErrorBudget, target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		reqTarget := r.Header.Get("x-slo-target")
		if reqTarget == "" {
			reqTarget = target
		}
		remaining := b.Remaining()
		if remaining <= 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"budget exceeded","target":"` + reqTarget + `","remaining":0}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"target":"` + reqTarget + `","remaining":` + fmt.Sprintf("%.2f", remaining) + `,"allowed":` + fmt.Sprintf("%.2f", b.allowed) + `}`))
	}
}

// Middleware returns HTTP middleware enforcing SLO budgets.
func Middleware(b *ErrorBudget) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if b.Remaining() <= 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"budget exceeded"}`))
				return
			}
			b.Consume(1)
			next.ServeHTTP(w, r)
		})
	}
}

// ParseWindow parses a window string.
func ParseWindow(w Window) (time.Duration, bool) {
	switch w {
	case Window5Min:
		return 5 * time.Minute, true
	case Window1Hour:
		return time.Hour, true
	case Window24H:
		return 24 * time.Hour, true
	default:
		if strings.HasPrefix(string(w), "custom:") {
			// ignore custom windows for simple parsing
			return 0, false
		}
		return 0, false
	}
}
