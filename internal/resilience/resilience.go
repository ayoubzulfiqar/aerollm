package resilience

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// State represents the current resilience mode.
type State int

const (
	// StateNormal means no degradation.
	StateNormal State = iota
	// StateDegraded means requests are being throttled/rejected.
	StateDegraded
	// StateRecovering means partial traffic is allowed back in.
	StateRecovering
)

// String returns the string representation of the state.
func (s State) String() string {
	switch s {
	case StateDegraded:
		return "degraded"
	case StateRecovering:
		return "recovering"
	default:
		return "normal"
	}
}

// Config configures the degraded mode handler.
type Config struct {
	Mode              State
	RetryAfter        time.Duration
	AllowedFraction   float64
	MaxConcurrency    int
	RequeueThreshold  int
}

// DefaultConfig returns a sane default config.
func DefaultConfig() Config {
	return Config{
		Mode:             StateNormal,
		RetryAfter:       2 * time.Second,
		AllowedFraction:  0.5,
		MaxConcurrency:   8,
		RequeueThreshold: 4,
	}
}

// CircuitBreaker tracks failures and trips after threshold.
type CircuitBreaker struct {
	mu            sync.Mutex
	failures      int
	threshold     int
	resetTimeout  time.Duration
	state         State
	lastFailure   time.Time
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{threshold: threshold, resetTimeout: resetTimeout, state: StateNormal}
}

// State returns the current circuit state.
func (c *CircuitBreaker) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == StateNormal && c.failures >= c.threshold && time.Since(c.lastFailure) < c.resetTimeout {
		c.state = StateDegraded
	}
	if c.state == StateDegraded && time.Since(c.lastFailure) >= c.resetTimeout {
		c.state = StateRecovering
		c.failures = 0
	}
	return c.state
}

// RecordFailure increments the failure count.
func (c *CircuitBreaker) RecordFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures++
	c.lastFailure = time.Now()
	if c.failures >= c.threshold {
		c.state = StateDegraded
	}
}

// RecordSuccess resets failure count.
func (c *CircuitBreaker) RecordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.state = StateNormal
}

// Bulkhead limits concurrency.
type Bulkhead struct {
	sem chan struct{}
}

// NewBulkhead creates a new bulkhead.
func NewBulkhead(maxConcurrency int) *Bulkhead {
	return &Bulkhead{sem: make(chan struct{}, maxConcurrency)}
}

// Acquire blocks until a slot is available or ctx is done.
func (b *Bulkhead) Acquire(ctx context.Context) bool {
	select {
	case b.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// Release frees a slot.
func (b *Bulkhead) Release() { <-b.sem }

// StatusResponse is the JSON response for /resilience/status.
type StatusResponse struct {
	State       string  `json:"state"`
	Failures    int     `json:"failures,omitempty"`
	Threshold   int     `json:"threshold,omitempty"`
	AllowedFrac float64 `json:"allowed_fraction,omitempty"`
}

// Handler returns a HTTP handler for degraded mode decisions.
func Handler(cfg Config, breaker *CircuitBreaker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = r
		state := breaker.State()
		if cfg.Mode == StateDegraded || state == StateDegraded {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(StatusResponse{State: "degraded", Threshold: breaker.threshold, AllowedFrac: cfg.AllowedFraction})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(StatusResponse{State: "ok", Threshold: breaker.threshold, AllowedFrac: cfg.AllowedFraction})
	}
}

// Middleware returns HTTP middleware that enforces bulkhead limits.
func Middleware(b *Bulkhead) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !b.Acquire(r.Context()) {
				http.Error(w, `{"error":"bulkhead full"}`, http.StatusServiceUnavailable)
				return
			}
			defer b.Release()
			next.ServeHTTP(w, r)
		})
	}
}
