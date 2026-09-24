package resilience

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
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
	Mode             State
	RetryAfter       time.Duration
	AllowedFraction  float64
	MaxConcurrency   int
	RequeueThreshold int
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

// defaultResetTimeout is used when NewCircuitBreaker gets a non-positive timeout.
const defaultResetTimeout = 30 * time.Second

// CircuitBreaker tracks failures and trips after threshold.
//
// States: Normal --(threshold consecutive failures)--> Degraded
// --(resetTimeout elapsed)--> Recovering --(success)--> Normal, and
// Recovering --(any failure)--> Degraded. It is safe for concurrent use.
type CircuitBreaker struct {
	mu           sync.Mutex
	failures     int
	threshold    int
	resetTimeout time.Duration
	state        State
	lastFailure  time.Time
}

// NewCircuitBreaker creates a new circuit breaker. A non-positive threshold
// is treated as 1 and a non-positive resetTimeout as 30s.
func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	if threshold <= 0 {
		threshold = 1
	}
	if resetTimeout <= 0 {
		resetTimeout = defaultResetTimeout
	}
	return &CircuitBreaker{threshold: threshold, resetTimeout: resetTimeout, state: StateNormal}
}

// advanceLocked moves Degraded to Recovering once the reset timeout has
// elapsed since the last failure. c.mu must be held.
func (c *CircuitBreaker) advanceLocked(now time.Time) {
	if c.state == StateDegraded && now.Sub(c.lastFailure) >= c.resetTimeout {
		c.state = StateRecovering
		c.failures = 0
	}
}

// State returns the current circuit state.
func (c *CircuitBreaker) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advanceLocked(time.Now())
	return c.state
}

// Allow reports whether traffic should be let through (not Degraded).
func (c *CircuitBreaker) Allow() bool {
	return c.State() != StateDegraded
}

// RecordFailure increments the failure count. A failure while Recovering
// re-trips the breaker immediately.
func (c *CircuitBreaker) RecordFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.advanceLocked(now)
	c.failures++
	c.lastFailure = now
	if c.state == StateRecovering || c.failures >= c.threshold {
		c.state = StateDegraded
	}
}

// RecordSuccess resets failure count and closes the breaker.
func (c *CircuitBreaker) RecordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.state = StateNormal
}

// Failures returns the current consecutive failure count.
func (c *CircuitBreaker) Failures() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failures
}

// Threshold returns the configured failure threshold.
func (c *CircuitBreaker) Threshold() int {
	return c.threshold // immutable after construction
}

// Bulkhead limits concurrency. It is safe for concurrent use.
type Bulkhead struct {
	sem chan struct{}
}

// NewBulkhead creates a new bulkhead. A non-positive maxConcurrency is
// treated as 1 (an unbuffered semaphore would deadlock every caller).
func NewBulkhead(maxConcurrency int) *Bulkhead {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	return &Bulkhead{sem: make(chan struct{}, maxConcurrency)}
}

// Acquire blocks until a slot is available or ctx is done. It never acquires
// a slot for an already-cancelled context.
func (b *Bulkhead) Acquire(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return false
	}
	select {
	case b.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// TryAcquire takes a slot only if one is immediately available.
func (b *Bulkhead) TryAcquire() bool {
	select {
	case b.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release frees a slot. An unbalanced Release (without a matching Acquire)
// is a no-op instead of blocking forever.
func (b *Bulkhead) Release() {
	select {
	case <-b.sem:
	default:
	}
}

// InUse returns the number of slots currently held.
func (b *Bulkhead) InUse() int { return len(b.sem) }

// Capacity returns the maximum number of concurrent holders.
func (b *Bulkhead) Capacity() int { return cap(b.sem) }

// StatusResponse is the JSON response for /resilience/status.
type StatusResponse struct {
	State       string  `json:"state"`
	Failures    int     `json:"failures,omitempty"`
	Threshold   int     `json:"threshold,omitempty"`
	AllowedFrac float64 `json:"allowed_fraction,omitempty"`
}

// retryAfterSeconds renders d as a Retry-After value (whole seconds, >= 1).
func retryAfterSeconds(d time.Duration) string {
	secs := int64(math.Ceil(d.Seconds()))
	if secs < 1 {
		secs = 1
	}
	return strconv.FormatInt(secs, 10)
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if r != nil && r.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

// Handler returns a HTTP handler for degraded mode decisions: 503 (with
// Retry-After) while degraded, 200 with state "recovering" while recovering
// and 200 with state "ok" otherwise. Only GET and HEAD are allowed.
func Handler(cfg Config, breaker *CircuitBreaker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeJSON(w, r, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		state := cfg.Mode
		resp := StatusResponse{AllowedFrac: cfg.AllowedFraction}
		if breaker != nil {
			// Degraded dominates; Recovering only overrides Normal.
			if bs := breaker.State(); bs == StateDegraded || (bs == StateRecovering && state == StateNormal) {
				state = bs
			}
			resp.Failures = breaker.Failures()
			resp.Threshold = breaker.Threshold()
		}
		switch state {
		case StateDegraded:
			resp.State = "degraded"
			w.Header().Set("Retry-After", retryAfterSeconds(cfg.RetryAfter))
			writeJSON(w, r, http.StatusServiceUnavailable, resp)
		case StateRecovering:
			resp.State = "recovering"
			writeJSON(w, r, http.StatusOK, resp)
		default:
			resp.State = "ok"
			writeJSON(w, r, http.StatusOK, resp)
		}
	}
}

// DefaultBulkheadWait is how long Middleware waits for a free slot before
// rejecting a request with 503.
const DefaultBulkheadWait = 50 * time.Millisecond

// Middleware returns HTTP middleware that enforces bulkhead limits. When the
// bulkhead is full it waits at most DefaultBulkheadWait, then responds 503
// with a JSON error and Retry-After.
func Middleware(b *Bulkhead) func(http.Handler) http.Handler {
	return MiddlewareWithWait(b, DefaultBulkheadWait)
}

// MiddlewareWithWait is Middleware with a configurable maximum wait for a
// slot (<= 0 rejects immediately when full). A nil bulkhead disables limiting.
func MiddlewareWithWait(b *Bulkhead, maxWait time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if b == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			acquired := b.TryAcquire()
			if !acquired && maxWait > 0 {
				ctx, cancel := context.WithTimeout(r.Context(), maxWait)
				acquired = b.Acquire(ctx)
				cancel()
			}
			if !acquired {
				w.Header().Set("Retry-After", "1")
				writeJSON(w, r, http.StatusServiceUnavailable, map[string]string{"error": "server busy: concurrency limit reached"})
				return
			}
			defer b.Release()
			next.ServeHTTP(w, r)
		})
	}
}
