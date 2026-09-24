// Package slo provides SLO error budgets and burn-rate tracking.
//
// ErrorBudget is a simple thread-safe counter of allowed errors, optionally
// replenished every window. Tracker implements the standard SLO math over a
// sliding window: error rate, remaining budget fraction and burn rate
// (error rate divided by the allowed error rate 1-objective), including
// multi-window burn-rate alerting.
package slo

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Window defines the SLO evaluation window.
type Window string

const (
	Window5Min  Window = "5m"
	Window1Hour Window = "1h"
	Window24H   Window = "24h"
)

// MaxWindow bounds parsed and tracked windows.
const MaxWindow = 366 * 24 * time.Hour

// Budget defines an SLO budget.
type Budget struct {
	Target        string
	Objective     float64
	AllowedErrors float64
	Window        Window
}

// ErrorBudget tracks remaining budget. It is safe for concurrent use.
type ErrorBudget struct {
	mu          sync.Mutex
	remaining   float64
	allowed     float64
	window      time.Duration
	windowStart time.Time
	now         func() time.Time
}

func sanitizeAmount(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0
	}
	return v
}

// NewErrorBudget creates a new error budget. Negative, NaN or infinite
// values are treated as 0 (no budget).
func NewErrorBudget(allowed float64) *ErrorBudget {
	allowed = sanitizeAmount(allowed)
	return &ErrorBudget{remaining: allowed, allowed: allowed, now: time.Now}
}

// NewWindowedErrorBudget creates a budget that is replenished to allowed at
// the start of every window (fixed windows aligned to creation time).
func NewWindowedErrorBudget(allowed float64, window time.Duration) *ErrorBudget {
	b := NewErrorBudget(allowed)
	if window > 0 {
		b.window = window
		b.windowStart = b.now()
	}
	return b
}

// rollLocked replenishes the budget when the current window has elapsed.
func (e *ErrorBudget) rollLocked() {
	if e.window <= 0 {
		return
	}
	now := e.now()
	if elapsed := now.Sub(e.windowStart); elapsed >= e.window {
		e.windowStart = e.windowStart.Add(elapsed - elapsed%e.window)
		e.remaining = e.allowed
	}
}

// Remaining returns remaining budget.
func (e *ErrorBudget) Remaining() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rollLocked()
	return e.remaining
}

// Allowed returns the budget size per window.
func (e *ErrorBudget) Allowed() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.allowed
}

// Consume deducts from the budget. Negative and NaN amounts are ignored.
func (e *ErrorBudget) Consume(n float64) {
	if math.IsNaN(n) || n <= 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rollLocked()
	e.remaining -= n
	if e.remaining < 0 || math.IsNaN(e.remaining) {
		e.remaining = 0
	}
}

// TryConsume deducts n only if enough budget remains and reports success.
func (e *ErrorBudget) TryConsume(n float64) bool {
	if math.IsNaN(n) || n < 0 {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rollLocked()
	if e.remaining < n || (n == 0 && e.remaining <= 0) {
		return false
	}
	e.remaining -= n
	return true
}

// Reset restores the full budget.
func (e *ErrorBudget) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.remaining = e.allowed
	if e.window > 0 {
		e.windowStart = e.now()
	}
}

// BudgetSnapshot is a point-in-time view of an ErrorBudget.
type BudgetSnapshot struct {
	Target           string  `json:"target"`
	Remaining        float64 `json:"remaining"`
	Allowed          float64 `json:"allowed"`
	ConsumedFraction float64 `json:"consumed_fraction"`
	WindowSeconds    float64 `json:"window_seconds,omitempty"`
}

// Snapshot returns the current budget state.
func (e *ErrorBudget) Snapshot() BudgetSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rollLocked()
	s := BudgetSnapshot{Remaining: e.remaining, Allowed: e.allowed, WindowSeconds: e.window.Seconds()}
	if e.allowed > 0 {
		s.ConsumedFraction = (e.allowed - e.remaining) / e.allowed
	} else {
		s.ConsumedFraction = 1
	}
	return s
}

const maxTargetLength = 64

func sanitizeTarget(t string) string {
	t = strings.TrimSpace(t)
	if len(t) > maxTargetLength {
		t = t[:maxTargetLength]
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, t)
}

// Handler returns an HTTP handler for /v1/slo/budget. It responds 429 when the
// budget is exhausted. The optional x-slo-target header only labels the
// response.
func Handler(b *ErrorBudget, target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		reqTarget := sanitizeTarget(r.Header.Get("x-slo-target"))
		if reqTarget == "" {
			reqTarget = target
		}
		snap := b.Snapshot()
		snap.Target = reqTarget
		if snap.Remaining <= 0 {
			writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
				"error":     "budget exceeded",
				"target":    reqTarget,
				"remaining": 0,
			})
			return
		}
		writeJSON(w, http.StatusOK, snap)
	}
}

// Middleware returns HTTP middleware enforcing SLO budgets: while budget
// remains requests pass through, and every server error (5xx or panic)
// consumes one unit. Once exhausted, requests are rejected with 429 until the
// budget is replenished (see NewWindowedErrorBudget) or Reset.
func Middleware(b *ErrorBudget) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if b.Remaining() <= 0 {
				writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "budget exceeded"})
				return
			}
			sw := &statusWriter{ResponseWriter: w}
			defer func() {
				if rec := recover(); rec != nil {
					b.Consume(1)
					panic(rec)
				}
				if sw.status() >= http.StatusInternalServerError {
					b.Consume(1)
				}
			}()
			next.ServeHTTP(sw, r)
		})
	}
}

// ParseWindow parses a window string: the predefined windows, Go durations
// ("30m", "2h"), day counts ("7d", "30d") and "custom:<duration>".
func ParseWindow(w Window) (time.Duration, bool) {
	switch w {
	case Window5Min:
		return 5 * time.Minute, true
	case Window1Hour:
		return time.Hour, true
	case Window24H:
		return 24 * time.Hour, true
	}
	s := strings.TrimSpace(strings.TrimPrefix(string(w), "custom:"))
	if s == "" {
		return 0, false
	}
	var d time.Duration
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil || math.IsNaN(n) || n <= 0 || n > MaxWindow.Hours()/24 {
			return 0, false
		}
		d = time.Duration(n * 24 * float64(time.Hour))
	} else {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return 0, false
		}
	}
	if d <= 0 || d > MaxWindow {
		return 0, false
	}
	return d, true
}

// Tracker computes SLO compliance over a sliding window using fixed-size
// time buckets. It is safe for concurrent use.
type Tracker struct {
	mu        sync.Mutex
	objective float64
	window    time.Duration
	bucketDur time.Duration
	buckets   []bucket
	now       func() time.Time
}

type bucket struct {
	epoch int64 // bucket index since the Unix epoch; -1 when unused
	good  uint64
	bad   uint64
}

// NumBuckets is the resolution of a Tracker window.
const NumBuckets = 120

// ErrInvalidSLO is returned for invalid objectives or windows.
var ErrInvalidSLO = errors.New("invalid SLO")

// NewTracker creates a tracker for objective (a fraction in (0,1), e.g.
// 0.999) over window (1s..MaxWindow).
func NewTracker(objective float64, window time.Duration) (*Tracker, error) {
	if math.IsNaN(objective) || objective <= 0 || objective >= 1 {
		return nil, fmt.Errorf("%w: objective must be in (0,1), got %v", ErrInvalidSLO, objective)
	}
	if window < time.Second || window > MaxWindow {
		return nil, fmt.Errorf("%w: window must be between 1s and %s", ErrInvalidSLO, MaxWindow)
	}
	bd := window / NumBuckets
	if bd < 10*time.Millisecond {
		bd = 10 * time.Millisecond
	}
	n := int((window + bd - 1) / bd)
	t := &Tracker{objective: objective, window: window, bucketDur: bd, buckets: make([]bucket, n), now: time.Now}
	for i := range t.buckets {
		t.buckets[i].epoch = -1
	}
	return t, nil
}

// Record records one request outcome at the current time.
func (t *Tracker) Record(success bool) { t.RecordAt(t.now(), success) }

// RecordAt records an outcome at ts. Events outside the window are ignored.
func (t *Tracker) RecordAt(ts time.Time, success bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	nowEpoch := t.now().UnixNano() / int64(t.bucketDur)
	epoch := ts.UnixNano() / int64(t.bucketDur)
	if epoch < 0 || epoch > nowEpoch || epoch <= nowEpoch-int64(len(t.buckets)) {
		return
	}
	b := &t.buckets[int(epoch%int64(len(t.buckets)))]
	if b.epoch != epoch {
		*b = bucket{epoch: epoch}
	}
	if success {
		b.good++
	} else {
		b.bad++
	}
}

// counts sums outcomes over the most recent `over` duration.
func (t *Tracker) counts(over time.Duration) (good, bad uint64) {
	k := int64((over + t.bucketDur - 1) / t.bucketDur)
	if k < 1 {
		k = 1
	}
	if k > int64(len(t.buckets)) {
		k = int64(len(t.buckets))
	}
	nowEpoch := t.now().UnixNano() / int64(t.bucketDur)
	for _, b := range t.buckets {
		if b.epoch > nowEpoch-k && b.epoch <= nowEpoch {
			good += b.good
			bad += b.bad
		}
	}
	return good, bad
}

// Snapshot is the SLO state over a window. All values are finite.
type Snapshot struct {
	Objective       float64 `json:"objective"`
	WindowSeconds   float64 `json:"window_seconds"`
	Total           uint64  `json:"total"`
	Errors          uint64  `json:"errors"`
	ErrorRate       float64 `json:"error_rate"`
	SLI             float64 `json:"sli"`
	AllowedErrors   float64 `json:"allowed_errors"`
	BudgetRemaining float64 `json:"budget_remaining"` // fraction; negative when overspent
	BurnRate        float64 `json:"burn_rate"`
}

func (t *Tracker) snapshotLocked(over time.Duration) Snapshot {
	good, bad := t.counts(over)
	total := good + bad
	s := Snapshot{Objective: t.objective, WindowSeconds: over.Seconds(), Total: total, Errors: bad, SLI: 1}
	allowedRate := 1 - t.objective // > 0 by construction
	s.AllowedErrors = allowedRate * float64(total)
	if total > 0 {
		s.ErrorRate = float64(bad) / float64(total)
		s.SLI = 1 - s.ErrorRate
	}
	s.BurnRate = s.ErrorRate / allowedRate
	s.BudgetRemaining = 1 - s.BurnRate
	return s
}

// Snapshot reports SLO state over the full window.
func (t *Tracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotLocked(t.window)
}

// SnapshotOver reports SLO state over the most recent `over` (clamped to the
// tracker window and bucket resolution).
func (t *Tracker) SnapshotOver(over time.Duration) Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	if over <= 0 || over > t.window {
		over = t.window
	}
	return t.snapshotLocked(over)
}

// BurnRate returns error rate / (1 - objective) over the most recent `over`.
// 1.0 means the budget would be exactly used up over the full window; 0 when
// there is no traffic.
func (t *Tracker) BurnRate(over time.Duration) float64 {
	return t.SnapshotOver(over).BurnRate
}

// ShouldAlert implements multi-window burn-rate alerting: it fires only when
// both the short and the long window burn faster than threshold (e.g.
// 14.4 over 5m and 1h for a 30-day SLO).
func (t *Tracker) ShouldAlert(short, long time.Duration, threshold float64) bool {
	if math.IsNaN(threshold) || threshold <= 0 {
		return false
	}
	return t.BurnRate(short) >= threshold && t.BurnRate(long) >= threshold
}

// Middleware records every request outcome (5xx and panics are errors).
func (t *Tracker) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			if rec := recover(); rec != nil {
				t.Record(false)
				panic(rec)
			}
			t.Record(sw.status() < http.StatusInternalServerError)
		}()
		next.ServeHTTP(sw, r)
	})
}

// Handler serves the tracker state with burn rates over 5m, 1h and the full
// window.
func (t *Tracker) Handler(target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"target":        target,
			"window":        t.Snapshot(),
			"burn_rate_5m":  t.BurnRate(5 * time.Minute),
			"burn_rate_1h":  t.BurnRate(time.Hour),
			"burn_rate_all": t.BurnRate(0),
		})
	}
}

// statusWriter captures the response status while preserving streaming.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer when it supports flushing.
func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (s *statusWriter) status() int {
	if s.code == 0 {
		return http.StatusOK
	}
	return s.code
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
