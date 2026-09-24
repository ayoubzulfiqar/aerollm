package backpressure

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Config holds backpressure configuration.
type Config struct {
	// MaxInflight is the maximum number of concurrently admitted requests.
	// 0 rejects every request (maintenance mode); negative selects 1000.
	MaxInflight int `json:"max_inflight"`
	// Window is the period over which drop statistics are accumulated.
	Window time.Duration `json:"window_ns"`
	// MaxDropRate is the drop rate (0..1) above which Healthy reports false.
	MaxDropRate float64 `json:"max_drop_rate"`
	// RetryAfter is advertised to rejected clients (default 1s).
	RetryAfter time.Duration `json:"retry_after_ns"`
}

// DefaultConfig returns default backpressure configuration.
func DefaultConfig() Config {
	return Config{
		MaxInflight: 1000,
		Window:      time.Minute,
		MaxDropRate: 0.1,
		RetryAfter:  time.Second,
	}
}

// BackpressureController bounds the number of in-flight requests and
// tracks admission statistics over a rolling window. It is safe for
// concurrent use.
type BackpressureController struct {
	config      Config
	mu          sync.Mutex
	inflight    int
	dropped     int64
	total       int64
	windowStart time.Time
	now         func() time.Time
}

// NewBackpressureController creates a new backpressure controller.
func NewBackpressureController(config Config) *BackpressureController {
	if config.MaxInflight < 0 {
		config.MaxInflight = 1000
	}
	if config.Window <= 0 {
		config.Window = time.Minute
	}
	if math.IsNaN(config.MaxDropRate) || config.MaxDropRate < 0 {
		config.MaxDropRate = 0
	}
	if config.MaxDropRate > 1 {
		config.MaxDropRate = 1
	}
	if config.RetryAfter <= 0 {
		config.RetryAfter = time.Second
	}
	return &BackpressureController{
		config:      config,
		windowStart: time.Now(),
		now:         time.Now,
	}
}

// Config returns the effective configuration.
func (b *BackpressureController) Config() Config { return b.config }

// Allow reports whether a request can be admitted. Every true result must
// be paired with exactly one Record call when the request finishes.
func (b *BackpressureController) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resetWindowIfNeededLocked()
	b.total++
	if b.inflight >= b.config.MaxInflight {
		b.dropped++
		return false
	}
	b.inflight++
	return true
}

// Record records the completion of an admitted request.
func (b *BackpressureController) Record(success bool) {
	_ = success
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inflight > 0 {
		b.inflight--
	}
}

// Metrics returns current backpressure metrics.
func (b *BackpressureController) Metrics() Metrics {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resetWindowIfNeededLocked()
	var dropRate float64
	if b.total > 0 {
		dropRate = float64(b.dropped) / float64(b.total)
	}
	return Metrics{
		Inflight:    int64(b.inflight),
		Dropped:     b.dropped,
		Total:       b.total,
		DropRate:    dropRate,
		WindowStart: b.windowStart,
		Config:      b.config,
		Healthy:     dropRate <= b.config.MaxDropRate,
	}
}

// Healthy reports whether the drop rate in the current window is within
// Config.MaxDropRate.
func (b *BackpressureController) Healthy() bool { return b.Metrics().Healthy }

// resetWindowIfNeededLocked starts a new statistics window when the
// current one elapsed. In-flight requests are NOT reset: they are still
// running and will call Record, so zeroing the gauge would let the limit
// be exceeded.
func (b *BackpressureController) resetWindowIfNeededLocked() {
	now := b.now()
	if now.Sub(b.windowStart) >= b.config.Window {
		b.dropped = 0
		b.total = 0
		b.windowStart = now
	}
}

// Metrics holds backpressure metrics.
type Metrics struct {
	Inflight    int64     `json:"inflight"`
	Dropped     int64     `json:"dropped"`
	Total       int64     `json:"total"`
	DropRate    float64   `json:"drop_rate"`
	WindowStart time.Time `json:"window_start"`
	Healthy     bool      `json:"healthy"`
	Config      Config    `json:"config"`
}

// Handler returns an HTTP handler for backpressure status (GET/HEAD).
func (b *BackpressureController) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
			return
		}
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodHead {
			return
		}
		_ = json.NewEncoder(w).Encode(b.Metrics())
	}
}

// Middleware returns HTTP middleware that enforces backpressure, rejecting
// excess requests with 503 and a Retry-After header.
func (b *BackpressureController) Middleware(next http.Handler) http.Handler {
	retryAfter := strconv.Itoa(int(math.Ceil(b.config.RetryAfter.Seconds())))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !b.Allow() {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", retryAfter)
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "backpressure: request dropped",
			})
			return
		}
		defer b.Record(true)
		next.ServeHTTP(w, r)
	})
}
