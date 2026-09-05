package backpressure

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Config holds backpressure configuration.
type Config struct {
	MaxInflight int
	Window      time.Duration
	MaxDropRate float64
}

// DefaultConfig returns default backpressure configuration.
func DefaultConfig() Config {
	return Config{
		MaxInflight: 1000,
		Window:      time.Minute,
		MaxDropRate: 0.1,
	}
}

// BackpressureController manages request flow and backpressure.
type BackpressureController struct {
	config     Config
	inflight   int
	dropped    int64
	total      int64
	mu         sync.RWMutex
	windowStart time.Time
}

// NewBackpressureController creates a new backpressure controller.
func NewBackpressureController(config Config) *BackpressureController {
	if config.MaxInflight < 0 {
		config.MaxInflight = 1000
	}
	if config.Window <= 0 {
		config.Window = time.Minute
	}
	if config.MaxDropRate < 0 {
		config.MaxDropRate = 0
	}
	return &BackpressureController{
		config:     config,
		windowStart: time.Now(),
	}
}

// Allow checks if a request can be accepted based on backpressure state.
func (b *BackpressureController) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	b.resetWindowIfNeeded()
	
	if b.inflight >= b.config.MaxInflight {
		b.dropped++
		b.total++
		return false
	}
	
	b.inflight++
	b.total++
	return true
}

// Record records the completion of a request.
func (b *BackpressureController) Record(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	b.inflight--
	if b.inflight < 0 {
		b.inflight = 0
	}
}

// Metrics returns current backpressure metrics.
func (b *BackpressureController) Metrics() Metrics {
	b.mu.RLock()
	defer b.mu.RUnlock()
	
	b.resetWindowIfNeeded()
	
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
	}
}

// resetWindowIfNeeded resets metrics if the window has elapsed.
func (b *BackpressureController) resetWindowIfNeeded() {
	if time.Since(b.windowStart) >= b.config.Window {
		b.inflight = 0
		b.dropped = 0
		b.total = 0
		b.windowStart = time.Now()
	}
}

// Metrics holds backpressure metrics.
type Metrics struct {
	Inflight    int64         `json:"inflight"`
	Dropped     int64         `json:"dropped"`
	Total       int64         `json:"total"`
	DropRate    float64       `json:"drop_rate"`
	WindowStart time.Time     `json:"window_start"`
	Config      Config        `json:"config"`
}

// Handler returns an HTTP handler for backpressure status.
func (b *BackpressureController) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		metrics := b.Metrics()
		metrics.Config = b.config
		
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(metrics)
	}
}

// Middleware returns HTTP middleware that enforces backpressure.
func (b *BackpressureController) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !b.Allow() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "backpressure: request dropped",
			})
			return
		}
		
		defer b.Record(true)
		next.ServeHTTP(w, r)
	})
}
