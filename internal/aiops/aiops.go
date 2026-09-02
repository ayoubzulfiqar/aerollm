package aiops

import (
	"context"
	"runtime"
	"sync"
	"time"
)

// MetricsSnapshot captures platform health signals.
type MetricsSnapshot struct {
	Timestamp     time.Time
	P99LatencyMs  float64
	ErrorRate     float64
	Goroutines    int
	HeapAllocMB   float64
	RequestsTotal int64
	ErrorsTotal   int64
}

// MetricsSource provides current metrics.
type MetricsSource interface {
	Snapshot() MetricsSnapshot
}

// TunerAction represents a runtime adjustment.
type TunerAction struct {
	Name   string
	Apply  func(ctx context.Context) error
	Revert func(ctx context.Context) error
}

// MetaAgentTuner evaluates metrics and applies runtime adjustments.
type MetaAgentTuner struct {
	mu        sync.RWMutex
	source    MetricsSource
	actions   []TunerAction
	interval  time.Duration
	cooldown  time.Duration
	lastApply time.Time
}

// NewMetaAgentTuner creates a new tuner.
func NewMetaAgentTuner(source MetricsSource, interval, cooldown time.Duration) *MetaAgentTuner {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if cooldown <= 0 {
		cooldown = 5 * time.Minute
	}
	return &MetaAgentTuner{source: source, interval: interval, cooldown: cooldown}
}

// RegisterAction adds a tunable action.
func (t *MetaAgentTuner) RegisterAction(action TunerAction) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.actions = append(t.actions, action)
}

// Run starts the control loop until the context is canceled.
func (t *MetaAgentTuner) Run(ctx context.Context) {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.evaluate(ctx)
		}
	}
}

func (t *MetaAgentTuner) evaluate(ctx context.Context) {
	snap := t.source.Snapshot()
	t.mu.RLock()
	actions := make([]TunerAction, len(t.actions))
	copy(actions, t.actions)
	t.mu.RUnlock()

	if len(actions) == 0 {
		return
	}
	if time.Since(t.lastApply) < t.cooldown {
		return
	}
	if snap.P99LatencyMs <= 0 && snap.ErrorRate <= 0 {
		return
	}
	action := actions[0]
	if action.Apply == nil {
		return
	}
	if err := action.Apply(ctx); err == nil {
		t.mu.Lock()
		t.lastApply = time.Now()
		t.mu.Unlock()
	}
}

// DefaultMetricsSource samples Go runtime stats plus external telemetry hooks.
type DefaultMetricsSource struct {
	requestsFn func() int64
	errorsFn   func() int64
	latencyFn  func() float64
}

// NewDefaultMetricsSource creates a source using Go runtime stats.
func NewDefaultMetricsSource(requestsFn func() int64, errorsFn func() int64, latencyFn func() float64) *DefaultMetricsSource {
	return &DefaultMetricsSource{requestsFn: requestsFn, errorsFn: errorsFn, latencyFn: latencyFn}
}

// Snapshot returns current metrics.
func (s *DefaultMetricsSource) Snapshot() MetricsSnapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	requests := int64(0)
	errors := int64(0)
	latency := 0.0
	if s.requestsFn != nil {
		requests = s.requestsFn()
	}
	if s.errorsFn != nil {
		errors = s.errorsFn()
	}
	if s.latencyFn != nil {
		latency = s.latencyFn()
	}
	errorRate := 0.0
	if requests > 0 {
		errorRate = float64(errors) / float64(requests)
	}
	return MetricsSnapshot{
		Timestamp:     time.Now(),
		P99LatencyMs:  latency,
		ErrorRate:     errorRate,
		Goroutines:    runtime.NumGoroutine(),
		HeapAllocMB:   float64(mem.Alloc) / 1024 / 1024,
		RequestsTotal: requests,
		ErrorsTotal:   errors,
	}
}
