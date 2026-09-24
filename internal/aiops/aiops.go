// Package aiops implements a small closed-loop runtime tuner ("meta agent")
// that watches platform health metrics and applies/reverts registered
// runtime adjustments when the platform is degraded/recovered.
//
// Two kinds of adjustments are supported:
//
//   - Actions (Register): independent closed-loop controllers with
//     per-target hysteresis, sustained-breach debouncing, cooldowns, a
//     dry-run mode (the default) and an audit trail. Built-ins:
//     LatencyStrategyAction, RateLimitAction and CircuitBreakerAction, all
//     driven by caller-supplied callbacks.
//   - TunerActions (RegisterAction, legacy): a single escalation ladder that
//     applies one action per cooldown while the platform is degraded.
package aiops

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"
)

// Default tuner parameters.
const (
	// DefaultInterval is the evaluation interval used when a non-positive
	// interval is supplied.
	DefaultInterval = 30 * time.Second
	// DefaultCooldown is the minimum time between two state changes
	// (apply or revert) used when a non-positive cooldown is supplied.
	DefaultCooldown = 5 * time.Minute
	// DefaultLatencyThresholdMs is the latency above which the platform is
	// considered degraded.
	DefaultLatencyThresholdMs = 2000.0
	// DefaultErrorRateThreshold is the (windowed) error rate above which the
	// platform is considered degraded.
	DefaultErrorRateThreshold = 0.05
	// DefaultRecoveryEvaluations is the number of consecutive healthy
	// evaluations required before the most recently applied action is reverted.
	DefaultRecoveryEvaluations = 3
)

// ErrInvalidConfig is returned when a tuner configuration is rejected.
var ErrInvalidConfig = errors.New("aiops: invalid tuner config")

// MetricsSnapshot captures platform health signals.
type MetricsSnapshot struct {
	Timestamp time.Time
	// P99LatencyMs is the tail latency in milliseconds. NOTE: when sourced
	// from DefaultMetricsSource this is populated with the AVERAGE latency
	// (the telemetry hook only exposes an average); it is kept for backward
	// compatibility. Prefer AvgLatencyMs for averages.
	P99LatencyMs float64
	// AvgLatencyMs is the average request latency in milliseconds.
	AvgLatencyMs float64
	// ErrorRate is the error rate reported by the source. DefaultMetricsSource
	// reports the cumulative (since process start) rate; the tuner itself
	// computes a windowed rate from RequestsTotal/ErrorsTotal deltas when
	// those counters are available.
	ErrorRate     float64
	Goroutines    int
	HeapAllocMB   float64
	RequestsTotal int64
	ErrorsTotal   int64
	// P95LatencyMs is the 95th percentile latency in milliseconds (0 when
	// the source does not provide it). Actions prefer it over P99/Avg.
	P95LatencyMs float64
	// Providers holds optional per-provider stats keyed by provider name
	// (see ProviderStats). Used by per-provider Actions.
	Providers map[string]ProviderStats
}

// MetricsSource provides current metrics.
type MetricsSource interface {
	Snapshot() MetricsSnapshot
}

// TunerAction represents a runtime adjustment. Actions form an escalation
// ladder in registration order: while degraded the tuner applies the next
// action that is not yet active; after sustained recovery it reverts the most
// recently applied action first (LIFO).
type TunerAction struct {
	Name   string
	Apply  func(ctx context.Context) error
	Revert func(ctx context.Context) error
}

// TunerConfig holds the health thresholds used by the tuner.
type TunerConfig struct {
	// LatencyThresholdMs: latency (max of P99LatencyMs and AvgLatencyMs)
	// strictly above this value marks the platform degraded.
	LatencyThresholdMs float64 `json:"latency_threshold_ms"`
	// ErrorRateThreshold: windowed error rate strictly above this value marks
	// the platform degraded. Must be in (0, 1].
	ErrorRateThreshold float64 `json:"error_rate_threshold"`
	// RecoveryEvaluations: consecutive healthy evaluations required before
	// reverting the most recent action.
	RecoveryEvaluations int `json:"recovery_evaluations"`
}

// DefaultTunerConfig returns the default thresholds.
func DefaultTunerConfig() TunerConfig {
	return TunerConfig{
		LatencyThresholdMs:  DefaultLatencyThresholdMs,
		ErrorRateThreshold:  DefaultErrorRateThreshold,
		RecoveryEvaluations: DefaultRecoveryEvaluations,
	}
}

// Validate reports whether the configuration is usable.
func (c TunerConfig) Validate() error {
	if math.IsNaN(c.LatencyThresholdMs) || math.IsInf(c.LatencyThresholdMs, 0) || c.LatencyThresholdMs <= 0 {
		return fmt.Errorf("%w: latency_threshold_ms must be a finite positive number", ErrInvalidConfig)
	}
	if math.IsNaN(c.ErrorRateThreshold) || c.ErrorRateThreshold <= 0 || c.ErrorRateThreshold > 1 {
		return fmt.Errorf("%w: error_rate_threshold must be in (0, 1]", ErrInvalidConfig)
	}
	if c.RecoveryEvaluations < 1 || c.RecoveryEvaluations > 10000 {
		return fmt.Errorf("%w: recovery_evaluations must be in [1, 10000]", ErrInvalidConfig)
	}
	return nil
}

// TunerStats is a point-in-time view of the tuner, suitable for exporting as
// Prometheus-style gauges/counters.
type TunerStats struct {
	Evaluations    int64     `json:"evaluations"`
	InvalidMetrics int64     `json:"invalid_metrics"`
	Applied        int64     `json:"applied"`
	ApplyFailures  int64     `json:"apply_failures"`
	Reverted       int64     `json:"reverted"`
	RevertFailures int64     `json:"revert_failures"`
	ActiveActions  []string  `json:"active_actions"`
	Degraded       bool      `json:"degraded"`
	LastLatencyMs  float64   `json:"last_latency_ms"`
	LastErrorRate  float64   `json:"last_error_rate"`
	LastError      string    `json:"last_error,omitempty"`
	LastEvaluation time.Time `json:"last_evaluation"`
	LastChange     time.Time `json:"last_change"`
	// DryRun reports whether registered Actions run in dry-run mode.
	DryRun bool `json:"dry_run"`
	// RegisteredActions is the number of Actions added via Register.
	RegisteredActions int `json:"registered_actions"`
}

// MetaAgentTuner evaluates metrics and applies runtime adjustments.
//
// It is a threshold-based controller: when the platform is degraded it
// escalates through the registered actions (one per cooldown period); when the
// platform has been healthy for RecoveryEvaluations consecutive evaluations it
// reverts the most recently applied action. Healthy traffic never triggers an
// action.
type MetaAgentTuner struct {
	// evalMu serializes evaluations so actions never run concurrently.
	evalMu sync.Mutex

	mu       sync.RWMutex
	source   MetricsSource
	actions  []TunerAction
	interval time.Duration
	cooldown time.Duration
	cfg      TunerConfig

	lastApply     time.Time // time of the last state change (apply or revert)
	active        []int     // indices into actions, in apply order (stack)
	prev          *MetricsSnapshot
	healthyStreak int
	stats         TunerStats

	// Action engine (see actions.go). live=false (the zero value) means
	// dry-run.
	clock      func() time.Time
	live       bool
	registered []*registeredAction
	audit      auditRing
}

// NewMetaAgentTuner creates a new tuner with default thresholds.
// Non-positive interval/cooldown values fall back to DefaultInterval and
// DefaultCooldown.
func NewMetaAgentTuner(source MetricsSource, interval, cooldown time.Duration) *MetaAgentTuner {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if cooldown <= 0 {
		cooldown = DefaultCooldown
	}
	return &MetaAgentTuner{source: source, interval: interval, cooldown: cooldown, cfg: DefaultTunerConfig()}
}

// SetConfig replaces the health thresholds after validating them.
func (t *MetaAgentTuner) SetConfig(cfg TunerConfig) error {
	if t == nil {
		return fmt.Errorf("aiops: tuner is nil")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	t.mu.Lock()
	t.cfg = cfg
	t.mu.Unlock()
	return nil
}

// SetThresholds updates the latency and error-rate thresholds, keeping the
// current recovery setting.
func (t *MetaAgentTuner) SetThresholds(latencyMs, errorRate float64) error {
	if t == nil {
		return fmt.Errorf("aiops: tuner is nil")
	}
	cfg := t.Config()
	cfg.LatencyThresholdMs = latencyMs
	cfg.ErrorRateThreshold = errorRate
	return t.SetConfig(cfg)
}

// Config returns the current thresholds.
func (t *MetaAgentTuner) Config() TunerConfig {
	if t == nil {
		return TunerConfig{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	cfg := t.cfg
	if cfg == (TunerConfig{}) {
		cfg = DefaultTunerConfig()
	}
	return cfg
}

// RegisterAction adds a tunable action to the end of the escalation ladder.
func (t *MetaAgentTuner) RegisterAction(action TunerAction) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.actions = append(t.actions, action)
}

// Stats returns a snapshot of the tuner's counters and state.
func (t *MetaAgentTuner) Stats() TunerStats {
	if t == nil {
		return TunerStats{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := t.stats
	out.ActiveActions = make([]string, 0, len(t.active))
	for _, idx := range t.active {
		out.ActiveActions = append(out.ActiveActions, t.actions[idx].Name)
	}
	out.DryRun = !t.live
	out.RegisteredActions = len(t.registered)
	return out
}

// Run starts the control loop until the context is canceled.
func (t *MetaAgentTuner) Run(ctx context.Context) {
	if t == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.RLock()
	interval := t.interval
	t.mu.RUnlock()
	if interval <= 0 {
		interval = DefaultInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			t.evaluate(ctx)
		}
	}
}

func validSnapshot(s MetricsSnapshot) bool {
	for _, v := range []float64{s.P99LatencyMs, s.AvgLatencyMs, s.P95LatencyMs, s.ErrorRate} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return false
		}
	}
	return s.RequestsTotal >= 0 && s.ErrorsTotal >= 0
}

// windowErrorRate computes the error rate over the window since prev using the
// cumulative counters. With no usable previous snapshot (first evaluation or a
// counter reset) it falls back to the cumulative counters, or to the
// source-provided ErrorRate when no counters are available.
func windowErrorRate(prev *MetricsSnapshot, cur MetricsSnapshot) float64 {
	fresh := func() float64 {
		if cur.RequestsTotal > 0 {
			return clampUnit(float64(cur.ErrorsTotal) / float64(cur.RequestsTotal))
		}
		return clampUnit(cur.ErrorRate)
	}
	if prev == nil {
		return fresh()
	}
	if cur.RequestsTotal == 0 && prev.RequestsTotal == 0 {
		// Source does not expose counters; trust its rate.
		return clampUnit(cur.ErrorRate)
	}
	if cur.RequestsTotal < prev.RequestsTotal || cur.ErrorsTotal < prev.ErrorsTotal {
		// Counter reset (e.g. process/metrics restart): treat as fresh.
		return fresh()
	}
	dReq := cur.RequestsTotal - prev.RequestsTotal
	dErr := cur.ErrorsTotal - prev.ErrorsTotal
	if dReq <= 0 {
		// No traffic in the window: no evidence of errors.
		return 0
	}
	return clampUnit(float64(dErr) / float64(dReq))
}

func clampUnit(v float64) float64 {
	if math.IsNaN(v) || v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// safeCall runs fn, converting a panic into an error so that a misbehaving
// action cannot crash the process.
func safeCall(ctx context.Context, fn func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("aiops: action panicked: %v", r)
		}
	}()
	return fn(ctx)
}

func (t *MetaAgentTuner) evaluate(ctx context.Context) {
	if t == nil {
		return
	}
	t.evalMu.Lock()
	defer t.evalMu.Unlock()

	t.mu.RLock()
	source := t.source
	t.mu.RUnlock()
	if source == nil {
		return
	}
	t.evaluateSnapshot(ctx, source.Snapshot())
}

// Observe evaluates a pushed snapshot immediately (instead of, or in
// addition to, polling the MetricsSource in Run). It runs the legacy ladder
// and every registered Action exactly like a scheduled evaluation.
func (t *MetaAgentTuner) Observe(ctx context.Context, snap MetricsSnapshot) {
	if t == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	t.evalMu.Lock()
	defer t.evalMu.Unlock()
	t.evaluateSnapshot(ctx, snap)
}

// evaluateSnapshot runs one evaluation. Caller holds t.evalMu.
func (t *MetaAgentTuner) evaluateSnapshot(ctx context.Context, snap MetricsSnapshot) {
	t.mu.Lock()
	now := t.now()
	t.stats.Evaluations++
	t.stats.LastEvaluation = now
	if !validSnapshot(snap) {
		t.stats.InvalidMetrics++
		t.stats.LastError = "invalid metrics snapshot (NaN/Inf/negative)"
		t.mu.Unlock()
		return
	}
	prevSnap := t.prev
	errRate := windowErrorRate(prevSnap, snap)
	sig := buildSignals(prevSnap, snap, errRate, now)
	stored := snap
	stored.Providers = copyProviders(snap.Providers)
	t.prev = &stored
	latency := math.Max(snap.P99LatencyMs, snap.AvgLatencyMs)
	cfg := t.cfg
	if cfg == (TunerConfig{}) {
		cfg = DefaultTunerConfig()
	}
	degraded := latency > cfg.LatencyThresholdMs || errRate > cfg.ErrorRateThreshold
	t.stats.Degraded = degraded
	t.stats.LastLatencyMs = latency
	t.stats.LastErrorRate = errRate
	cooldownElapsed := t.lastApply.IsZero() || now.Sub(t.lastApply) >= t.cooldown
	reason := fmt.Sprintf("latency_ms=%.1f error_rate=%.4f", latency, errRate)

	applyIdx, revertIdx := -1, -1
	if degraded {
		t.healthyStreak = 0
		if cooldownElapsed {
			applyIdx = t.nextActionLocked()
		}
	} else {
		t.healthyStreak++
		if len(t.active) > 0 && t.healthyStreak >= cfg.RecoveryEvaluations && cooldownElapsed {
			revertIdx = t.active[len(t.active)-1]
		}
	}
	var action TunerAction
	switch {
	case applyIdx >= 0:
		action = t.actions[applyIdx]
	case revertIdx >= 0:
		action = t.actions[revertIdx]
	}
	t.mu.Unlock()

	switch {
	case applyIdx >= 0:
		t.ladderApply(ctx, applyIdx, action, reason)
	case revertIdx >= 0:
		t.ladderRevert(ctx, revertIdx, action, reason)
	}
	t.runActions(ctx, sig)
}

// ladderApply applies a legacy TunerAction. Caller holds t.evalMu.
func (t *MetaAgentTuner) ladderApply(ctx context.Context, idx int, action TunerAction, reason string) {
	err := safeCall(ctx, action.Apply)

	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastApply = t.now() // back off after both success and failure
	if err != nil {
		t.stats.ApplyFailures++
		t.recordLocked(action.Name, "", OutcomeApplyFailed, false, reason, "", err, t.lastApply)
		t.stats.LastError = fmt.Sprintf("apply %q: %v", action.Name, err)
		return
	}
	t.active = append(t.active, idx)
	t.stats.Applied++
	t.stats.LastChange = t.lastApply
	t.recordLocked(action.Name, "", OutcomeApplied, false, reason, "", nil, t.lastApply)
}

// ladderRevert reverts the most recently applied legacy TunerAction. Caller
// holds t.evalMu.
func (t *MetaAgentTuner) ladderRevert(ctx context.Context, idx int, action TunerAction, reason string) {
	var err error
	if action.Revert != nil {
		err = safeCall(ctx, action.Revert)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.healthyStreak = 0
	if err != nil {
		t.stats.RevertFailures++
		t.recordLocked(action.Name, "", OutcomeRevertFailed, false, reason, "", err, t.now())
		t.stats.LastError = fmt.Sprintf("revert %q: %v", action.Name, err)
		return
	}
	// Pop the action. Evaluations are serialized by evalMu, so the top of
	// the stack is still idx.
	if n := len(t.active); n > 0 && t.active[n-1] == idx {
		t.active = t.active[:n-1]
	}
	if action.Revert != nil {
		t.stats.Reverted++
	}
	t.lastApply = t.now()
	t.stats.LastChange = t.lastApply
	t.recordLocked(action.Name, "", OutcomeReverted, false, reason, "", nil, t.lastApply)
}

// nextActionLocked returns the index of the first registered action with a
// non-nil Apply that is not currently active, or -1. Caller holds t.mu.
func (t *MetaAgentTuner) nextActionLocked() int {
	for i, a := range t.actions {
		if a.Apply == nil {
			continue
		}
		isActive := false
		for _, j := range t.active {
			if j == i {
				isActive = true
				break
			}
		}
		if !isActive {
			return i
		}
	}
	return -1
}

// DefaultMetricsSource samples Go runtime stats plus external telemetry hooks.
type DefaultMetricsSource struct {
	requestsFn  func() int64
	errorsFn    func() int64
	latencyFn   func() float64
	p95Fn       func() float64
	providersFn func() map[string]ProviderStats
}

// NewDefaultMetricsSource creates a source using Go runtime stats.
// latencyFn is expected to return the AVERAGE request latency in ms.
func NewDefaultMetricsSource(requestsFn func() int64, errorsFn func() int64, latencyFn func() float64) *DefaultMetricsSource {
	return &DefaultMetricsSource{requestsFn: requestsFn, errorsFn: errorsFn, latencyFn: latencyFn}
}

// WithP95 sets a hook returning the p95 request latency in ms, used by
// latency-driven Actions. Call it before the tuner starts. It returns s.
func (s *DefaultMetricsSource) WithP95(fn func() float64) *DefaultMetricsSource {
	if s != nil {
		s.p95Fn = fn
	}
	return s
}

// WithProviders sets a hook returning per-provider stats (preferably
// cumulative RequestsTotal/ErrorsTotal counters), used by per-provider
// Actions such as CircuitBreakerAction. Call it before the tuner starts. It
// returns s.
func (s *DefaultMetricsSource) WithProviders(fn func() map[string]ProviderStats) *DefaultMetricsSource {
	if s != nil {
		s.providersFn = fn
	}
	return s
}

// Snapshot returns current metrics. ErrorRate is cumulative since process
// start; P99LatencyMs mirrors AvgLatencyMs (see MetricsSnapshot).
func (s *DefaultMetricsSource) Snapshot() MetricsSnapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	requests := int64(0)
	errs := int64(0)
	latency := 0.0
	p95 := 0.0
	var providers map[string]ProviderStats
	if s != nil {
		if s.p95Fn != nil {
			p95 = s.p95Fn()
		}
		if s.providersFn != nil {
			providers = copyProviders(s.providersFn())
		}
		if s.requestsFn != nil {
			requests = s.requestsFn()
		}
		if s.errorsFn != nil {
			errs = s.errorsFn()
		}
		if s.latencyFn != nil {
			latency = s.latencyFn()
		}
	}
	errorRate := 0.0
	if requests > 0 && errs >= 0 {
		errorRate = float64(errs) / float64(requests)
	}
	return MetricsSnapshot{
		Timestamp:     time.Now(),
		P99LatencyMs:  latency,
		AvgLatencyMs:  latency,
		ErrorRate:     errorRate,
		Goroutines:    runtime.NumGoroutine(),
		HeapAllocMB:   float64(mem.Alloc) / 1024 / 1024,
		RequestsTotal: requests,
		ErrorsTotal:   errs,
		P95LatencyMs:  p95,
		Providers:     providers,
	}
}
