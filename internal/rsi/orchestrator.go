package rsi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// ---------------------------------------------------------------------------
// RSI Orchestrator: full lifecycle coordination
// ---------------------------------------------------------------------------
//
// Safety model:
//   - Only one cycle (or rollback) runs at a time. A cycle requested while
//     another is in flight is rejected with ErrCycleInProgress; it is never
//     queued, so API callers get a deterministic answer.
//   - A candidate policy is deployed only when, on held-out scenarios it was
//     not optimised on, it beats the baseline by ImprovementThresholdPct with
//     at least MinSamples paired samples and a one-sided paired t-test
//     p-value <= SignificanceLevel. DryRun evaluates everything but never
//     calls OnDeploy.
//   - If OnDeploy fails, OnRollback (when set) is invoked to restore the
//     previously deployed policy for that dimension. Rollback() reverts the
//     most recent successful deployment on demand.
//   - The orchestrator itself never touches files, processes or the network;
//     any live effect happens only inside the injected hooks. The only
//     exception is the opt-in persist.Store attached by EnablePersistence
//     (see persistence.go), which is written through on every state change.

// Sentinel errors returned by the orchestrator.
var (
	ErrNilOrchestrator   = errors.New("rsi: orchestrator is nil")
	ErrCycleInProgress   = errors.New("rsi: a cycle is already in progress")
	ErrInvalidConfig     = errors.New("rsi: invalid config")
	ErrNothingToRollback = errors.New("rsi: no deployment to roll back")
	ErrNoRollbackHook    = errors.New("rsi: no rollback hook configured")
)

// Configuration limits enforced by RSIConfig.Validate.
const (
	MinCycleInterval           = time.Second
	MaxCycleInterval           = 7 * 24 * time.Hour
	MaxCycleTimeout            = time.Hour
	MaxKFold                   = 20
	MaxImprovementThresholdPct = 1000.0
	MaxHistoryLimit            = 10000
	MaxMinSamples              = 1_000_000

	// maxDeploymentStack bounds how many past deployments can be rolled back.
	maxDeploymentStack = 32
)

// Deploy decisions recorded in RSICycle.DeployDecision.
const (
	DecisionDeployed            = "deployed"
	DecisionDryRun              = "dry_run"
	DecisionNoDeployHook        = "no_deploy_hook"
	DecisionDeployFailed        = "deploy_failed"
	DecisionBelowThreshold      = "below_threshold"
	DecisionNotSignificant      = "not_significant"
	DecisionInsufficientSamples = "insufficient_samples"
	DecisionInsufficientData    = "insufficient_data"
	DecisionLowHeadroom         = "headroom_below_threshold"
	DecisionError               = "error"
)

// RSIConfig configures the RSI orchestrator's behavior.
//
// Zero values of CycleInterval, CycleTimeout, MinSamples, SignificanceLevel
// and MaxHistory select the defaults from DefaultRSIConfig, so a partial
// config never silently disables a safety gate. Durations are encoded in
// JSON as nanoseconds; on input they may also be Go duration strings such as
// "5m".
type RSIConfig struct {
	ImprovementThresholdPct float64       `json:"improvement_threshold_pct"`
	BroadIterations         int           `json:"broad_iterations"`
	DeepIterations          int           `json:"deep_iterations"`
	CycleInterval           time.Duration `json:"cycle_interval"`
	KFold                   int           `json:"k_fold"`
	HeadroomThreshold       float64       `json:"headroom_threshold"`
	SampleSize              int           `json:"sample_size"`

	// MinSamples is the minimum number of held-out paired samples required
	// before a candidate may be deployed.
	MinSamples int `json:"min_samples"`
	// SignificanceLevel is the one-sided alpha for the paired t-test that
	// candidate > baseline. 1 disables the significance requirement.
	SignificanceLevel float64 `json:"significance_level"`
	// MaxHistory bounds the number of cycles kept in memory.
	MaxHistory int `json:"max_history"`
	// CycleTimeout bounds the wall-clock time of one cycle (including the
	// deploy hook).
	CycleTimeout time.Duration `json:"cycle_timeout"`
	// DryRun evaluates candidates but never calls OnDeploy.
	DryRun bool `json:"dry_run"`
}

// DefaultRSIConfig returns a production-ready configuration.
func DefaultRSIConfig() RSIConfig {
	return RSIConfig{
		ImprovementThresholdPct: 5.0,
		BroadIterations:         10,
		DeepIterations:          5,
		CycleInterval:           5 * time.Minute,
		KFold:                   3,
		HeadroomThreshold:       0.1,
		SampleSize:              0,
		MinSamples:              30,
		SignificanceLevel:       0.05,
		MaxHistory:              100,
		CycleTimeout:            2 * time.Minute,
		DryRun:                  false,
	}
}

// withDefaults fills zero-valued defaultable fields.
func (c RSIConfig) withDefaults() RSIConfig {
	d := DefaultRSIConfig()
	if c.CycleInterval == 0 {
		c.CycleInterval = d.CycleInterval
	}
	if c.CycleTimeout == 0 {
		c.CycleTimeout = d.CycleTimeout
	}
	if c.MinSamples == 0 {
		c.MinSamples = d.MinSamples
	}
	if c.SignificanceLevel == 0 {
		c.SignificanceLevel = d.SignificanceLevel
	}
	if c.MaxHistory == 0 {
		c.MaxHistory = d.MaxHistory
	}
	return c
}

// sanitized is used for configs passed to NewRSIOrchestrator (a trusted,
// programmatic path that cannot return an error). It applies defaults and
// replaces values that would make the loop panic, spin, or deploy
// regressions; everything else is kept as given. Runtime updates go through
// SetConfig, which validates strictly instead.
func (c RSIConfig) sanitized() RSIConfig {
	c = c.withDefaults()
	d := DefaultRSIConfig()
	if c.CycleInterval < 0 {
		c.CycleInterval = d.CycleInterval
	}
	if c.CycleTimeout < 0 {
		c.CycleTimeout = d.CycleTimeout
	}
	if c.MinSamples < 0 {
		c.MinSamples = d.MinSamples
	}
	if !(c.SignificanceLevel > 0 && c.SignificanceLevel <= 1) {
		c.SignificanceLevel = d.SignificanceLevel
	}
	if c.MaxHistory < 0 {
		c.MaxHistory = d.MaxHistory
	}
	if c.MaxHistory > MaxHistoryLimit {
		c.MaxHistory = MaxHistoryLimit
	}
	c.BroadIterations = clampIterations(c.BroadIterations)
	c.DeepIterations = clampIterations(c.DeepIterations)
	if c.KFold < 0 {
		c.KFold = 0
	}
	if c.KFold > MaxKFold {
		c.KFold = MaxKFold
	}
	if c.SampleSize < 0 {
		c.SampleSize = 0
	}
	if !isFinite(c.ImprovementThresholdPct) || c.ImprovementThresholdPct < 0 {
		// A negative threshold would deploy regressions.
		c.ImprovementThresholdPct = d.ImprovementThresholdPct
	}
	if !isFinite(c.HeadroomThreshold) {
		c.HeadroomThreshold = d.HeadroomThreshold
	}
	return c
}

// Validate reports every out-of-range field. Zero values of defaultable
// fields (see RSIConfig) are accepted. Use it before SetConfig on untrusted
// input such as an HTTP PUT body.
func (c RSIConfig) Validate() error {
	var problems []string
	add := func(format string, args ...interface{}) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	if !(c.ImprovementThresholdPct >= 0 && c.ImprovementThresholdPct <= MaxImprovementThresholdPct) {
		add("improvement_threshold_pct must be within [0, %g]", MaxImprovementThresholdPct)
	}
	if c.BroadIterations < 0 || c.BroadIterations > MaxExploreIterations {
		add("broad_iterations must be within [0, %d]", MaxExploreIterations)
	}
	if c.DeepIterations < 0 || c.DeepIterations > MaxExploreIterations {
		add("deep_iterations must be within [0, %d]", MaxExploreIterations)
	}
	if c.CycleInterval != 0 && (c.CycleInterval < MinCycleInterval || c.CycleInterval > MaxCycleInterval) {
		add("cycle_interval must be 0 (default) or within [%s, %s]", MinCycleInterval, MaxCycleInterval)
	}
	if c.KFold < 0 || c.KFold == 1 || c.KFold > MaxKFold {
		add("k_fold must be 0 (80/20 split) or within [2, %d]", MaxKFold)
	}
	if !(c.HeadroomThreshold >= 0 && c.HeadroomThreshold <= 1) {
		add("headroom_threshold must be within [0, 1]")
	}
	if c.SampleSize < 0 || c.SampleSize > MaxReplayScenarios {
		add("sample_size must be 0 (all, capped) or within [1, %d]", MaxReplayScenarios)
	}
	if c.MinSamples != 0 && (c.MinSamples < 2 || c.MinSamples > MaxMinSamples) {
		add("min_samples must be 0 (default) or within [2, %d]", MaxMinSamples)
	}
	if c.SignificanceLevel != 0 && !(c.SignificanceLevel > 0 && c.SignificanceLevel <= 1) {
		add("significance_level must be 0 (default) or within (0, 1]")
	}
	if c.MaxHistory < 0 || c.MaxHistory > MaxHistoryLimit {
		add("max_history must be 0 (default) or within [1, %d]", MaxHistoryLimit)
	}
	if c.CycleTimeout != 0 && (c.CycleTimeout < MinCycleInterval || c.CycleTimeout > MaxCycleTimeout) {
		add("cycle_timeout must be 0 (default) or within [%s, %s]", MinCycleInterval, MaxCycleTimeout)
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalidConfig, strings.Join(problems, "; "))
	}
	return nil
}

// UnmarshalJSON decodes an RSIConfig, accepting cycle_interval and
// cycle_timeout either as integer nanoseconds or as Go duration strings
// ("5m", "90s"). Fields absent from the input keep their current values, so
// decoding onto Config() yields a partial update.
func (c *RSIConfig) UnmarshalJSON(b []byte) error {
	type plain RSIConfig
	aux := struct {
		*plain
		CycleInterval json.RawMessage `json:"cycle_interval,omitempty"`
		CycleTimeout  json.RawMessage `json:"cycle_timeout,omitempty"`
	}{plain: (*plain)(c)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if len(aux.CycleInterval) > 0 {
		d, err := parseJSONDuration(aux.CycleInterval)
		if err != nil {
			return fmt.Errorf("cycle_interval: %w", err)
		}
		c.CycleInterval = d
	}
	if len(aux.CycleTimeout) > 0 {
		d, err := parseJSONDuration(aux.CycleTimeout)
		if err != nil {
			return fmt.Errorf("cycle_timeout: %w", err)
		}
		c.CycleTimeout = d
	}
	return nil
}

func parseJSONDuration(raw json.RawMessage) (time.Duration, error) {
	s := strings.TrimSpace(string(raw))
	if s == "null" {
		return 0, nil
	}
	if strings.HasPrefix(s, `"`) {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return 0, err
		}
		return time.ParseDuration(str)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("must be integer nanoseconds or a duration string: %w", err)
	}
	return time.Duration(n), nil
}

// DeployFunc is called when a policy improvement passes the deploy gate.
// The server wires this to apply the policy via the aiops MetaAgentTuner.
// It must be idempotent and should apply the policy atomically; if it
// returns an error the orchestrator treats the deployment as failed and
// invokes OnRollback.
type DeployFunc func(ctx context.Context, policy Policy, cycle RSICycle) error

// RollbackFunc restores a previous policy. restore is the policy that was
// deployed for the same dimension before the one being reverted, or nil when
// RSI had not deployed anything for that dimension (i.e. restore the
// gateway's pre-RSI behaviour). cycle is the cycle whose deployment is being
// reverted.
type RollbackFunc func(ctx context.Context, restore Policy, cycle RSICycle) error

// RSICycle records the outcome of a single RSI cycle.
type RSICycle struct {
	ID                int                            `json:"id"`
	Timestamp         time.Time                      `json:"timestamp"`
	BestPolicy        string                         `json:"best_policy"`
	Dimension         string                         `json:"dimension"`
	ImprovementPct    float64                        `json:"improvement_pct"`
	TrainScore        float64                        `json:"train_score"`
	TestScore         float64                        `json:"test_score"`
	GeneralizationGap float64                        `json:"generalization_gap"`
	Deployed          bool                           `json:"deployed"`
	Error             string                         `json:"error,omitempty"`
	Headroom          map[string]*HeadroomAssessment `json:"headroom,omitempty"`

	// ImprovementPct above is measured on held-out scenarios (candidate
	// mean score vs baseline mean score). TrainImprovementPct is the
	// in-sample improvement found during exploration.
	TrainImprovementPct float64 `json:"train_improvement_pct"`
	BaselineScore       float64 `json:"baseline_score"`
	CandidateScore      float64 `json:"candidate_score"`
	HoldoutSamples      int     `json:"holdout_samples"`
	// PValue is the one-sided paired t-test p-value (candidate > baseline).
	PValue      float64 `json:"p_value"`
	Significant bool    `json:"significant"`
	// DeployDecision is one of the Decision* constants; Reason explains it.
	DeployDecision string `json:"deploy_decision,omitempty"`
	Reason         string `json:"reason,omitempty"`
	// WouldDeploy is true when the candidate passed every gate but was not
	// deployed (dry run or no deploy hook).
	WouldDeploy   bool                   `json:"would_deploy,omitempty"`
	DryRun        bool                   `json:"dry_run,omitempty"`
	RolledBack    bool                   `json:"rolled_back,omitempty"`
	RollbackError string                 `json:"rollback_error,omitempty"`
	PolicyParams  map[string]interface{} `json:"policy_params,omitempty"`
	DurationMs    float64                `json:"duration_ms"`
}

// clone returns a copy whose maps are not shared with the receiver.
func (c RSICycle) clone() RSICycle {
	out := c
	if c.Headroom != nil {
		out.Headroom = make(map[string]*HeadroomAssessment, len(c.Headroom))
		for k, v := range c.Headroom {
			out.Headroom[k] = v
		}
	}
	if c.PolicyParams != nil {
		out.PolicyParams = make(map[string]interface{}, len(c.PolicyParams))
		for k, v := range c.PolicyParams {
			out.PolicyParams[k] = v
		}
	}
	return out
}

// RSIStats are cumulative counters and gauges for monitoring (e.g. export
// as Prometheus metrics). DryRunCandidates counts candidates that passed
// every gate but were not deployed (dry run, or no deploy hook configured).
// CandidatesRejected counts candidates stopped by a gate (too few samples,
// below threshold, not significant). LastImprovementPct is the held-out
// improvement of the most recent cycle that evaluated a candidate.
type RSIStats struct {
	CyclesRun          uint64    `json:"cycles_run"`
	CyclesFailed       uint64    `json:"cycles_failed"`
	CyclesSkipped      uint64    `json:"cycles_skipped"`
	CyclesRejected     uint64    `json:"cycles_rejected_in_progress"`
	CandidatesRejected uint64    `json:"candidates_rejected"`
	DryRunCandidates   uint64    `json:"dry_run_candidates"`
	Deployments        uint64    `json:"deployments"`
	DeployFailures     uint64    `json:"deploy_failures"`
	Rollbacks          uint64    `json:"rollbacks"`
	RollbackFailures   uint64    `json:"rollback_failures"`
	CycleInProgress    bool      `json:"cycle_in_progress"`
	LastCycleID        int       `json:"last_cycle_id"`
	LastCycleAt        time.Time `json:"last_cycle_at"`
	LastCycleDuration  float64   `json:"last_cycle_duration_ms"`
	LastImprovementPct float64   `json:"last_improvement_pct"`
	LastDeployedAt     time.Time `json:"last_deployed_at"`
	DeployedPolicy     string    `json:"deployed_policy,omitempty"`
	HistorySize        int       `json:"history_size"`
	// Persistent reports whether EnablePersistence attached a store.
	Persistent bool `json:"persistent,omitempty"`
	// PersistErrors counts failed write-throughs (store errors and deployed
	// policies that have no PolicyCodec) since the process started.
	PersistErrors uint64 `json:"persist_errors,omitempty"`
}

// deploymentRecord tracks one successful deployment for rollback.
type deploymentRecord struct {
	dimension HeadroomDimension
	policy    Policy
	previous  Policy
	cycleID   int
}

// RSIOrchestrator coordinates the full RSI lifecycle:
// assess -> explore -> evaluate -> deploy.
type RSIOrchestrator struct {
	sim      *DreamSimulator
	hci      *HCIEngine
	mod      *ModularEvaluator
	explorer *AutonomousExplorer

	// cycleMu is held for the whole duration of a cycle or rollback.
	cycleMu sync.Mutex
	running atomic.Bool
	wake    chan struct{}

	mu            sync.RWMutex
	config        RSIConfig
	cycles        []RSICycle
	cycleID       int
	deployed      Policy
	deployedByDim map[HeadroomDimension]Policy
	deployments   []deploymentRecord
	stats         RSIStats
	// codecs are the custom PolicyCodecs registered through
	// RegisterPolicyCodec (built-in policy types need none).
	codecs []PolicyCodec

	// Opt-in persistence (see EnablePersistence). persistMu serialises store
	// writes and guards ps; it is never acquired while holding mu.
	persistMu      sync.Mutex
	ps             persist.Store
	persistOn      atomic.Bool
	persistErrs    atomic.Uint64
	lastPersistErr atomic.Pointer[persistError]

	// OnDeploy is invoked when a cycle produces a policy that passes the
	// deploy gate (see package safety model). Set it before calling Run, or
	// use SetHooks for race-free updates at runtime.
	OnDeploy DeployFunc

	// OnRollback is invoked to restore the previous policy when OnDeploy
	// fails, and by Rollback.
	OnRollback RollbackFunc

	// OnCycle, when set, receives a copy of every recorded cycle — use it to
	// persist history or emit logs/metrics. It must not block for long.
	OnCycle func(RSICycle)
}

// NewRSIOrchestrator creates a fully wired orchestrator from the existing
// internal components. Out-of-range config values that would break the
// loop are replaced by defaults (see RSIConfig); use SetConfig for validated
// updates.
func NewRSIOrchestrator(
	ledgerStore LedgerReader,
	metrics MetricsProvider,
	costCalc CostCalculator,
	providerLister ProviderLister,
	config RSIConfig,
) *RSIOrchestrator {
	sim := NewDreamSimulator(ledgerStore, costCalc, metrics)
	if providerLister != nil {
		sim.SetProviderFilter(providerLister.ProviderNames)
	}
	hci := NewHCIEngine(ledgerStore, metrics, costCalc, providerLister, DefaultHCIConfig())
	mod := NewModularEvaluator(sim)
	explorer := NewAutonomousExplorer(sim, hci)

	return &RSIOrchestrator{
		sim:           sim,
		hci:           hci,
		mod:           mod,
		explorer:      explorer,
		config:        config.sanitized(),
		wake:          make(chan struct{}, 1),
		deployedByDim: make(map[HeadroomDimension]Policy),
	}
}

// SetHooks atomically replaces the deploy, rollback and cycle hooks.
func (o *RSIOrchestrator) SetHooks(deploy DeployFunc, rollback RollbackFunc, onCycle func(RSICycle)) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.OnDeploy = deploy
	o.OnRollback = rollback
	o.OnCycle = onCycle
	o.mu.Unlock()
}

// SetBasePolicyFunc overrides the starting policy explored for a dimension
// that has no RSI-deployed policy yet (nil restores built-in defaults). Use
// it to seed exploration from the gateway's live configuration.
func (o *RSIOrchestrator) SetBasePolicyFunc(fn func(HeadroomDimension) Policy) {
	if o == nil {
		return
	}
	o.explorer.SetBasePolicyFunc(fn)
}

// Run starts the continuous RSI cycle loop. It blocks until the context is
// canceled. Config changes (SetConfig) take effect immediately, including a
// new CycleInterval. Calling Run while another Run is active returns at once.
func (o *RSIOrchestrator) Run(ctx context.Context) {
	if o == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !o.running.CompareAndSwap(false, true) {
		return
	}
	defer o.running.Store(false)

	timer := time.NewTimer(o.cycleInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-o.wake:
			// Go >= 1.23 timers: Reset needs no drain.
			timer.Reset(o.cycleInterval())
		case <-timer.C:
			// Failures are recorded in history and stats; a cycle already
			// in flight (manual trigger) simply means this tick is skipped.
			_, _ = o.RunCycle(ctx)
			if ctx.Err() != nil {
				return
			}
			timer.Reset(o.cycleInterval())
		}
	}
}

func (o *RSIOrchestrator) cycleInterval() time.Duration {
	o.mu.RLock()
	d := o.config.CycleInterval
	o.mu.RUnlock()
	if d <= 0 {
		return DefaultRSIConfig().CycleInterval
	}
	return d
}

// cycleHooks is a snapshot of the hooks taken at the start of a cycle.
type cycleHooks struct {
	deploy   DeployFunc
	rollback RollbackFunc
	onCycle  func(RSICycle)
}

// RunCycle executes one complete RSI cycle and records the result. It
// returns ErrCycleInProgress (and a nil cycle) if another cycle or rollback
// is running. On failure the recorded cycle is returned together with the
// error.
func (o *RSIOrchestrator) RunCycle(ctx context.Context) (*RSICycle, error) {
	if o == nil {
		return nil, ErrNilOrchestrator
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !o.cycleMu.TryLock() {
		o.mu.Lock()
		o.stats.CyclesRejected++
		o.mu.Unlock()
		return nil, ErrCycleInProgress
	}
	defer o.cycleMu.Unlock()

	o.mu.Lock()
	cfg := o.config
	hooks := cycleHooks{deploy: o.OnDeploy, rollback: o.OnRollback, onCycle: o.OnCycle}
	o.cycleID++
	cycle := RSICycle{ID: o.cycleID, Timestamp: time.Now().UTC(), DryRun: cfg.DryRun}
	o.stats.CycleInProgress = true
	o.mu.Unlock()

	if cfg.CycleTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.CycleTimeout)
		defer cancel()
	}

	start := time.Now()
	err := o.runCycleSafely(ctx, cfg, hooks, &cycle)
	cycle.DurationMs = float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		if cycle.Error == "" {
			cycle.Error = err.Error()
		}
		if cycle.DeployDecision == "" {
			cycle.DeployDecision = DecisionError
		}
	}

	removed := o.recordCycle(cycle, cfg, err)
	o.persistCycle(cycle, removed)
	if hooks.onCycle != nil {
		func() {
			defer func() { _ = recover() }()
			hooks.onCycle(cycle.clone())
		}()
	}
	out := cycle.clone()
	return &out, err
}

// runCycleSafely converts a panic anywhere in the cycle (e.g. inside a
// Policy implementation) into an error so the background loop survives.
func (o *RSIOrchestrator) runCycleSafely(ctx context.Context, cfg RSIConfig, hooks cycleHooks, c *RSICycle) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("rsi: cycle panicked: %v", r)
		}
	}()
	return o.runCycle(ctx, cfg, hooks, c)
}

func (o *RSIOrchestrator) runCycle(ctx context.Context, cfg RSIConfig, hooks cycleHooks, c *RSICycle) error {
	// Step 1: Load fresh scenarios from the ledger and invalidate the HCI
	// cache so headroom reflects traffic recorded since the last cycle.
	o.hci.Refresh()
	if err := o.sim.LoadFromLedger(ctx, TimeRange{}, cfg.SampleSize); err != nil {
		if errors.Is(err, ErrNoScenarios) {
			c.DeployDecision = DecisionInsufficientData
			c.Reason = err.Error()
			return nil
		}
		return fmt.Errorf("loading scenarios: %w", err)
	}

	// Step 2: Assess headroom across all dimensions.
	assessments, err := o.hci.AssessAll(ctx)
	if err != nil {
		return fmt.Errorf("HCI assessment: %w", err)
	}
	c.Headroom = make(map[string]*HeadroomAssessment, len(assessments))
	for dim, a := range assessments {
		c.Headroom[string(dim)] = a
	}

	// Step 3: Find the dimension with the highest actionable headroom.
	bestDim := o.hci.Prioritize()
	c.Dimension = string(bestDim)
	bestAssessment := assessments[bestDim]
	if bestAssessment == nil || bestAssessment.Confidence <= 0 {
		c.DeployDecision = DecisionInsufficientData
		c.Reason = "no dimension has enough data for a confident headroom assessment"
		return nil
	}
	if bestAssessment.HeadroomPct < cfg.HeadroomThreshold*100 {
		c.DeployDecision = DecisionLowHeadroom
		c.Reason = fmt.Sprintf("headroom %.2f%% is below the %.2f%% threshold",
			bestAssessment.HeadroomPct, cfg.HeadroomThreshold*100)
		return nil
	}

	// Step 4: Split scenarios. Exploration only sees the training split;
	// the deploy decision is made on the held-out split.
	scenarios := o.sim.Scenarios()
	partitions := o.mod.CreatePartitions(scenarios, cfg.KFold)
	if len(partitions) == 0 || partitions[0] == nil {
		c.DeployDecision = DecisionInsufficientData
		c.Reason = "no scenarios to partition"
		return nil
	}
	train := partitions[0].TrainScenarios
	holdout := partitions[0].TestScenarios
	if len(train) == 0 {
		c.DeployDecision = DecisionInsufficientData
		c.Reason = "not enough scenarios for a train/holdout split"
		return nil
	}

	// Step 5: Broad-then-deep exploration from the current policy for the
	// dimension (the last RSI deployment, else the base policy).
	bestPolicy, result, err := o.explorer.ExploreWithOptions(ctx, bestDim, ExploreOptions{
		BroadIterations: cfg.BroadIterations,
		DeepIterations:  cfg.DeepIterations,
		Scenarios:       train,
		BasePolicy:      o.currentPolicyClone(bestDim),
	})
	if err != nil {
		return fmt.Errorf("exploration: %w", err)
	}
	c.BestPolicy = policyName(bestPolicy)
	c.PolicyParams = describePolicy(bestPolicy)
	c.TrainImprovementPct = result.ImprovementPct

	// Step 6: Disjoint k-fold evaluation to report generalization.
	disjoint, err := o.mod.EvaluateDisjoint(ctx, bestPolicy, partitions)
	if err != nil {
		return fmt.Errorf("disjoint evaluation: %w", err)
	}
	c.TrainScore = disjoint.TrainScore
	c.TestScore = disjoint.TestScore
	c.GeneralizationGap = disjoint.GeneralizationGap

	// Step 7: Paired comparison against the baseline on held-out data.
	cmp, n, err := o.compareOnHoldout(ctx, result.BasePolicy, bestPolicy, holdout)
	if err != nil {
		return fmt.Errorf("holdout comparison: %w", err)
	}
	c.HoldoutSamples = n
	c.PValue = 1
	if n > 0 {
		c.BaselineScore = cmp.BaselineMean
		c.CandidateScore = cmp.CandidateMean
		c.ImprovementPct = cmp.ImprovementPct
		c.PValue = cmp.PValue
	}
	c.Significant = n >= 2 && c.PValue <= cfg.SignificanceLevel

	// Step 8: Gate and deploy.
	o.decideAndDeploy(ctx, cfg, hooks, c, bestDim, bestPolicy)
	return nil
}

// compareOnHoldout replays both policies on the held-out scenarios and runs
// a paired comparison of the per-scenario scores. n is the number of pairs.
func (o *RSIOrchestrator) compareOnHoldout(ctx context.Context, base, cand Policy, holdout []*DreamReplay) (Comparison, int, error) {
	if len(holdout) == 0 || base == nil || cand == nil {
		return Comparison{}, 0, nil
	}
	_, baseOut, err := o.sim.ReplayDetailed(ctx, base, holdout)
	if err != nil {
		return Comparison{}, 0, err
	}
	_, candOut, err := o.sim.ReplayDetailed(ctx, cand, holdout)
	if err != nil {
		return Comparison{}, 0, err
	}
	if len(baseOut) != len(candOut) {
		return Comparison{}, 0, fmt.Errorf("rsi: outcome count mismatch (%d vs %d)", len(baseOut), len(candOut))
	}
	baseScores := make([]float64, 0, len(baseOut))
	candScores := make([]float64, 0, len(candOut))
	for i := range baseOut {
		if baseOut[i].Skipped || candOut[i].Skipped {
			continue
		}
		baseScores = append(baseScores, baseOut[i].Score)
		candScores = append(candScores, candOut[i].Score)
	}
	if len(baseScores) == 0 {
		return Comparison{}, 0, nil
	}
	cmp, err := ComparePaired(baseScores, candScores)
	if err != nil {
		return Comparison{}, 0, err
	}
	return cmp, cmp.Samples, nil
}

// decideAndDeploy applies the deploy gate and, if it passes, calls the
// deploy hook, rolling back on failure.
func (o *RSIOrchestrator) decideAndDeploy(ctx context.Context, cfg RSIConfig, hooks cycleHooks, c *RSICycle, dim HeadroomDimension, policy Policy) {
	switch {
	case c.HoldoutSamples < cfg.MinSamples:
		c.DeployDecision = DecisionInsufficientSamples
		c.Reason = fmt.Sprintf("%d held-out samples < min_samples %d", c.HoldoutSamples, cfg.MinSamples)
		return
	case !(c.ImprovementPct > 0) || c.ImprovementPct < cfg.ImprovementThresholdPct:
		c.DeployDecision = DecisionBelowThreshold
		c.Reason = fmt.Sprintf("held-out improvement %.4f%% does not exceed the %.4f%% threshold",
			c.ImprovementPct, cfg.ImprovementThresholdPct)
		return
	case !c.Significant:
		c.DeployDecision = DecisionNotSignificant
		c.Reason = fmt.Sprintf("p-value %.4g > significance level %.4g", c.PValue, cfg.SignificanceLevel)
		return
	case cfg.DryRun:
		c.DeployDecision = DecisionDryRun
		c.WouldDeploy = true
		c.Reason = "dry run: candidate passed every gate but was not deployed"
		return
	case hooks.deploy == nil:
		c.DeployDecision = DecisionNoDeployHook
		c.WouldDeploy = true
		c.Reason = "no deploy hook configured"
		return
	}

	previous := o.currentPolicy(dim)
	if err := callDeploy(ctx, hooks.deploy, policy, *c); err != nil {
		c.DeployDecision = DecisionDeployFailed
		c.Error = "deploy failed: " + err.Error()
		c.Reason = "deploy hook returned an error"
		if hooks.rollback != nil {
			// The hook may have partially applied the policy; restore the
			// previous state.
			if rbErr := callRollback(ctx, hooks.rollback, previous, *c); rbErr != nil {
				c.RollbackError = rbErr.Error()
			} else {
				c.RolledBack = true
			}
		}
		return
	}

	c.Deployed = true
	c.DeployDecision = DecisionDeployed
	c.Reason = "candidate beat baseline on held-out data"
	o.mu.Lock()
	o.deployed = policy
	o.deployedByDim[dim] = policy
	o.deployments = append(o.deployments, deploymentRecord{dimension: dim, policy: policy, previous: previous, cycleID: c.ID})
	if len(o.deployments) > maxDeploymentStack {
		o.deployments = o.deployments[len(o.deployments)-maxDeploymentStack:]
	}
	o.mu.Unlock()
	// Write the deployment through immediately so it survives a crash before
	// the cycle is recorded.
	o.persistState()
}

func callDeploy(ctx context.Context, fn DeployFunc, p Policy, c RSICycle) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("deploy hook panicked: %v", r)
		}
	}()
	return fn(ctx, p, c)
}

func callRollback(ctx context.Context, fn RollbackFunc, restore Policy, c RSICycle) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("rollback hook panicked: %v", r)
		}
	}()
	return fn(ctx, restore, c)
}

// currentPolicy returns the policy RSI last deployed for dim, or nil.
func (o *RSIOrchestrator) currentPolicy(dim HeadroomDimension) Policy {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.deployedByDim[dim]
}

// currentPolicyClone returns a clone of the deployed policy for dim (so
// exploration can never mutate the live object), or nil.
func (o *RSIOrchestrator) currentPolicyClone(dim HeadroomDimension) Policy {
	if p := o.currentPolicy(dim); p != nil {
		return p.Clone()
	}
	return nil
}

// Rollback reverts the most recent successful deployment by calling
// OnRollback with the policy that was active before it. It fails with
// ErrCycleInProgress while a cycle runs, ErrNothingToRollback when there is
// no deployment left, and ErrNoRollbackHook when OnRollback is unset.
func (o *RSIOrchestrator) Rollback(ctx context.Context) error {
	if o == nil {
		return ErrNilOrchestrator
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !o.cycleMu.TryLock() {
		return ErrCycleInProgress
	}
	defer o.cycleMu.Unlock()

	o.mu.RLock()
	hook := o.OnRollback
	if len(o.deployments) == 0 {
		o.mu.RUnlock()
		return ErrNothingToRollback
	}
	last := o.deployments[len(o.deployments)-1]
	cycle := RSICycle{ID: last.cycleID, Dimension: string(last.dimension), BestPolicy: policyName(last.policy)}
	for i := range o.cycles {
		if o.cycles[i].ID == last.cycleID {
			cycle = o.cycles[i].clone()
			break
		}
	}
	o.mu.RUnlock()

	if hook == nil {
		return ErrNoRollbackHook
	}
	if err := callRollback(ctx, hook, last.previous, cycle); err != nil {
		o.mu.Lock()
		o.stats.RollbackFailures++
		o.mu.Unlock()
		o.persistState()
		return fmt.Errorf("rsi: rollback of cycle %d failed: %w", last.cycleID, err)
	}

	o.mu.Lock()
	o.deployments = o.deployments[:len(o.deployments)-1]
	if last.previous != nil {
		o.deployedByDim[last.dimension] = last.previous
	} else {
		delete(o.deployedByDim, last.dimension)
	}
	o.deployed = nil
	if n := len(o.deployments); n > 0 {
		o.deployed = o.deployments[n-1].policy
	}
	for i := range o.cycles {
		if o.cycles[i].ID == last.cycleID {
			o.cycles[i].RolledBack = true
		}
	}
	o.stats.Rollbacks++
	o.mu.Unlock()
	o.persistRollback(last.cycleID)
	return nil
}

// Headroom returns the current HCI assessment across all dimensions.
func (o *RSIOrchestrator) Headroom(ctx context.Context) (map[HeadroomDimension]*HeadroomAssessment, error) {
	if o == nil {
		return nil, ErrNilOrchestrator
	}
	return o.hci.AssessAll(ctx)
}

// Cycles returns a copy of the cycle history (oldest first, at most
// MaxHistory entries).
func (o *RSIOrchestrator) Cycles() []RSICycle {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]RSICycle, len(o.cycles))
	for i := range o.cycles {
		out[i] = o.cycles[i].clone()
	}
	return out
}

// CurrentCycle returns the most recent cycle, or nil if none exist.
func (o *RSIOrchestrator) CurrentCycle() *RSICycle {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if len(o.cycles) == 0 {
		return nil
	}
	c := o.cycles[len(o.cycles)-1].clone()
	return &c
}

// CycleInProgress reports whether a cycle or rollback is currently running.
func (o *RSIOrchestrator) CycleInProgress() bool {
	if o == nil {
		return false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.stats.CycleInProgress
}

// DeployedPolicy returns the last deployed policy, or nil.
func (o *RSIOrchestrator) DeployedPolicy() Policy {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.deployed
}

// Stats returns a snapshot of the orchestrator's counters and gauges.
func (o *RSIOrchestrator) Stats() RSIStats {
	if o == nil {
		return RSIStats{}
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	s := o.stats
	s.HistorySize = len(o.cycles)
	if o.deployed != nil {
		s.DeployedPolicy = policyName(o.deployed)
	}
	s.Persistent = o.persistOn.Load()
	s.PersistErrors = o.persistErrs.Load()
	return s
}

// Config returns a copy of the current RSI configuration.
func (o *RSIOrchestrator) Config() RSIConfig {
	if o == nil {
		return RSIConfig{}
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.config
}

// SetConfig validates and applies a new configuration at runtime. Zero
// values of defaultable fields are filled with defaults first. On error the
// current configuration is left unchanged. A running Run loop picks up a new
// CycleInterval immediately; the change applies to the next cycle, never to
// one already in flight.
func (o *RSIOrchestrator) SetConfig(cfg RSIConfig) error {
	if o == nil {
		return ErrNilOrchestrator
	}
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	o.mu.Lock()
	o.config = cfg
	removed := o.trimHistoryLocked(cfg.MaxHistory)
	o.mu.Unlock()
	o.persistConfig(removed)
	select {
	case o.wake <- struct{}{}:
	default:
	}
	return nil
}

// RestoreHistory loads previously persisted cycles (e.g. captured through
// OnCycle) into the in-memory history, keeping the most recent MaxHistory
// and advancing the cycle ID counter past them. Deployed policies are not
// restored by this method; use EnablePersistence for full durable state
// (history, deployed policies and the rollback stack). When persistence is
// enabled the restored cycles are written through to the store.
func (o *RSIOrchestrator) RestoreHistory(cycles []RSICycle) {
	if o == nil || len(cycles) == 0 {
		return
	}
	restored := make([]RSICycle, 0, len(cycles))
	for _, c := range cycles {
		if c.ID <= 0 {
			continue
		}
		restored = append(restored, c.clone())
	}
	sort.SliceStable(restored, func(i, j int) bool { return restored[i].ID < restored[j].ID })

	o.mu.Lock()
	merged := append(restored, o.cycles...)
	o.cycles = merged
	for _, c := range merged {
		if c.ID > o.cycleID {
			o.cycleID = c.ID
		}
	}
	removed := o.trimHistoryLocked(o.config.MaxHistory)
	o.mu.Unlock()
	o.persistCycles(restored, removed)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// recordCycle appends the cycle to the bounded history and updates stats.
// It returns the IDs of cycles trimmed from the history.
func (o *RSIOrchestrator) recordCycle(c RSICycle, cfg RSIConfig, err error) []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cycles = append(o.cycles, c.clone())
	removed := o.trimHistoryLocked(cfg.MaxHistory)

	s := &o.stats
	s.CycleInProgress = false
	s.CyclesRun++
	s.LastCycleID = c.ID
	s.LastCycleAt = c.Timestamp
	s.LastCycleDuration = c.DurationMs
	if err != nil {
		s.CyclesFailed++
		return removed
	}
	if c.HoldoutSamples > 0 {
		s.LastImprovementPct = c.ImprovementPct
	}
	switch c.DeployDecision {
	case DecisionInsufficientData, DecisionLowHeadroom:
		s.CyclesSkipped++
	case DecisionInsufficientSamples, DecisionBelowThreshold, DecisionNotSignificant:
		s.CandidatesRejected++
	case DecisionDryRun, DecisionNoDeployHook:
		s.DryRunCandidates++
	case DecisionDeployed:
		s.Deployments++
		s.LastDeployedAt = c.Timestamp
	case DecisionDeployFailed:
		s.DeployFailures++
		if c.RolledBack {
			s.Rollbacks++
		} else if c.RollbackError != "" {
			s.RollbackFailures++
		}
	}
	return removed
}

// trimHistoryLocked keeps the most recent maxHistory cycles and returns the
// IDs of the cycles it dropped.
func (o *RSIOrchestrator) trimHistoryLocked(maxHistory int) []int {
	if maxHistory <= 0 {
		maxHistory = DefaultRSIConfig().MaxHistory
	}
	if len(o.cycles) <= maxHistory {
		return nil
	}
	drop := len(o.cycles) - maxHistory
	removed := make([]int, 0, drop)
	for _, c := range o.cycles[:drop] {
		removed = append(removed, c.ID)
	}
	trimmed := make([]RSICycle, maxHistory)
	copy(trimmed, o.cycles[drop:])
	o.cycles = trimmed
	return removed
}

// policyName returns a human-readable name for a Policy implementation.
func policyName(p Policy) string {
	if p == nil {
		return "nil"
	}
	switch p.(type) {
	case *RoutingPolicy:
		return "RoutingPolicy"
	case *CachePolicy:
		return "CachePolicy"
	case *GuardrailPolicy:
		return "GuardrailPolicy"
	case *AgentWorkflowPolicy:
		return "AgentWorkflowPolicy"
	default:
		return fmt.Sprintf("%T", p)
	}
}
