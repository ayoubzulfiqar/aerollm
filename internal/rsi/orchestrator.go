package rsi

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// RSI Orchestrator: full lifecycle coordination
// ---------------------------------------------------------------------------

// RSIConfig configures the RSI orchestrator's behavior.
type RSIConfig struct {
	ImprovementThresholdPct float64       `json:"improvement_threshold_pct"`
	BroadIterations         int           `json:"broad_iterations"`
	DeepIterations          int           `json:"deep_iterations"`
	CycleInterval           time.Duration `json:"cycle_interval"`
	KFold                   int           `json:"k_fold"`
	HeadroomThreshold       float64       `json:"headroom_threshold"`
	SampleSize              int           `json:"sample_size"`
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
	}
}

// DeployFunc is called when a policy improvement exceeds the threshold.
// The server wires this to apply the policy via the aiops MetaAgentTuner.
type DeployFunc func(ctx context.Context, policy Policy, cycle RSICycle) error

// RSICycle records the outcome of a single RSI cycle.
type RSICycle struct {
	ID         int                             `json:"id"`
	Timestamp  time.Time                       `json:"timestamp"`
	BestPolicy string                           `json:"best_policy"`
	Dimension  string                           `json:"dimension"`
	ImprovementPct          float64 `json:"improvement_pct"`
	TrainScore              float64 `json:"train_score"`
	TestScore               float64 `json:"test_score"`
	GeneralizationGap       float64 `json:"generalization_gap"`
	Deployed                bool    `json:"deployed"`
	Error                   string  `json:"error,omitempty"`
	Headroom                map[string]*HeadroomAssessment `json:"headroom,omitempty"`
}

// RSIOrchestrator coordinates the full RSI lifecycle:
// assess -> explore -> evaluate -> deploy.
type RSIOrchestrator struct {
	sim      *DreamSimulator
	hci      *HCIEngine
	mod      *ModularEvaluator
	explorer *AutonomousExplorer

	config RSIConfig

	mu        sync.RWMutex
	cycles    []RSICycle
	cycleID   int
	deployed  Policy

	// OnDeploy is invoked when a cycle produces a policy that improves
	// over the base by more than ImprovementThresholdPct.
	OnDeploy DeployFunc
}

// NewRSIOrchestrator creates a fully wired orchestrator from the existing
// internal components.
func NewRSIOrchestrator(
	ledgerStore LedgerReader,
	metrics MetricsProvider,
	costCalc CostCalculator,
	providerLister ProviderLister,
	config RSIConfig,
) *RSIOrchestrator {
	sim := NewDreamSimulator(ledgerStore, costCalc, metrics)
	hci := NewHCIEngine(ledgerStore, metrics, costCalc, providerLister, DefaultHCIConfig())
	mod := NewModularEvaluator(sim)
	explorer := NewAutonomousExplorer(sim, hci)

	return &RSIOrchestrator{
		sim:      sim,
		hci:      hci,
		mod:      mod,
		explorer: explorer,
		config:   config,
	}
}

// Run starts the continuous RSI cycle loop. It blocks until the context
// is canceled.
func (o *RSIOrchestrator) Run(ctx context.Context) {
	ticker := time.NewTicker(o.config.CycleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := o.RunCycle(ctx); err != nil {
				// Log and continue — do not crash the loop on cycle failure.
				_ = err
			}
		}
	}
}

// RunCycle executes one complete RSI cycle and records the result.
func (o *RSIOrchestrator) RunCycle(ctx context.Context) (*RSICycle, error) {
	if o == nil {
		return nil, fmt.Errorf("orchestrator is nil")
	}

	cycle := RSICycle{
		ID:        o.nextCycleID(),
		Timestamp: time.Now(),
	}

	// Step 1: Load scenarios from the ledger.
	if err := o.sim.LoadFromLedger(ctx, TimeRange{}, o.config.SampleSize); err != nil {
		cycle.Error = err.Error()
		o.recordCycle(cycle)
		return &cycle, fmt.Errorf("loading scenarios: %w", err)
	}

	// Step 2: Assess headroom across all dimensions.
	assessments, err := o.hci.AssessAll(ctx)
	if err != nil {
		cycle.Error = err.Error()
		o.recordCycle(cycle)
		return &cycle, fmt.Errorf("HCI assessment: %w", err)
	}
	cycle.Headroom = make(map[string]*HeadroomAssessment, len(assessments))
	for dim, a := range assessments {
		cycle.Headroom[string(dim)] = a
	}

	// Step 3: Find the dimension with the highest actionable headroom.
	bestDim := o.hci.Prioritize()
	cycle.Dimension = string(bestDim)

	// Skip exploration if headroom is below threshold.
	bestAssessment := assessments[bestDim]
	if bestAssessment == nil || bestAssessment.HeadroomPct < o.config.HeadroomThreshold*100 {
		o.recordCycle(cycle)
		return &cycle, nil
	}

	// Step 4: Broad-then-deep exploration on the target dimension.
	scenarios := o.sim.Scenarios()

	bestPolicy, result, err := o.explorer.Explore(ctx, bestDim, o.config.BroadIterations)
	if err != nil {
		cycle.Error = err.Error()
		o.recordCycle(cycle)
		return &cycle, fmt.Errorf("exploration: %w", err)
	}

	// Step 5: Disjoint evaluation to check for overfitting.
	partitions := o.mod.CreatePartitions(scenarios, o.config.KFold)
	disjoint, err := o.mod.EvaluateDisjoint(ctx, bestPolicy, partitions)
	if err != nil {
		cycle.Error = err.Error()
		o.recordCycle(cycle)
		return &cycle, fmt.Errorf("disjoint evaluation: %w", err)
	}

	cycle.BestPolicy = policyName(bestPolicy)
	cycle.ImprovementPct = result.ImprovementPct
	cycle.TrainScore = disjoint.TrainScore
	cycle.TestScore = disjoint.TestScore
	cycle.GeneralizationGap = disjoint.GeneralizationGap

	// Step 6: Deploy if improvement exceeds threshold.
	if result.ImprovementPct >= o.config.ImprovementThresholdPct {
		if o.OnDeploy != nil {
			if deployErr := o.OnDeploy(ctx, bestPolicy, cycle); deployErr != nil {
				cycle.Error = deployErr.Error()
			} else {
				cycle.Deployed = true
				o.mu.Lock()
				o.deployed = bestPolicy
				o.mu.Unlock()
			}
		}
	}

	o.recordCycle(cycle)
	return &cycle, nil
}

// Headroom returns the current HCI assessment across all dimensions.
func (o *RSIOrchestrator) Headroom(ctx context.Context) (map[HeadroomDimension]*HeadroomAssessment, error) {
	if o == nil {
		return nil, fmt.Errorf("orchestrator is nil")
	}
	return o.hci.AssessAll(ctx)
}

// Cycles returns a copy of the cycle history.
func (o *RSIOrchestrator) Cycles() []RSICycle {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]RSICycle, len(o.cycles))
	copy(out, o.cycles)
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
	c := o.cycles[len(o.cycles)-1]
	return &c
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

// Config returns a copy of the current RSI configuration.
func (o *RSIOrchestrator) Config() RSIConfig {
	if o == nil {
		return RSIConfig{}
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.config
}

// SetConfig updates the orchestrator's configuration at runtime.
func (o *RSIOrchestrator) SetConfig(cfg RSIConfig) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.config = cfg
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (o *RSIOrchestrator) nextCycleID() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cycleID++
	return o.cycleID
}

func (o *RSIOrchestrator) recordCycle(c RSICycle) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cycles = append(o.cycles, c)
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
