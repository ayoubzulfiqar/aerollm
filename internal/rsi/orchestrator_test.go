package rsi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helpers for orchestrator tests
// ---------------------------------------------------------------------------

func newTestOrchestrator(records []ledger.LedgerRecord, config RSIConfig) *RSIOrchestrator {
	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.02}}
	metrics := &mockMetricsProvider{reqCount: int64(len(records)), errCount: 0, avgLat: 200}
	providers := &mockProviderLister{names: []string{"openai", "anthropic"}}
	return NewRSIOrchestrator(store, metrics, costCalc, providers, config)
}

func newTestOrchestratorNoData(config RSIConfig) *RSIOrchestrator {
	store := &mockLedgerReader{records: nil}
	costCalc := &mockCostCalculator{}
	metrics := &mockMetricsProvider{}
	providers := &mockProviderLister{names: []string{"openai"}}
	return NewRSIOrchestrator(store, metrics, costCalc, providers, config)
}

// improvableBase returns a deliberately poor (non-caching) base policy whose
// mutation (mockPolicy.Mutate turns caching on) is strictly better on every
// scenario, so a cycle has a genuine, significant improvement to deploy.
func improvableBase(HeadroomDimension) Policy {
	return &mockPolicy{provider: "openai", cached: false, latency: 100}
}

// deployableConfig is a config under which improvableBase's mutation passes
// every deploy gate with 20 test records (10 held-out samples).
func deployableConfig() RSIConfig {
	return RSIConfig{
		ImprovementThresholdPct: 0.0,
		BroadIterations:         3,
		DeepIterations:          1,
		KFold:                   2,
		HeadroomThreshold:       0.0,
		MinSamples:              5,
	}
}

// ---------------------------------------------------------------------------
// NewRSIOrchestrator Tests
// ---------------------------------------------------------------------------

func TestNewRSIOrchestrator(t *testing.T) {
	orch := newTestOrchestrator(makeLedgerRecords(5, "openai", "gpt-4o", time.Now()), DefaultRSIConfig())
	require.NotNil(t, orch)
	assert.NotNil(t, orch.sim)
	assert.NotNil(t, orch.hci)
	assert.NotNil(t, orch.mod)
	assert.NotNil(t, orch.explorer)
}

func TestNewRSIOrchestrator_NilDependencies(t *testing.T) {
	// Even with nil-ish dependencies (empty records), orchestrator should construct.
	orch := NewRSIOrchestrator(&mockLedgerReader{}, &mockMetricsProvider{}, &mockCostCalculator{}, &mockProviderLister{}, DefaultRSIConfig())
	require.NotNil(t, orch)
}

// ---------------------------------------------------------------------------
// RunCycle Tests
// ---------------------------------------------------------------------------

func TestRunCycle_CompletesSuccessfully(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 0.0, // always trigger deploy for test
		BroadIterations:         3,
		DeepIterations:          1,
		KFold:                   2,
		HeadroomThreshold:       0.0, // always explore
	})

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cycle)
	assert.Greater(t, cycle.ID, 0)
	assert.False(t, cycle.Timestamp.IsZero())
	assert.NotEmpty(t, cycle.Dimension)
	assert.GreaterOrEqual(t, cycle.ImprovementPct, 0.0)
	assert.NotEmpty(t, cycle.Headroom)
	assert.Empty(t, cycle.Error)
}

func TestRunCycle_NilOrchestrator(t *testing.T) {
	var orch *RSIOrchestrator
	cycle, err := orch.RunCycle(context.Background())
	require.Error(t, err)
	assert.Nil(t, cycle)
}

func TestRunCycle_EmptyLedger(t *testing.T) {
	orch := newTestOrchestratorNoData(RSIConfig{
		BroadIterations:   3,
		HeadroomThreshold: 0.0,
	})

	cycle, err := orch.RunCycle(context.Background())
	// Empty ledger → HCI assessment will have no data, but should still complete.
	// Either error or empty cycle is acceptable.
	if err != nil {
		assert.NotNil(t, cycle, "cycle should still be recorded on error")
		assert.NotEmpty(t, cycle.Error)
	} else {
		assert.NotNil(t, cycle)
	}
}

func TestRunCycle_DeployTriggered(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, deployableConfig())
	orch.SetBasePolicyFunc(improvableBase)

	var deployed bool
	orch.OnDeploy = func(_ context.Context, policy Policy, cycle RSICycle) error {
		deployed = true
		return nil
	}

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.True(t, deployed, "OnDeploy should be called when improvement exceeds threshold")
	assert.True(t, cycle.Deployed, "cycle.Deployed should be true")
	assert.Equal(t, DecisionDeployed, cycle.DeployDecision)
	assert.Greater(t, cycle.ImprovementPct, 0.0)
	assert.True(t, cycle.Significant)
	assert.Equal(t, 10, cycle.HoldoutSamples)
}

func TestRunCycle_DeployNotTriggered_BelowThreshold(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 1_000_000.0, // impossibly high → never deploy
		BroadIterations:         3,
		DeepIterations:          1,
		KFold:                   2,
		HeadroomThreshold:       0.0,
	})

	var deployed bool
	orch.OnDeploy = func(_ context.Context, policy Policy, cycle RSICycle) error {
		deployed = true
		return nil
	}

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.False(t, deployed, "OnDeploy should NOT be called when below threshold")
	assert.False(t, cycle.Deployed, "cycle.Deployed should be false")
}

func TestRunCycle_DeployError(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, deployableConfig())
	orch.SetBasePolicyFunc(improvableBase)

	orch.OnDeploy = func(_ context.Context, policy Policy, cycle RSICycle) error {
		return assert.AnError
	}

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err) // RunCycle itself succeeds; deploy error is recorded
	assert.False(t, cycle.Deployed)
	assert.NotEmpty(t, cycle.Error)
}

func TestRunCycle_DeployNilCallback(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 0.0,
		BroadIterations:         3,
		DeepIterations:          1,
		KFold:                   2,
		HeadroomThreshold:       0.0,
	})
	// OnDeploy is nil — should not crash.
	orch.OnDeploy = nil

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.False(t, cycle.Deployed, "should not deploy when OnDeploy is nil")
}

func TestRunCycle_HeadroomRecorded(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 1_000_000.0,
		BroadIterations:         3,
		HeadroomThreshold:       -1.0, // force exploration
	})

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cycle)
	assert.Len(t, cycle.Headroom, 6, "all 6 dimensions should be assessed")
	for dim, h := range cycle.Headroom {
		assert.NotEmpty(t, dim)
		assert.NotNil(t, h)
	}
}

func TestRunCycle_CycleIDIncrementing(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 1_000_000.0,
		BroadIterations:         3,
		HeadroomThreshold:       -1.0,
	})

	c1, _ := orch.RunCycle(context.Background())
	c2, _ := orch.RunCycle(context.Background())
	require.NotNil(t, c1)
	require.NotNil(t, c2)
	assert.Equal(t, c1.ID+1, c2.ID)
}

func TestRunCycle_ConsecutiveCyclesStored(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 1_000_000.0,
		BroadIterations:         3,
		HeadroomThreshold:       -1.0,
	})

	for i := 0; i < 3; i++ {
		_, err := orch.RunCycle(context.Background())
		require.NoError(t, err)
	}

	assert.Len(t, orch.Cycles(), 3)
}

// ---------------------------------------------------------------------------
// Headroom Tests
// ---------------------------------------------------------------------------

func TestHeadroom_Success(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, DefaultRSIConfig())

	assessments, err := orch.Headroom(context.Background())
	require.NoError(t, err)
	assert.Len(t, assessments, 6)
}

func TestHeadroom_NilOrchestrator(t *testing.T) {
	var orch *RSIOrchestrator
	assessments, err := orch.Headroom(context.Background())
	require.Error(t, err)
	assert.Nil(t, assessments)
}

// ---------------------------------------------------------------------------
// Cycles / CurrentCycle Tests
// ---------------------------------------------------------------------------

func TestCycles_Empty(t *testing.T) {
	orch := newTestOrchestratorNoData(DefaultRSIConfig())
	assert.Empty(t, orch.Cycles())
}

func TestCycles_ReturnsCopy(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 1_000_000.0,
		BroadIterations:         3,
		HeadroomThreshold:       -1.0,
	})

	_, err := orch.RunCycle(context.Background())
	require.NoError(t, err)

	cycles := orch.Cycles()
	require.Len(t, cycles, 1)

	// Mutate returned copy — internal state should be unaffected.
	cycles[0].ID = 999
	again := orch.Cycles()
	require.Len(t, again, 1)
	assert.Equal(t, 1, again[0].ID, "internal state should be unaffected by external mutation")
}

func TestCurrentCycle_Empty(t *testing.T) {
	orch := newTestOrchestratorNoData(DefaultRSIConfig())
	assert.Nil(t, orch.CurrentCycle())
}

func TestCurrentCycle_ReturnsLatest(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 1_000_000.0,
		BroadIterations:         3,
		HeadroomThreshold:       -1.0,
	})

	orch.RunCycle(context.Background())
	orch.RunCycle(context.Background())

	cycle := orch.CurrentCycle()
	require.NotNil(t, cycle)
	assert.Equal(t, 2, cycle.ID)
}

// ---------------------------------------------------------------------------
// DeployedPolicy Tests
// ---------------------------------------------------------------------------

func TestDeployedPolicy_NilInitially(t *testing.T) {
	orch := newTestOrchestratorNoData(DefaultRSIConfig())
	assert.Nil(t, orch.DeployedPolicy())
}

func TestDeployedPolicy_SetAfterDeploy(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, deployableConfig())
	orch.SetBasePolicyFunc(improvableBase)

	orch.OnDeploy = func(_ context.Context, policy Policy, _ RSICycle) error { return nil }
	orch.RunCycle(context.Background())
	assert.NotNil(t, orch.DeployedPolicy())
}

// ---------------------------------------------------------------------------
// RSICycle JSON Serialization
// ---------------------------------------------------------------------------

func TestRSICycle_JSONRoundTrip(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 1_000_000.0,
		BroadIterations:         3,
		HeadroomThreshold:       -1.0,
	})

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cycle)

	data, err := json.Marshal(cycle)
	require.NoError(t, err)

	var restored RSICycle
	require.NoError(t, json.Unmarshal(data, &restored))
	assert.Equal(t, cycle.ID, restored.ID)
	assert.Equal(t, cycle.Dimension, restored.Dimension)
	assert.InDelta(t, cycle.ImprovementPct, restored.ImprovementPct, 0.01)
	assert.InDelta(t, cycle.TrainScore, restored.TrainScore, 0.01)
	assert.Equal(t, cycle.Deployed, restored.Deployed)
}

// ---------------------------------------------------------------------------
// DefaultRSIConfig Tests
// ---------------------------------------------------------------------------

func TestDefaultRSIConfig(t *testing.T) {
	cfg := DefaultRSIConfig()
	assert.Equal(t, 5.0, cfg.ImprovementThresholdPct)
	assert.Equal(t, 10, cfg.BroadIterations)
	assert.Equal(t, 5, cfg.DeepIterations)
	assert.Equal(t, 3, cfg.KFold)
	assert.Equal(t, 0.1, cfg.HeadroomThreshold)
	assert.Equal(t, 5*time.Minute, cfg.CycleInterval)
}

// ---------------------------------------------------------------------------
// Run (background ticker) Tests
// ---------------------------------------------------------------------------

func TestRun_StopsOnContextCancel(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, RSIConfig{
		ImprovementThresholdPct: 1_000_000.0,
		BroadIterations:         3,
		DeepIterations:          1,
		KFold:                   2,
		HeadroomThreshold:       -1.0,
		CycleInterval:           10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		orch.Run(ctx)
		close(done)
	}()

	// Let it run for a short time.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	// Verify cycles were recorded.
	assert.Greater(t, len(orch.Cycles()), 0)
}

// ---------------------------------------------------------------------------
// policyName Tests
// ---------------------------------------------------------------------------

func TestPolicyName(t *testing.T) {
	assert.Equal(t, "nil", policyName(nil))
	assert.Equal(t, "RoutingPolicy", policyName(&RoutingPolicy{}))
	assert.Equal(t, "CachePolicy", policyName(&CachePolicy{}))
	assert.Equal(t, "GuardrailPolicy", policyName(&GuardrailPolicy{}))
	assert.Equal(t, "AgentWorkflowPolicy", policyName(&AgentWorkflowPolicy{}))
}

func TestPolicyName_UnknownType(t *testing.T) {
	// Use a mock policy to test unknown type name.
	p := &mockPolicy{provider: "test"}
	name := policyName(p)
	assert.Equal(t, "*rsi.mockPolicy", name)
}

// ---------------------------------------------------------------------------
// Integration: Full Lifecycle
// ---------------------------------------------------------------------------

func TestRSIOrchestrator_Integration_FullLifecycle(t *testing.T) {
	now := time.Now()
	// Create scenarios with duplicates so caching policy can show improvement.
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	// Add duplicates to create caching opportunity.
	dupes := makeLedgerRecords(20, "openai", "gpt-4o", now)
	allRecords := append(records, dupes...)

	orch := newTestOrchestrator(allRecords, RSIConfig{
		ImprovementThresholdPct: 0.0,
		BroadIterations:         5,
		DeepIterations:          2,
		KFold:                   3,
		HeadroomThreshold:       -1.0,
	})

	deployedPolicy := make(chan Policy, 1)
	orch.OnDeploy = func(_ context.Context, policy Policy, cycle RSICycle) error {
		deployedPolicy <- policy
		return nil
	}

	// Run one full cycle.
	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cycle)

	// Verify cycle has all fields populated.
	assert.NotEmpty(t, cycle.BestPolicy)
	assert.NotEmpty(t, cycle.Dimension)
	assert.GreaterOrEqual(t, cycle.ImprovementPct, 0.0)
	assert.NotNil(t, cycle.Headroom)
	assert.Len(t, cycle.Headroom, 6)

	// Verify headroom assessments are valid.
	for _, h := range cycle.Headroom {
		assert.GreaterOrEqual(t, h.HeadroomPct, 0.0)
		assert.LessOrEqual(t, h.HeadroomPct, 100.0)
	}

	// Check deploy channel.
	select {
	case p := <-deployedPolicy:
		assert.NotNil(t, p)
		assert.True(t, cycle.Deployed)
	default:
		// Deploy might not be triggered if improvement is 0.
		// That's fine — the cycle still completed successfully.
	}
}

// ---------------------------------------------------------------------------
// Config Tests
// ---------------------------------------------------------------------------

func TestConfig_GetDefault(t *testing.T) {
	orch := newTestOrchestrator(makeLedgerRecords(5, "openai", "gpt-4o", time.Now()), DefaultRSIConfig())
	cfg := orch.Config()
	assert.Equal(t, 5.0, cfg.ImprovementThresholdPct)
	assert.Equal(t, 10, cfg.BroadIterations)
	assert.Equal(t, 5, cfg.DeepIterations)
	assert.Equal(t, 3, cfg.KFold)
	assert.Equal(t, 0.1, cfg.HeadroomThreshold)
	assert.Equal(t, 5*time.Minute, cfg.CycleInterval)
}

func TestConfig_GetNilOrchestrator(t *testing.T) {
	var orch *RSIOrchestrator
	cfg := orch.Config()
	assert.Equal(t, RSIConfig{}, cfg)
}

func TestConfig_SetAndRetrieve(t *testing.T) {
	orch := newTestOrchestrator(makeLedgerRecords(5, "openai", "gpt-4o", time.Now()), DefaultRSIConfig())

	newCfg := RSIConfig{
		ImprovementThresholdPct: 10.0,
		BroadIterations:         20,
		DeepIterations:          10,
		CycleInterval:           10 * time.Minute,
		KFold:                   5,
		HeadroomThreshold:       0.2,
	}
	require.NoError(t, orch.SetConfig(newCfg))

	cfg := orch.Config()
	assert.Equal(t, 10.0, cfg.ImprovementThresholdPct)
	assert.Equal(t, 20, cfg.BroadIterations)
	assert.Equal(t, 10, cfg.DeepIterations)
	assert.Equal(t, 5, cfg.KFold)
	assert.Equal(t, 0.2, cfg.HeadroomThreshold)
	assert.Equal(t, 10*time.Minute, cfg.CycleInterval)
}

func TestConfig_SetConfigAffectsRunCycle(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	orch := newTestOrchestrator(records, DefaultRSIConfig())

	// Lower the headroom threshold to force exploration.
	require.NoError(t, orch.SetConfig(RSIConfig{
		ImprovementThresholdPct: 1000.0,
		BroadIterations:         3,
		DeepIterations:          1,
		KFold:                   2,
		HeadroomThreshold:       0.0,
	}))

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cycle)
	assert.NotEmpty(t, cycle.Dimension, "should have explored a dimension with forced low threshold")
}
