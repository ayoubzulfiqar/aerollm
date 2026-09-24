package rsi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

func TestStudentTUpperTail_KnownValues(t *testing.T) {
	cases := []struct {
		t, df, want float64
	}{
		{0, 10, 0.5},
		{2.0, 10, 0.036694},
		{1.812461, 10, 0.05},
		{-2.0, 10, 1 - 0.036694},
		{2.228139, 10, 0.025},
		{1.644854, 1e7, 0.05}, // large df → standard normal
		{12.706205, 1, 0.025},
	}
	for _, c := range cases {
		got := studentTUpperTail(c.t, c.df)
		assert.InDelta(t, c.want, got, 1e-4, "t=%v df=%v", c.t, c.df)
	}
	assert.Equal(t, 1.0, studentTUpperTail(math.NaN(), 10))
	assert.Equal(t, 0.0, studentTUpperTail(math.Inf(1), 10))
	assert.Equal(t, 1.0, studentTUpperTail(1, 0))
}

func TestComparePaired(t *testing.T) {
	_, err := ComparePaired(nil, nil)
	assert.ErrorIs(t, err, ErrComparisonInput)
	_, err = ComparePaired([]float64{1}, []float64{1, 2})
	assert.ErrorIs(t, err, ErrComparisonInput)
	_, err = ComparePaired([]float64{1, math.NaN()}, []float64{1, 2})
	assert.ErrorIs(t, err, ErrComparisonInput)

	// Constant positive shift: zero variance → certain improvement.
	c, err := ComparePaired([]float64{0.5, 0.5, 0.5}, []float64{0.6, 0.6, 0.6})
	require.NoError(t, err)
	assert.Equal(t, 0.0, c.PValue)
	assert.InDelta(t, 20.0, c.ImprovementPct, 1e-9)
	assert.Equal(t, 0.0, c.TStat, "t is reported as 0 when variance is zero")

	// Identical policies: no improvement, p = 1.
	c, err = ComparePaired([]float64{0.5, 0.4}, []float64{0.5, 0.4})
	require.NoError(t, err)
	assert.Equal(t, 1.0, c.PValue)
	assert.Equal(t, 0.0, c.ImprovementPct)

	// Single sample: cannot establish significance.
	c, err = ComparePaired([]float64{0.1}, []float64{0.9})
	require.NoError(t, err)
	assert.Equal(t, 1.0, c.PValue)

	// Noisy, mean-zero differences must not look significant.
	base := make([]float64, 40)
	cand := make([]float64, 40)
	for i := range base {
		base[i] = 0.5
		if i%2 == 0 {
			cand[i] = 0.6
		} else {
			cand[i] = 0.4
		}
	}
	c, err = ComparePaired(base, cand)
	require.NoError(t, err)
	assert.Greater(t, c.PValue, 0.4)

	// Result must always be JSON-encodable.
	_, err = json.Marshal(c)
	require.NoError(t, err)
}

func TestRelativeImprovementPct_EdgeCases(t *testing.T) {
	assert.Equal(t, 100.0, relativeImprovementPct(0, 0.5), "zero baseline, strict improvement → capped 100")
	assert.Equal(t, 0.0, relativeImprovementPct(0, 0))
	assert.Equal(t, 0.0, relativeImprovementPct(math.NaN(), 1))
	assert.Equal(t, 0.0, relativeImprovementPct(1, math.Inf(1)))
	assert.InDelta(t, -50.0, relativeImprovementPct(0.5, 0.25), 1e-9)
}

func TestClampHandlesNaN(t *testing.T) {
	assert.Equal(t, 0.0, clamp(math.NaN(), 0, 1))
	assert.Equal(t, 1.0, clamp(math.Inf(1), 0, 1))
	assert.Equal(t, 0.0, clamp(math.Inf(-1), 0, 1))
}

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------

func TestScoreMetrics_ErrorsNeverOutscoreSuccess(t *testing.T) {
	// Before the fix a policy failing every request (zero cost/latency)
	// scored 0.6 and beat a working, non-cached policy.
	allErrors := &SimulatedMetrics{ErrorRate: 1, Requests: 10}
	working := &SimulatedMetrics{AvgLatency: 300, Cost: 0.05, Requests: 10}
	assert.Equal(t, 0.0, scoreMetrics(allErrors))
	assert.Greater(t, scoreMetrics(working), scoreMetrics(allErrors))
	assert.Equal(t, 0.0, scoreOutcome(ScenarioOutcome{Error: true}))
}

func TestScoreMetrics_PerRequestCostNormalisation(t *testing.T) {
	// Same per-request cost, different scenario counts → same score.
	small := &SimulatedMetrics{AvgLatency: 100, Cost: 0.002 * 10, Requests: 10}
	large := &SimulatedMetrics{AvgLatency: 100, Cost: 0.002 * 1000, Requests: 1000}
	assert.InDelta(t, scoreMetrics(small), scoreMetrics(large), 1e-12)
}

func TestScoreMetrics_NonFiniteInputsNeverNaN(t *testing.T) {
	for _, m := range []*SimulatedMetrics{
		{AvgLatency: math.NaN(), Cost: math.NaN(), ErrorRate: math.NaN(), CacheHitRate: math.NaN()},
		{AvgLatency: math.Inf(1), Cost: math.Inf(1), ErrorRate: math.Inf(-1), CacheHitRate: math.Inf(1)},
		{AvgLatency: -5, Cost: -1, Requests: 3},
	} {
		s := scoreMetrics(m)
		assert.False(t, math.IsNaN(s))
		assert.GreaterOrEqual(t, s, 0.0)
		assert.LessOrEqual(t, s, 1.0)
	}
	// Invalid latency must score as the worst case, not the best.
	assert.Less(t, scoreOutcome(ScenarioOutcome{LatencyMs: math.NaN()}), scoreOutcome(ScenarioOutcome{LatencyMs: 10}))
}

// ---------------------------------------------------------------------------
// HCI robustness
// ---------------------------------------------------------------------------

type nanCostCalculator struct{}

func (nanCostCalculator) CalculateCost(string, *models.Usage) float64 { return math.NaN() }

func TestHCI_NonFiniteInputsStayJSONEncodable(t *testing.T) {
	records := makeLedgerRecords(10, "openai", "gpt-4o", time.Now())
	for i := range records {
		records[i].Metadata = map[string]interface{}{"latency_ms": "NaN"}
	}
	records[0].Metadata = map[string]interface{}{"latency_ms": math.Inf(1)}
	records[1].Metadata = map[string]interface{}{"latency_ms": -20.0}

	store := &mockLedgerReader{records: records}
	metrics := &mockMetricsProvider{reqCount: 10, avgLat: math.NaN()}
	engine := NewHCIEngine(store, metrics, nanCostCalculator{}, &mockProviderLister{names: []string{"openai", "anthropic"}}, DefaultHCIConfig())

	assessments, err := engine.AssessAll(context.Background())
	require.NoError(t, err)
	b, err := json.Marshal(assessments)
	require.NoError(t, err, "assessments must never contain NaN/Inf")
	assert.NotContains(t, string(b), "NaN")

	// No usable latency samples → the latency dimension reports no data.
	lat := assessments[DimensionLatency]
	assert.Equal(t, 0.0, lat.Confidence)
	// NaN prices → the cost dimension reports zero confidence.
	assert.Equal(t, 0.0, assessments[DimensionCost].Confidence)
}

func TestHCI_InvalidConfigFallsBackToDefaults(t *testing.T) {
	engine := NewHCIEngine(&mockLedgerReader{}, nil, nil, nil, HCIConfig{
		MinRecords:              -3,
		TargetLatencyMs:         -100,
		CostEfficiencyThreshold: math.NaN(),
		MaxRecords:              -1,
		CacheTTL:                -time.Second,
	})
	def := DefaultHCIConfig()
	assert.Equal(t, def.MinRecords, engine.cfg.MinRecords)
	assert.Equal(t, def.TargetLatencyMs, engine.cfg.TargetLatencyMs)
	assert.Equal(t, def.CostEfficiencyThreshold, engine.cfg.CostEfficiencyThreshold)
	assert.Equal(t, def.MaxRecords, engine.cfg.MaxRecords)
	assert.Equal(t, def.CacheTTL, engine.cfg.CacheTTL)
}

func TestHCI_PrioritizeIgnoresZeroConfidenceDimensions(t *testing.T) {
	// No latency data and no prices: latency and cost report 100%/0% headroom
	// with zero confidence. Routing has real, confident headroom and must win.
	records := makeLedgerRecords(20, "openai", "gpt-4o", time.Now())
	store := &mockLedgerReader{records: records}
	engine := NewHCIEngine(store, nil, nil, &mockProviderLister{names: []string{"openai", "anthropic"}}, DefaultHCIConfig())
	assessments, err := engine.AssessAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0.0, assessments[DimensionLatency].Confidence)
	require.Equal(t, 100.0, assessments[DimensionLatency].HeadroomPct)

	dim := engine.Prioritize()
	assert.NotEqual(t, DimensionLatency, dim)
	assert.Greater(t, assessments[dim].Confidence, 0.0)
}

func TestHCI_CacheTTLReloadsLedger(t *testing.T) {
	store := &mockLedgerReader{records: makeLedgerRecords(5, "openai", "gpt-4o", time.Now())}
	cfg := DefaultHCIConfig()
	cfg.CacheTTL = time.Millisecond
	engine := NewHCIEngine(store, nil, nil, nil, cfg)

	a1, err := engine.Assess(context.Background(), DimensionCache)
	require.NoError(t, err)
	assert.Equal(t, 5, a1.Details["total_requests"])

	store.records = append(store.records, makeLedgerRecords(5, "openai", "gpt-4o", time.Now())...)
	time.Sleep(5 * time.Millisecond)
	a2, err := engine.Assess(context.Background(), DimensionCache)
	require.NoError(t, err)
	assert.Equal(t, 10, a2.Details["total_requests"], "expired cache must reload the ledger")
}

func TestHCI_MaxRecordsKeepsMostRecent(t *testing.T) {
	store := &mockLedgerReader{records: makeLedgerRecords(30, "openai", "gpt-4o", time.Now())}
	cfg := DefaultHCIConfig()
	cfg.MaxRecords = 10
	engine := NewHCIEngine(store, nil, nil, nil, cfg)
	a, err := engine.Assess(context.Background(), DimensionCache)
	require.NoError(t, err)
	assert.Equal(t, 10, a.Details["total_requests"])
}

// ---------------------------------------------------------------------------
// Dream simulator
// ---------------------------------------------------------------------------

func loadedSim(t *testing.T, n int, costCalc CostCalculator, metrics MetricsProvider) *DreamSimulator {
	t.Helper()
	sim := NewDreamSimulator(&mockLedgerReader{records: makeLedgerRecords(n, "openai", "gpt-4o", time.Now())}, costCalc, metrics)
	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))
	return sim
}

func TestReplayDetailed_OutcomesAlignedAndNilSkipped(t *testing.T) {
	sim := loadedSim(t, 4, &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.001}}, nil)
	scenarios := sim.Scenarios()
	scenarios = append(scenarios[:2], append([]*DreamReplay{nil}, scenarios[2:]...)...)

	m, outcomes, err := sim.ReplayDetailed(context.Background(), &mockPolicy{provider: "openai", latency: 100}, scenarios)
	require.NoError(t, err)
	require.Len(t, outcomes, 5)
	assert.True(t, outcomes[2].Skipped)
	assert.Equal(t, 4, m.Requests, "nil scenarios are not counted")
	for i, o := range outcomes {
		if i == 2 {
			continue
		}
		assert.InDelta(t, 0.001, o.CostUSD, 1e-12)
		assert.Greater(t, o.Score, 0.0)
	}
}

func TestReplay_AvgLatencyExcludesFailedApplies(t *testing.T) {
	sim := loadedSim(t, 4, nil, nil)
	policy := &flakyPolicy{latency: 100}
	m, err := sim.ReplayTraffic(context.Background(), policy, sim.Scenarios())
	require.NoError(t, err)
	assert.InDelta(t, 0.5, m.ErrorRate, 1e-9)
	assert.InDelta(t, 100.0, m.AvgLatency, 1e-9, "failed applies have no latency and must not drag the mean to zero")
}

// flakyPolicy fails every other Apply call.
type flakyPolicy struct {
	mu      sync.Mutex
	n       int
	latency float64
}

func (p *flakyPolicy) Apply(context.Context, *models.LLMRequest) (*Response, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
	if p.n%2 == 0 {
		return nil, errors.New("boom")
	}
	return &Response{Provider: "openai", LatencyMs: p.latency}, nil
}
func (p *flakyPolicy) Mutate() Policy { return &flakyPolicy{latency: p.latency} }
func (p *flakyPolicy) Clone() Policy  { return &flakyPolicy{latency: p.latency} }

func TestReplay_ProviderFilterTurnsUnknownProvidersIntoErrors(t *testing.T) {
	sim := loadedSim(t, 3, nil, nil)
	sim.SetProviderFilter(func() []string { return []string{"openai"} })

	m, err := sim.ReplayTraffic(context.Background(), &mockPolicy{provider: "ghost-provider", latency: 10}, sim.Scenarios())
	require.NoError(t, err)
	assert.Equal(t, 1.0, m.ErrorRate)

	// Cache hits never reach a provider, so they are unaffected.
	m, err = sim.ReplayTraffic(context.Background(), &mockPolicy{provider: "ghost-provider", cached: true}, sim.Scenarios())
	require.NoError(t, err)
	assert.Equal(t, 0.0, m.ErrorRate)

	// Empty list = availability unknown → filter disabled.
	sim.SetProviderFilter(func() []string { return nil })
	m, err = sim.ReplayTraffic(context.Background(), &mockPolicy{provider: "ghost-provider", latency: 10}, sim.Scenarios())
	require.NoError(t, err)
	assert.Equal(t, 0.0, m.ErrorRate)
}

func TestSimulateOutcome_CounterfactualProviderUsesPolicyEstimate(t *testing.T) {
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.05}}
	sim := NewDreamSimulator(nil, costCalc, nil)
	scenario := &DreamReplay{
		Request:  makeRequest("gpt-4o", "hi"),
		Response: makeResponse("openai", "ok", &models.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2}, nil),
	}
	// Same provider as recorded → recorded price.
	_, cost := sim.simulateOutcome(&Response{Provider: "openai", CostUSD: 0.0001}, scenario)
	assert.Equal(t, 0.05, cost)
	// Different provider with an estimate → the estimate.
	_, cost = sim.simulateOutcome(&Response{Provider: "anthropic", CostUSD: 0.0001}, scenario)
	assert.Equal(t, 0.0001, cost)
	// Non-finite hints are ignored.
	lat, cost := sim.simulateOutcome(&Response{Provider: "anthropic", CostUSD: math.NaN(), LatencyMs: math.Inf(1)}, scenario)
	assert.Equal(t, 0.05, cost)
	assert.Equal(t, 0.0, lat)
}

func TestSimulateOutcome_ErrorTakesPrecedenceOverCache(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)
	out := sim.outcomeFor(&Response{Cached: true, Error: true, LatencyMs: 3}, &DreamReplay{}, nil)
	assert.True(t, out.Error)
	assert.False(t, out.Cached)
	assert.Equal(t, 0.0, out.Score)
}

func TestScenarios_ReturnsCopy(t *testing.T) {
	sim := loadedSim(t, 3, nil, nil)
	s := sim.Scenarios()
	s[0] = nil
	assert.NotNil(t, sim.Scenarios()[0])
}

func TestLoadFromLedger_Cancelled(t *testing.T) {
	sim := NewDreamSimulator(&mockLedgerReader{records: makeLedgerRecords(3, "openai", "gpt-4o", time.Now())}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := sim.LoadFromLedger(ctx, TimeRange{}, 0)
	assert.ErrorIs(t, err, context.Canceled)
}

// ---------------------------------------------------------------------------
// Explorer
// ---------------------------------------------------------------------------

func TestExploreBroad_ProducesDistinctMutations(t *testing.T) {
	sim := loadedSim(t, 10, nil, nil)
	explorer := NewAutonomousExplorer(sim, nil)
	base := &RoutingPolicy{Weights: map[string]float64{"openai": 0.5, "anthropic": 0.5}}

	candidates, err := explorer.ExploreBroad(context.Background(), base, 8)
	require.NoError(t, err)
	require.Len(t, candidates, 9)
	seen := map[string]bool{}
	for _, c := range candidates {
		rp := c.(*RoutingPolicy)
		seen[fmt.Sprintf("%.6f/%.6f/%v", rp.Weights["openai"], rp.Weights["anthropic"], rp.CacheEnabled)] = true
	}
	// Before the fix every mutation of the same parent was identical.
	assert.Greater(t, len(seen), 5, "broad exploration must generate diverse candidates")
}

func TestExplore_DeterministicForSeed(t *testing.T) {
	run := func() map[string]interface{} {
		store := &mockLedgerReader{records: makeLedgerRecords(12, "openai", "gpt-4o", time.Now())}
		sim := NewDreamSimulator(store, nil, nil)
		require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))
		explorer := NewAutonomousExplorer(sim, NewHCIEngine(store, nil, nil, nil, DefaultHCIConfig()))
		explorer.SetSeed(42)
		p, _, err := explorer.ExploreWithOptions(context.Background(), DimensionRouting, ExploreOptions{BroadIterations: 6, DeepIterations: 4})
		require.NoError(t, err)
		return describePolicy(p)
	}
	assert.Equal(t, run(), run())
}

func TestRoutingPolicy_MutateIsDeterministic(t *testing.T) {
	p := &RoutingPolicy{Weights: map[string]float64{"a": 1, "b": 2, "c": 3, "d": 4}, seed: 7}
	m1 := p.Mutate().(*RoutingPolicy)
	m2 := p.Mutate().(*RoutingPolicy)
	assert.Equal(t, m1.Weights, m2.Weights, "Mutate must not depend on map iteration order")
}

func TestExplore_IterationBoundsAndCancellation(t *testing.T) {
	sim := loadedSim(t, 5, nil, nil)
	explorer := NewAutonomousExplorer(sim, nil)

	// Negative counts used to panic (make with negative capacity).
	candidates, err := explorer.ExploreBroad(context.Background(), &mockPolicy{provider: "openai"}, -5)
	require.NoError(t, err)
	assert.Len(t, candidates, 1)
	assert.Equal(t, MaxExploreIterations, clampIterations(1<<30))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = explorer.ExploreBroad(ctx, &mockPolicy{provider: "openai"}, 3)
	assert.ErrorIs(t, err, context.Canceled)
	_, err = explorer.ExploreDeep(ctx, []Policy{&mockPolicy{provider: "openai"}}, 3)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestPolicies_NonFiniteParamsAreSafe(t *testing.T) {
	req := makeRequest("gpt-4o", strings.Repeat("x", 100))
	policies := []Policy{
		&RoutingPolicy{Weights: map[string]float64{"openai": math.NaN(), "anthropic": math.Inf(1)}},
		&CachePolicy{CacheableThreshold: math.Inf(1)},
		&CachePolicy{CacheableThreshold: math.NaN()},
		&GuardrailPolicy{InjectionSensitivity: math.NaN(), BudgetLimitUSD: math.NaN()},
		&AgentWorkflowPolicy{MaxDepth: -4, MaxTools: -2},
	}
	for _, p := range policies {
		resp, err := p.Apply(context.Background(), req)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, resp.LatencyMs, 0.0)
		assert.GreaterOrEqual(t, resp.CostUSD, 0.0)
		m := p.Mutate()
		require.NotNil(t, m)
		_, err = json.Marshal(describePolicy(m))
		require.NoError(t, err, "%T params must be JSON-encodable", p)
	}
}

// ---------------------------------------------------------------------------
// Config validation
// ---------------------------------------------------------------------------

func TestRSIConfig_Validate(t *testing.T) {
	require.NoError(t, DefaultRSIConfig().Validate())
	require.NoError(t, RSIConfig{}.Validate(), "zero config means defaults")

	bad := []RSIConfig{
		{CycleInterval: -time.Second},
		{CycleInterval: time.Millisecond},
		{HeadroomThreshold: 1.5},
		{HeadroomThreshold: -0.1},
		{HeadroomThreshold: math.NaN()},
		{ImprovementThresholdPct: -1},
		{ImprovementThresholdPct: math.Inf(1)},
		{KFold: 1},
		{KFold: -2},
		{KFold: 50},
		{BroadIterations: -1},
		{DeepIterations: MaxExploreIterations + 1},
		{MinSamples: 1},
		{SignificanceLevel: 1.5},
		{SignificanceLevel: math.NaN()},
		{SignificanceLevel: -0.05},
		{MaxHistory: -1},
		{SampleSize: -1},
		{CycleTimeout: 2 * time.Hour},
	}
	for _, cfg := range bad {
		err := cfg.Validate()
		assert.ErrorIs(t, err, ErrInvalidConfig, "%+v should be invalid", cfg)
	}

	// All problems are reported at once.
	err := RSIConfig{KFold: 1, HeadroomThreshold: 2}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "k_fold")
	assert.Contains(t, err.Error(), "headroom_threshold")
}

func TestSetConfig_RejectsInvalidAndKeepsCurrent(t *testing.T) {
	orch := newTestOrchestrator(nil, DefaultRSIConfig())
	err := orch.SetConfig(RSIConfig{CycleInterval: -time.Minute, HeadroomThreshold: 7})
	require.ErrorIs(t, err, ErrInvalidConfig)
	assert.Equal(t, DefaultRSIConfig(), orch.Config())

	require.NoError(t, orch.SetConfig(RSIConfig{BroadIterations: 2, KFold: 4}))
	cfg := orch.Config()
	assert.Equal(t, 2, cfg.BroadIterations)
	assert.Equal(t, 30, cfg.MinSamples, "zero values take defaults, never disabling a gate")
	assert.Equal(t, 0.05, cfg.SignificanceLevel)
	assert.Equal(t, 5*time.Minute, cfg.CycleInterval)
}

func TestNewRSIOrchestrator_SanitizesDangerousValues(t *testing.T) {
	orch := newTestOrchestrator(nil, RSIConfig{
		CycleInterval:           -time.Second,
		ImprovementThresholdPct: -50,
		SignificanceLevel:       math.NaN(),
		BroadIterations:         -3,
		MaxHistory:              MaxHistoryLimit * 10,
	})
	cfg := orch.Config()
	assert.Equal(t, 5*time.Minute, cfg.CycleInterval)
	assert.Equal(t, 5.0, cfg.ImprovementThresholdPct, "negative threshold would deploy regressions")
	assert.Equal(t, 0.05, cfg.SignificanceLevel)
	assert.Equal(t, 0, cfg.BroadIterations)
	assert.Equal(t, MaxHistoryLimit, cfg.MaxHistory)
}

func TestRSIConfig_UnmarshalJSON(t *testing.T) {
	cfg := DefaultRSIConfig()
	require.NoError(t, json.Unmarshal([]byte(`{"cycle_interval":"90s","k_fold":4}`), &cfg))
	assert.Equal(t, 90*time.Second, cfg.CycleInterval)
	assert.Equal(t, 4, cfg.KFold)
	assert.Equal(t, 10, cfg.BroadIterations, "absent fields keep current values (partial update)")

	require.NoError(t, json.Unmarshal([]byte(`{"cycle_interval":60000000000,"cycle_timeout":"30s","dry_run":true}`), &cfg))
	assert.Equal(t, time.Minute, cfg.CycleInterval)
	assert.Equal(t, 30*time.Second, cfg.CycleTimeout)
	assert.True(t, cfg.DryRun)

	assert.Error(t, json.Unmarshal([]byte(`{"cycle_interval":"soon"}`), &cfg))
	assert.Error(t, json.Unmarshal([]byte(`{"cycle_interval":1.5}`), &cfg))

	// Round trip.
	b, err := json.Marshal(DefaultRSIConfig())
	require.NoError(t, err)
	var back RSIConfig
	require.NoError(t, json.Unmarshal(b, &back))
	assert.Equal(t, DefaultRSIConfig(), back)
}

// ---------------------------------------------------------------------------
// Orchestrator: gating, deploy, rollback, concurrency
// ---------------------------------------------------------------------------

func deployableOrchestrator(t *testing.T, mutate func(*RSIConfig)) *RSIOrchestrator {
	t.Helper()
	cfg := deployableConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	orch := newTestOrchestrator(makeLedgerRecords(20, "openai", "gpt-4o", time.Now()), cfg)
	orch.SetBasePolicyFunc(improvableBase)
	return orch
}

func TestRunCycle_InsufficientSamplesBlocksDeploy(t *testing.T) {
	orch := deployableOrchestrator(t, func(c *RSIConfig) { c.MinSamples = 1000 })
	var calls int32
	orch.OnDeploy = func(context.Context, Policy, RSICycle) error { atomic.AddInt32(&calls, 1); return nil }

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, DecisionInsufficientSamples, cycle.DeployDecision)
	assert.Zero(t, atomic.LoadInt32(&calls))
	assert.Greater(t, cycle.ImprovementPct, 0.0, "the improvement is real, but there is not enough evidence")
}

func TestDecideAndDeploy_Gates(t *testing.T) {
	orch := newTestOrchestrator(nil, DefaultRSIConfig())
	cfg := DefaultRSIConfig()
	var calls int32
	hooks := cycleHooks{deploy: func(context.Context, Policy, RSICycle) error { atomic.AddInt32(&calls, 1); return nil }}
	p := &mockPolicy{provider: "openai"}

	cases := []struct {
		name  string
		cycle RSICycle
		want  string
	}{
		{"zero improvement", RSICycle{HoldoutSamples: 100, ImprovementPct: 0, PValue: 0, Significant: true}, DecisionBelowThreshold},
		{"negative improvement", RSICycle{HoldoutSamples: 100, ImprovementPct: -8, PValue: 0, Significant: true}, DecisionBelowThreshold},
		{"below margin", RSICycle{HoldoutSamples: 100, ImprovementPct: 4.9, PValue: 0, Significant: true}, DecisionBelowThreshold},
		{"not significant", RSICycle{HoldoutSamples: 100, ImprovementPct: 20, PValue: 0.3, Significant: false}, DecisionNotSignificant},
		{"too few samples", RSICycle{HoldoutSamples: 29, ImprovementPct: 20, PValue: 0, Significant: true}, DecisionInsufficientSamples},
		{"passes", RSICycle{HoldoutSamples: 30, ImprovementPct: 5, PValue: 0.01, Significant: true}, DecisionDeployed},
	}
	for _, tc := range cases {
		c := tc.cycle
		before := atomic.LoadInt32(&calls)
		orch.decideAndDeploy(context.Background(), cfg, hooks, &c, DimensionRouting, p)
		assert.Equal(t, tc.want, c.DeployDecision, tc.name)
		assert.Equal(t, tc.want == DecisionDeployed, atomic.LoadInt32(&calls) > before, tc.name)
		assert.Equal(t, tc.want == DecisionDeployed, c.Deployed, tc.name)
	}
}

func TestRunCycle_DryRunNeverDeploys(t *testing.T) {
	orch := deployableOrchestrator(t, func(c *RSIConfig) { c.DryRun = true })
	var calls int32
	orch.OnDeploy = func(context.Context, Policy, RSICycle) error { atomic.AddInt32(&calls, 1); return nil }

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.Zero(t, atomic.LoadInt32(&calls))
	assert.False(t, cycle.Deployed)
	assert.True(t, cycle.WouldDeploy)
	assert.True(t, cycle.DryRun)
	assert.Equal(t, DecisionDryRun, cycle.DeployDecision)
	assert.Nil(t, orch.DeployedPolicy())
	assert.Equal(t, uint64(1), orch.Stats().DryRunCandidates)
}

func TestRunCycle_DeployFailureRollsBack(t *testing.T) {
	orch := deployableOrchestrator(t, nil)
	var restored []Policy
	orch.OnDeploy = func(context.Context, Policy, RSICycle) error { return errors.New("router refused") }
	orch.OnRollback = func(_ context.Context, restore Policy, _ RSICycle) error {
		restored = append(restored, restore)
		return nil
	}

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, DecisionDeployFailed, cycle.DeployDecision)
	assert.Contains(t, cycle.Error, "router refused")
	assert.True(t, cycle.RolledBack)
	require.Len(t, restored, 1)
	assert.Nil(t, restored[0], "nothing was deployed before, so restore the pre-RSI state")
	assert.Nil(t, orch.DeployedPolicy())

	s := orch.Stats()
	assert.Equal(t, uint64(1), s.DeployFailures)
	assert.Equal(t, uint64(1), s.Rollbacks)
	assert.Equal(t, uint64(0), s.Deployments)
}

func TestRunCycle_DeployHookPanicIsContained(t *testing.T) {
	orch := deployableOrchestrator(t, nil)
	orch.OnDeploy = func(context.Context, Policy, RSICycle) error { panic("kaboom") }
	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, DecisionDeployFailed, cycle.DeployDecision)
	assert.Contains(t, cycle.Error, "kaboom")
}

func TestRollback_RevertsLastDeployment(t *testing.T) {
	orch := deployableOrchestrator(t, nil)
	orch.OnDeploy = func(context.Context, Policy, RSICycle) error { return nil }

	require.ErrorIs(t, orch.Rollback(context.Background()), ErrNothingToRollback)

	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	require.True(t, cycle.Deployed)
	require.NotNil(t, orch.DeployedPolicy())

	require.ErrorIs(t, orch.Rollback(context.Background()), ErrNoRollbackHook)

	var got []Policy
	var gotCycle RSICycle
	orch.OnRollback = func(_ context.Context, restore Policy, c RSICycle) error {
		got = append(got, restore)
		gotCycle = c
		return nil
	}
	require.NoError(t, orch.Rollback(context.Background()))
	require.Len(t, got, 1)
	assert.Nil(t, got[0])
	assert.Equal(t, cycle.ID, gotCycle.ID)
	assert.Nil(t, orch.DeployedPolicy())
	assert.True(t, orch.Cycles()[0].RolledBack)
	assert.Equal(t, uint64(1), orch.Stats().Rollbacks)
	require.ErrorIs(t, orch.Rollback(context.Background()), ErrNothingToRollback)
}

func TestRollback_FailureKeepsDeployment(t *testing.T) {
	orch := deployableOrchestrator(t, nil)
	orch.OnDeploy = func(context.Context, Policy, RSICycle) error { return nil }
	_, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	orch.OnRollback = func(context.Context, Policy, RSICycle) error { return errors.New("nope") }
	require.Error(t, orch.Rollback(context.Background()))
	assert.NotNil(t, orch.DeployedPolicy())
	assert.Equal(t, uint64(1), orch.Stats().RollbackFailures)
}

func TestRunCycle_BaselineIsCurrentlyDeployedPolicy(t *testing.T) {
	orch := deployableOrchestrator(t, nil)
	var calls int32
	orch.OnDeploy = func(context.Context, Policy, RSICycle) error { atomic.AddInt32(&calls, 1); return nil }

	c1, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	require.True(t, c1.Deployed)

	// The next cycle explores from the deployed (already caching) policy.
	// Its "improvement" over the stale base must not be re-deployed.
	c2, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.False(t, c2.Deployed)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestRunCycle_RejectsConcurrentCycle(t *testing.T) {
	orch := deployableOrchestrator(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	orch.OnDeploy = func(ctx context.Context, _ Policy, _ RSICycle) error {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}

	done := make(chan error, 1)
	go func() {
		_, err := orch.RunCycle(context.Background())
		done <- err
	}()
	<-entered

	assert.True(t, orch.CycleInProgress())
	c, err := orch.RunCycle(context.Background())
	assert.ErrorIs(t, err, ErrCycleInProgress)
	assert.Nil(t, c)
	assert.ErrorIs(t, orch.Rollback(context.Background()), ErrCycleInProgress)
	assert.Equal(t, uint64(1), orch.Stats().CyclesRejected)

	close(release)
	require.NoError(t, <-done)
	assert.False(t, orch.CycleInProgress())
	assert.Len(t, orch.Cycles(), 1, "the rejected trigger must not be recorded as a cycle")
}

func TestRunCycle_ConcurrentTriggersAreRaceFree(t *testing.T) {
	orch := newTestOrchestrator(makeLedgerRecords(15, "openai", "gpt-4o", time.Now()), RSIConfig{BroadIterations: 2, KFold: 2})
	var wg sync.WaitGroup
	var ok, busy int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := orch.RunCycle(context.Background())
			switch {
			case errors.Is(err, ErrCycleInProgress):
				atomic.AddInt32(&busy, 1)
			case err == nil:
				atomic.AddInt32(&ok, 1)
			}
			_ = orch.Stats()
			_ = orch.Cycles()
			_ = orch.Config()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = orch.SetConfig(RSIConfig{BroadIterations: 1, KFold: 2})
	}()
	wg.Wait()
	assert.Equal(t, int32(8), ok+busy)
	assert.Equal(t, int(ok), len(orch.Cycles()))
}

type panicPolicy struct{}

func (panicPolicy) Apply(context.Context, *models.LLMRequest) (*Response, error) {
	panic("policy exploded")
}
func (panicPolicy) Mutate() Policy { return panicPolicy{} }
func (panicPolicy) Clone() Policy  { return panicPolicy{} }

func TestRunCycle_PolicyPanicIsRecordedNotFatal(t *testing.T) {
	orch := deployableOrchestrator(t, nil)
	orch.SetBasePolicyFunc(func(HeadroomDimension) Policy { return panicPolicy{} })
	cycle, err := orch.RunCycle(context.Background())
	require.Error(t, err)
	require.NotNil(t, cycle)
	assert.Contains(t, cycle.Error, "panicked")
	assert.Equal(t, DecisionError, cycle.DeployDecision)
	assert.Equal(t, uint64(1), orch.Stats().CyclesFailed)

	// The orchestrator keeps working afterwards.
	orch.SetBasePolicyFunc(improvableBase)
	_, err = orch.RunCycle(context.Background())
	require.NoError(t, err)
}

func TestRunCycle_EmptyLedgerIsSkippedNotFailed(t *testing.T) {
	orch := newTestOrchestratorNoData(DefaultRSIConfig())
	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, DecisionInsufficientData, cycle.DeployDecision)
	assert.NotEmpty(t, cycle.Reason)
	s := orch.Stats()
	assert.Equal(t, uint64(1), s.CyclesSkipped)
	assert.Equal(t, uint64(0), s.CyclesFailed)
}

func TestHistory_BoundedAndRestorable(t *testing.T) {
	orch := newTestOrchestrator(makeLedgerRecords(10, "openai", "gpt-4o", time.Now()), RSIConfig{BroadIterations: 1, KFold: 2, MaxHistory: 3})
	var seen []int
	var mu sync.Mutex
	orch.OnCycle = func(c RSICycle) {
		mu.Lock()
		seen = append(seen, c.ID)
		mu.Unlock()
	}
	for i := 0; i < 5; i++ {
		_, err := orch.RunCycle(context.Background())
		require.NoError(t, err)
	}
	cycles := orch.Cycles()
	require.Len(t, cycles, 3)
	assert.Equal(t, []int{3, 4, 5}, []int{cycles[0].ID, cycles[1].ID, cycles[2].ID})
	assert.Equal(t, []int{1, 2, 3, 4, 5}, seen)

	// Persist via OnCycle, restore into a fresh orchestrator.
	fresh := newTestOrchestrator(makeLedgerRecords(10, "openai", "gpt-4o", time.Now()), RSIConfig{BroadIterations: 1, KFold: 2, MaxHistory: 3})
	fresh.RestoreHistory(cycles)
	c, err := fresh.RunCycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 6, c.ID, "cycle IDs continue after restored history")
	assert.Len(t, fresh.Cycles(), 3)
}

func TestOnCyclePanicDoesNotBreakCycle(t *testing.T) {
	orch := newTestOrchestrator(makeLedgerRecords(10, "openai", "gpt-4o", time.Now()), RSIConfig{BroadIterations: 1, KFold: 2})
	orch.OnCycle = func(RSICycle) { panic("sink down") }
	_, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
}

func TestCycles_DeepCopiesMaps(t *testing.T) {
	orch := deployableOrchestrator(t, nil)
	_, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	c := orch.Cycles()[0]
	require.NotEmpty(t, c.Headroom)
	for k := range c.Headroom {
		delete(c.Headroom, k)
	}
	assert.NotEmpty(t, orch.Cycles()[0].Headroom)
}

func TestRun_SecondRunReturnsImmediatelyAndConfigWakesLoop(t *testing.T) {
	orch := newTestOrchestrator(makeLedgerRecords(8, "openai", "gpt-4o", time.Now()), RSIConfig{
		BroadIterations: 1, KFold: 2, CycleInterval: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		orch.Run(ctx)
		close(done)
	}()
	require.Eventually(t, func() bool { return orch.running.Load() }, time.Second, time.Millisecond)

	second := make(chan struct{})
	go func() {
		orch.Run(ctx)
		close(second)
	}()
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("second Run should return immediately")
	}

	// With a 1h interval nothing runs; shortening it via SetConfig must
	// take effect without waiting for the old timer.
	require.NoError(t, orch.SetConfig(RSIConfig{BroadIterations: 1, KFold: 2, CycleInterval: time.Second}))
	require.Eventually(t, func() bool { return len(orch.Cycles()) > 0 }, 5*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestRSICycle_JSONNeverFailsOnBadInputs(t *testing.T) {
	records := makeLedgerRecords(20, "openai", "gpt-4o", time.Now())
	for i := range records {
		records[i].Metadata = map[string]interface{}{"latency_ms": math.Inf(1)}
	}
	store := &mockLedgerReader{records: records}
	orch := NewRSIOrchestrator(store, &mockMetricsProvider{reqCount: 20, avgLat: math.NaN()}, nanCostCalculator{}, &mockProviderLister{names: []string{"openai"}}, deployableConfig())
	orch.SetBasePolicyFunc(improvableBase)
	cycle, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	_, err = json.Marshal(cycle)
	require.NoError(t, err)
	_, err = json.Marshal(orch.Stats())
	require.NoError(t, err)
}

func TestNilOrchestratorMethods(t *testing.T) {
	var o *RSIOrchestrator
	assert.ErrorIs(t, o.SetConfig(DefaultRSIConfig()), ErrNilOrchestrator)
	assert.ErrorIs(t, o.Rollback(context.Background()), ErrNilOrchestrator)
	assert.Equal(t, RSIStats{}, o.Stats())
	assert.False(t, o.CycleInProgress())
	o.Run(context.Background())
	o.RestoreHistory([]RSICycle{{ID: 1}})
	o.SetHooks(nil, nil, nil)
	o.SetBasePolicyFunc(nil)
}

// ---------------------------------------------------------------------------
// Safety: the RSI package must not be able to touch files, processes or the
// network. Any live effect must go through injected hooks.
// ---------------------------------------------------------------------------

func TestRSIPackageHasNoSideEffectImports(t *testing.T) {
	forbidden := map[string]bool{
		"os": true, "os/exec": true, "syscall": true, "unsafe": true, "plugin": true,
		"net": true, "net/http": true, "io/ioutil": true, "io/fs": true, "path/filepath": true,
		"golang.org/x/sys/unix": true,
	}
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		require.NoError(t, err)
		parsed, err := parser.ParseFile(fset, f, src, parser.ImportsOnly)
		require.NoError(t, err)
		for _, imp := range parsed.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			assert.False(t, forbidden[path], "%s imports %q; RSI must stay side-effect free", f, path)
		}
		checked++
	}
	assert.Greater(t, checked, 5)
}

var _ ledger.LedgerRecord
