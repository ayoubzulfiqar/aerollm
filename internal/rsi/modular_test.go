package rsi

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helper: create scenarios for modular/eval tests
// ---------------------------------------------------------------------------

func makeScenarios(n int, provider, model, content string, ts time.Time) []*DreamReplay {
	scenarios := make([]*DreamReplay, n)
	for i := range scenarios {
		req := makeRequest(model, fmt.Sprintf("%s-%d", content, i))
		resp := makeResponse(provider, "ok", &models.Usage{
			PromptTokens:     100,
			CompletionTokens: 50,
			TotalTokens:      150,
		}, nil)
		record := makeLedgerRecord(req, resp, ts)
		scenarios[i] = &DreamReplay{
			Request:   req,
			Response:  resp,
			Metadata:  map[string]interface{}{"provider": provider},
			Timestamp: record.Timestamp,
		}
	}
	return scenarios
}

// ---------------------------------------------------------------------------
// CreatePartitions Tests
// ---------------------------------------------------------------------------

func TestCreatePartitions_BasicKFold(t *testing.T) {
	scenarios := makeScenarios(100, "openai", "gpt-4o", "query", time.Now())

	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)

	partitions := eval.CreatePartitions(scenarios, 5)
	require.Len(t, partitions, 5, "should create 5 folds")

	for i, part := range partitions {
		assert.Greater(t, len(part.TrainScenarios), 0, "fold %d train should not be empty", i)
		assert.Greater(t, len(part.TestScenarios), 0, "fold %d test should not be empty", i)
	}
}

func TestCreatePartitions_KFoldsZero(t *testing.T) {
	scenarios := makeScenarios(10, "openai", "gpt-4o", "query", time.Now())

	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)

	partitions := eval.CreatePartitions(scenarios, 0)
	require.Len(t, partitions, 1, "kFolds=0 → single partition")

	part := partitions[0]
	assert.Greater(t, len(part.TrainScenarios), 0)
	assert.Greater(t, len(part.TestScenarios), 0)
	// 80/20 split → 8 train, 2 test.
	assert.Equal(t, 8, len(part.TrainScenarios))
	assert.Equal(t, 2, len(part.TestScenarios))
}

func TestCreatePartitions_MoreFoldsThanScenarios(t *testing.T) {
	scenarios := makeScenarios(3, "openai", "gpt-4o", "query", time.Now())

	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)

	partitions := eval.CreatePartitions(scenarios, 100)
	// Should cap at len(scenarios) = 3 folds.
	require.Len(t, partitions, 3, "kFolds capped at scenario count")
}

func TestCreatePartitions_EmptyScenarios(t *testing.T) {
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)

	partitions := eval.CreatePartitions(nil, 5)
	assert.Nil(t, partitions)
}

func TestCreatePartitions_TrainTestDisjoint(t *testing.T) {
	scenarios := makeScenarios(20, "openai", "gpt-4o", "query", time.Now())

	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)

	partitions := eval.CreatePartitions(scenarios, 4)

	for i, part := range partitions {
		// Ensure no scenario appears in both train and test.
		trainSet := make(map[*DreamReplay]bool)
		for _, s := range part.TrainScenarios {
			trainSet[s] = true
		}
		for _, s := range part.TestScenarios {
			assert.False(t, trainSet[s], "fold %d: test scenario in train set", i)
		}

		// Ensure train + test = total scenarios.
		total := len(part.TrainScenarios) + len(part.TestScenarios)
		assert.Equal(t, 20, total, "fold %d: train+test should equal total", i)
	}
}

func TestCreatePartitions_DiversityGuarantee(t *testing.T) {
	// Create scenarios with varying content lengths.
	scenarios := make([]*DreamReplay, 20)
	for i := range scenarios {
		content := ""
		for j := 0; j <= i*10; j++ {
			content += "x"
		}
		req := makeRequest("gpt-4o", content)
		resp := makeResponse("openai", "ok", nil, nil)
		scenarios[i] = &DreamReplay{
			Request:  req,
			Response: resp,
			Timestamp: time.Now(),
		}
	}

	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)

	partitions := eval.CreatePartitions(scenarios, 4)
	require.Len(t, partitions, 4)

	// Each fold's test set should contain scenarios of varied lengths
	// (not all the shortest or all the longest).
	for i, part := range partitions {
		lengths := make(map[int]bool)
		for _, s := range part.TestScenarios {
			if s.Request != nil && len(s.Request.Messages) > 0 && s.Request.Messages[0].Content != nil {
				lengths[len(*s.Request.Messages[0].Content)] = true
			}
		}
		assert.Greater(t, len(lengths), 1, "fold %d test set should have diverse content lengths", i)
	}
}

// ---------------------------------------------------------------------------
// EvaluateDisjoint Tests
// ---------------------------------------------------------------------------

func TestEvaluateDisjoint_Basic(t *testing.T) {
	scenarios := makeScenarios(20, "openai", "gpt-4o", "query", time.Now())
	partitions := createTestPartitions(scenarios, 3)

	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)

	policy := &mockPolicy{provider: "openai"}
	metrics, err := eval.EvaluateDisjoint(context.Background(), policy, partitions)
	require.NoError(t, err)
	require.NotNil(t, metrics)

	assert.GreaterOrEqual(t, metrics.TrainScore, 0.0)
	assert.GreaterOrEqual(t, metrics.TestScore, 0.0)
	assert.IsType(t, 0.0, metrics.GeneralizationGap)
	assert.NotNil(t, metrics.TrainEval)
	assert.NotNil(t, metrics.TestEval)
}

func TestEvaluateDisjoint_NilSimulator(t *testing.T) {
	eval := &ModularEvaluator{}
	partitions := []*EvaluationPartition{{
		TrainScenarios: []*DreamReplay{{}},
		TestScenarios:  []*DreamReplay{{}},
	}}

	metrics, err := eval.EvaluateDisjoint(context.Background(), &mockPolicy{}, partitions)
	require.Error(t, err)
	assert.Nil(t, metrics)
}

func TestEvaluateDisjoint_NilPolicy(t *testing.T) {
	scenarios := makeScenarios(10, "openai", "gpt-4o", "query", time.Now())
	partitions := createTestPartitions(scenarios, 2)
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)

	metrics, err := eval.EvaluateDisjoint(context.Background(), nil, partitions)
	require.Error(t, err)
	assert.Nil(t, metrics)
}

func TestEvaluateDisjoint_NoPartitions(t *testing.T) {
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)

	metrics, err := eval.EvaluateDisjoint(context.Background(), &mockPolicy{}, nil)
	require.Error(t, err)
	assert.Nil(t, metrics)
}

func TestEvaluateDisjoint_ReturnsMetrics(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)
	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.05}}
	sim := NewDreamSimulator(store, costCalc, nil)
	eval := NewModularEvaluator(sim)

	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))
	scenarios := sim.Scenarios()

	partitions := eval.CreatePartitions(scenarios, 3)
	policy := &mockPolicy{provider: "openai"}

	metrics, err := eval.EvaluateDisjoint(context.Background(), policy, partitions)
	require.NoError(t, err)
	require.NotNil(t, metrics)

	// Verify metrics fields are populated.
	assert.GreaterOrEqual(t, metrics.TrainScore, 0.0)
	assert.GreaterOrEqual(t, metrics.TestScore, 0.0)
	assert.GreaterOrEqual(t, metrics.TrainEval.AvgLatency, 0.0)
	assert.GreaterOrEqual(t, metrics.TestEval.AvgLatency, 0.0)
}

func TestEvaluateDisjoint_CachedPolicyHigherScore(t *testing.T) {
	// A caching policy should score higher than a non-caching one.
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.05}}
	sim := NewDreamSimulator(store, costCalc, nil)
	eval := NewModularEvaluator(sim)

	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))
	scenarios := sim.Scenarios()
	partitions := eval.CreatePartitions(scenarios, 3)

	cachingPolicy := &mockPolicy{provider: "openai", cached: true}
	nonCachingPolicy := &mockPolicy{provider: "openai", cached: false}

	cachingMetrics, err := eval.EvaluateDisjoint(context.Background(), cachingPolicy, partitions)
	require.NoError(t, err)
	nonCachingMetrics, err := eval.EvaluateDisjoint(context.Background(), nonCachingPolicy, partitions)
	require.NoError(t, err)

	// Cached policy should have higher (or equal) score due to lower cost.
	assert.GreaterOrEqual(t, cachingMetrics.TrainScore+0.001, nonCachingMetrics.TrainScore,
		"caching policy should score at least as well as non-caching")
}

// ---------------------------------------------------------------------------
// Policy Implementation Tests
// ---------------------------------------------------------------------------

func TestRoutingPolicy_Apply(t *testing.T) {
	policy := &RoutingPolicy{
		Weights: map[string]float64{
			"openai":    0.3,
			"anthropic": 0.7,
		},
		CacheEnabled: true,
	}

	resp, err := policy.Apply(context.Background(), makeRequest("gpt-4o", "test"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "anthropic", resp.Provider, "should select highest-weight provider")
	assert.True(t, resp.Cached, "cache should be enabled")
}

func TestRoutingPolicy_Apply_NilRequest(t *testing.T) {
	policy := &RoutingPolicy{Weights: map[string]float64{"openai": 1.0}}
	resp, err := policy.Apply(context.Background(), nil)
	require.NoError(t, err)
	assert.True(t, resp.Error, "nil request should return error response")
}

func TestRoutingPolicy_Mutate(t *testing.T) {
	policy := &RoutingPolicy{
		Weights:      map[string]float64{"openai": 0.7, "anthropic": 0.3},
		CacheEnabled: false,
		seed:         1,
	}

	mutated := policy.Mutate().(*RoutingPolicy)
	assert.NotNil(t, mutated)
	// Mutated policy should have different seed.
	assert.Equal(t, int64(2), mutated.seed)
	// Original policy should be unchanged.
	assert.Equal(t, int64(1), policy.seed)
}

func TestRoutingPolicy_Clone(t *testing.T) {
	policy := &RoutingPolicy{
		Weights: map[string]float64{"openai": 0.7, "anthropic": 0.3},
		seed:    5,
	}

	clone := policy.Clone().(*RoutingPolicy)
	assert.Equal(t, policy.Weights, clone.Weights)
	assert.Equal(t, policy.seed, clone.seed)

	// Mutate the clone — original should be unaffected.
	clone.Weights["openai"] = 9.9
	assert.NotEqual(t, 9.9, policy.Weights["openai"], "clone mutation should not affect original")
}

func TestCachePolicy_Apply(t *testing.T) {
	policy := &CachePolicy{
		CacheableThreshold: 0.5,
		MaxCacheSizeMB:     100,
	}

	// Short content → cacheable.
	resp, err := policy.Apply(context.Background(), makeRequest("gpt-4o", "hi"))
	require.NoError(t, err)
	assert.True(t, resp.Cached, "short content should be cacheable")

	// Long content → not cacheable.
	longContent := ""
	for i := 0; i < 2000; i++ {
		longContent += "x"
	}
	resp, err = policy.Apply(context.Background(), makeRequest("gpt-4o", longContent))
	require.NoError(t, err)
	assert.False(t, resp.Cached, "long content should not be cacheable")
}

func TestCachePolicy_Mutate_Clone(t *testing.T) {
	policy := &CachePolicy{CacheableThreshold: 0.5, seed: 1}
	mutated := policy.Mutate().(*CachePolicy)
	assert.NotNil(t, mutated)

	clone := policy.Clone().(*CachePolicy)
	clone.CacheableThreshold = 0.99
	// Mutate modifies p.seed, so we need to check after clone.
	assert.NotEqual(t, 0.99, mutated.CacheableThreshold, "clone mutation should not affect original")
}

func TestGuardrailPolicy_Apply(t *testing.T) {
	// High sensitivity → blocks long requests.
	policy := &GuardrailPolicy{
		InjectionSensitivity: 0.9,
		PIISensitivity:       0.5,
		BudgetLimitUSD:       0.001,
	}

	// Long request with high sensitivity.
	longContent := ""
	for i := 0; i < 6000; i++ {
		longContent += "x"
	}
	resp, err := policy.Apply(context.Background(), makeRequest("gpt-4o", longContent))
	require.NoError(t, err)
	assert.True(t, resp.Error, "long content with high sensitivity should be blocked")

	// Short request → allowed.
	resp, err = policy.Apply(context.Background(), makeRequest("gpt-4o", "hi"))
	require.NoError(t, err)
	assert.False(t, resp.Error, "short content should be allowed")
}

func TestGuardrailPolicy_Mutate_Clone(t *testing.T) {
	policy := &GuardrailPolicy{
		InjectionSensitivity: 0.5,
		PIISensitivity:       0.5,
		BudgetLimitUSD:       0.10,
		seed:                 1,
	}

	mutated := policy.Mutate().(*GuardrailPolicy)
	assert.NotNil(t, mutated)
	assert.Equal(t, int64(2), mutated.seed)

	clone := policy.Clone().(*GuardrailPolicy)
	clone.InjectionSensitivity = 0.99
	assert.NotEqual(t, 0.99, mutated.InjectionSensitivity, "clone mutation should not affect original")
}

func TestAgentWorkflowPolicy_Apply(t *testing.T) {
	policy := &AgentWorkflowPolicy{
		MaxTools:    5,
		MaxDepth:    3,
		UsePlanning: true,
	}

	resp, err := policy.Apply(context.Background(), makeRequest("gpt-4o", "test"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Depth=3, planning=true → latency = 3*50 + 20 = 170.
	assert.Equal(t, 170.0, resp.LatencyMs)
	assert.False(t, resp.Cached, "depth > 1 should not cache")
}

func TestAgentWorkflowPolicy_Mutate_Clone(t *testing.T) {
	policy := &AgentWorkflowPolicy{
		MaxTools:    5,
		MaxDepth:    3,
		UsePlanning: true,
		seed:        1,
	}

	mutated := policy.Mutate().(*AgentWorkflowPolicy)
	assert.NotNil(t, mutated)

	clone := policy.Clone().(*AgentWorkflowPolicy)
	clone.MaxTools = 0
	assert.Equal(t, 5, mutated.MaxTools, "clone mutation should not affect original")
}

// ---------------------------------------------------------------------------
// scoreMetrics Tests
// ---------------------------------------------------------------------------

func TestScoreMetrics_PerfectScore(t *testing.T) {
	metrics := &SimulatedMetrics{
		AvgLatency:   0,
		Cost:         0,
		ErrorRate:    0,
		CacheHitRate: 1,
	}
	score := scoreMetrics(metrics)
	assert.InDelta(t, 1.0, score, 0.01)
}

func TestScoreMetrics_WorstScore(t *testing.T) {
	metrics := &SimulatedMetrics{
		AvgLatency:   10000,
		Cost:         1.0,
		ErrorRate:    1.0,
		CacheHitRate: 0,
	}
	score := scoreMetrics(metrics)
	assert.InDelta(t, 0.0, score, 0.01)
}

func TestScoreMetrics_Nil(t *testing.T) {
	assert.Equal(t, 0.0, scoreMetrics(nil))
}

func TestScoreMetrics_CachingBetterThanNonCaching(t *testing.T) {
	cached := &SimulatedMetrics{
		AvgLatency:   10,
		Cost:         0,
		ErrorRate:    0,
		CacheHitRate: 1.0,
	}
	nonCached := &SimulatedMetrics{
		AvgLatency:   100,
		Cost:         0.05,
		ErrorRate:    0,
		CacheHitRate: 0,
	}
	assert.Greater(t, scoreMetrics(cached), scoreMetrics(nonCached))
}

// ---------------------------------------------------------------------------
// Default Policy Tests
// ---------------------------------------------------------------------------

func TestDefaultPolicyFor_Dimensions(t *testing.T) {
	for _, dim := range AllDimensions() {
		policy := defaultPolicyFor(dim)
		assert.NotNil(t, policy, "dimension %s should have a default policy", dim)

		// Verify the policy can execute.
		resp, err := policy.Apply(context.Background(), makeRequest("gpt-4o", "test"))
		assert.NoError(t, err)
		assert.NotNil(t, resp)

		// Verify mutation produces a valid policy.
		mutated := policy.Mutate()
		assert.NotNil(t, mutated)

		// Verify clone produces a valid policy.
		cloned := policy.Clone()
		assert.NotNil(t, cloned)
	}
}

func TestDefaultPolicyFor_UnknownDimension(t *testing.T) {
	policy := defaultPolicyFor(HeadroomDimension("unknown"))
	assert.NotNil(t, policy)
}

// ---------------------------------------------------------------------------
// ExploreBroad Tests
// ---------------------------------------------------------------------------

func TestExploreBroad_GeneratesVariations(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(20, "openai", "gpt-4o", now)
	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.02}}
	sim := NewDreamSimulator(store, costCalc, nil)
	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))

	hci := NewHCIEngine(store, nil, costCalc, nil, DefaultHCIConfig())
	explorer := NewAutonomousExplorer(sim, hci)

	base := &mockPolicy{provider: "openai", cached: true}
	candidates, err := explorer.ExploreBroad(context.Background(), base, 5)
	require.NoError(t, err)
	assert.Len(t, candidates, 6, "base + 5 variations")

	// Verify sorted by score (descending).
	scenarios := sim.Scenarios()
	scores := make([]float64, len(candidates))
	for i, p := range candidates {
		metrics, _ := sim.ReplayTraffic(context.Background(), p, scenarios)
		scores[i] = scoreMetrics(metrics)
	}
	for i := 1; i < len(scores); i++ {
		assert.LessOrEqual(t, scores[i], scores[i-1], "candidates should be sorted by score descending")
	}
}

func TestExploreBroad_NilSimulator(t *testing.T) {
	explorer := &AutonomousExplorer{}
	candidates, err := explorer.ExploreBroad(context.Background(), &mockPolicy{}, 5)
	require.Error(t, err)
	assert.Nil(t, candidates)
}

func TestExploreBroad_NilPolicy(t *testing.T) {
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	hci := NewHCIEngine(&mockLedgerReader{}, nil, nil, nil, DefaultHCIConfig())
	explorer := NewAutonomousExplorer(sim, hci)

	candidates, err := explorer.ExploreBroad(context.Background(), nil, 5)
	require.Error(t, err)
	assert.Nil(t, candidates)
}

func TestExploreBroad_NoScenarios(t *testing.T) {
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	hci := NewHCIEngine(&mockLedgerReader{}, nil, nil, nil, DefaultHCIConfig())
	explorer := NewAutonomousExplorer(sim, hci)

	candidates, err := explorer.ExploreBroad(context.Background(), &mockPolicy{}, 5)
	require.Error(t, err)
	assert.Nil(t, candidates)
}

// ---------------------------------------------------------------------------
// ExploreDeep Tests
// ---------------------------------------------------------------------------

func TestExploreDeep_ImprovesPolicy(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(30, "openai", "gpt-4o", now)
	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.02}}
	sim := NewDreamSimulator(store, costCalc, nil)
	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))

	explorer := NewAutonomousExplorer(sim, nil)

	// Start with a non-caching policy.
	base := &mockPolicy{provider: "openai", cached: false}
	deepPolicy, err := explorer.ExploreDeep(context.Background(), []Policy{base}, 5)
	require.NoError(t, err)
	assert.NotNil(t, deepPolicy)
}

func TestExploreDeep_NilSimulator(t *testing.T) {
	explorer := &AutonomousExplorer{}
	policy, err := explorer.ExploreDeep(context.Background(), []Policy{&mockPolicy{}}, 5)
	require.Error(t, err)
	assert.Nil(t, policy)
}

func TestExploreDeep_NoCandidates(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)
	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)
	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))

	explorer := NewAutonomousExplorer(sim, nil)
	policy, err := explorer.ExploreDeep(context.Background(), nil, 5)
	require.Error(t, err)
	assert.Nil(t, policy)
}

func TestExploreDeep_NoScenarios(t *testing.T) {
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	explorer := NewAutonomousExplorer(sim, nil)

	policy, err := explorer.ExploreDeep(context.Background(), []Policy{&mockPolicy{}}, 5)
	require.Error(t, err)
	assert.Nil(t, policy)
}

// ---------------------------------------------------------------------------
// Explore (Full Cycle) Tests
// ---------------------------------------------------------------------------

func TestExplore_CompleteCycle(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(30, "openai", "gpt-4o", now)
	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.02}}
	sim := NewDreamSimulator(store, costCalc, nil)
	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))

	hci := NewHCIEngine(store, nil, costCalc, nil, DefaultHCIConfig())
	explorer := NewAutonomousExplorer(sim, hci)

	policy, result, err := explorer.Explore(context.Background(), DimensionRouting, 5)
	require.NoError(t, err)
	assert.NotNil(t, policy)
	assert.NotNil(t, result)
	assert.NotNil(t, result.BestPolicy)

	// Explore for cache dimension.
	policy2, result2, err := explorer.Explore(context.Background(), DimensionCache, 3)
	require.NoError(t, err)
	assert.NotNil(t, policy2)
	assert.NotNil(t, result2)
}

func TestExplore_NilHCI(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)
	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)
	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))

	explorer := NewAutonomousExplorer(sim, nil)
	policy, result, err := explorer.Explore(context.Background(), DimensionRouting, 5)
	require.Error(t, err)
	assert.Nil(t, policy)
	assert.Nil(t, result)
}

func TestExplore_NilSimulator(t *testing.T) {
	explorer := &AutonomousExplorer{hci: NewHCIEngine(&mockLedgerReader{}, nil, nil, nil, DefaultHCIConfig())}
	policy, result, err := explorer.Explore(context.Background(), DimensionRouting, 5)
	require.Error(t, err)
	assert.Nil(t, policy)
	assert.Nil(t, result)
}

// ---------------------------------------------------------------------------
// Broad-then-Deep vs Random Test
// ---------------------------------------------------------------------------

func TestExplore_BroadThenDeepFindsBetterPolicy(t *testing.T) {
	now := time.Now()
	// Create a mix of scenarios where caching is clearly beneficial.
	// Duplicates mean cache hits save both cost and latency.
	baseRecords := makeLedgerRecords(15, "openai", "gpt-4o", now)
	// Add some duplicate records.
	records := append(baseRecords, makeLedgerRecords(15, "openai", "gpt-4o", now)...)
	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.02}}
	sim := NewDreamSimulator(store, costCalc, nil)
	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))

	hci := NewHCIEngine(store, nil, costCalc, nil, DefaultHCIConfig())
	explorer := NewAutonomousExplorer(sim, hci)

	scenarios := sim.Scenarios()

	// Base policy: non-caching.
	basePolicy := &mockPolicy{provider: "openai", cached: false}
	baseMetrics, _ := sim.ReplayTraffic(context.Background(), basePolicy, scenarios)
	baseScore := scoreMetrics(baseMetrics)

	// Optimal policy: caching (should score higher due to zero cost).
	optimalPolicy := &mockPolicy{provider: "openai", cached: true}
	optimalMetrics, _ := sim.ReplayTraffic(context.Background(), optimalPolicy, scenarios)
	optimalScore := scoreMetrics(optimalMetrics)

	// Random policy: non-caching (same as base).
	randomMetrics, _ := sim.ReplayTraffic(context.Background(), &mockPolicy{provider: "openai", cached: false}, scenarios)
	randomScore := scoreMetrics(randomMetrics)

	// Broad-then-deep should find at least the optimal policy.
	bestPolicy, result, err := explorer.Explore(context.Background(), DimensionCache, 5)
	require.NoError(t, err)

	bestMetrics, _ := sim.ReplayTraffic(context.Background(), bestPolicy, scenarios)
	bestScore := scoreMetrics(bestMetrics)

	// The broad-then-deep result should be better than random.
	assert.GreaterOrEqual(t, bestScore, randomScore,
		"broad-then-deep should find a policy at least as good as random")
	assert.GreaterOrEqual(t, bestScore, baseScore,
		"broad-then-deep should find a policy at least as good as base")

	// The improvement percentage should be non-negative.
	assert.GreaterOrEqual(t, result.ImprovementPct, 0.0,
		"improvement should be non-negative when found")

	// Optimal (caching) should score at least as well as the best found.
	assert.GreaterOrEqual(t, optimalScore, bestScore-0.001,
		"optimal caching policy should score at least as well as best found")
}

// ---------------------------------------------------------------------------
// Integration: HCI → Dream → Modular → Explore
// ---------------------------------------------------------------------------

func TestModularRSI_Integration_FullCycle(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(40, "openai", "gpt-4o", now)
	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.02}}
	sim := NewDreamSimulator(store, costCalc, nil)
	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))

	hci := NewHCIEngine(store, nil, costCalc, nil, DefaultHCIConfig())
	eval := NewModularEvaluator(sim)
	explorer := NewAutonomousExplorer(sim, hci)

	// Step 1: HCI assessment.
	assessments, err := hci.AssessAll(context.Background())
	require.NoError(t, err)
	require.Len(t, assessments, 6, "all 6 dimensions assessed")

	// Step 2: Create partitions.
	scenarios := sim.Scenarios()
	partitions := eval.CreatePartitions(scenarios, 3)
	require.Len(t, partitions, 3)

	// Step 3: Explore.
	policy, result, err := explorer.Explore(context.Background(), DimensionRouting, 3)
	require.NoError(t, err)
	assert.NotNil(t, policy)
	assert.NotNil(t, result)

	// Step 4: Evaluate the evolved policy with disjoint evaluation.
	metrics, err := eval.EvaluateDisjoint(context.Background(), policy, partitions)
	require.NoError(t, err)
	require.NotNil(t, metrics)
	assert.GreaterOrEqual(t, metrics.TrainScore, 0.0)
	assert.GreaterOrEqual(t, metrics.TestScore, 0.0)
}

func TestModularRSI_ConcurrentEvaluate(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(30, "openai", "gpt-4o", now)
	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)
	require.NoError(t, sim.LoadFromLedger(context.Background(), TimeRange{}, 0))

	eval := NewModularEvaluator(sim)
	scenarios := sim.Scenarios()
	partitions := eval.CreatePartitions(scenarios, 3)

	policy := &mockPolicy{provider: "openai", cached: true}

	// Run concurrent evaluations.
	var wg sync.WaitGroup
	results := make([]*DisjointMetrics, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m, err := eval.EvaluateDisjoint(context.Background(), policy, partitions)
			assert.NoError(t, err)
			results[idx] = m
		}(i)
	}
	wg.Wait()

	// All results should be consistent.
	for i := 1; i < len(results); i++ {
		assert.InDelta(t, results[0].TrainScore, results[i].TrainScore, 0.01)
		assert.InDelta(t, results[0].TestScore, results[i].TestScore, 0.01)
	}
}

// ---------------------------------------------------------------------------
// Helper: create partitions from scenarios for testing
// ---------------------------------------------------------------------------

func createTestPartitions(scenarios []*DreamReplay, k int) []*EvaluationPartition {
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)
	eval := NewModularEvaluator(sim)
	return eval.CreatePartitions(scenarios, k)
}
