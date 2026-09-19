package rsi

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sequencePolicy returns a predefined sequence of responses, one per Apply call.
// It is used in tests where different scenarios need different policy decisions.
type sequencePolicy struct {
	mu        sync.Mutex
	responses []*Response
	index     int
}

func (p *sequencePolicy) Apply(_ context.Context, _ *models.LLMRequest) (*Response, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.index >= len(p.responses) {
		return &Response{Provider: "default", LatencyMs: 100}, nil
	}
	resp := p.responses[p.index]
	p.index++
	return resp, nil
}

func (p *sequencePolicy) Mutate() Policy {
	return &sequencePolicy{
		responses: p.responses,
	}
}

func (p *sequencePolicy) Clone() Policy {
	return &sequencePolicy{
		responses: p.responses,
	}
}

// alwaysErrorPolicy always returns an error from Apply.
type alwaysErrorPolicy struct{}

func (a *alwaysErrorPolicy) Apply(_ context.Context, _ *models.LLMRequest) (*Response, error) {
	return nil, fmt.Errorf("simulated policy failure")
}

func (a *alwaysErrorPolicy) Mutate() Policy { return &alwaysErrorPolicy{} }
func (a *alwaysErrorPolicy) Clone() Policy  { return &alwaysErrorPolicy{} }

// ---------------------------------------------------------------------------
// LoadFromLedger Tests
// ---------------------------------------------------------------------------

func TestDreamSimulator_LoadFromLedger(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(5, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	require.NoError(t, err)

	assert.Equal(t, 5, sim.ScenarioCount(), "should load all 5 records")

	scenarios := sim.Scenarios()
	require.Len(t, scenarios, 5)

	// Verify first scenario is correctly parsed.
	s := scenarios[0]
	require.NotNil(t, s.Request)
	assert.Equal(t, "gpt-4o", s.Request.Model)
	require.NotNil(t, s.Response)
	assert.Equal(t, "openai", s.Response.Model)
	assert.Equal(t, now.UTC().Format("2006-01-02"), s.Timestamp.UTC().Format("2006-01-02"))
}

func TestDreamSimulator_LoadFromLedgerWithCost(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(3, "openai", "gpt-4o", now)

	costCalc := &mockCostCalculator{
		costs: map[string]float64{"gpt-4o": 0.05},
	}
	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, costCalc, nil)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	require.NoError(t, err)

	scenarios := sim.Scenarios()
	require.Len(t, scenarios, 3)

	// Verify cost is computed in metadata.
	meta := scenarios[0].Metadata
	cost, ok := meta["cost_usd"]
	assert.True(t, ok, "cost_usd should be in metadata")
	assert.InDelta(t, 0.05, cost.(float64), 0.001)

	// Verify provider is in metadata.
	provider, ok := meta["provider"]
	assert.True(t, ok)
	assert.Equal(t, "openai", provider)
}

func TestDreamSimulator_LoadFromLedgerTimeRange(t *testing.T) {
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	// Create records across different time periods.
	records := make([]ledger.LedgerRecord, 6)
	for i := range records {
		ts := base.AddDate(0, 0, i*30) // Spread across months
		req := makeRequest("gpt-4o", fmt.Sprintf("query %d", i))
		resp := makeResponse("openai", "answer", &models.Usage{
			PromptTokens: 50, CompletionTokens: 25, TotalTokens: 75,
		}, nil)
		records[i] = makeLedgerRecord(req, resp, ts)
	}

	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)

	// Filter to June 2024 only (first 2 records: days 0 and 30).
	tr := TimeRange{
		Start: base,
		End:   base.AddDate(0, 0, 15), // End at day 15 → only record 0 matches
	}

	err := sim.LoadFromLedger(context.Background(), tr, 0)
	require.NoError(t, err)

	// Only records within June 1–15 should be loaded.
	assert.LessOrEqual(t, sim.ScenarioCount(), 2)
}

func TestDreamSimulator_LoadFromLedgerSampleSize(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(100, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)

	// Sample down to 10 records.
	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 10)
	require.NoError(t, err)

	assert.Equal(t, 10, sim.ScenarioCount(), "should sample exactly 10 records")
}

func TestDreamSimulator_LoadFromLedgerNilStore(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)
	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	assert.Error(t, err)
}

func TestDreamSimulator_LoadFromLedgerEmpty(t *testing.T) {
	store := &mockLedgerReader{records: []ledger.LedgerRecord{}}
	sim := NewDreamSimulator(store, nil, nil)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	assert.Error(t, err, "should error on empty ledger")
}

func TestDreamSimulator_LoadFromLedgerMalformedRecords(t *testing.T) {
	records := []ledger.LedgerRecord{
		{RequestPayload: "not json", ResponsePayload: "not json", Timestamp: time.Now()},
		{RequestPayload: "not json", ResponsePayload: "also not json", Timestamp: time.Now()},
	}
	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	// LoadFromLedger should succeed (records are skipped, not fatal).
	// But since both records are malformed and skipped, we get 0 scenarios.
	require.NoError(t, err)
	assert.Equal(t, 0, sim.ScenarioCount(), "malformed records should be skipped")
}

// ---------------------------------------------------------------------------
// ReplayTraffic Tests
// ---------------------------------------------------------------------------

func TestDreamSimulator_ReplayTrafficBasic(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(5, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.05}}
	metrics := &mockMetricsProvider{reqCount: 5, avgLat: 100.0}
	sim := NewDreamSimulator(store, costCalc, metrics)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	require.NoError(t, err)

	scenarios := sim.Scenarios()
	require.Len(t, scenarios, 5)

	// Policy: route to provider, no cache, no error, latency=120ms.
	policy := &mockPolicy{
		provider:  "openai",
		latency:   120,
	}

	metrics_result, err := sim.ReplayTraffic(context.Background(), policy, scenarios)
	require.NoError(t, err)
	require.NotNil(t, metrics_result)

	// All 5 requests routed → no cache hits.
	assert.Equal(t, 0.0, metrics_result.CacheHitRate)

	// All 5 should incur cost (5 * 0.05 = 0.25).
	assert.InDelta(t, 0.25, metrics_result.Cost, 0.01)

	// Latency from policy hint = 120ms for all.
	assert.InDelta(t, 120.0, metrics_result.AvgLatency, 0.1)

	// No errors.
	assert.Equal(t, 0.0, metrics_result.ErrorRate)
}

func TestDreamSimulator_ReplayTrafficCached(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(5, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.05}}
	sim := NewDreamSimulator(store, costCalc, nil)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	require.NoError(t, err)

	scenarios := sim.Scenarios()
	require.Len(t, scenarios, 5)

	// Policy: cache everything.
	policy := &mockPolicy{
		cached:  true,
		latency: 1.0, // cache lookup latency
	}

	result, err := sim.ReplayTraffic(context.Background(), policy, scenarios)
	require.NoError(t, err)

	// All cached → 100% cache hit rate, zero cost.
	assert.Equal(t, 1.0, result.CacheHitRate)
	assert.Equal(t, 0.0, result.Cost)
	assert.InDelta(t, 1.0, result.AvgLatency, 0.1) // cache lookup latency
	assert.Equal(t, 0.0, result.ErrorRate)
}

func TestDreamSimulator_ReplayTrafficErrors(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(5, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	require.NoError(t, err)

	scenarios := sim.Scenarios()

	// Policy: error on all requests.
	policy := &mockPolicy{
		errorFlag: true,
	}

	result, err := sim.ReplayTraffic(context.Background(), policy, scenarios)
	require.NoError(t, err)
	require.NotNil(t, result)

	// All errored → 100% error rate, zero cost.
	assert.Equal(t, 1.0, result.ErrorRate)
	assert.Equal(t, 0.0, result.Cost)
}

func TestDreamSimulator_ReplayTrafficMixedDecisions(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.05}}
	metrics := &mockMetricsProvider{avgLat: 100.0}
	sim := NewDreamSimulator(store, costCalc, metrics)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	require.NoError(t, err)

	scenarios := sim.Scenarios()
	require.Len(t, scenarios, 10)

	// Policy: first 3 cached, next 4 routed (error=false), last 3 errored.
	// Using sequencePolicy to return different decisions per call.
	policy := &sequencePolicy{
		responses: []*Response{
			{Cached: true, LatencyMs: 1.0}, // cache
			{Cached: true, LatencyMs: 1.0}, // cache
			{Cached: true, LatencyMs: 1.0}, // cache
			{Provider: "openai", LatencyMs: 100}, // route
			{Provider: "openai", LatencyMs: 100}, // route
			{Provider: "openai", LatencyMs: 100}, // route
			{Provider: "openai", LatencyMs: 100}, // route
			{Error: true, LatencyMs: 50},         // error
			{Error: true, LatencyMs: 50},         // error
			{Error: true, LatencyMs: 50},         // error
		},
	}

	result, err := sim.ReplayTraffic(context.Background(), policy, scenarios)
	require.NoError(t, err)
	require.NotNil(t, result)

	// 3 cache hits / 10 = 0.3
	assert.InDelta(t, 0.3, result.CacheHitRate, 0.01)

	// 3 errors / 10 = 0.3
	assert.InDelta(t, 0.3, result.ErrorRate, 0.01)

	// 4 routed requests with cost 0.05 each = 0.20 total
	assert.InDelta(t, 0.20, result.Cost, 0.01)

	// 3 cached (1ms) + 4 routed (100ms) + 3 errored (50ms) = 3+400+150 = 553ms total
	// avg = 553/10 = 55.3ms
	expectedAvg := (3*1.0 + 4*100 + 3*50) / 10.0
	assert.InDelta(t, expectedAvg, result.AvgLatency, 1.0)
}

func TestDreamSimulator_ReplayTrafficEmpty(t *testing.T) {
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)

	result, err := sim.ReplayTraffic(context.Background(), &mockPolicy{}, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Empty scenarios → zero-value metrics.
	assert.Equal(t, 0.0, result.AvgLatency)
	assert.Equal(t, 0.0, result.Cost)
	assert.Equal(t, 0.0, result.ErrorRate)
	assert.Equal(t, 0.0, result.CacheHitRate)
}

func TestDreamSimulator_ReplayTrafficNilPolicy(t *testing.T) {
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)

	now := time.Now()
	scenarios := make([]*DreamReplay, 3)
	for i := range scenarios {
		req := makeRequest("gpt-4o", fmt.Sprintf("query %d", i))
		resp := makeResponse("openai", "answer", nil, nil)
		scenarios[i] = &DreamReplay{Request: req, Response: resp, Timestamp: now}
	}

	_, err := sim.ReplayTraffic(context.Background(), nil, scenarios)
	assert.Error(t, err, "should error with nil policy")
}

func TestDreamSimulator_ReplayTrafficPolicyError(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(5, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	require.NoError(t, err)

	scenarios := sim.Scenarios()
	policy := &errorPolicy{}

	result, err := sim.ReplayTraffic(context.Background(), policy, scenarios)
	require.NoError(t, err)

	// All policy calls error → all counted as errors.
	assert.InDelta(t, 1.0, result.ErrorRate, 0.01)
}

func TestDreamSimulator_ReplayTrafficContextCancellation(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(100, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, nil, nil)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 100)
	require.NoError(t, err)

	scenarios := sim.Scenarios()
	require.Len(t, scenarios, 100)

	// Create a context that is already cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	policy := &mockPolicy{provider: "openai"}
	_, err = sim.ReplayTraffic(ctx, policy, scenarios)
	assert.Error(t, err, "should error on cancelled context")
}

func TestDreamSimulator_ReplayTrafficP99Latency(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(100, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	metrics := &mockMetricsProvider{avgLat: 100.0}
	sim := NewDreamSimulator(store, nil, metrics)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 100)
	require.NoError(t, err)

	scenarios := sim.Scenarios()

	// Policy with varying latencies: first 99 are fast (50ms), last 1 is slow (500ms).
	decisions := make([]*Response, 100)
	for i := 0; i < 99; i++ {
		decisions[i] = &Response{Provider: "openai", LatencyMs: 50}
	}
	decisions[99] = &Response{Provider: "openai", LatencyMs: 500}

	policy := &sequencePolicy{responses: decisions}

	result, err := sim.ReplayTraffic(context.Background(), policy, scenarios)
	require.NoError(t, err)

	// P99 should be close to the 99th percentile.
	// Sorted: 99 values of 50, 1 value of 500.
	// P99 = 50*0.99 + 500*0.01 = 54.5 (linear interpolation)
	// Avg = (50*99 + 500)/100 = 5450/100 = 54.5
	assert.InDelta(t, 54.5, result.P99Latency, 5.0)
	assert.InDelta(t, 54.5, result.AvgLatency, 1.0)
}

// ---------------------------------------------------------------------------
// ReplayLoaded Tests
// ---------------------------------------------------------------------------

func TestDreamSimulator_ReplayLoaded(t *testing.T) {
	now := time.Now()
	records := makeLedgerRecords(10, "openai", "gpt-4o", now)

	store := &mockLedgerReader{records: records}
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.02}}
	sim := NewDreamSimulator(store, costCalc, nil)

	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	require.NoError(t, err)

	policy := &mockPolicy{
		provider: "openai",
		latency:  80,
	}

	result, err := sim.ReplayLoaded(context.Background(), policy)
	require.NoError(t, err)
	require.NotNil(t, result)

	// 10 requests, all routed → 10 * 0.02 = 0.20 cost
	assert.InDelta(t, 0.20, result.Cost, 0.01)
	assert.InDelta(t, 80.0, result.AvgLatency, 1.0)
}

func TestDreamSimulator_ReplayLoadedNoScenarios(t *testing.T) {
	sim := NewDreamSimulator(&mockLedgerReader{}, nil, nil)

	policy := &mockPolicy{}
	_, err := sim.ReplayLoaded(context.Background(), policy)
	assert.Error(t, err, "should error when no scenarios are loaded")
}

// ---------------------------------------------------------------------------
// Simulate outcome tests
// ---------------------------------------------------------------------------

func TestDreamSimulator_SimulateOutcomeCached(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)
	decision := &Response{Cached: true, LatencyMs: 2.0}
	scenario := &DreamReplay{Request: makeRequest("gpt-4o", "test")}

	latency, cost := sim.simulateOutcome(decision, scenario)
	assert.Equal(t, 2.0, latency) // uses policy's latency hint
	assert.Equal(t, 0.0, cost)    // cached = zero cost
}

func TestDreamSimulator_SimulateOutcomeCachedDefaultLatency(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)
	decision := &Response{Cached: true} // no latency hint
	scenario := &DreamReplay{Request: makeRequest("gpt-4o", "test")}

	latency, cost := sim.simulateOutcome(decision, scenario)
	assert.Equal(t, 1.0, latency, "default cache latency should be 1ms")
	assert.Equal(t, 0.0, cost)
}

func TestDreamSimulator_SimulateOutcomeError(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)
	decision := &Response{Error: true, LatencyMs: 50.0}
	scenario := &DreamReplay{Request: makeRequest("gpt-4o", "test")}

	latency, cost := sim.simulateOutcome(decision, scenario)
	assert.Equal(t, 50.0, latency)
	assert.Equal(t, 0.0, cost) // error = zero cost
}

func TestDreamSimulator_SimulateOutcomeRoutedWithCostCalc(t *testing.T) {
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.05}}
	metrics := &mockMetricsProvider{avgLat: 120.0}
	sim := NewDreamSimulator(nil, costCalc, metrics)

	resp := makeResponse("openai", "answer", &models.Usage{
		PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150,
	}, nil)
	scenario := &DreamReplay{
		Request:  makeRequest("gpt-4o", "test"),
		Response: resp,
	}

	decision := &Response{Provider: "openai"} // no latency hint
	latency, cost := sim.simulateOutcome(decision, scenario)

	assert.Equal(t, 0.05, cost, "cost should come from cost calculator")
	assert.Equal(t, 120.0, latency, "latency should come from trace metrics")
}

func TestDreamSimulator_SimulateOutcomeRoutedNoDeps(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)

	resp := makeResponse("openai", "answer", &models.Usage{
		PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150,
	}, nil)
	scenario := &DreamReplay{
		Request:  makeRequest("gpt-4o", "test"),
		Response: resp,
	}

	decision := &Response{Provider: "openai"}
	latency, cost := sim.simulateOutcome(decision, scenario)
	assert.Equal(t, 0.0, cost, "no cost calculator → cost is 0")
	assert.Equal(t, 0.0, latency, "no metrics or metadata → latency is 0")
}

func TestDreamSimulator_SimulateOutcomeLatencyFromMetadata(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)

	resp := makeResponse("openai", "answer", &models.Usage{
		PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150,
	}, nil)
	scenario := &DreamReplay{
		Request:  makeRequest("gpt-4o", "test"),
		Response: resp,
		Metadata: map[string]interface{}{"latency_ms": 250.0},
	}

	decision := &Response{Provider: "openai"}
	latency, _ := sim.simulateOutcome(decision, scenario)
	assert.Equal(t, 250.0, latency, "latency should come from metadata when no metrics provider")
}

// ---------------------------------------------------------------------------
// recordToReplay Tests
// ---------------------------------------------------------------------------

func TestDreamSimulator_RecordToReplay(t *testing.T) {
	now := time.Now()
	costCalc := &mockCostCalculator{costs: map[string]float64{"gpt-4o": 0.10}}
	metrics := &mockMetricsProvider{avgLat: 100.0}
	sim := NewDreamSimulator(nil, costCalc, metrics)

	req := makeRequest("gpt-4o", "test query")
	resp := makeResponse("openai", "test answer", &models.Usage{
		PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150,
	}, nil)

	rec := makeLedgerRecord(req, resp, now)
	replay, err := sim.recordToReplay(&rec)
	require.NoError(t, err)
	require.NotNil(t, replay)

	assert.Equal(t, "gpt-4o", replay.Request.Model)
	assert.Equal(t, "openai", replay.Response.Model)
	assert.Equal(t, now.UTC(), replay.Timestamp.UTC())

	// Verify metadata.
	assert.Equal(t, "openai", replay.Metadata["provider"])
	assert.Equal(t, 100, replay.Metadata["prompt_tokens"])
	assert.Equal(t, 50, replay.Metadata["completion_tokens"])
	assert.Equal(t, 150, replay.Metadata["total_tokens"])
	assert.InDelta(t, 0.10, replay.Metadata["cost_usd"], 0.001)
	assert.Equal(t, 100.0, replay.Metadata["latency_ms"])
}

func TestDreamSimulator_RecordToReplayNoUsage(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)
	req := makeRequest("gpt-4o", "test")
	resp := makeResponse("openai", "answer", nil, nil) // no usage
	rec := makeLedgerRecord(req, resp, time.Now())

	replay, err := sim.recordToReplay(&rec)
	require.NoError(t, err)
	require.NotNil(t, replay)
	assert.Equal(t, 0.0, replay.Metadata["cost_usd"])
}

func TestDreamSimulator_RecordToReplayBothNil(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)
	rec := ledger.LedgerRecord{
		RequestPayload:  "not json",
		ResponsePayload: "not json",
		Timestamp:       time.Now(),
	}

	replay, err := sim.recordToReplay(&rec)
	// Both payloads unparseable → should still return a replay with empty structs.
	// Wait — parseRequest and parseResponse return nil for unparseable JSON.
	// recordToReplay checks if both are nil and returns an error.
	require.Error(t, err)
	assert.Nil(t, replay)
}

func TestDreamSimulator_RecordToReplayToolCalls(t *testing.T) {
	sim := NewDreamSimulator(nil, nil, nil)

	toolCall := models.ToolCall{
		ID:   "call_1",
		Type: "function",
		Function: models.ToolFunction{
			Name:      "calculator",
			Arguments: `{"x": 1}`,
		},
	}

	req := makeRequest("gpt-4o", "calculate")
	resp := makeResponse("openai", "", nil, []models.ToolCall{toolCall})
	rec := makeLedgerRecord(req, resp, time.Now())

	replay, err := sim.recordToReplay(&rec)
	require.NoError(t, err)
	require.NotNil(t, replay)

	tools, ok := replay.Metadata["tools_used"].([]string)
	assert.True(t, ok)
	assert.Contains(t, tools, "calculator")
	assert.Equal(t, 1, replay.Metadata["tool_call_count"])
}

// ---------------------------------------------------------------------------
// Integration: Load → Replay flow
// ---------------------------------------------------------------------------

func TestDreamSimulator_Integration_LoadReplayEvaluate(t *testing.T) {
	now := time.Now()

	// Create a mixed ledger: some duplicates, different providers, tool calls.
	req1 := makeRequest("gpt-4o", "what is 2+2?")
	req1Dup := makeRequest("gpt-4o", "what is 2+2?") // duplicate for cache

	toolCall := models.ToolCall{
		ID:   "call_1",
		Type: "function",
		Function: models.ToolFunction{
			Name:      "calculator",
			Arguments: `{"x": 1}`,
		},
	}
	resp1 := makeResponse("openai", "", &models.Usage{
		PromptTokens: 50, CompletionTokens: 25, TotalTokens: 75,
	}, []models.ToolCall{toolCall})
	resp2 := makeResponse("anthropic", "4", &models.Usage{
		PromptTokens: 50, CompletionTokens: 25, TotalTokens: 75,
	}, nil)

	records := []ledger.LedgerRecord{
		makeLedgerRecord(req1, resp1, now),
		makeLedgerRecord(req1Dup, resp2, now), // duplicate request, different provider
	}
	records = append(records, makeLedgerRecords(8, "openai", "gpt-4o", now)...)

	costCalc := &mockCostCalculator{costs: map[string]float64{
		"gpt-4o": 0.03,
	}}
	metrics := &mockMetricsProvider{avgLat: 90.0}
	store := &mockLedgerReader{records: records}
	sim := NewDreamSimulator(store, costCalc, metrics)

	// Step 1: Load scenarios from ledger.
	err := sim.LoadFromLedger(context.Background(), TimeRange{}, 0)
	require.NoError(t, err)
	assert.Equal(t, 10, sim.ScenarioCount())

	// Step 2: Evaluate a routing policy (route to cheapest, cache aggressively).
	policy := &mockPolicy{
		provider: "openai",
		cached:   true,
		latency:  2.0, // cache latency
	}

	result, err := sim.ReplayLoaded(context.Background(), policy)
	require.NoError(t, err)
	require.NotNil(t, result)

	// All cached → 100% hit rate, zero cost.
	assert.Equal(t, 1.0, result.CacheHitRate)
	assert.Equal(t, 0.0, result.Cost)
	assert.Equal(t, 0.0, result.ErrorRate)

	// Step 3: Evaluate a non-caching routing policy.
	policy2 := &mockPolicy{
		provider: "openai",
		latency:  90,
	}
	result2, err := sim.ReplayLoaded(context.Background(), policy2)
	require.NoError(t, err)

	assert.Equal(t, 0.0, result2.CacheHitRate)
	assert.InDelta(t, 0.30, result2.Cost, 0.01) // 10 * 0.03 = 0.30
	assert.Equal(t, 0.0, result2.ErrorRate)

	// Caching policy is clearly better: zero cost, near-zero latency.
	assert.Less(t, result.Cost, result2.Cost, "cached policy should be cheaper")
	assert.Less(t, result.AvgLatency, result2.AvgLatency, "cached policy should be faster")
}

// ---------------------------------------------------------------------------
// Interface compliance
// ---------------------------------------------------------------------------

var _ Policy = (*sequencePolicy)(nil)
var _ Policy = (*alwaysErrorPolicy)(nil)
var _ LedgerReader = (*mockLedgerReader)(nil)
var _ MetricsProvider = (*mockMetricsProvider)(nil)
var _ CostCalculator = (*mockCostCalculator)(nil)
