package rsi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ---------------------------------------------------------------------------
// Dream-RSI: Discovery History as a Replay Simulator
// ---------------------------------------------------------------------------
//
// The DreamSimulator implements the core Dream-RSI paradigm: "a fast and
// inexpensive simulator of discovery would allow many exploration policies
// to be evaluated before costly online deployment." Completed discovery
// histories (ledger records) serve as replay worlds — grounded models of
// the search space already observed. Alternative policies navigate the
// recorded tree deterministically, evaluating routing, caching, and guardrail
// decisions without rerunning the underlying LLM provider.
//
// Key design principle: NO real LLM calls are made during replay. Every
// outcome is derived from recorded request/response pairs stored in the
// ledger. This transforms meta-policy improvement from expensive online
// trial-and-error into fast, simulation-based "dreaming."

// DreamReplay is a single recorded request/response pair loaded from the
// ledger. Each replay represents one node in the discovery tree — a
// generation–evaluation attempt with its full observation.
//
// In Dream-RSI terms, a DreamReplay is a leaf in the discovery tree: it
// captures the initial workspace state (Request), the realized outcome
// (Response), and metadata (cost, latency, provider, tools used) that
// allows the simulator to estimate alternative outcomes under different
// policy decisions.
type DreamReplay struct {
	// Request is the original LLM request that was sent to a provider.
	Request *models.LLMRequest `json:"request"`

	// Response is the recorded LLM response returned by the provider.
	// The simulator replays this response for any policy decision,
	// adjusting cost and latency estimates as needed.
	Response *models.LLMResponse `json:"response"`

	// Metadata carries per-request context extracted from the ledger
	// record: latency (ms), cost (USD), provider name, and tools used.
	// This enables the simulator to reconstruct the original execution
	// environment without re-invoking any provider.
	Metadata map[string]interface{} `json:"metadata"`

	// Timestamp is when the original request was processed. Used for
	// time-range filtering and temporal analysis.
	Timestamp time.Time `json:"timestamp"`
}

// SimulatedMetrics is the aggregate outcome of replaying a policy against
// a set of DreamReplay scenarios. These metrics mirror what you would
// observe from a live policy deployment, but computed entirely from
// recorded data — zero provider API calls.
type SimulatedMetrics struct {
	// AvgLatency is the average request latency in milliseconds across
	// the replayed scenarios that produced an outcome.
	AvgLatency float64 `json:"avg_latency_ms"`

	// P99Latency is the 99th-percentile request latency in milliseconds,
	// capturing tail-latency behavior under the simulated policy.
	P99Latency float64 `json:"p99_latency_ms"`

	// Cost is the total simulated cost in USD for all replayed requests.
	// Cache hits contribute zero cost; non-cached requests use per-model
	// pricing from the CostCalculator.
	Cost float64 `json:"cost_usd"`

	// ErrorRate is the fraction of replayed requests that resulted in
	// errors (policy decision errors or provider failures).
	ErrorRate float64 `json:"error_rate"`

	// CacheHitRate is the fraction of requests served from cache
	// under the simulated policy.
	CacheHitRate float64 `json:"cache_hit_rate"`

	// Requests is the number of scenarios replayed. It lets scorers
	// normalise Cost to a per-request figure so that scores do not depend on
	// how many scenarios happened to be replayed. Zero means "unknown"
	// (hand-built metrics), in which case Cost is treated as per-request.
	Requests int `json:"requests"`
}

// ScenarioOutcome is the simulated outcome of one scenario under a policy.
// Per-scenario outcomes let the orchestrator run paired statistical tests
// between a baseline and a candidate policy on the same scenarios.
type ScenarioOutcome struct {
	LatencyMs float64 `json:"latency_ms"`
	CostUSD   float64 `json:"cost_usd"`
	Cached    bool    `json:"cached"`
	Error     bool    `json:"error"`
	// Skipped is true for nil scenarios, which are excluded from all
	// aggregates.
	Skipped bool `json:"skipped,omitempty"`
	// Score is the per-scenario composite score in [0,1] (see scoreOutcome).
	Score float64 `json:"score"`
}

// MaxReplayScenarios bounds how many scenarios LoadFromLedger keeps. When the
// ledger holds more matching records (and no smaller sampleSize is given),
// records are down-sampled deterministically. This keeps memory and replay
// CPU bounded no matter how large the ledger grows.
const MaxReplayScenarios = 50000

// ErrNoScenarios is returned by LoadFromLedger when no usable scenarios could
// be built (empty ledger, empty time range, or only unparseable records).
var ErrNoScenarios = errors.New("dream: no replayable scenarios")

// DreamSimulator is the replay-based policy evaluation engine.
//
// It loads historical request/response pairs from the ledger into
// DreamReplay scenarios, then evaluates policies by simulating their
// routing, caching, and guardrail decisions against these recorded
// scenarios. No real LLM calls are made during replay.
//
// The simulator depends on:
//   - LedgerReader: for loading historical scenarios
//   - CostCalculator: for estimating costs of alternative routing decisions
//   - MetricsProvider: for latency baselines (optional)
type DreamSimulator struct {
	mu        sync.RWMutex
	ledger    LedgerReader
	costCalc  CostCalculator
	metrics   MetricsProvider
	scenarios []*DreamReplay
	loaded    bool

	// providers, when set and non-empty, lists the providers that actually
	// exist. A non-cached decision routed to any other provider is simulated
	// as an error: RSI must never "discover" a policy that routes traffic to
	// a provider the gateway does not have.
	providers func() []string
}

// NewDreamSimulator creates a new DreamSimulator with the given dependencies.
//
// Parameters:
//   - store: ledger reader for loading historical scenarios (required)
//   - costCalc: cost calculator for cost estimation during replay (optional)
//   - metrics: trace metrics provider for latency baselines (optional)
func NewDreamSimulator(store LedgerReader, costCalc CostCalculator, metrics MetricsProvider) *DreamSimulator {
	return &DreamSimulator{
		ledger:   store,
		costCalc: costCalc,
		metrics:  metrics,
	}
}

// SetProviderFilter installs a function returning the names of providers
// that are currently available. When it returns a non-empty list, replayed
// decisions that route (non-cached) traffic to an unknown provider are
// counted as errors. Pass nil to disable the check.
func (s *DreamSimulator) SetProviderFilter(fn func() []string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.providers = fn
	s.mu.Unlock()
}

// LoadFromLedger populates the simulator's replay scenarios from the ledger.
//
// This is the "Construct Replay Simulator" stage of Dream-RSI: recorded
// discovery history is converted into a reusable replay pool. The method:
//  1. Reads all records from the ledger store.
//  2. Filters records by the given TimeRange (if non-zero).
//  3. Samples down to sampleSize records if specified (deterministic
//     stride-based sampling preserves temporal ordering).
//  4. Parses each record's RequestPayload and ResponsePayload into
//     typed DreamReplay structs with extracted metadata.
//
// If sampleSize is 0 or negative, all matching records are loaded, capped at
// MaxReplayScenarios. The loaded scenarios are cached internally and can be
// retrieved via Scenarios() for use with ReplayTraffic. When no usable
// scenario results, the pool is cleared and an error wrapping ErrNoScenarios
// is returned.
func (s *DreamSimulator) LoadFromLedger(ctx context.Context, timeRange TimeRange, sampleSize int) error {
	if s == nil {
		return fmt.Errorf("dream: simulator is nil")
	}
	if s.ledger == nil {
		return fmt.Errorf("dream: ledger reader is nil")
	}

	// Select the context for cancellation checks.
	if ctx == nil {
		ctx = context.Background()
	}

	records, err := s.ledger.All(ctx)
	if err != nil {
		return fmt.Errorf("dream: failed to read ledger: %w", err)
	}

	// Filter by time range.
	filtered := make([]ledger.LedgerRecord, 0, len(records))
	for _, rec := range records {
		if timeRange.Contains(rec.Timestamp) {
			filtered = append(filtered, rec)
		}
	}

	if len(filtered) == 0 {
		s.setScenarios(nil)
		return fmt.Errorf("%w: no ledger records match the given time range", ErrNoScenarios)
	}

	// Deterministic stride-based sampling to preserve temporal ordering.
	if sampleSize <= 0 || sampleSize > MaxReplayScenarios {
		sampleSize = MaxReplayScenarios
	}
	sampled := sampleRecords(filtered, sampleSize)

	// Parse records into DreamReplay scenarios.
	scenarios := make([]*DreamReplay, 0, len(sampled))
	for i := range sampled {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("dream: loading cancelled: %w", err)
			}
		}
		scenario, err := s.recordToReplay(&sampled[i])
		if err != nil {
			// Skip unparseable records but continue processing the rest.
			continue
		}
		scenarios = append(scenarios, scenario)
	}

	s.setScenarios(scenarios)
	if len(scenarios) == 0 {
		return fmt.Errorf("%w: all %d ledger records were unparseable", ErrNoScenarios, len(sampled))
	}
	return nil
}

func (s *DreamSimulator) setScenarios(scenarios []*DreamReplay) {
	s.mu.Lock()
	s.scenarios = scenarios
	s.loaded = true
	s.mu.Unlock()
}

// Scenarios returns the most recently loaded replay scenarios.
// Returns nil if LoadFromLedger has not been called or failed. The returned
// slice is a copy; the scenarios themselves are shared and must be treated
// as read-only.
func (s *DreamSimulator) Scenarios() []*DreamReplay {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.scenarios == nil {
		return nil
	}
	out := make([]*DreamReplay, len(s.scenarios))
	copy(out, s.scenarios)
	return out
}

// ScenarioCount returns the number of loaded replay scenarios.
func (s *DreamSimulator) ScenarioCount() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.scenarios)
}

// ReplayTraffic evaluates a policy against a set of DreamReplay scenarios
// by simulating routing, caching, and guardrail decisions.
//
// This is the "Dreaming-based Policy Improvement" stage of Dream-RSI:
// thousands of candidate policies can be tested within the replay
// simulator using pre-stored outcomes, without incurring any LLM API cost.
//
// The simulation proceeds as follows for each scenario:
//  1. The policy's Apply method is called with the scenario's request,
//     yielding a decision Response (provider selection, cache decision,
//     latency/cost hints, error flag).
//  2. If the policy signals an error (or routes to an unavailable provider,
//     see SetProviderFilter): the request is counted as failed.
//  3. If the policy decides to cache: the simulated latency is reduced to
//     near-zero (cache lookup) and the cost is zero (no LLM call).
//  4. Otherwise the cost is computed from the recorded token usage and the
//     CostCalculator (or the policy's estimate when it routes to a different
//     provider than the one recorded), and the latency is taken from the
//     policy's hint, the recorded per-request latency, or the trace baseline.
//
// The simulator MUST NOT make real LLM calls. All outcomes are derived
// from the recorded Request/Response pairs and the injected dependencies
// (cost calculator, trace metrics). This is the fundamental guarantee
// that enables fast, zero-execution-cost off-policy evaluation.
func (s *DreamSimulator) ReplayTraffic(
	ctx context.Context,
	policy Policy,
	scenarios []*DreamReplay,
) (*SimulatedMetrics, error) {
	m, _, err := s.ReplayDetailed(ctx, policy, scenarios)
	return m, err
}

// ReplayDetailed is ReplayTraffic that also returns one ScenarioOutcome per
// input scenario (same order; nil scenarios are marked Skipped).
func (s *DreamSimulator) ReplayDetailed(
	ctx context.Context,
	policy Policy,
	scenarios []*DreamReplay,
) (*SimulatedMetrics, []ScenarioOutcome, error) {
	if s == nil {
		return nil, nil, fmt.Errorf("dream: simulator is nil")
	}
	if policy == nil {
		return nil, nil, fmt.Errorf("dream: policy cannot be nil")
	}
	if len(scenarios) == 0 {
		// Return zero-value metrics rather than an error — an empty
		// scenario set is a valid (if uninformative) evaluation result.
		return &SimulatedMetrics{}, nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	available := s.availableProviders()

	outcomes := make([]ScenarioOutcome, len(scenarios))
	// Collect latencies for percentile computation.
	latencies := make([]float64, 0, len(scenarios))

	var (
		totalCost     float64
		errorCount    int
		cacheHitCount int
		total         int
	)

	for i, scenario := range scenarios {
		// Check for context cancellation between scenarios.
		if err := ctx.Err(); err != nil {
			return nil, nil, fmt.Errorf("dream: replay cancelled: %w", err)
		}
		if scenario == nil {
			outcomes[i] = ScenarioOutcome{Skipped: true}
			continue
		}
		total++

		// Apply the policy to the scenario's request.
		// The policy returns a decision: which provider to route to,
		// whether to cache, and hints about expected latency/cost.
		decision, err := policy.Apply(ctx, scenario.Request)
		if err != nil || decision == nil {
			errorCount++
			outcomes[i] = ScenarioOutcome{Error: true}
			continue
		}

		// Simulate the outcome based on the policy decision.
		out := s.outcomeFor(decision, scenario, available)
		outcomes[i] = out
		latencies = append(latencies, out.LatencyMs)
		totalCost += out.CostUSD
		if out.Error {
			errorCount++
		}
		if out.Cached {
			cacheHitCount++
		}
	}

	if total == 0 {
		return &SimulatedMetrics{}, outcomes, nil
	}
	errorRate := float64(errorCount) / float64(total)
	cacheHitRate := float64(cacheHitCount) / float64(total)

	// Sort latencies for percentile computation.
	sort.Float64s(latencies)

	avgLatency := 0.0
	for _, l := range latencies {
		avgLatency += l
	}
	if len(latencies) > 0 {
		// Average over the scenarios that actually produced a latency;
		// dividing by all scenarios would bias the mean towards zero
		// whenever a policy fails outright.
		avgLatency /= float64(len(latencies))
	}

	p99Latency := percentile(latencies, 0.99)

	return &SimulatedMetrics{
		AvgLatency:   avgLatency,
		P99Latency:   p99Latency,
		Cost:         totalCost,
		ErrorRate:    errorRate,
		CacheHitRate: cacheHitRate,
		Requests:     total,
	}, outcomes, nil
}

// ReplayLoaded is a convenience method that evaluates a policy against
// the scenarios previously loaded via LoadFromLedger. This is equivalent
// to calling ReplayTraffic with Scenarios() as the scenarios argument,
// but avoids holding the mutex across the entire replay loop.
func (s *DreamSimulator) ReplayLoaded(ctx context.Context, policy Policy) (*SimulatedMetrics, error) {
	scenarios := s.Scenarios()
	if scenarios == nil {
		return nil, fmt.Errorf("dream: no scenarios loaded; call LoadFromLedger first")
	}
	return s.ReplayTraffic(ctx, policy, scenarios)
}

func (s *DreamSimulator) availableProviders() map[string]struct{} {
	s.mu.RLock()
	fn := s.providers
	s.mu.RUnlock()
	if fn == nil {
		return nil
	}
	names := fn()
	if len(names) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	return set
}

// ---------------------------------------------------------------------------
// Internal: scenario construction and outcome simulation
// ---------------------------------------------------------------------------

// recordToReplay converts a ledger.LedgerRecord into a DreamReplay by
// parsing the JSON-encoded RequestPayload and ResponsePayload and
// extracting metadata for the simulation.
func (s *DreamSimulator) recordToReplay(rec *ledger.LedgerRecord) (*DreamReplay, error) {
	req := parseRequest(rec.RequestPayload)
	resp := parseResponse(rec.ResponsePayload)
	if req == nil && resp == nil {
		return nil, fmt.Errorf("dream: record has unparseable payload at %s", rec.Timestamp)
	}
	if req == nil {
		req = &models.LLMRequest{}
	}
	if resp == nil {
		resp = &models.LLMResponse{}
	}

	// Build metadata from the response and ledger record.
	meta := make(map[string]interface{})

	// Provider name from the recorded response (resp.Model is set to
	// the provider name by the API handler, not the LLM model name).
	meta["provider"] = resp.Model

	// Extract token usage for cost computation.
	if resp.Usage != nil {
		meta["prompt_tokens"] = resp.Usage.PromptTokens
		meta["completion_tokens"] = resp.Usage.CompletionTokens
		meta["total_tokens"] = resp.Usage.TotalTokens

		// Compute the actual cost that was incurred, if a cost
		// calculator is available. The request's Model field holds
		// the LLM model name used for pricing.
		if s.costCalc != nil {
			meta["cost_usd"] = sanitizeNonNegative(s.costCalc.CalculateCost(req.Model, resp.Usage))
		}
	}

	// Extract tool usage information.
	toolsUsed := extractToolsUsed(resp)
	if len(toolsUsed) > 0 {
		meta["tools_used"] = toolsUsed
	}
	meta["tool_call_count"] = countToolCalls(resp)

	// Extract any latency or cost data already present in the ledger
	// record's metadata (may be populated by middleware or instrumentation).
	if rec.Metadata != nil {
		for k, v := range rec.Metadata {
			if _, exists := meta[k]; !exists {
				meta[k] = v
			}
		}
	}

	// If no latency was found in metadata, populate from the trace provider
	// baseline. This is an approximation — the trace AvgLatency is a global
	// average, not per-request.
	if _, ok := metadataLatency(meta); !ok {
		delete(meta, "latency_ms") // drop unusable values such as "NaN"
		if base := s.baselineLatency(); base > 0 {
			meta["latency_ms"] = base
		}
	}

	// If no cost was computed, default to 0.
	if _, ok := meta["cost_usd"]; !ok {
		meta["cost_usd"] = 0.0
	}

	return &DreamReplay{
		Request:   req,
		Response:  resp,
		Metadata:  meta,
		Timestamp: rec.Timestamp,
	}, nil
}

// simulateOutcome computes the simulated latency and cost for a single
// scenario based on the policy's decision and the recorded replay data.
//
// Decision logic:
//   - Error=true   → latency = decision hint (or baseline), cost = 0
//   - Cached=true  → latency ≈ cache lookup cost, cost = 0 (no LLM call)
//   - Otherwise    → cost = recorded cost for the request model (or the
//     policy's estimate when it routes to a different provider);
//     latency = decision hint, recorded per-request latency, or trace baseline
//
// The method never invokes a real provider. All cost estimates use the
// CostCalculator against recorded token usage, and all latency estimates
// use pre-computed values from ledger metadata or trace metrics.
func (s *DreamSimulator) simulateOutcome(decision *Response, scenario *DreamReplay) (latency float64, cost float64) {
	out := s.outcomeFor(decision, scenario, nil)
	return out.LatencyMs, out.CostUSD
}

// outcomeFor simulates one scenario. available, when non-nil, is the set of
// providers that exist; routing elsewhere is simulated as an error.
func (s *DreamSimulator) outcomeFor(decision *Response, scenario *DreamReplay, available map[string]struct{}) ScenarioOutcome {
	if decision == nil {
		return ScenarioOutcome{Error: true}
	}
	hint := sanitizeNonNegative(decision.LatencyMs)

	isError := decision.Error
	if !isError && !decision.Cached && available != nil && decision.Provider != "" {
		if _, ok := available[decision.Provider]; !ok {
			isError = true
		}
	}

	// Error: no cost (request failed before reaching the provider).
	if isError {
		latency := hint
		if latency <= 0 {
			latency = s.baselineLatency()
		}
		out := ScenarioOutcome{LatencyMs: latency, Error: true}
		out.Score = scoreOutcome(out)
		return out
	}

	// Cache hit: near-zero latency, zero marginal cost.
	if decision.Cached {
		// Use the policy's latency hint if provided (represents cache
		// lookup time, typically < 1ms), otherwise default to 1ms.
		latency := hint
		if latency <= 0 {
			latency = 1.0
		}
		out := ScenarioOutcome{LatencyMs: latency, Cached: true}
		out.Score = scoreOutcome(out)
		return out
	}

	var cost float64
	recorded := recordedProvider(scenario)
	policyEstimate := sanitizeNonNegative(decision.CostUSD)
	switch {
	case decision.Provider != "" && recorded != "" && decision.Provider != recorded && policyEstimate > 0:
		// Counterfactual routing: the recorded price belongs to a different
		// provider, so the policy's own estimate is the best available.
		cost = policyEstimate
	case scenario != nil && scenario.Response != nil && scenario.Response.Usage != nil && s.costCalc != nil:
		// Provider routing: compute cost from recorded usage and the
		// request's model name (which determines per-model pricing).
		model := ""
		if scenario.Request != nil {
			model = scenario.Request.Model
		}
		if model == "" {
			model = scenario.Response.Model
		}
		cost = sanitizeNonNegative(s.costCalc.CalculateCost(model, scenario.Response.Usage))
	default:
		cost = policyEstimate
	}

	// Determine latency:
	// 1. Policy-provided hint (highest priority — policy has learned
	//    provider-specific latency profiles during training).
	// 2. Recorded latency from the scenario's metadata (per-request).
	// 3. Trace metrics baseline (global average).
	// 4. Zero (no data available).
	latency := hint
	if latency <= 0 {
		if scenario != nil {
			if l, ok := metadataLatency(scenario.Metadata); ok {
				latency = l
			}
		}
		if latency <= 0 {
			latency = s.baselineLatency()
		}
	}

	out := ScenarioOutcome{LatencyMs: latency, CostUSD: cost}
	out.Score = scoreOutcome(out)
	return out
}

// baselineLatency returns the trace provider's average latency when it is a
// sane value, otherwise 0.
func (s *DreamSimulator) baselineLatency() float64 {
	if s.metrics == nil {
		return 0
	}
	return sanitizeNonNegative(s.metrics.AvgLatency())
}

// recordedProvider returns the provider that served the recorded response.
func recordedProvider(scenario *DreamReplay) string {
	if scenario == nil {
		return ""
	}
	if scenario.Metadata != nil {
		if p, ok := scenario.Metadata["provider"].(string); ok && p != "" {
			return p
		}
	}
	if scenario.Response != nil {
		return scenario.Response.Model
	}
	return ""
}

// sanitizeNonNegative maps NaN, ±Inf and negative values to 0.
func sanitizeNonNegative(v float64) float64 {
	if !isFinite(v) || v < 0 {
		return 0
	}
	return v
}

// sampleRecords performs deterministic stride-based sampling to select
// exactly sampleSize records from the input slice while preserving
// chronological ordering. This ensures reproducible replay results
// across runs.
//
// If sampleSize <= 0 or sampleSize >= len(records), all records are returned.
func sampleRecords(records []ledger.LedgerRecord, sampleSize int) []ledger.LedgerRecord {
	if sampleSize <= 0 || len(records) <= sampleSize {
		return records
	}

	result := make([]ledger.LedgerRecord, 0, sampleSize)
	step := float64(len(records)) / float64(sampleSize)
	for i := 0; i < sampleSize; i++ {
		idx := int(float64(i) * step)
		if idx >= len(records) {
			idx = len(records) - 1
		}
		result = append(result, records[idx])
	}
	return result
}

// extractToolsUsed returns the names of tools invoked in the LLM response.
func extractToolsUsed(resp *models.LLMResponse) []string {
	if resp == nil || len(resp.Choices) == 0 {
		return nil
	}
	var tools []string
	for _, choice := range resp.Choices {
		for _, tc := range choice.Message.ToolCalls {
			if tc.Function.Name != "" {
				tools = append(tools, tc.Function.Name)
			}
		}
	}
	return tools
}

// countToolCalls returns the total number of tool calls in the response.
func countToolCalls(resp *models.LLMResponse) int {
	if resp == nil || len(resp.Choices) == 0 {
		return 0
	}
	count := 0
	for _, choice := range resp.Choices {
		count += len(choice.Message.ToolCalls)
	}
	return count
}

// percentile computes the p-th percentile of a sorted slice of float64 values.
// Values must be sorted in ascending order. Returns 0 for empty slices.
func percentile(sortedValues []float64, p float64) float64 {
	if len(sortedValues) == 0 {
		return 0
	}
	if math.IsNaN(p) {
		p = 0
	}
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	// Use linear interpolation (same as Go's sort.Search-based approach).
	rank := p * float64(len(sortedValues)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return sortedValues[lower]
	}
	frac := rank - float64(lower)
	return sortedValues[lower]*(1-frac) + sortedValues[upper]*frac
}
