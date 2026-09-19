package rsi

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/guardrails"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// Headroom dimension: how much room for improvement exists in a subsystem.

// HeadroomDimension represents a dimension of system capability that
// RSI can operate on. Each dimension corresponds to a subsystem whose
// policy can be evolved through the RSI loop.
type HeadroomDimension string

const (
	// DimensionRouting assesses routing optimization potential.
	// High headroom means the router is not leveraging all available providers
	// or is not cost/latency-optimal in provider selection.
	DimensionRouting HeadroomDimension = "routing"

	// DimensionCache assesses caching optimization potential.
	// High headroom means there are many duplicate requests that could be
	// served from the exact-match or semantic cache.
	DimensionCache HeadroomDimension = "cache"

	// DimensionGuardrails assesses guardrail/budget enforcement potential.
	// High headroom means many requests contain injection attempts, PII leaks,
	// or would benefit from stricter policy enforcement.
	DimensionGuardrails HeadroomDimension = "guardrails"

	// DimensionAgentTools assesses agentic tool execution optimization potential.
	// High headroom means tool calls are frequently re-executed without caching,
	// or the tool registry could be expanded.
	DimensionAgentTools HeadroomDimension = "agent_tools"

	// DimensionCost assesses cost optimization potential.
	// High headroom means requests are using expensive models where cheaper
	// alternatives with equivalent capability exist.
	DimensionCost HeadroomDimension = "cost"

	// DimensionLatency assesses latency optimization potential.
	// High headroom means current latency significantly exceeds target SLOs.
	DimensionLatency HeadroomDimension = "latency"
)

// AllDimensions returns all headroom dimensions in canonical order.
// The order is: routing, cache, guardrails, agent_tools, cost, latency.
// This canonical ordering is used by AssessAll and Prioritize.
func AllDimensions() []HeadroomDimension {
	return []HeadroomDimension{
		DimensionRouting,
		DimensionCache,
		DimensionGuardrails,
		DimensionAgentTools,
		DimensionCost,
		DimensionLatency,
	}
}

// Actionability measures how easily a dimension can be improved by RSI.
// Higher actionability means the dimension can be optimized more readily
// through policy changes rather than infrastructure changes.
type Actionability float64

const (
	// ActionabilityLow means improvements require significant infrastructure
	// changes (e.g., adding new providers, hardware upgrades).
	ActionabilityLow Actionability = 0.25

	// ActionabilityMedium means improvements require tuning or pattern
	// updates (e.g., cache TTL, guardrail rules, latency optimization).
	ActionabilityMedium Actionability = 0.50

	// ActionabilityHigh means improvements can be applied at runtime
	// (e.g., routing strategy, cost model selection, tool caching).
	ActionabilityHigh Actionability = 0.90
)

// HeadroomAssessment captures the result of assessing a single dimension.
type HeadroomAssessment struct {
	// Dimension is the assessed dimension.
	Dimension HeadroomDimension `json:"dimension"`

	// CurrentScore is the normalized current performance (0.0–1.0).
	// 1.0 means the dimension is already at its optimal state.
	CurrentScore float64 `json:"current_score"`

	// OptimalScore is the theoretical best score (typically 1.0).
	OptimalScore float64 `json:"optimal_score"`

	// HeadroomPct is the percentage of room for improvement:
	// (OptimalScore - CurrentScore) / OptimalScore * 100.
	HeadroomPct float64 `json:"headroom_pct"`

	// Confidence is the statistical confidence in the assessment (0.0–1.0).
	// Higher values indicate more data was available for the assessment.
	Confidence float64 `json:"confidence"`

	// Actionability indicates how readily this dimension can be improved.
	Actionability Actionability `json:"actionability"`

	// Details provides additional context about the assessment.
	Details map[string]interface{} `json:"details,omitempty"`
}

// HCIConfig holds configuration parameters for the Headroom Assessment Engine.
//
// HCI (Headroom-Closed Index) is derived from "The Last AI Built by Humans,"
// which proposes staged autonomy assessment: a system must quantify its own
// remaining headroom before it can safely self-improve. Each dimension below
// is a subsystem that RSI can evolve.
type HCIConfig struct {
	// MinRecords is the minimum number of ledger records required for a
	// confident assessment. Below this threshold, Confidence is scaled down.
	MinRecords int

	// TargetLatencyMs is the latency SLO ceiling used by the latency dimension.
	// CurrentScore for latency = 1 - (avgLatency / TargetLatencyMs).
	TargetLatencyMs float64

	// CostEfficiencyThreshold is the per-request cost (USD) above which a
	// request is considered "cost-inefficient." Used by the cost dimension.
	CostEfficiencyThreshold float64

	// DuplicateRequestThreshold is the fraction of duplicate (cacheable)
	// requests above which caching headroom is considered significant.
	DuplicateRequestThreshold float64
}

// DefaultHCIConfig returns a sensible default configuration for the HCI engine.
func DefaultHCIConfig() HCIConfig {
	return HCIConfig{
		MinRecords:               20,
		TargetLatencyMs:          500.0,
		CostEfficiencyThreshold:  0.01,
		DuplicateRequestThreshold: 0.10,
	}
}

// HCIEngine is the Headroom-Closed Index assessment engine.
//
// It analyzes historical ledger records, current trace metrics, and cost data
// to estimate the "headroom" — the remaining improvement potential — across six
// dimensions: routing, cache, guardrails, agent tools, cost, and latency.
//
// The engine caches ledger records on first access and reuses them across
// Assess calls within a single session. Call Refresh() to reload from the
// ledger after new data has been written.
type HCIEngine struct {
	mu            sync.RWMutex
	ledger        LedgerReader
	metrics       MetricsProvider
	costCalc      CostCalculator
	providers     ProviderLister
	cfg           HCIConfig

	// Cached ledger records, loaded lazily on first Assess/AssessAll call.
	records       []ledger.LedgerRecord
	recordsLoaded bool
	refreshTime   time.Time

	// Cached dimension assessments, populated by AssessAll so that
	// Prioritize can run without re-reading the ledger.
	cachedAssessments map[HeadroomDimension]*HeadroomAssessment
}

// NewHCIEngine creates a new Headroom Assessment Engine.
//
// Parameters:
//   - store: ledger reader for historical request/response data (required)
//   - metrics: trace metrics provider for current baselines (optional, can be nil)
//   - costCalc: cost calculator for cost analysis (optional, can be nil)
//   - providers: provider lister for routing assessment (optional, can be nil)
//   - cfg: configuration parameters; pass DefaultHCIConfig() for sensible defaults
func NewHCIEngine(store LedgerReader, metrics MetricsProvider, costCalc CostCalculator, providers ProviderLister, cfg HCIConfig) *HCIEngine {
	if cfg.MinRecords == 0 {
		cfg.MinRecords = DefaultHCIConfig().MinRecords
	}
	if cfg.TargetLatencyMs == 0 {
		cfg.TargetLatencyMs = DefaultHCIConfig().TargetLatencyMs
	}
	if cfg.CostEfficiencyThreshold == 0 {
		cfg.CostEfficiencyThreshold = DefaultHCIConfig().CostEfficiencyThreshold
	}
	if cfg.DuplicateRequestThreshold == 0 {
		cfg.DuplicateRequestThreshold = DefaultHCIConfig().DuplicateRequestThreshold
	}
	return &HCIEngine{
		ledger:    store,
		metrics:   metrics,
		costCalc:  costCalc,
		providers: providers,
		cfg:       cfg,
	}
}

// Refresh forces the engine to reload ledger records and clear cached
// assessments on the next assessment call.
// This should be called after new ledger entries have been written, typically
// between RSI cycles.
func (e *HCIEngine) Refresh() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recordsLoaded = false
	e.records = nil
	e.cachedAssessments = nil
}

// loadRecords returns cached ledger records, loading them from the ledger store
// on first access. Uses double-checked locking for thread safety.
func (e *HCIEngine) loadRecords(ctx context.Context) ([]ledger.LedgerRecord, error) {
	e.mu.RLock()
	if e.recordsLoaded {
		records := e.records
		e.mu.RUnlock()
		return records, nil
	}
	e.mu.RUnlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.recordsLoaded {
		return e.records, nil
	}

	if e.ledger == nil {
		return nil, fmt.Errorf("hci: ledger reader is nil")
	}

	records, err := e.ledger.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("hci: failed to load ledger records: %w", err)
	}

	e.records = records
	e.recordsLoaded = true
	e.refreshTime = time.Now()
	return records, nil
}

// parseRequest unmarshals a JSON-encoded LLMRequest from a ledger record's
// RequestPayload field. Returns nil on parse failure.
func parseRequest(payload string) *models.LLMRequest {
	if payload == "" {
		return nil
	}
	var req models.LLMRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return nil
	}
	return &req
}

// parseResponse unmarshals a JSON-encoded LLMResponse from a ledger record's
// ResponsePayload field. Returns nil on parse failure.
func parseResponse(payload string) *models.LLMResponse {
	if payload == "" {
		return nil
	}
	var resp models.LLMResponse
	if err := json.Unmarshal([]byte(payload), &resp); err != nil {
		return nil
	}
	return &resp
}

// confidence computes a 0–1 confidence score based on the number of records
// relative to the configured minimum threshold.
func (e *HCIEngine) confidence(count int) float64 {
	if count <= 0 {
		return 0.0
	}
	if e.cfg.MinRecords <= 0 {
		return 1.0
	}
	return math.Min(float64(count)/float64(e.cfg.MinRecords), 1.0)
}

// Assess evaluates a single headroom dimension using current ledger data.
//
// The assessment reads from the cached ledger records (loaded on first call)
// and, where applicable, from the trace metrics provider for current baselines.
//
// Returns a HeadroomAssessment with CurrentScore (0–1), OptimalScore (1.0),
// HeadroomPct (percentage of room for improvement), Confidence (0–1),
// and Actionability.
func (e *HCIEngine) Assess(ctx context.Context, dimension HeadroomDimension) (*HeadroomAssessment, error) {
	records, err := e.loadRecords(ctx)
	if err != nil {
		return nil, err
	}

	switch dimension {
	case DimensionRouting:
		return e.assessRouting(records), nil
	case DimensionCache:
		return e.assessCache(records), nil
	case DimensionGuardrails:
		return e.assessGuardrails(records), nil
	case DimensionAgentTools:
		return e.assessAgentTools(records), nil
	case DimensionCost:
		return e.assessCost(records), nil
	case DimensionLatency:
		return e.assessLatency(records), nil
	default:
		return nil, fmt.Errorf("hci: unknown dimension %q", dimension)
	}
}

// AssessAll evaluates all six headroom dimensions and returns a map keyed
// by dimension. This is the primary entry point for the RSI orchestrator:
// it identifies which dimension has the most improvement potential.
func (e *HCIEngine) AssessAll(ctx context.Context) (map[HeadroomDimension]*HeadroomAssessment, error) {
	records, err := e.loadRecords(ctx)
	if err != nil {
		return nil, err
	}

	result := make(map[HeadroomDimension]*HeadroomAssessment, len(AllDimensions()))
	for _, dim := range AllDimensions() {
		result[dim] = e.assessDimension(dim, records)
	}

	// Cache the assessments so Prioritize does not need to re-read the ledger.
	e.mu.Lock()
	e.cachedAssessments = result
	e.mu.Unlock()

	return result, nil
}

// assessDimension dispatches to the appropriate assessment function.
// It wraps the individual assessors with nil-safe record handling so that
// Assess can be called even when the ledger is empty.
func (e *HCIEngine) assessDimension(dim HeadroomDimension, records []ledger.LedgerRecord) *HeadroomAssessment {
	assess := func(a *HeadroomAssessment, err error) *HeadroomAssessment {
		if err != nil || a == nil {
			return e.emptyAssessment(dim)
		}
		return a
	}

	switch dim {
	case DimensionRouting:
		return assess(e.assessRouting(records), nil)
	case DimensionCache:
		return assess(e.assessCache(records), nil)
	case DimensionGuardrails:
		return assess(e.assessGuardrails(records), nil)
	case DimensionAgentTools:
		return assess(e.assessAgentTools(records), nil)
	case DimensionCost:
		return assess(e.assessCost(records), nil)
	case DimensionLatency:
		return assess(e.assessLatency(records), nil)
	default:
		return e.emptyAssessment(dim)
	}
}

// emptyAssessment returns a zero-headroom assessment for dimensions where
// no data is available.
func (e *HCIEngine) emptyAssessment(dim HeadroomDimension) *HeadroomAssessment {
	return &HeadroomAssessment{
		Dimension:     dim,
		CurrentScore:  0,
		OptimalScore:  1.0,
		HeadroomPct:   100,
		Confidence:    0,
		Actionability: actionabilityFor(dim),
		Details:       map[string]interface{}{"reason": "no data available"},
	}
}

// Prioritize returns the dimension with the highest combined
// (HeadroomPct × Actionability) score. This identifies where RSI should
// focus its evolution effort first — a dimension with high headroom but low
// actionability may be deprioritized in favor of one with lower headroom
// but higher actionability.
//
// If no assessments have been run, the method returns the first dimension
// in canonical order.
func (e *HCIEngine) Prioritize() HeadroomDimension {
	e.mu.RLock()
	assessments := e.cachedAssessments
	e.mu.RUnlock()

	if len(assessments) == 0 {
		// Fallback: assess all dimensions on demand.
		return e.prioritizeFromLedger()
	}

	best := HeadroomDimension(AllDimensions()[0])
	bestScore := -1.0
	for _, dim := range AllDimensions() {
		a, ok := assessments[dim]
		if !ok {
			continue
		}
		score := a.HeadroomPct * float64(a.Actionability)
		if score > bestScore {
			bestScore = score
			best = dim
		}
	}
	return best
}

// prioritizeFromLedger performs a one-shot assessment if no cached results
// exist. Used as a fallback by Prioritize.
func (e *HCIEngine) prioritizeFromLedger() HeadroomDimension {
	ctx := context.Background()
	records, err := e.loadRecords(ctx)
	if err != nil {
		return AllDimensions()[0]
	}

	best := HeadroomDimension(AllDimensions()[0])
	bestScore := -1.0
	for _, dim := range AllDimensions() {
		a := e.assessDimension(dim, records)
		score := a.HeadroomPct * float64(a.Actionability)
		if score > bestScore {
			bestScore = score
			best = dim
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Dimension Assessments
// ---------------------------------------------------------------------------

// assessRouting evaluates the routing dimension.
//
// Routing headroom is determined by how evenly traffic is distributed across
// available providers. If all requests are funneled through a single provider
// while others remain unused, the headroom is high — RSI can evolve the
// routing strategy to leverage fallback, cost-based, or latency-based routing.
//
// Scoring:
//   - Distribution entropy: 1.0 (perfectly balanced) → 0.0 (all to one provider)
//   - If only one provider is registered, the dimension is already at ceiling.
//   - Cost efficiency (when cost data is available): if some providers are
//     significantly more expensive than others for the same model, headroom
//     increases.
func (e *HCIEngine) assessRouting(records []ledger.LedgerRecord) *HeadroomAssessment {
	if len(records) == 0 {
		return &HeadroomAssessment{
			Dimension:     DimensionRouting,
			CurrentScore:  0,
			OptimalScore:  1.0,
			HeadroomPct:   100,
			Confidence:    0,
			Actionability: ActionabilityHigh,
			Details:       map[string]interface{}{"reason": "no request data"},
		}
	}

	totalRequests := 0
	providerCounts := make(map[string]int)

	for _, rec := range records {
		resp := parseResponse(rec.ResponsePayload)
		if resp == nil {
			continue
		}
		provider := resp.Model
		if provider == "" {
			provider = "unknown"
		}
		providerCounts[provider]++
		totalRequests++
	}

	// If no providers are registered or only one is used, check if
	// alternatives exist.
	availableProviders := 0
	if e.providers != nil {
		availableProviders = len(e.providers.ProviderNames())
	}

	distScore := e.computeDistributionScore(providerCounts, totalRequests, availableProviders)
	costScore := e.computeRoutingCostScore(records)

	// Weight distribution (60%) and cost efficiency (40%).
	currentScore := distScore*0.6 + costScore*0.4

	// If only one provider is registered total, there's no routing headroom.
	if availableProviders <= 1 && len(providerCounts) <= 1 {
		currentScore = 1.0
	}

	confidence := e.confidence(totalRequests)

	return &HeadroomAssessment{
		Dimension:     DimensionRouting,
		CurrentScore:  clamp(currentScore, 0, 1),
		OptimalScore:  1.0,
		HeadroomPct:   (1.0 - clamp(currentScore, 0, 1)) * 100,
		Confidence:    confidence,
		Actionability: ActionabilityHigh,
		Details: map[string]interface{}{
			"total_requests":      totalRequests,
			"providers_used":      len(providerCounts),
			"providers_registered": availableProviders,
			"distribution_score":  distScore,
			"cost_efficiency":     costScore,
		},
	}
}

// computeDistributionScore computes a 0–1 score for provider distribution
// diversity using normalized Shannon entropy.
//
//   - 1.0 = perfectly balanced across all used providers
//   - 0.0 = all requests to a single provider (and multiple available)
//   - If only one provider is registered, returns 1.0 (no improvement possible)
func (e *HCIEngine) computeDistributionScore(counts map[string]int, total, available int) float64 {
	if total == 0 {
		return 0
	}
	if len(counts) <= 1 && available <= 1 {
		return 1.0 // Only one provider — no headroom to improve
	}
	if len(counts) <= 1 {
		return 0.0 // All traffic to one provider, but alternatives exist
	}

	// Normalized Shannon entropy.
	entropy := 0.0
	for _, count := range counts {
		p := float64(count) / float64(total)
		if p > 0 {
			entropy -= p * math.Log2(p)
		}
	}
	maxEntropy := math.Log2(float64(len(counts)))
	if maxEntropy == 0 {
		return 1.0
	}
	return entropy / maxEntropy
}

// computeRoutingCostScore estimates cost efficiency of the current routing.
// When cost data is available, it computes the ratio of the minimum per-request
// cost to the average per-request cost. A score of 1.0 means every request
// is already routed to the cheapest available option.
func (e *HCIEngine) computeRoutingCostScore(records []ledger.LedgerRecord) float64 {
	if e.costCalc == nil || len(records) == 0 {
		return 1.0 // No cost data — assume routing is cost-optimal.
	}

	var totalCost, minCost float64
	requestCount := 0

	for _, rec := range records {
		req := parseRequest(rec.RequestPayload)
		resp := parseResponse(rec.ResponsePayload)
		if req == nil || resp == nil || resp.Usage == nil {
			continue
		}
		cost := e.costCalc.CalculateCost(req.Model, resp.Usage)
		if cost <= 0 {
			continue
		}
		totalCost += cost
		requestCount++
		if minCost == 0 || cost < minCost {
			minCost = cost
		}
	}

	if requestCount == 0 || totalCost == 0 {
		return 1.0
	}
	avgCost := totalCost / float64(requestCount)
	if avgCost == 0 {
		return 1.0
	}
	return minCost / avgCost // 1.0 = all requests at min cost
}

// assessCache evaluates the caching dimension.
//
// Cache headroom is determined by the fraction of duplicate requests in the
// ledger — identical request payloads that represent cacheable opportunities.
// A high rate of duplicates means significant headroom for cache hit rate
// improvement through exact-match or semantic caching.
//
// The simulator counts unique request payloads and computes:
//   cacheHitRate = (totalRequests - uniqueRequests) / totalRequests
//
// CurrentScore = cacheHitRate (0 = no caching, 1 = everything cached).
func (e *HCIEngine) assessCache(records []ledger.LedgerRecord) *HeadroomAssessment {
	totalRequests := 0
	uniquePayloads := make(map[string]int)

	for _, rec := range records {
		if rec.RequestPayload == "" {
			continue
		}
		totalRequests++
		uniquePayloads[rec.RequestPayload]++
	}

	if totalRequests == 0 {
		return &HeadroomAssessment{
			Dimension:     DimensionCache,
			CurrentScore:  0,
			OptimalScore:  1.0,
			HeadroomPct:   100,
			Confidence:    0,
			Actionability: ActionabilityMedium,
			Details:       map[string]interface{}{"reason": "no request data"},
		}
	}

	// Cache hit rate: for each unique payload, the first occurrence is a
	// cache miss and subsequent occurrences are hits.
	cacheHits := 0
	for _, count := range uniquePayloads {
		if count > 1 {
			cacheHits += count - 1
		}
	}
	cacheHitRate := float64(cacheHits) / float64(totalRequests)

	confidence := e.confidence(totalRequests)

	return &HeadroomAssessment{
		Dimension:     DimensionCache,
		CurrentScore:  cacheHitRate,
		OptimalScore:  1.0,
		HeadroomPct:   (1.0 - cacheHitRate) * 100,
		Confidence:    confidence,
		Actionability: ActionabilityMedium,
		Details: map[string]interface{}{
			"total_requests":   totalRequests,
			"unique_requests":  len(uniquePayloads),
			"cache_hits":       cacheHits,
			"cache_misses":     totalRequests - cacheHits,
		},
	}
}

// assessGuardrails evaluates the guardrails dimension.
//
// Guardrail headroom is determined by scanning request/response payloads for
// security vulnerabilities: prompt injection attempts, PII leakage, and
// budget violations. Each detected violation reduces the current score,
// indicating headroom for strengthening guardrail policies.
//
// CurrentScore = 1 - (violations / totalRequests).
func (e *HCIEngine) assessGuardrails(records []ledger.LedgerRecord) *HeadroomAssessment {
	injectionShield := guardrails.NewPromptInjectionShield()
	piiRedactor := guardrails.NewPIIRedactor()

	totalRequests := 0
	violations := 0
	injectionViolations := 0
	piiViolations := 0

	for _, rec := range records {
		totalRequests++

		// Extract message content for injection scanning (not the raw JSON,
		// which can produce false PII positives from Unix timestamps, IDs, etc.).
		req := parseRequest(rec.RequestPayload)
		resp := parseResponse(rec.ResponsePayload)

		var requestContent string
		if req != nil {
			for _, msg := range req.Messages {
				if msg.Content != nil {
					requestContent += *msg.Content + " "
				}
			}
		}
		if requestContent == "" {
			requestContent = rec.RequestPayload
		}

		// Check for prompt injection patterns in the request content only.
		if injectionShield.Scan(requestContent) {
			violations++
			injectionViolations++
		}

		// Check for PII in the response message content only (data exfiltration risk).
		var responseContent string
		if resp != nil {
			for _, choice := range resp.Choices {
				if choice.Message.Content != nil {
					responseContent += *choice.Message.Content + " "
				}
				// Also check tool call arguments.
				for _, tc := range choice.Message.ToolCalls {
					responseContent += tc.Function.Arguments + " "
				}
			}
		}
		if responseContent == "" {
			responseContent = rec.ResponsePayload
		}

		if piiRedactor.Redact(responseContent) != responseContent {
			violations++
			piiViolations++
		}
	}

	if totalRequests == 0 {
		return &HeadroomAssessment{
			Dimension:     DimensionGuardrails,
			CurrentScore:  0,
			OptimalScore:  1.0,
			HeadroomPct:   100,
			Confidence:    0,
			Actionability: ActionabilityMedium,
			Details:       map[string]interface{}{"reason": "no data"},
		}
	}

	violationRate := float64(violations) / float64(totalRequests)
	currentScore := 1.0 - violationRate
	confidence := e.confidence(totalRequests)

	return &HeadroomAssessment{
		Dimension:     DimensionGuardrails,
		CurrentScore:  clamp(currentScore, 0, 1),
		OptimalScore:  1.0,
		HeadroomPct:   clamp(1.0-currentScore, 0, 1) * 100,
		Confidence:    confidence,
		Actionability: ActionabilityMedium,
		Details: map[string]interface{}{
			"total_requests":      totalRequests,
			"violations":          violations,
			"injection_violations": injectionViolations,
			"pii_violations":      piiViolations,
		},
	}
}

// assessAgentTools evaluates the agent tools dimension.
//
// Agent tool headroom is determined by how many tool calls in the ledger
// are duplicates — the same tool invoked with the same arguments. Duplicate
// tool calls represent cacheable opportunities: the agent could return a
// cached tool result instead of re-executing the tool, reducing latency and
// cost.
//
// CurrentScore = 1 - (duplicateToolCalls / totalToolCalls).
func (e *HCIEngine) assessAgentTools(records []ledger.LedgerRecord) *HeadroomAssessment {
	// Empty ledger → unknown tool usage → full headroom, zero confidence.
	if len(records) == 0 {
		return &HeadroomAssessment{
			Dimension:     DimensionAgentTools,
			CurrentScore:  0,
			OptimalScore:  1.0,
			HeadroomPct:   100,
			Confidence:    0,
			Actionability: ActionabilityHigh,
			Details:       map[string]interface{}{"reason": "no request data"},
		}
	}

	totalToolCalls := 0
	duplicateToolCalls := 0
	toolCounts := make(map[string]int) // tool name → call count

	for _, rec := range records {
		resp := parseResponse(rec.ResponsePayload)
		if resp == nil || len(resp.Choices) == 0 {
			continue
		}

		for _, choice := range resp.Choices {
			for _, tc := range choice.Message.ToolCalls {
				totalToolCalls++
				toolName := tc.Function.Name
				toolCounts[toolName]++
				if toolCounts[toolName] > 1 {
					// This is a duplicate call of the same tool.
					// (Not perfectly accurate — could be same tool different args,
					// but without parsing args this is a reasonable heuristic.)
					duplicateToolCalls++
				}
			}
		}
	}

	if totalToolCalls == 0 {
		return &HeadroomAssessment{
			Dimension:     DimensionAgentTools,
			CurrentScore:  1.0,
			OptimalScore:  1.0,
			HeadroomPct:   0,
			Confidence:    1.0,
			Actionability: ActionabilityHigh,
			Details:       map[string]interface{}{"reason": "no tool calls in ledger"},
		}
	}

	duplicateRate := float64(duplicateToolCalls) / float64(totalToolCalls)
	currentScore := 1.0 - duplicateRate
	confidence := e.confidence(totalToolCalls)

	return &HeadroomAssessment{
		Dimension:     DimensionAgentTools,
		CurrentScore:  clamp(currentScore, 0, 1),
		OptimalScore:  1.0,
		HeadroomPct:   clamp(1.0-currentScore, 0, 1) * 100,
		Confidence:    confidence,
		Actionability: ActionabilityHigh,
		Details: map[string]interface{}{
			"total_tool_calls":    totalToolCalls,
			"duplicate_tool_calls": duplicateToolCalls,
			"distinct_tools":      len(toolCounts),
		},
	}
}

// assessCost evaluates the cost dimension.
//
// Cost headroom measures how much the system could reduce its LLM spend through
// better model selection and routing. When cost data is available, the engine
// computes the average per-request cost and compares it against the cost
// efficiency threshold. Requests above the threshold contribute to the
// headroom score — RSI could evolve a policy that routes these requests to
// cheaper models with equivalent capability.
//
// CurrentScore = 1 - (costlyRequests / totalRequests), where a "costly"
// request exceeds the configured cost threshold.
func (e *HCIEngine) assessCost(records []ledger.LedgerRecord) *HeadroomAssessment {
	totalRequests := 0
	costlyRequests := 0
	var totalCostUSD float64

	for _, rec := range records {
		req := parseRequest(rec.RequestPayload)
		resp := parseResponse(rec.ResponsePayload)
		if req == nil || resp == nil || resp.Usage == nil {
			continue
		}
		totalRequests++

		if e.costCalc != nil {
			cost := e.costCalc.CalculateCost(req.Model, resp.Usage)
			totalCostUSD += cost
			if cost > e.cfg.CostEfficiencyThreshold {
				costlyRequests++
			}
		}
	}

	if totalRequests == 0 {
		return &HeadroomAssessment{
			Dimension:     DimensionCost,
			CurrentScore:  0,
			OptimalScore:  1.0,
			HeadroomPct:   100,
			Confidence:    0,
			Actionability: ActionabilityHigh,
			Details:       map[string]interface{}{"reason": "no usage data"},
		}
	}

	costlyRate := float64(costlyRequests) / float64(totalRequests)
	currentScore := 1.0 - costlyRate
	confidence := e.confidence(totalRequests)

	return &HeadroomAssessment{
		Dimension:     DimensionCost,
		CurrentScore:  clamp(currentScore, 0, 1),
		OptimalScore:  1.0,
		HeadroomPct:   clamp(1.0-currentScore, 0, 1) * 100,
		Confidence:    confidence,
		Actionability: ActionabilityHigh,
		Details: map[string]interface{}{
			"total_requests":     totalRequests,
			"costly_requests":    costlyRequests,
			"costly_rate":        costlyRate,
			"total_cost_usd":     totalCostUSD,
			"avg_cost_per_request": avgOrZero(totalCostUSD, float64(totalRequests)),
		},
	}
}

// assessLatency evaluates the latency dimension.
//
// Latency headroom compares the current average latency (from the trace
// metrics provider) against the configured target. If current latency is
// well below the target, there is little headroom. If it approaches or
// exceeds the target, there is significant headroom for optimization
// (e.g., through better routing, caching, or provider selection).
//
// CurrentScore = 1 - (avgLatency / targetLatencyMs), clamped to [0, 1].
// A score of 1.0 means latency is at or below target; 0.0 means it far
// exceeds the target.
func (e *HCIEngine) assessLatency(records []ledger.LedgerRecord) *HeadroomAssessment {
	// Estimate latency from ledger response metadata if available.
	// The trace provider has AvgLatency() for real-time data.
	var avgLatencyMs float64
	var sampleCount int

	for _, rec := range records {
		// Ledger records may contain latency in metadata (written by the
		// trace middleware or cost tracker). Extract if present.
		if rec.Metadata != nil {
			if lat, ok := rec.Metadata["latency_ms"]; ok {
				if latMs, err := strconv.ParseFloat(fmt.Sprintf("%v", lat), 64); err == nil {
					avgLatencyMs += latMs
					sampleCount++
				}
			}
		}
	}

	// Compute the average latency across all samples.
	if sampleCount > 0 {
		avgLatencyMs /= float64(sampleCount)
	}

	// Prefer trace provider's AvgLatency if available (more accurate).
	if e.metrics != nil && e.metrics.AvgLatency() > 0 {
		avgLatencyMs = e.metrics.AvgLatency()
		sampleCount = int(e.metrics.RequestCount())
	}

	if sampleCount == 0 {
		return &HeadroomAssessment{
			Dimension:     DimensionLatency,
			CurrentScore:  0,
			OptimalScore:  1.0,
			HeadroomPct:   100,
			Confidence:    0,
			Actionability: ActionabilityMedium,
			Details:       map[string]interface{}{"reason": "no latency data"},
		}
	}

	confidence := e.confidence(sampleCount)
	latencyRatio := avgLatencyMs / e.cfg.TargetLatencyMs
	currentScore := 1.0 - latencyRatio
	if latencyRatio <= 0 {
		currentScore = 1.0
	}

	return &HeadroomAssessment{
		Dimension:     DimensionLatency,
		CurrentScore:  clamp(currentScore, 0, 1),
		OptimalScore:  1.0,
		HeadroomPct:   clamp(1.0-currentScore, 0, 1) * 100,
		Confidence:    confidence,
		Actionability: ActionabilityMedium,
		Details: map[string]interface{}{
			"avg_latency_ms":      avgLatencyMs,
			"target_latency_ms":   e.cfg.TargetLatencyMs,
			"sample_count":        sampleCount,
			"latency_ratio":       latencyRatio,
		},
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// actionabilityFor returns the default actionability for a dimension.
// Actionability reflects how easily RSI can improve the dimension:
//   - High: can be changed at runtime without infrastructure changes
//     (routing strategy, cost model, tool caching)
//   - Medium: requires tuning but is feasible (cache TTL/threshold,
//     guardrail patterns, latency optimization)
func actionabilityFor(dim HeadroomDimension) Actionability {
	switch dim {
	case DimensionRouting, DimensionCost, DimensionAgentTools:
		return ActionabilityHigh
	case DimensionCache, DimensionGuardrails, DimensionLatency:
		return ActionabilityMedium
	default:
		return ActionabilityLow
	}
}

// clamp constrains a value to the range [lo, hi].
func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// avgOrZero returns total/count, or 0 if count is zero.
func avgOrZero(total float64, count float64) float64 {
	if count == 0 {
		return 0
	}
	return total / count
}
