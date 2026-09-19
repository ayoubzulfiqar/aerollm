package rsi

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ---------------------------------------------------------------------------
// Autonomous Explorer: broad-then-deep policy search
// ---------------------------------------------------------------------------

// ExplorationStep records a single mutation+evaluation step.
type ExplorationStep struct {
	Iteration int
	Policy    Policy
	Score     float64
	Metrics   *SimulatedMetrics
}

// ExplorationResult captures the outcome of a full broad-then-deep cycle.
type ExplorationResult struct {
	BestPolicy       Policy
	ImprovementPct   float64
	ExplorationTrace []ExplorationStep
}

// AutonomousExplorer drives broad-then-deep policy search using the DreamSimulator
// for offline policy evaluation and the HCIEngine for headroom guidance.
type AutonomousExplorer struct {
	sim *DreamSimulator
	hci *HCIEngine
}

// NewAutonomousExplorer creates an explorer wired to the given simulator and HCI engine.
func NewAutonomousExplorer(sim *DreamSimulator, hci *HCIEngine) *AutonomousExplorer {
	return &AutonomousExplorer{
		sim: sim,
		hci: hci,
	}
}

// ---------------------------------------------------------------------------
// Composite score
// ---------------------------------------------------------------------------

// scoreMetrics produces a composite policy score in [0, 1] from simulated metrics.
// Higher is better: lower latency/cost/errors and higher cache-hit rate.
func scoreMetrics(m *SimulatedMetrics) float64 {
	if m == nil {
		return 0.0
	}
	latencyScore := 1.0 - math.Min(m.AvgLatency/1000.0, 1.0)
	costScore := 1.0 - math.Min(m.Cost*100.0, 1.0)
	errorScore := 1.0 - m.ErrorRate
	cacheScore := m.CacheHitRate
	return (latencyScore*0.3 +
		costScore*0.3 +
		errorScore*0.2 +
		cacheScore*0.2)
}

// ---------------------------------------------------------------------------
// Concrete Policy Implementations
// ---------------------------------------------------------------------------

// RoutingPolicy routes requests to providers based on weighted preference.
// Higher weights mean higher probability of selection for that provider.
type RoutingPolicy struct {
	Weights      map[string]float64 // provider -> routing weight
	CacheEnabled bool
	CostAware    bool
	seed         int64
}

// Apply selects the highest-weight provider for the request.
func (p *RoutingPolicy) Apply(_ context.Context, req *models.LLMRequest) (*Response, error) {
	if req == nil {
		return &Response{Provider: "unknown", Error: true}, nil
	}

	provider := p.selectProvider()
	latency := providerLatency(provider)
	cost := providerCost(provider, req)
	cached := p.CacheEnabled && p.shouldCache(req)

	return &Response{
		Provider:  provider,
		Cached:    cached,
		Error:     false,
		LatencyMs: latency,
		CostUSD:   cost,
	}, nil
}

// Mutate perturbs routing weights and toggles cache/cost flags.
func (p *RoutingPolicy) Mutate() Policy {
	r := rand.New(rand.NewSource(p.seed + 1))
	mutated := p.Clone().(*RoutingPolicy)
	mutated.seed = p.seed + 1

	for provider := range mutated.Weights {
		delta := r.Float64()*0.4 - 0.2
		mutated.Weights[provider] = math.Max(0.01, mutated.Weights[provider]*(1.0+delta))
	}

	if r.Float64() < 0.2 {
		mutated.CacheEnabled = !mutated.CacheEnabled
	}
	return mutated
}

// Clone creates a deep copy of the policy.
func (p *RoutingPolicy) Clone() Policy {
	weights := make(map[string]float64, len(p.Weights))
	for k, v := range p.Weights {
		weights[k] = v
	}
	return &RoutingPolicy{
		Weights:      weights,
		CacheEnabled: p.CacheEnabled,
		CostAware:    p.CostAware,
		seed:         p.seed,
	}
}

func (p *RoutingPolicy) selectProvider() string {
	best := ""
	bestWeight := math.Inf(-1)
	for provider, weight := range p.Weights {
		if weight > bestWeight || (weight == bestWeight && provider < best) {
			best = provider
			bestWeight = weight
		}
	}
	if best == "" {
		return "openai"
	}
	return best
}

func (p *RoutingPolicy) shouldCache(req *models.LLMRequest) bool {
	total := 0
	if req.Messages != nil {
		for _, m := range req.Messages {
			if m.Content != nil {
				total += len(*m.Content)
			}
		}
	}
	return total < 2048
}

// --- CachePolicy ---

// CachePolicy decides whether a request is cacheable based on content size.
type CachePolicy struct {
	CacheableThreshold float64 // fraction of max cache bytes that is cacheable (0-1)
	MaxCacheSizeMB     int
	seed               int64
}

// Apply returns cached=true for short requests.
func (p *CachePolicy) Apply(_ context.Context, req *models.LLMRequest) (*Response, error) {
	if req == nil {
		return &Response{Error: true}, nil
	}

	contentLen := 0
	if req.Messages != nil {
		for _, m := range req.Messages {
			if m.Content != nil {
				contentLen += len(*m.Content)
			}
		}
	}

	// CacheableThreshold is a fraction (0-1) of a 2048-char baseline.
	threshold := int(2048.0 * p.CacheableThreshold)
	cached := threshold > 0 && contentLen <= threshold

	latency := 5.0
	cost := 0.0
	if !cached {
		latency = 100.0
		cost = 0.002
	}

	return &Response{
		Provider:  "openai",
		Cached:    cached,
		Error:     false,
		LatencyMs: latency,
		CostUSD:   cost,
	}, nil
}

// Mutate perturbs the cacheable threshold.
func (p *CachePolicy) Mutate() Policy {
	r := rand.New(rand.NewSource(p.seed + 1))
	mutated := p.Clone().(*CachePolicy)
	mutated.seed = p.seed + 1

	delta := r.Float64()*0.2 - 0.1
	mutated.CacheableThreshold = clamp(mutated.CacheableThreshold*(1.0+delta), 0.01, 1.0)
	return mutated
}

// Clone creates a deep copy.
func (p *CachePolicy) Clone() Policy {
	return &CachePolicy{
		CacheableThreshold: p.CacheableThreshold,
		MaxCacheSizeMB:     p.MaxCacheSizeMB,
		seed:               p.seed,
	}
}

// --- GuardrailPolicy ---

// GuardrailPolicy decides whether to block a request based on length and cost.
type GuardrailPolicy struct {
	InjectionSensitivity float64 // 0-1, higher = more aggressive blocking
	PIISensitivity       float64 // 0-1, higher = stricter PII detection
	BudgetLimitUSD       float64 // max cost per request before blocking
	seed                 int64
}

// Apply blocks requests that exceed length thresholds.
func (p *GuardrailPolicy) Apply(_ context.Context, req *models.LLMRequest) (*Response, error) {
	if req == nil {
		return &Response{Error: true}, nil
	}

	contentLen := 0
	if req.Messages != nil {
		for _, m := range req.Messages {
			if m.Content != nil {
				contentLen += len(*m.Content)
			}
		}
	}

	maxLen := int(5000.0 / math.Max(0.01, p.InjectionSensitivity))
	blocked := contentLen > maxLen

	return &Response{
		Provider:  "openai",
		Cached:    false,
		Error:     blocked,
		LatencyMs: 1.0,
		CostUSD:   0,
	}, nil
}

// Mutate perturbs sensitivity thresholds.
func (p *GuardrailPolicy) Mutate() Policy {
	r := rand.New(rand.NewSource(p.seed + 1))
	mutated := p.Clone().(*GuardrailPolicy)
	mutated.seed = p.seed + 1

	delta := r.Float64()*0.2 - 0.1
	mutated.InjectionSensitivity = clamp(mutated.InjectionSensitivity*(1.0+delta), 0.01, 0.99)
	delta = r.Float64()*0.2 - 0.1
	mutated.PIISensitivity = clamp(mutated.PIISensitivity*(1.0+delta), 0.01, 0.99)
	delta = r.Float64()*0.3 - 0.15
	mutated.BudgetLimitUSD = math.Max(0.0001, mutated.BudgetLimitUSD*(1.0+delta))
	return mutated
}

// Clone creates a deep copy.
func (p *GuardrailPolicy) Clone() Policy {
	return &GuardrailPolicy{
		InjectionSensitivity: p.InjectionSensitivity,
		PIISensitivity:       p.PIISensitivity,
		BudgetLimitUSD:       p.BudgetLimitUSD,
		seed:                 p.seed,
	}
}

// --- AgentWorkflowPolicy ---

// AgentWorkflowPolicy configures agent execution depth and tool selection.
type AgentWorkflowPolicy struct {
	MaxTools    int  // maximum tools selectable
	MaxDepth    int  // maximum agent recursion depth
	UsePlanning bool // whether to use a planner step
	seed        int64
}

// Apply returns latency/cost estimated from workflow params.
func (p *AgentWorkflowPolicy) Apply(_ context.Context, _ *models.LLMRequest) (*Response, error) {
	latency := float64(p.MaxDepth) * 50.0
	if p.UsePlanning {
		latency += 20.0
	}
	cost := float64(p.MaxTools) * 0.001
	cached := p.MaxDepth <= 1 && !p.UsePlanning

	return &Response{
		Provider:  "openai",
		Cached:    cached,
		Error:     false,
		LatencyMs: latency,
		CostUSD:   cost,
	}, nil
}

// Mutate perturbs workflow parameters.
func (p *AgentWorkflowPolicy) Mutate() Policy {
	r := rand.New(rand.NewSource(p.seed + 1))
	mutated := p.Clone().(*AgentWorkflowPolicy)
	mutated.seed = p.seed + 1

	d := int(r.Intn(3)) - 1 // -1, 0, or +1
	mutated.MaxTools = max(1, min(10, p.MaxTools+d))
	mutated.MaxDepth = max(1, min(5, p.MaxDepth+d))

	if r.Float64() < 0.2 {
		mutated.UsePlanning = !p.UsePlanning
	}
	return mutated
}

// Clone creates a deep copy.
func (p *AgentWorkflowPolicy) Clone() Policy {
	return &AgentWorkflowPolicy{
		MaxTools:    p.MaxTools,
		MaxDepth:    p.MaxDepth,
		UsePlanning: p.UsePlanning,
		seed:        p.seed,
	}
}

// ---------------------------------------------------------------------------
// Provider helpers
// ---------------------------------------------------------------------------

func providerLatency(provider string) float64 {
	switch provider {
	case "openai":
		return 200.0
	case "anthropic":
		return 250.0
	case "local":
		return 50.0
	default:
		return 200.0
	}
}

func providerCost(provider string, req *models.LLMRequest) float64 {
	tokens := 150
	if req.Messages != nil {
		for _, m := range req.Messages {
			if m.Content != nil {
				tokens += len(*m.Content) / 4
			}
		}
	}
	switch provider {
	case "openai":
		return float64(tokens) * 0.00002
	case "anthropic":
		return float64(tokens) * 0.000015
	case "local":
		return float64(tokens) * 0.000001
	default:
		return float64(tokens) * 0.00002
	}
}

// ---------------------------------------------------------------------------
// defaultPolicyFor returns the canonical base policy for a headroom dimension.
// ---------------------------------------------------------------------------

func defaultPolicyFor(dim HeadroomDimension) Policy {
	switch dim {
	case DimensionRouting:
		return &RoutingPolicy{
			Weights:      map[string]float64{"openai": 0.5, "anthropic": 0.3, "local": 0.2},
			CacheEnabled: true,
			seed:         0,
		}
	case DimensionCache:
		return &CachePolicy{
			CacheableThreshold: 0.5,
			MaxCacheSizeMB:     100,
			seed:               0,
		}
	case DimensionGuardrails:
		return &GuardrailPolicy{
			InjectionSensitivity: 0.5,
			PIISensitivity:       0.5,
			BudgetLimitUSD:       0.10,
			seed:                 0,
		}
	case DimensionAgentTools:
		return &AgentWorkflowPolicy{
			MaxTools:    5,
			MaxDepth:    3,
			UsePlanning: true,
			seed:        0,
		}
	case DimensionCost:
		return &RoutingPolicy{
			Weights:      map[string]float64{"local": 0.7, "openai": 0.2, "anthropic": 0.1},
			CacheEnabled: true,
			CostAware:    true,
			seed:         0,
		}
	case DimensionLatency:
		return &RoutingPolicy{
			Weights:      map[string]float64{"local": 0.9, "openai": 0.1},
			CacheEnabled: true,
			seed:         0,
		}
	default:
		return &RoutingPolicy{
			Weights: map[string]float64{"openai": 1.0},
			seed:    0,
		}
	}
}

// ---------------------------------------------------------------------------
// ExploreBroad: generate many mutations from a base policy
// ---------------------------------------------------------------------------

// ExploreBroad generates base.Clone() plus numVariations mutated copies,
// evaluated and sorted by score (descending).
func (e *AutonomousExplorer) ExploreBroad(ctx context.Context, base Policy, numVariations int) ([]Policy, error) {
	if e == nil || e.sim == nil {
		return nil, fmt.Errorf("explorer: simulator is required")
	}
	if base == nil {
		return nil, fmt.Errorf("explorer: base policy is nil")
	}
	if len(e.sim.Scenarios()) == 0 {
		return nil, fmt.Errorf("explorer: no scenarios loaded in simulator")
	}

	scenarios := e.sim.Scenarios()
	candidates := make([]Policy, 0, numVariations+1)
	candidates = append(candidates, base.Clone())
	for i := 0; i < numVariations; i++ {
		candidates = append(candidates, base.Mutate())
	}

	// Evaluate each candidate.
	type scoredPolicy struct {
		policy Policy
		score  float64
	}
	scored := make([]scoredPolicy, len(candidates))
	for i, p := range candidates {
		metrics, err := e.sim.ReplayTraffic(ctx, p, scenarios)
		if err != nil {
			scored[i] = scoredPolicy{policy: p, score: 0}
			continue
		}
		scored[i] = scoredPolicy{policy: p, score: scoreMetrics(metrics)}
	}

	// Sort by score descending.
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	result := make([]Policy, len(scored))
	for i, s := range scored {
		result[i] = s.policy
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// ExploreDeep: iteratively optimize the best candidate
// ---------------------------------------------------------------------------

// ExploreDeep runs iterations of mutate->evaluate->select-best on the
// provided candidates and returns the single best policy found.
func (e *AutonomousExplorer) ExploreDeep(ctx context.Context, candidates []Policy, iterations int) (Policy, error) {
	if e == nil || e.sim == nil {
		return nil, fmt.Errorf("explorer: simulator is required")
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("explorer: no candidates provided")
	}
	if len(e.sim.Scenarios()) == 0 {
		return nil, fmt.Errorf("explorer: no scenarios loaded in simulator")
	}

	scenarios := e.sim.Scenarios()
	best := candidates[0]
	bestMetrics, err := e.sim.ReplayTraffic(ctx, best, scenarios)
	if err != nil {
		bestMetrics = nil
	}
	bestScore := scoreMetrics(bestMetrics)

	for i := 0; i < iterations; i++ {
		mutated := best.Mutate()
		metrics, err := e.sim.ReplayTraffic(ctx, mutated, scenarios)
		if err != nil {
			continue
		}
		score := scoreMetrics(metrics)
		if score > bestScore {
			best = mutated
			bestScore = score
		}
	}

	return best, nil
}

// ---------------------------------------------------------------------------
// Explore: full broad-then-deep cycle
// ---------------------------------------------------------------------------

// Explore assesses headroom for the given dimension via HCI, generates
// broad mutations, runs deep optimization, and returns the best policy
// along with an ExplorationResult.
func (e *AutonomousExplorer) Explore(ctx context.Context, dim HeadroomDimension, broadIterations int) (Policy, *ExplorationResult, error) {
	if e == nil || e.sim == nil {
		return nil, nil, fmt.Errorf("explorer: simulator is required")
	}

	// Assess headroom for the target dimension.
	if e.hci == nil {
		return nil, nil, fmt.Errorf("explorer: HCI engine is required for Explore")
	}
	_, err := e.hci.Assess(ctx, dim)
	if err != nil {
		return nil, nil, fmt.Errorf("explorer: HCI assessment failed for %s: %w", dim, err)
	}

	basePolicy := defaultPolicyFor(dim)

	// Broad phase.
	candidates, err := e.ExploreBroad(ctx, basePolicy, broadIterations)
	if err != nil {
		return nil, nil, fmt.Errorf("broad exploration failed: %w", err)
	}

	scenarios := e.sim.Scenarios()
	baseMetrics, err := e.sim.ReplayTraffic(ctx, basePolicy, scenarios)
	if err != nil {
		return nil, nil, fmt.Errorf("base policy evaluation failed: %w", err)
	}
	baseScore := scoreMetrics(baseMetrics)

	// Deep phase.
	deepIterations := 3
	bestPolicy, err := e.ExploreDeep(ctx, candidates, deepIterations)
	if err != nil {
		return nil, nil, fmt.Errorf("deep exploration failed: %w", err)
	}

	bestMetrics, err := e.sim.ReplayTraffic(ctx, bestPolicy, scenarios)
	if err != nil {
		return nil, nil, fmt.Errorf("final policy evaluation failed: %w", err)
	}
	bestScore := scoreMetrics(bestMetrics)

	// Calculate improvement percentage.
	var improvementPct float64
	if baseScore > 0 {
		improvementPct = ((bestScore - baseScore) / baseScore) * 100.0
	} else if bestScore > 0 {
		improvementPct = 100.0
	}

	trace := []ExplorationStep{
		{
			Iteration: 0,
			Policy:    basePolicy,
			Score:     baseScore,
			Metrics:   baseMetrics,
		},
		{
			Iteration: broadIterations + deepIterations + 1,
			Policy:    bestPolicy,
			Score:     bestScore,
			Metrics:   bestMetrics,
		},
	}

	result := &ExplorationResult{
		BestPolicy:       bestPolicy,
		ImprovementPct: improvementPct,
		ExplorationTrace: trace,
	}

	return bestPolicy, result, nil
}
