package rsi

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"

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
	BestPolicy Policy
	// BasePolicy is the policy exploration started from (the baseline the
	// improvement is measured against).
	BasePolicy Policy
	// BaseScore and BestScore are the composite scores (scoreMetrics) on the
	// scenarios exploration ran on.
	BaseScore float64
	BestScore float64
	// ImprovementPct is the relative improvement of BestScore over BaseScore
	// on the exploration (training) scenarios. It is an in-sample figure;
	// the orchestrator's deploy gate uses a held-out comparison instead.
	ImprovementPct   float64
	ExplorationTrace []ExplorationStep
}

// ExploreOptions configures ExploreWithOptions.
type ExploreOptions struct {
	// BroadIterations is the number of mutated variants generated in the
	// broad phase (clamped to [0, MaxExploreIterations]).
	BroadIterations int
	// DeepIterations is the number of hill-climbing steps in the deep phase
	// (clamped to [0, MaxExploreIterations]).
	DeepIterations int
	// Scenarios restricts exploration to these scenarios (e.g. a training
	// split). Nil means all scenarios loaded in the simulator.
	Scenarios []*DreamReplay
	// BasePolicy is the starting point. Nil means the explorer's base
	// policy for the dimension (see AutonomousExplorer.SetBasePolicyFunc).
	BasePolicy Policy
}

// MaxExploreIterations caps broad and deep iterations so a misconfiguration
// cannot turn one RSI cycle into an unbounded CPU burn.
const MaxExploreIterations = 1000

// DefaultDeepIterations is the deep-phase length used by Explore.
const DefaultDeepIterations = 3

// RandMutator is optionally implemented by policies that can mutate using a
// caller-supplied random source. The explorer uses it so that repeated
// mutations of the same parent yield *different* children (Policy.Mutate is
// deterministic in the policy's seed, so calling it N times on one parent
// yields N identical children). The explorer's source is seeded, keeping
// exploration reproducible.
type RandMutator interface {
	MutateRand(r *rand.Rand) Policy
}

// PolicyDescriber is optionally implemented by policies that can describe
// their parameters for audit logs and cycle history.
type PolicyDescriber interface {
	Params() map[string]interface{}
}

// AutonomousExplorer drives broad-then-deep policy search using the DreamSimulator
// for offline policy evaluation and the HCIEngine for headroom guidance.
type AutonomousExplorer struct {
	sim *DreamSimulator
	hci *HCIEngine

	mu         sync.Mutex
	rng        *rand.Rand
	basePolicy func(HeadroomDimension) Policy
}

// NewAutonomousExplorer creates an explorer wired to the given simulator and HCI engine.
func NewAutonomousExplorer(sim *DreamSimulator, hci *HCIEngine) *AutonomousExplorer {
	return &AutonomousExplorer{
		sim: sim,
		hci: hci,
		rng: rand.New(rand.NewSource(1)),
	}
}

// SetSeed reseeds the explorer's mutation source (exploration is fully
// deterministic for a given seed and scenario set).
func (e *AutonomousExplorer) SetSeed(seed int64) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.rng = rand.New(rand.NewSource(seed))
	e.mu.Unlock()
}

// SetBasePolicyFunc overrides the base policy used for a dimension when
// ExploreOptions.BasePolicy is nil. Returning nil falls back to the built-in
// default for that dimension. Pass nil to restore the defaults.
func (e *AutonomousExplorer) SetBasePolicyFunc(fn func(HeadroomDimension) Policy) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.basePolicy = fn
	e.mu.Unlock()
}

// basePolicyFor returns the starting policy for a dimension.
func (e *AutonomousExplorer) basePolicyFor(dim HeadroomDimension) Policy {
	e.mu.Lock()
	fn := e.basePolicy
	e.mu.Unlock()
	if fn != nil {
		if p := fn(dim); p != nil {
			return p
		}
	}
	return defaultPolicyFor(dim)
}

// mutate derives a child from p, using the explorer's seeded source when the
// policy supports it.
func (e *AutonomousExplorer) mutate(p Policy) Policy {
	if rm, ok := p.(RandMutator); ok {
		e.mu.Lock()
		if e.rng == nil {
			e.rng = rand.New(rand.NewSource(1))
		}
		child := rm.MutateRand(e.rng)
		e.mu.Unlock()
		if child != nil {
			return child
		}
	}
	return p.Mutate()
}

// ---------------------------------------------------------------------------
// Composite score
// ---------------------------------------------------------------------------

// Composite score weights and scales. Errors are not a scored dimension:
// an errored request scores 0 (the worst possible outcome), and the aggregate
// score is scaled by the success rate. Previously errors carried a fixed
// 0.2 weight while failing requests also reported zero cost and latency, so a
// policy that blocked or failed every request out-scored a working one and
// the explorer evolved towards "reject everything".
const (
	scoreWeightLatency = 0.375
	scoreWeightCost    = 0.375
	scoreWeightCache   = 0.25
	latencyScaleMs     = 1000.0 // latency at/above which the latency score is 0
	costScalePerReq    = 100.0  // $0.01 per request or more → cost score 0
)

// scoreMetrics produces a composite policy score in [0, 1] from simulated metrics.
// Higher is better: lower latency/cost/errors and higher cache-hit rate.
// Cost is normalised per request (using Requests) so the score does not
// depend on how many scenarios were replayed. The result is never NaN.
func scoreMetrics(m *SimulatedMetrics) float64 {
	if m == nil {
		return 0.0
	}
	perReqCost := m.Cost
	if m.Requests > 0 {
		perReqCost = m.Cost / float64(m.Requests)
	}
	success := successScore(m.AvgLatency, perReqCost, m.CacheHitRate)
	errRate := m.ErrorRate
	if !isFinite(errRate) {
		errRate = 1
	}
	return clamp((1-clamp(errRate, 0, 1))*success, 0, 1)
}

// scoreOutcome scores a single scenario outcome in [0, 1].
func scoreOutcome(o ScenarioOutcome) float64 {
	if o.Error || o.Skipped {
		return 0
	}
	cache := 0.0
	if o.Cached {
		cache = 1
	}
	return successScore(o.LatencyMs, o.CostUSD, cache)
}

// successScore combines latency, per-request cost and cache-hit rate.
// Non-finite or negative latency/cost inputs score as the worst case so bad
// data can never look like a perfect outcome.
func successScore(latencyMs, costPerReq, cacheRate float64) float64 {
	lat := inverseUnitScore(latencyMs, latencyScaleMs)
	cost := inverseUnitScore(costPerReq*costScalePerReq, 1)
	return clamp(scoreWeightLatency*lat+scoreWeightCost*cost+scoreWeightCache*clamp(cacheRate, 0, 1), 0, 1)
}

// inverseUnitScore maps v in [0, scale] linearly onto [1, 0]; anything
// invalid (NaN, Inf, negative) scores 0.
func inverseUnitScore(v, scale float64) float64 {
	if !isFinite(v) || v < 0 {
		return 0
	}
	return 1 - math.Min(v/scale, 1)
}

// ---------------------------------------------------------------------------
// Concrete Policy Implementations
// ---------------------------------------------------------------------------
//
// These policies are *simulation models*: Apply returns a decision that the
// DreamSimulator scores against recorded traffic. Nothing here touches live
// routing, the filesystem, or the network. Deploying a policy is delegated
// to the orchestrator's OnDeploy hook.

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

// Mutate perturbs routing weights and toggles cache/cost flags. It is
// deterministic in the policy's seed.
func (p *RoutingPolicy) Mutate() Policy {
	child := p.MutateRand(rand.New(rand.NewSource(p.seed + 1))).(*RoutingPolicy)
	child.seed = p.seed + 1
	return child
}

// MutateRand perturbs routing weights (±20%) and toggles caching with 20%
// probability, drawing randomness from r.
func (p *RoutingPolicy) MutateRand(r *rand.Rand) Policy {
	mutated := p.Clone().(*RoutingPolicy)

	// Iterate providers in sorted order so the mutation is reproducible
	// (map iteration order is random).
	names := make([]string, 0, len(mutated.Weights))
	for provider := range mutated.Weights {
		names = append(names, provider)
	}
	sort.Strings(names)
	for _, provider := range names {
		w := mutated.Weights[provider]
		if !isFinite(w) || w <= 0 {
			w = 0.01
		}
		delta := r.Float64()*0.4 - 0.2
		mutated.Weights[provider] = math.Max(0.01, w*(1.0+delta))
	}

	if r.Float64() < 0.2 {
		mutated.CacheEnabled = !mutated.CacheEnabled
	}
	mutated.seed = r.Int63()
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

// Params describes the policy for audit logs.
func (p *RoutingPolicy) Params() map[string]interface{} {
	weights := make(map[string]float64, len(p.Weights))
	for k, v := range p.Weights {
		if isFinite(v) {
			weights[k] = v
		}
	}
	return map[string]interface{}{
		"weights":       weights,
		"cache_enabled": p.CacheEnabled,
		"cost_aware":    p.CostAware,
		"provider":      p.selectProvider(),
	}
}

func (p *RoutingPolicy) selectProvider() string {
	best := ""
	bestWeight := math.Inf(-1)
	for provider, weight := range p.Weights {
		if !isFinite(weight) {
			continue
		}
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
	return requestContentLength(req) < 2048
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

	contentLen := requestContentLength(req)

	// CacheableThreshold is a fraction (0-1) of a 2048-char baseline.
	// Out-of-range or non-finite values are clamped (NaN disables caching).
	threshold := int(2048.0 * clamp(p.CacheableThreshold, 0, 1))
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

// Mutate perturbs the cacheable threshold. It is deterministic in the seed.
func (p *CachePolicy) Mutate() Policy {
	child := p.MutateRand(rand.New(rand.NewSource(p.seed + 1))).(*CachePolicy)
	child.seed = p.seed + 1
	return child
}

// MutateRand perturbs the cacheable threshold by ±10%.
func (p *CachePolicy) MutateRand(r *rand.Rand) Policy {
	mutated := p.Clone().(*CachePolicy)
	delta := r.Float64()*0.2 - 0.1
	mutated.CacheableThreshold = clamp(mutated.CacheableThreshold*(1.0+delta), 0.01, 1.0)
	mutated.seed = r.Int63()
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

// Params describes the policy for audit logs.
func (p *CachePolicy) Params() map[string]interface{} {
	return map[string]interface{}{
		"cacheable_threshold": clamp(p.CacheableThreshold, 0, 1),
		"max_cache_size_mb":   p.MaxCacheSizeMB,
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

	contentLen := requestContentLength(req)

	maxLen := int(5000.0 / clamp(p.InjectionSensitivity, 0.01, 1))
	blocked := contentLen > maxLen

	return &Response{
		Provider:  "openai",
		Cached:    false,
		Error:     blocked,
		LatencyMs: 1.0,
		CostUSD:   0,
	}, nil
}

// Mutate perturbs sensitivity thresholds. It is deterministic in the seed.
func (p *GuardrailPolicy) Mutate() Policy {
	child := p.MutateRand(rand.New(rand.NewSource(p.seed + 1))).(*GuardrailPolicy)
	child.seed = p.seed + 1
	return child
}

// MutateRand perturbs sensitivities (±10%) and the budget (±15%).
func (p *GuardrailPolicy) MutateRand(r *rand.Rand) Policy {
	mutated := p.Clone().(*GuardrailPolicy)

	delta := r.Float64()*0.2 - 0.1
	mutated.InjectionSensitivity = clamp(mutated.InjectionSensitivity*(1.0+delta), 0.01, 0.99)
	delta = r.Float64()*0.2 - 0.1
	mutated.PIISensitivity = clamp(mutated.PIISensitivity*(1.0+delta), 0.01, 0.99)
	delta = r.Float64()*0.3 - 0.15
	budget := mutated.BudgetLimitUSD
	if !isFinite(budget) {
		budget = 0.0001
	}
	mutated.BudgetLimitUSD = math.Max(0.0001, budget*(1.0+delta))
	mutated.seed = r.Int63()
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

// Params describes the policy for audit logs.
func (p *GuardrailPolicy) Params() map[string]interface{} {
	budget := p.BudgetLimitUSD
	if !isFinite(budget) {
		budget = 0
	}
	return map[string]interface{}{
		"injection_sensitivity": clamp(p.InjectionSensitivity, 0, 1),
		"pii_sensitivity":       clamp(p.PIISensitivity, 0, 1),
		"budget_limit_usd":      budget,
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
	depth := max(0, p.MaxDepth)
	tools := max(0, p.MaxTools)
	latency := float64(depth) * 50.0
	if p.UsePlanning {
		latency += 20.0
	}
	cost := float64(tools) * 0.001
	cached := depth <= 1 && !p.UsePlanning

	return &Response{
		Provider:  "openai",
		Cached:    cached,
		Error:     false,
		LatencyMs: latency,
		CostUSD:   cost,
	}, nil
}

// Mutate perturbs workflow parameters. It is deterministic in the seed.
func (p *AgentWorkflowPolicy) Mutate() Policy {
	child := p.MutateRand(rand.New(rand.NewSource(p.seed + 1))).(*AgentWorkflowPolicy)
	child.seed = p.seed + 1
	return child
}

// MutateRand moves tool/depth limits by -1, 0 or +1 and toggles planning
// with 20% probability.
func (p *AgentWorkflowPolicy) MutateRand(r *rand.Rand) Policy {
	mutated := p.Clone().(*AgentWorkflowPolicy)

	d := r.Intn(3) - 1 // -1, 0, or +1
	mutated.MaxTools = max(1, min(10, p.MaxTools+d))
	mutated.MaxDepth = max(1, min(5, p.MaxDepth+d))

	if r.Float64() < 0.2 {
		mutated.UsePlanning = !p.UsePlanning
	}
	mutated.seed = r.Int63()
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

// Params describes the policy for audit logs.
func (p *AgentWorkflowPolicy) Params() map[string]interface{} {
	return map[string]interface{}{
		"max_tools":    p.MaxTools,
		"max_depth":    p.MaxDepth,
		"use_planning": p.UsePlanning,
	}
}

// describePolicy returns the policy's parameters, or nil when unknown.
func describePolicy(p Policy) map[string]interface{} {
	if d, ok := p.(PolicyDescriber); ok && d != nil {
		return d.Params()
	}
	return nil
}

// requestContentLength returns the total message content length of req.
func requestContentLength(req *models.LLMRequest) int {
	if req == nil {
		return 0
	}
	total := 0
	for _, m := range req.Messages {
		if m.Content != nil {
			total += len(*m.Content)
		}
	}
	return total
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
	tokens := 150 + requestContentLength(req)/4
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

// clampIterations bounds an iteration count to [0, MaxExploreIterations].
func clampIterations(n int) int {
	if n < 0 {
		return 0
	}
	if n > MaxExploreIterations {
		return MaxExploreIterations
	}
	return n
}

// scorePolicy replays p over scenarios and returns its composite score.
// Replay errors caused by context cancellation are returned; any other
// policy failure scores 0.
func (e *AutonomousExplorer) scorePolicy(ctx context.Context, p Policy, scenarios []*DreamReplay) (float64, *SimulatedMetrics, error) {
	metrics, err := e.sim.ReplayTraffic(ctx, p, scenarios)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, nil, fmt.Errorf("explorer: cancelled: %w", ctxErr)
		}
		return 0, nil, nil
	}
	return scoreMetrics(metrics), metrics, nil
}

// ---------------------------------------------------------------------------
// ExploreBroad: generate many mutations from a base policy
// ---------------------------------------------------------------------------

// ExploreBroad generates base.Clone() plus numVariations mutated copies,
// evaluated and sorted by score (descending, stable).
func (e *AutonomousExplorer) ExploreBroad(ctx context.Context, base Policy, numVariations int) ([]Policy, error) {
	if e == nil || e.sim == nil {
		return nil, fmt.Errorf("explorer: simulator is required")
	}
	policies, _, err := e.exploreBroad(ctx, base, numVariations, e.sim.Scenarios())
	return policies, err
}

func (e *AutonomousExplorer) exploreBroad(ctx context.Context, base Policy, numVariations int, scenarios []*DreamReplay) ([]Policy, []float64, error) {
	if base == nil {
		return nil, nil, fmt.Errorf("explorer: base policy is nil")
	}
	if len(scenarios) == 0 {
		return nil, nil, fmt.Errorf("explorer: no scenarios loaded in simulator")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	numVariations = clampIterations(numVariations)

	candidates := make([]Policy, 0, numVariations+1)
	candidates = append(candidates, base.Clone())
	for i := 0; i < numVariations; i++ {
		if child := e.mutate(base); child != nil {
			candidates = append(candidates, child)
		}
	}

	// Evaluate each candidate.
	type scoredPolicy struct {
		policy Policy
		score  float64
	}
	scored := make([]scoredPolicy, len(candidates))
	for i, p := range candidates {
		score, _, err := e.scorePolicy(ctx, p, scenarios)
		if err != nil {
			return nil, nil, err
		}
		scored[i] = scoredPolicy{policy: p, score: score}
	}

	// Sort by score descending; stable so ties keep generation order
	// (the unmodified base clone wins ties, avoiding churn for no gain).
	sort.SliceStable(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	policies := make([]Policy, len(scored))
	scores := make([]float64, len(scored))
	for i, s := range scored {
		policies[i] = s.policy
		scores[i] = s.score
	}
	return policies, scores, nil
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
	best, _, _, err := e.exploreDeep(ctx, candidates, iterations, e.sim.Scenarios())
	return best, err
}

func (e *AutonomousExplorer) exploreDeep(ctx context.Context, candidates []Policy, iterations int, scenarios []*DreamReplay) (Policy, float64, []ExplorationStep, error) {
	var best Policy
	for _, c := range candidates {
		if c != nil {
			best = c
			break
		}
	}
	if best == nil {
		return nil, 0, nil, fmt.Errorf("explorer: no candidates provided")
	}
	if len(scenarios) == 0 {
		return nil, 0, nil, fmt.Errorf("explorer: no scenarios loaded in simulator")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	iterations = clampIterations(iterations)

	bestScore, _, err := e.scorePolicy(ctx, best, scenarios)
	if err != nil {
		return nil, 0, nil, err
	}

	var steps []ExplorationStep
	for i := 0; i < iterations; i++ {
		mutated := e.mutate(best)
		if mutated == nil {
			continue
		}
		score, metrics, err := e.scorePolicy(ctx, mutated, scenarios)
		if err != nil {
			return nil, 0, nil, err
		}
		if score > bestScore {
			best = mutated
			bestScore = score
			steps = append(steps, ExplorationStep{Iteration: i + 1, Policy: mutated, Score: score, Metrics: metrics})
		}
	}

	return best, bestScore, steps, nil
}

// ---------------------------------------------------------------------------
// Explore: full broad-then-deep cycle
// ---------------------------------------------------------------------------

// Explore assesses headroom for the given dimension via HCI, generates
// broad mutations, runs deep optimization (DefaultDeepIterations steps), and
// returns the best policy along with an ExplorationResult.
func (e *AutonomousExplorer) Explore(ctx context.Context, dim HeadroomDimension, broadIterations int) (Policy, *ExplorationResult, error) {
	return e.ExploreWithOptions(ctx, dim, ExploreOptions{
		BroadIterations: broadIterations,
		DeepIterations:  DefaultDeepIterations,
	})
}

// ExploreWithOptions is Explore with explicit broad/deep iteration counts,
// an optional scenario subset (e.g. a training split) and an optional base
// policy.
func (e *AutonomousExplorer) ExploreWithOptions(ctx context.Context, dim HeadroomDimension, opts ExploreOptions) (Policy, *ExplorationResult, error) {
	if e == nil || e.sim == nil {
		return nil, nil, fmt.Errorf("explorer: simulator is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Assess headroom for the target dimension (also validates it).
	if e.hci == nil {
		return nil, nil, fmt.Errorf("explorer: HCI engine is required for Explore")
	}
	if _, err := e.hci.Assess(ctx, dim); err != nil {
		return nil, nil, fmt.Errorf("explorer: HCI assessment failed for %s: %w", dim, err)
	}

	scenarios := opts.Scenarios
	if scenarios == nil {
		scenarios = e.sim.Scenarios()
	}
	basePolicy := opts.BasePolicy
	if basePolicy == nil {
		basePolicy = e.basePolicyFor(dim)
	}
	broad := clampIterations(opts.BroadIterations)
	deep := clampIterations(opts.DeepIterations)

	// Broad phase.
	candidates, _, err := e.exploreBroad(ctx, basePolicy, broad, scenarios)
	if err != nil {
		return nil, nil, fmt.Errorf("broad exploration failed: %w", err)
	}

	baseScore, baseMetrics, err := e.scorePolicy(ctx, basePolicy, scenarios)
	if err != nil {
		return nil, nil, fmt.Errorf("base policy evaluation failed: %w", err)
	}

	// Deep phase.
	bestPolicy, bestScore, deepSteps, err := e.exploreDeep(ctx, candidates, deep, scenarios)
	if err != nil {
		return nil, nil, fmt.Errorf("deep exploration failed: %w", err)
	}
	bestScore, bestMetrics, err := e.scorePolicy(ctx, bestPolicy, scenarios)
	if err != nil {
		return nil, nil, fmt.Errorf("final policy evaluation failed: %w", err)
	}
	if bestScore < baseScore {
		// Never report a regression as the "best" policy.
		bestPolicy, bestScore, bestMetrics = basePolicy, baseScore, baseMetrics
	}

	trace := make([]ExplorationStep, 0, len(deepSteps)+2)
	trace = append(trace, ExplorationStep{Iteration: 0, Policy: basePolicy, Score: baseScore, Metrics: baseMetrics})
	for _, st := range deepSteps {
		st.Iteration += broad
		trace = append(trace, st)
	}
	trace = append(trace, ExplorationStep{
		Iteration: broad + deep + 1,
		Policy:    bestPolicy,
		Score:     bestScore,
		Metrics:   bestMetrics,
	})

	result := &ExplorationResult{
		BestPolicy:       bestPolicy,
		BasePolicy:       basePolicy,
		BaseScore:        baseScore,
		BestScore:        bestScore,
		ImprovementPct:   relativeImprovementPct(baseScore, bestScore),
		ExplorationTrace: trace,
	}

	return bestPolicy, result, nil
}
