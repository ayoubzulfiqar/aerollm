package router

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// Defaults applied when configuration values are missing or invalid.
const (
	DefaultMaxFailures      = 5
	DefaultResetTimeout     = 60 * time.Second
	DefaultHalfOpenMaxCalls = 1
	DefaultMaxAttempts      = 3

	// defaultOutputTokens is the output-size estimate used by cost routing
	// when a request does not set max_tokens.
	defaultOutputTokens = 256
	// latencyEWMAAlpha weights the newest latency sample.
	latencyEWMAAlpha = 0.2
	// rateWindow is the length of a rate-limit accounting window.
	rateWindow = time.Minute
)

// ProviderRateLimits holds the TPM and RPM limits for a provider.
type ProviderRateLimits struct {
	TPM int64 // Tokens Per Minute
	RPM int64 // Requests Per Minute
}

// ProviderUsage holds real-time usage metrics for rate-limit-aware routing.
//
// InflightRequests is maintained atomically. The per-minute counters,
// LastReset and HealthLatencyMs are guarded by an internal mutex; use the
// CircuitBreaker methods rather than touching them concurrently.
type ProviderUsage struct {
	InflightRequests   int64     // atomic counter for inflight requests
	TokenCountMinute   int64     // tokens in current minute window
	RequestCountMinute int64     // requests in current minute window
	LastReset          time.Time // start of the current minute window
	HealthLatencyMs    float64   // EWMA of observed successful-call latency

	mu sync.Mutex
}

// rollLocked starts a new window when the current one has elapsed (or the
// clock went backwards). Caller must hold u.mu.
func (u *ProviderUsage) rollLocked(now time.Time) {
	if u.LastReset.IsZero() || now.Sub(u.LastReset) >= rateWindow || now.Before(u.LastReset) {
		u.TokenCountMinute = 0
		u.RequestCountMinute = 0
		u.LastReset = now
	}
}

func (u *ProviderUsage) utilization(limits ProviderRateLimits, now time.Time) float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollLocked(now)
	var maxFraction float64
	if limits.RPM > 0 {
		if f := float64(u.RequestCountMinute) / float64(limits.RPM); f > maxFraction {
			maxFraction = f
		}
	}
	if limits.TPM > 0 {
		if f := float64(u.TokenCountMinute) / float64(limits.TPM); f > maxFraction {
			maxFraction = f
		}
	}
	return maxFraction
}

func (u *ProviderUsage) addRequest(now time.Time) {
	u.mu.Lock()
	u.rollLocked(now)
	u.RequestCountMinute++
	u.mu.Unlock()
}

func (u *ProviderUsage) addTokens(tokens int64, now time.Time) {
	if tokens <= 0 {
		return
	}
	u.mu.Lock()
	u.rollLocked(now)
	u.TokenCountMinute += tokens
	u.mu.Unlock()
}

// Config holds router configuration.
type Config struct {
	Strategy      string
	BreakerConfig CircuitBreakerConfig
	// ProviderRateLimits maps provider name to its rate limits.
	ProviderRateLimits map[string]ProviderRateLimits
	// CostMap provides dynamic model pricing for cost-based routing.
	CostMap *intelligence.ModelCostMap
	// MaxAttempts bounds how many providers ExecuteWithFallback tries
	// (default 3, capped at the number of candidates).
	MaxAttempts int
}

// CircuitBreakerConfig holds circuit breaker settings.
type CircuitBreakerConfig struct {
	MaxFailures      int           `json:"max_failures"`
	ResetTimeout     time.Duration `json:"reset_timeout"`
	HalfOpenMaxCalls int           `json:"half_open_max_calls"`
}

func (c CircuitBreakerConfig) withDefaults() CircuitBreakerConfig {
	if c.MaxFailures <= 0 {
		c.MaxFailures = DefaultMaxFailures
	}
	if c.ResetTimeout <= 0 {
		c.ResetTimeout = DefaultResetTimeout
	}
	if c.HalfOpenMaxCalls <= 0 {
		c.HalfOpenMaxCalls = DefaultHalfOpenMaxCalls
	}
	return c
}

// CircuitBreakerState represents the state of a circuit breaker.
type CircuitBreakerState int

const (
	StateClosed CircuitBreakerState = iota
	StateHalfOpen
	StateOpen
)

// String returns the state name.
func (s CircuitBreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half_open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

// outcome classifies a call result for the circuit breaker.
type outcome int

const (
	outcomeSuccess outcome = iota // healthy response
	outcomeFailure                // provider-health failure (retryable)
	outcomeNeutral                // non-retryable error: provider is alive
	outcomeIgnore                 // says nothing about provider health
)

// classify maps a call result onto a breaker outcome. Only errors that
// indicate an unhealthy provider (providers.IsRetryable) count as failures;
// caller cancellation is ignored and other errors (4xx) prove liveness.
func classify(ctx context.Context, err error) outcome {
	switch {
	case err == nil:
		return outcomeSuccess
	case ctx != nil && ctx.Err() != nil,
		errors.Is(err, context.Canceled),
		errors.Is(err, providers.ErrStreamingNotSupported),
		errors.Is(err, providers.ErrCircuitOpen):
		return outcomeIgnore
	case providers.IsRetryable(err):
		return outcomeFailure
	default:
		return outcomeNeutral
	}
}

// CircuitBreaker wraps a provider with circuit breaker logic and inflight
// tracking. It implements providers.Provider and providers.StreamingProvider.
type CircuitBreaker struct {
	provider         providers.Provider
	maxFailures      int
	resetTimeout     time.Duration
	halfOpenMaxCalls int
	usage            *ProviderUsage

	mu               sync.RWMutex
	state            CircuitBreakerState
	gen              uint64 // incremented on every state transition
	failures         int64  // consecutive failures in the current generation
	totalFailures    int64
	lastFailTime     time.Time // when the breaker last opened / last failure
	halfOpenInflight int
	latencyEWMA      float64
	latencySamples   int64
}

// NewCircuitBreaker creates a new circuit breaker for a provider. Invalid
// maxFailures/resetTimeout fall back to defaults; half-open admits a single
// probe call (see NewCircuitBreakerWithConfig).
func NewCircuitBreaker(p providers.Provider, maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return NewCircuitBreakerWithConfig(p, CircuitBreakerConfig{MaxFailures: maxFailures, ResetTimeout: resetTimeout})
}

// NewCircuitBreakerWithConfig creates a circuit breaker from a config.
func NewCircuitBreakerWithConfig(p providers.Provider, cfg CircuitBreakerConfig) *CircuitBreaker {
	cfg = cfg.withDefaults()
	return &CircuitBreaker{
		provider:         p,
		maxFailures:      cfg.MaxFailures,
		resetTimeout:     cfg.ResetTimeout,
		halfOpenMaxCalls: cfg.HalfOpenMaxCalls,
		state:            StateClosed,
		usage:            &ProviderUsage{LastReset: time.Now()},
	}
}

// Name returns the provider name.
func (c *CircuitBreaker) Name() string { return c.provider.Name() }

// Type returns the provider type string.
func (c *CircuitBreaker) Type() providers.ProviderType { return c.provider.Type() }

// Unwrap returns the wrapped provider (e.g. to reach optional interfaces
// such as providers.MultiEndpointProvider).
func (c *CircuitBreaker) Unwrap() providers.Provider { return c.provider }

// SupportsModel forwards to the wrapped provider's providers.ModelSupporter
// implementation; providers that do not declare models support everything.
func (c *CircuitBreaker) SupportsModel(model string) bool {
	if ms, ok := c.provider.(providers.ModelSupporter); ok {
		return ms.SupportsModel(model)
	}
	return true
}

// DefaultModel forwards to the wrapped provider's providers.DefaultModeler
// implementation, or returns "".
func (c *CircuitBreaker) DefaultModel() string {
	if dm, ok := c.provider.(providers.DefaultModeler); ok {
		return dm.DefaultModel()
	}
	return ""
}

// State returns the current breaker state. An open breaker whose reset
// timeout has elapsed reports StateHalfOpen (it will admit a probe).
func (c *CircuitBreaker) State() CircuitBreakerState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state == StateOpen && time.Since(c.lastFailTime) >= c.resetTimeout {
		return StateHalfOpen
	}
	return c.state
}

// Inflight returns the current number of inflight requests.
func (c *CircuitBreaker) Inflight() int64 {
	return atomic.LoadInt64(&c.usage.InflightRequests)
}

// RecordInflight increments the inflight counter.
func (c *CircuitBreaker) RecordInflight() {
	atomic.AddInt64(&c.usage.InflightRequests, 1)
}

// ReleaseInflight decrements the inflight counter.
func (c *CircuitBreaker) ReleaseInflight() {
	atomic.AddInt64(&c.usage.InflightRequests, -1)
}

// Utilization returns the provider's current TPM/RPM usage fraction
// (0.0 = idle, 1.0 = at the limit, >1 = over) without recording anything.
func (c *CircuitBreaker) Utilization(limits ProviderRateLimits) float64 {
	if limits.TPM <= 0 && limits.RPM <= 0 {
		return 0
	}
	return c.usage.utilization(limits, time.Now())
}

// CheckRateLimit returns the current usage fraction (0.0 to 1.0+) against
// limits. It is read-only: requests are counted when they are actually
// executed (ChatCompletions/StreamChatCompletions or RecordRequest), not when
// a provider is merely evaluated for routing.
func (c *CircuitBreaker) CheckRateLimit(limits ProviderRateLimits) float64 {
	return c.Utilization(limits)
}

// RecordRequest counts one request against the current minute window.
func (c *CircuitBreaker) RecordRequest() {
	c.usage.addRequest(time.Now())
}

// RecordTokens adds tokens to the current minute's usage count.
func (c *CircuitBreaker) RecordTokens(tokens int64) {
	c.usage.addTokens(tokens, time.Now())
}

// admit decides whether a call may proceed. It returns the breaker
// generation the call belongs to and whether it is a half-open probe.
func (c *CircuitBreaker) admit() (gen uint64, probe bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case StateClosed:
		return c.gen, false, nil
	case StateOpen:
		remaining := c.resetTimeout - time.Since(c.lastFailTime)
		if remaining > 0 {
			return 0, false, &CircuitBreakerOpenError{Provider: c.provider.Name(), RetryAfter: remaining}
		}
		c.setStateLocked(StateHalfOpen)
	}
	// Half-open: admit a bounded number of concurrent probes.
	if c.halfOpenInflight >= c.halfOpenMaxCalls {
		return 0, false, &CircuitBreakerOpenError{Provider: c.provider.Name(), HalfOpen: true}
	}
	c.halfOpenInflight++
	return c.gen, true, nil
}

// setStateLocked transitions the breaker. Caller must hold c.mu.
func (c *CircuitBreaker) setStateLocked(s CircuitBreakerState) {
	c.state = s
	c.gen++
	c.halfOpenInflight = 0
	switch s {
	case StateClosed:
		c.failures = 0
	case StateOpen:
		c.lastFailTime = time.Now()
	}
}

// finish records the result of an admitted call. latency <= 0 records no
// latency sample.
func (c *CircuitBreaker) finish(gen uint64, probe bool, oc outcome, latency time.Duration) {
	var ewma float64
	var sampled bool

	c.mu.Lock()
	if probe && gen == c.gen && c.state == StateHalfOpen && c.halfOpenInflight > 0 {
		c.halfOpenInflight--
	}
	switch oc {
	case outcomeFailure:
		c.totalFailures++
	case outcomeSuccess:
		if latency > 0 {
			ms := float64(latency) / float64(time.Millisecond)
			if c.latencySamples == 0 {
				c.latencyEWMA = ms
			} else {
				c.latencyEWMA = latencyEWMAAlpha*ms + (1-latencyEWMAAlpha)*c.latencyEWMA
			}
			c.latencySamples++
			ewma, sampled = c.latencyEWMA, true
		}
	}
	// Results from an older generation (e.g. calls admitted before the
	// breaker opened) must not drive state transitions.
	if gen == c.gen {
		switch oc {
		case outcomeSuccess, outcomeNeutral:
			if c.state == StateHalfOpen {
				c.setStateLocked(StateClosed)
			} else if oc == outcomeSuccess {
				c.failures = 0
			}
		case outcomeFailure:
			c.failures++
			c.lastFailTime = time.Now()
			if c.state == StateHalfOpen || c.failures >= int64(c.maxFailures) {
				c.setStateLocked(StateOpen)
			}
		}
	}
	c.mu.Unlock()

	if sampled {
		c.usage.mu.Lock()
		c.usage.HealthLatencyMs = ewma
		c.usage.mu.Unlock()
	}
}

// ChatCompletions delegates to the underlying provider, recording
// success/failure and managing inflight tracking.
func (c *CircuitBreaker) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	gen, probe, err := c.admit()
	if err != nil {
		return nil, err
	}
	c.RecordInflight()
	defer c.ReleaseInflight()
	c.RecordRequest()

	start := time.Now()
	resp, err := c.provider.ChatCompletions(ctx, req)
	if err == nil && resp == nil {
		err = providers.BadResponseError(c.provider.Name(), errors.New("provider returned no response"))
	}
	c.finish(gen, probe, classify(ctx, err), time.Since(start))
	if err != nil {
		return nil, err
	}
	if resp.Usage != nil {
		c.RecordTokens(int64(resp.Usage.TotalTokens))
	}
	return resp, nil
}

// StreamChatCompletions streams through the wrapped provider when it
// implements providers.StreamingProvider, otherwise it returns
// providers.ErrStreamingNotSupported (callers should fall back to
// ChatCompletions). A failure to start the stream counts against the
// breaker; so does a retryable mid-stream error chunk.
func (c *CircuitBreaker) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	sp, ok := c.provider.(providers.StreamingProvider)
	if !ok {
		return nil, providers.ErrStreamingNotSupported
	}
	gen, probe, err := c.admit()
	if err != nil {
		return nil, err
	}
	c.RecordInflight()
	c.RecordRequest()

	in, err := sp.StreamChatCompletions(ctx, req)
	if err == nil && in == nil {
		err = providers.BadResponseError(c.provider.Name(), errors.New("provider returned no stream"))
	}
	if err != nil {
		c.ReleaseInflight()
		c.finish(gen, probe, classify(ctx, err), 0)
		return nil, err
	}

	out := make(chan models.StreamChunk)
	go func() {
		oc := outcomeSuccess
		var usage *models.Usage
		defer func() {
			if usage != nil {
				c.RecordTokens(int64(usage.TotalTokens))
			}
			c.finish(gen, probe, oc, 0)
			c.ReleaseInflight()
			close(out)
		}()
		// abandon stops forwarding; the producer is required to close its
		// channel on ctx cancellation, so drain it to let it exit.
		abandon := func() {
			oc = outcomeIgnore
			go func() {
				for range in {
				}
			}()
		}
		for {
			select {
			case <-ctx.Done():
				abandon()
				return
			case chunk, ok := <-in:
				if !ok {
					return
				}
				if chunk.Usage != nil {
					usage = chunk.Usage
				}
				if chunk.Err != nil {
					oc = classify(ctx, chunk.Err)
				}
				select {
				case out <- chunk:
				case <-ctx.Done():
					abandon()
					return
				}
			}
		}
	}()
	return out, nil
}

// latencyMs returns the observed latency EWMA, falling back to the
// provider-reported latency. Invalid values map to +Inf.
func (c *CircuitBreaker) latencyMs() float64 {
	c.mu.RLock()
	samples, ewma := c.latencySamples, c.latencyEWMA
	c.mu.RUnlock()
	v := ewma
	if samples == 0 {
		v = c.provider.Health().LatencyMs
	}
	if math.IsNaN(v) || v < 0 {
		return math.Inf(1)
	}
	return v
}

// Health returns the health status including circuit breaker state.
func (c *CircuitBreaker) Health() providers.ProviderHealth {
	base := c.provider.Health() // never call out while holding c.mu

	c.mu.RLock()
	state := c.state
	openExpired := state == StateOpen && time.Since(c.lastFailTime) >= c.resetTimeout
	totalFailures := c.totalFailures
	ewma, samples := c.latencyEWMA, c.latencySamples
	c.mu.RUnlock()

	open := state == StateOpen && !openExpired
	base.CircuitOpen = base.CircuitOpen || open
	if open {
		base.Healthy = false
	}
	if (base.LatencyMs == 0 || math.IsNaN(base.LatencyMs)) && samples > 0 {
		base.LatencyMs = ewma
	}
	if totalFailures > base.Failures {
		base.Failures = totalFailures
	}
	if base.Name == "" {
		base.Name = c.provider.Name()
	}
	return base
}

// Close releases resources.
func (c *CircuitBreaker) Close() error { return c.provider.Close() }

// CircuitBreakerOpenError is returned when the circuit breaker rejects a
// call. It matches providers.ErrCircuitOpen via errors.Is and is retryable.
type CircuitBreakerOpenError struct {
	Provider string
	// RetryAfter is the remaining open time (0 when unknown / half-open).
	RetryAfter time.Duration
	// HalfOpen is true when the rejection was due to the half-open probe
	// limit rather than a fully open circuit.
	HalfOpen bool
}

// Error implements error.
func (e *CircuitBreakerOpenError) Error() string {
	if e.HalfOpen {
		return "circuit breaker half-open (probe limit reached) for provider: " + e.Provider
	}
	return "circuit breaker open for provider: " + e.Provider
}

// Is makes errors.Is(err, providers.ErrCircuitOpen) true.
func (e *CircuitBreakerOpenError) Is(target error) bool {
	return target == providers.ErrCircuitOpen
}

// Router routes requests to LLM providers with intelligent strategies.
type Router struct {
	mu           sync.RWMutex
	providers    []*CircuitBreaker
	strategy     string
	breakerCfg   CircuitBreakerConfig
	rateLimits   map[string]ProviderRateLimits
	costMap      *intelligence.ModelCostMap
	maxAttempts  int
	currentIndex atomic.Uint64
}

// New creates a new Router with the given configuration.
func New(cfg Config) *Router {
	return &Router{
		strategy:    cfg.Strategy,
		breakerCfg:  cfg.BreakerConfig.withDefaults(),
		rateLimits:  copyLimits(cfg.ProviderRateLimits),
		costMap:     cfg.CostMap,
		maxAttempts: cfg.MaxAttempts,
	}
}

func copyLimits(in map[string]ProviderRateLimits) map[string]ProviderRateLimits {
	if in == nil {
		return nil
	}
	out := make(map[string]ProviderRateLimits, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// RegisterProvider adds a provider to the router with circuit breaker
// protection. Registering a name that already exists replaces that entry
// (with a fresh breaker) in place; the old provider is not closed.
func (r *Router) RegisterProvider(p providers.Provider) {
	if p == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cb := NewCircuitBreakerWithConfig(p, r.breakerCfg)
	for i, existing := range r.providers {
		if existing.Name() == p.Name() {
			r.providers[i] = cb
			return
		}
	}
	r.providers = append(r.providers, cb)
}

// RemoveProvider unregisters the named provider. It reports whether a
// provider was removed. The provider is not closed.
func (r *Router) RemoveProvider(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, cb := range r.providers {
		if cb.Name() == name {
			r.providers = append(r.providers[:i:i], r.providers[i+1:]...)
			return true
		}
	}
	return false
}

// GetProvider returns a provider by name.
func (r *Router) GetProvider(name string) (providers.Provider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, cb := range r.providers {
		if cb.Name() == name {
			return cb, true
		}
	}
	return nil, false
}

// routeSnapshot is a consistent copy of router state for one decision.
type routeSnapshot struct {
	providers   []*CircuitBreaker
	strategy    string
	rateLimits  map[string]ProviderRateLimits
	costMap     *intelligence.ModelCostMap
	maxAttempts int
}

func (r *Router) snapshot() routeSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ps := make([]*CircuitBreaker, len(r.providers))
	copy(ps, r.providers)
	return routeSnapshot{
		providers:   ps,
		strategy:    r.strategy,
		rateLimits:  r.rateLimits,
		costMap:     r.costMap,
		maxAttempts: r.maxAttempts,
	}
}

// Route selects a provider for the given request based on the configured
// strategy. It never returns (nil, nil).
func (r *Router) Route(ctx context.Context, req *models.LLMRequest) (providers.Provider, error) {
	cands, err := r.Candidates(ctx, req)
	if err != nil {
		return nil, err
	}
	return cands[0], nil
}

// Candidates returns the available providers for req ordered by the
// configured strategy: the strategy's pick first, then the remaining
// providers in the order the strategy prefers them. Providers whose circuit
// is open, or that declare (providers.ModelSupporter) they do not serve
// req.Model, are excluded. The returned providers are circuit-breaker
// wrapped. It returns a *NoProviderError when nothing is available.
func (r *Router) Candidates(ctx context.Context, req *models.LLMRequest) ([]providers.Provider, error) {
	_ = ctx
	if req == nil {
		req = &models.LLMRequest{}
	}
	snap := r.snapshot()
	available := filterAvailable(snap.providers, req.Model, time.Now())
	if len(available) == 0 {
		return nil, &NoProviderError{Model: req.Model}
	}

	var ordered []*CircuitBreaker
	switch snap.strategy {
	case "least_busy":
		ordered = sortByKey(available, func(cb *CircuitBreaker) float64 { return float64(cb.Inflight()) })
	case "usage_based":
		ordered = sortByKey(available, func(cb *CircuitBreaker) float64 {
			limits := snap.rateLimits[cb.Name()]
			if limits.TPM <= 0 && limits.RPM <= 0 {
				return -1 // no limits configured: full headroom
			}
			return -(1 - cb.Utilization(limits)) // most headroom first
		})
	case "latency":
		ordered = sortByKey(available, func(cb *CircuitBreaker) float64 { return cb.latencyMs() })
	case "cost":
		ordered = sortByKey(available, func(cb *CircuitBreaker) float64 {
			return estimateCostWithMap(cb, req, snap.costMap)
		})
	case "fallback":
		ordered = available
	default: // "round_robin" and unknown strategies
		start := int((r.currentIndex.Add(1) - 1) % uint64(len(available)))
		ordered = make([]*CircuitBreaker, 0, len(available))
		ordered = append(ordered, available[start:]...)
		ordered = append(ordered, available[:start]...)
	}

	out := make([]providers.Provider, len(ordered))
	for i, cb := range ordered {
		out[i] = cb
	}
	return out, nil
}

// filterAvailable returns breakers that can accept a call now and that do
// not declare lack of support for model, preserving registration order.
func filterAvailable(all []*CircuitBreaker, model string, now time.Time) []*CircuitBreaker {
	var available []*CircuitBreaker
	for _, cb := range all {
		if model != "" {
			if ms, ok := cb.provider.(providers.ModelSupporter); ok && !ms.SupportsModel(model) {
				continue
			}
		}
		if cb.available(now) {
			available = append(available, cb)
		}
	}
	return available
}

// available reports whether the breaker would admit a call at now.
func (c *CircuitBreaker) available(now time.Time) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	switch c.state {
	case StateClosed:
		return true
	case StateHalfOpen:
		return c.halfOpenInflight < c.halfOpenMaxCalls
	default:
		return now.Sub(c.lastFailTime) >= c.resetTimeout
	}
}

// sortByKey returns a copy of in stably sorted by ascending key. NaN keys
// sort last; ties keep registration order.
func sortByKey(in []*CircuitBreaker, key func(*CircuitBreaker) float64) []*CircuitBreaker {
	type keyed struct {
		cb  *CircuitBreaker
		key float64
	}
	ks := make([]keyed, len(in))
	for i, cb := range in {
		k := key(cb)
		if math.IsNaN(k) {
			k = math.Inf(1)
		}
		ks[i] = keyed{cb: cb, key: k}
	}
	sort.SliceStable(ks, func(i, j int) bool { return ks[i].key < ks[j].key })
	out := make([]*CircuitBreaker, len(ks))
	for i, k := range ks {
		out[i] = k.cb
	}
	return out
}

// servedModel returns the model p would serve for reqModel: the requested
// model when p declares support for it (or has no default model), else p's
// default model.
func servedModel(p providers.Provider, reqModel string) string {
	var def string
	if dm, ok := p.(providers.DefaultModeler); ok {
		def = dm.DefaultModel()
	}
	if cb, ok := p.(*CircuitBreaker); ok {
		p = cb.provider
	}
	if reqModel != "" {
		if ms, ok := p.(providers.ModelSupporter); ok && ms.SupportsModel(reqModel) {
			return reqModel
		}
		if def == "" {
			return reqModel
		}
	}
	if def != "" {
		return def
	}
	return reqModel
}

// estimateTokens approximates prompt and output token counts for req.
func estimateTokens(req *models.LLMRequest) (prompt, output float64) {
	chars := 0
	for _, m := range req.Messages {
		if m.Content != nil {
			chars += len(*m.Content)
		}
		chars += 16 // role/formatting overhead per message
	}
	prompt = math.Max(1, float64(chars)/4)
	output = defaultOutputTokens
	if mt := req.EffectiveMaxTokens(); mt != nil && *mt > 0 {
		output = float64(*mt)
	}
	return prompt, output
}

// estimateCostWithMap computes the estimated USD cost of serving req on p,
// using the model p would serve and per-1M-token pricing from costMap when
// available; otherwise a flat token heuristic (so all providers tie).
func estimateCostWithMap(p providers.Provider, req *models.LLMRequest, costMap *intelligence.ModelCostMap) float64 {
	prompt, output := estimateTokens(req)
	model := servedModel(p, req.Model)
	if costMap != nil && model != "" {
		if cost, ok := costMap.Lookup(model); ok {
			v := prompt/intelligence.CostScale*cost.InputCostPer1M + output/intelligence.CostScale*cost.OutputCostPer1M
			if math.IsNaN(v) || v < 0 {
				return math.Inf(1)
			}
			return v
		}
	}
	return (prompt + output) * 0.001
}

// ExecuteWithFallback calls fn with each candidate provider (see
// Candidates) in strategy order until one succeeds. It moves on to the next
// provider only when providers.IsRetryable(err) is true, and stops
// immediately when ctx is done. Rejections by an open circuit breaker and
// providers.ErrStreamingNotSupported skip the provider without consuming an
// attempt; at most Config.MaxAttempts (default 3) providers are otherwise
// tried.
//
// On success it returns the provider that succeeded. On failure it returns
// the last provider tried (nil if none) and an error wrapping the last
// provider error (errors.As works for *providers.UpstreamError).
func (r *Router) ExecuteWithFallback(ctx context.Context, req *models.LLMRequest, fn func(p providers.Provider) error) (providers.Provider, error) {
	if fn == nil {
		return nil, errors.New("router: nil fallback function")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cands, err := r.Candidates(ctx, req)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	maxAttempts := r.maxAttempts
	r.mu.RUnlock()
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}

	var (
		last     providers.Provider
		lastErr  error // last error from a real attempt
		skipErr  error // last skip reason (circuit open / no streaming)
		attempts int
	)
	for _, p := range cands {
		if attempts >= maxAttempts {
			break
		}
		if cerr := ctx.Err(); cerr != nil {
			if lastErr != nil {
				return last, errors.Join(cerr, lastErr)
			}
			return last, cerr
		}
		err := fn(p)
		if err == nil {
			return p, nil
		}
		if errors.Is(err, providers.ErrCircuitOpen) || errors.Is(err, providers.ErrStreamingNotSupported) {
			skipErr = err
			continue
		}
		attempts++
		last, lastErr = p, err
		if cerr := ctx.Err(); cerr != nil {
			return last, errors.Join(cerr, err)
		}
		if !providers.IsRetryable(err) {
			return last, err
		}
	}
	if lastErr == nil {
		if skipErr == nil {
			skipErr = &NoProviderError{Model: modelOf(req)}
		}
		return nil, skipErr
	}
	if attempts == 1 {
		return last, lastErr
	}
	return last, fmt.Errorf("router: %d providers failed, last error: %w", attempts, lastErr)
}

func modelOf(req *models.LLMRequest) string {
	if req == nil {
		return ""
	}
	return req.Model
}

// ChatCompletionsWithFallback routes req and executes it with fallback (see
// ExecuteWithFallback). It returns the response and the provider that
// served it (or the last provider tried on error).
func (r *Router) ChatCompletionsWithFallback(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, providers.Provider, error) {
	var resp *models.LLMResponse
	p, err := r.ExecuteWithFallback(ctx, req, func(p providers.Provider) error {
		out, err := p.ChatCompletions(ctx, req)
		if err != nil {
			return err
		}
		if out == nil {
			return providers.BadResponseError(p.Name(), errors.New("provider returned no response"))
		}
		resp = out
		return nil
	})
	if err != nil {
		return nil, p, err
	}
	return resp, p, nil
}

// StreamWithFallback starts a streaming completion with fallback. Fallback
// only happens while the stream has not started (a direct error from
// StreamChatCompletions); providers that cannot stream are skipped. When no
// candidate can stream it returns providers.ErrStreamingNotSupported so the
// caller can fall back to ChatCompletionsWithFallback.
func (r *Router) StreamWithFallback(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, providers.Provider, error) {
	var stream <-chan models.StreamChunk
	p, err := r.ExecuteWithFallback(ctx, req, func(p providers.Provider) error {
		sp, ok := p.(providers.StreamingProvider)
		if !ok {
			return providers.ErrStreamingNotSupported
		}
		ch, err := sp.StreamChatCompletions(ctx, req)
		if err != nil {
			return err
		}
		if ch == nil {
			return providers.BadResponseError(p.Name(), errors.New("provider returned no stream"))
		}
		stream = ch
		return nil
	})
	if err != nil {
		return nil, p, err
	}
	return stream, p, nil
}

// NoProviderError is returned when no provider is available.
type NoProviderError struct {
	// Model is the requested model, if any.
	Model string
}

// Error implements error.
func (e *NoProviderError) Error() string {
	if e.Model == "" {
		return "no available provider"
	}
	return fmt.Sprintf("no available provider for model %q", e.Model)
}

// Providers returns all registered circuit breakers.
func (r *Router) Providers() []*CircuitBreaker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*CircuitBreaker, len(r.providers))
	copy(out, r.providers)
	return out
}

// Strategy returns the current routing strategy.
func (r *Router) Strategy() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.strategy
}

// SetStrategy atomically updates the routing strategy at runtime.
func (r *Router) SetStrategy(strategy string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.strategy = strategy
}

// SetRateLimits atomically updates the rate limit configuration for providers.
func (r *Router) SetRateLimits(limits map[string]ProviderRateLimits) {
	limits = copyLimits(limits)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rateLimits = limits
}

// SetCostMap atomically replaces the cost map used by cost routing.
func (r *Router) SetCostMap(m *intelligence.ModelCostMap) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.costMap = m
}

// SetMaxAttempts atomically updates the ExecuteWithFallback attempt bound
// (<= 0 means the default).
func (r *Router) SetMaxAttempts(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxAttempts = n
}
