package router

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// ProviderRateLimits holds the TPM and RPM limits for a provider.
type ProviderRateLimits struct {
	TPM int64 // Tokens Per Minute
	RPM int64 // Requests Per Minute
}

// ProviderUsage holds real-time usage metrics for rate-limit-aware routing.
type ProviderUsage struct {
	InflightRequests int64     // atomic counter for inflight requests
	TokenCountMinute int64     // atomic counter for tokens in current minute
	RequestCountMinute int64   // atomic counter for requests in current minute
	LastReset        time.Time // last minute-window reset
	HealthLatencyMs  float64   // last measured latency
}

// Config holds router configuration.
type Config struct {
	Strategy     string
	BreakerConfig CircuitBreakerConfig
	// ProviderRateLimits maps provider name to its rate limits.
	ProviderRateLimits map[string]ProviderRateLimits
}

// CircuitBreakerConfig holds circuit breaker settings.
type CircuitBreakerConfig struct {
	MaxFailures     int           `json:"max_failures"`
	ResetTimeout    time.Duration `json:"reset_timeout"`
	HalfOpenMaxCalls int          `json:"half_open_max_calls"`
}

// CircuitBreakerState represents the state of a circuit breaker.
type CircuitBreakerState int

const (
	StateClosed CircuitBreakerState = iota
	StateHalfOpen
	StateOpen
)

// CircuitBreaker wraps a provider with circuit breaker logic and inflight tracking.
type CircuitBreaker struct {
	provider     providers.Provider
	state        CircuitBreakerState
	failures     int64
	lastFailTime time.Time
	maxFailures  int
	resetTimeout time.Duration
	mu           sync.RWMutex
	usage        *ProviderUsage
}

// NewCircuitBreaker creates a new circuit breaker for a provider.
func NewCircuitBreaker(p providers.Provider, maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		provider:     p,
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		state:        StateClosed,
		usage: &ProviderUsage{
			LastReset: time.Now(),
		},
	}
}

// Name returns the provider name.
func (c *CircuitBreaker) Name() string { return c.provider.Name() }

// Type returns the provider type string.
func (c *CircuitBreaker) Type() providers.ProviderType { return c.provider.Type() }

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

// CheckRateLimit returns true if the provider is approaching its TPM/RPM limit.
// Returns the current usage fraction (0.0 to 1.0).
func (c *CircuitBreaker) CheckRateLimit(limits ProviderRateLimits) float64 {
	if limits.TPM <= 0 && limits.RPM <= 0 {
		return 0
	}

	// Reset minute window if needed.
	now := time.Now()
	if now.Sub(c.usage.LastReset) >= time.Minute {
		atomic.StoreInt64(&c.usage.TokenCountMinute, 0)
		atomic.StoreInt64(&c.usage.RequestCountMinute, 0)
		c.usage.LastReset = now
	}

	var maxFraction float64
	if limits.RPM > 0 {
		reqCount := atomic.LoadInt64(&c.usage.RequestCountMinute)
		reqFraction := float64(reqCount) / float64(limits.RPM)
		if reqFraction > maxFraction {
			maxFraction = reqFraction
		}
	}
	if limits.TPM > 0 {
		tokCount := atomic.LoadInt64(&c.usage.TokenCountMinute)
		tokFraction := float64(tokCount) / float64(limits.TPM)
		if tokFraction > maxFraction {
			maxFraction = tokFraction
		}
	}

	// Increment request count for this call.
	if limits.RPM > 0 {
		atomic.AddInt64(&c.usage.RequestCountMinute, 1)
	}

	return maxFraction
}

// RecordTokens adds tokens to the current minute's usage count.
func (c *CircuitBreaker) RecordTokens(tokens int64) {
	atomic.AddInt64(&c.usage.TokenCountMinute, tokens)
}

// ChatCompletions delegates to the underlying provider, recording success/failure
// and managing inflight tracking.
func (c *CircuitBreaker) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	c.mu.RLock()
	state := c.state
	c.mu.RUnlock()

	if state == StateOpen {
		if time.Since(c.lastFailTime) < c.resetTimeout {
			return nil, &CircuitBreakerOpenError{Provider: c.provider.Name()}
		}
		c.mu.Lock()
		c.state = StateHalfOpen
		c.mu.Unlock()
	}

	// Track inflight requests.
	c.RecordInflight()
	defer c.ReleaseInflight()

	resp, err := c.provider.ChatCompletions(ctx, req)

	c.mu.Lock()
	defer c.mu.Unlock()

	if err != nil {
		c.failures++
		c.lastFailTime = time.Now()
		if c.failures >= int64(c.maxFailures) {
			c.state = StateOpen
		}
		return nil, err
	}

	c.failures = 0
	c.state = StateClosed

	// Record token usage if available.
	if resp.Usage != nil {
		totalTokens := int64(resp.Usage.TotalTokens)
		c.RecordTokens(totalTokens)
	}

	return resp, nil
}

// Health returns the health status including circuit breaker state.
func (c *CircuitBreaker) Health() providers.ProviderHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()

	baseHealth := c.provider.Health()
	baseHealth.CircuitOpen = baseHealth.CircuitOpen || c.state == StateOpen
	return baseHealth
}

// Close releases resources.
func (c *CircuitBreaker) Close() error { return c.provider.Close() }

// CircuitBreakerOpenError is returned when the circuit breaker is open.
type CircuitBreakerOpenError struct {
	Provider string
}

// Error implements error.
func (e *CircuitBreakerOpenError) Error() string {
	return "circuit breaker open for provider: " + e.Provider
}

// Router routes requests to LLM providers with intelligent strategies.
type Router struct {
	providers    []*CircuitBreaker
	strategy     string
	breakerCfg   CircuitBreakerConfig
	rateLimits   map[string]ProviderRateLimits
	currentIndex atomic.Uint64
	mu           sync.RWMutex
}

// New creates a new Router with the given configuration.
func New(cfg Config) *Router {
	return &Router{
		strategy:    cfg.Strategy,
		breakerCfg:  cfg.BreakerConfig,
		rateLimits:  cfg.ProviderRateLimits,
	}
}

// RegisterProvider adds a provider to the router with circuit breaker protection.
func (r *Router) RegisterProvider(p providers.Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()

	maxFailures := r.breakerCfg.MaxFailures
	if maxFailures <= 0 {
		maxFailures = 5
	}
	resetTimeout := r.breakerCfg.ResetTimeout
	if resetTimeout <= 0 {
		resetTimeout = 60 * time.Second
	}

	cb := NewCircuitBreaker(p, maxFailures, resetTimeout)
	r.providers = append(r.providers, cb)
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

// Route selects a provider for the given request based on the configured strategy.
func (r *Router) Route(ctx context.Context, req *models.LLMRequest) (providers.Provider, error) {
	r.mu.RLock()
	available := r.getAvailableProviders()
	rateLimits := r.rateLimits
	r.mu.RUnlock()

	if len(available) == 0 {
		return nil, &NoProviderError{}
	}

	switch r.strategy {
	case "round_robin":
		return r.roundRobin(available), nil
	case "least_busy":
		return r.leastBusy(available), nil
	case "usage_based":
		return r.usageBased(available, rateLimits, req), nil
	case "latency":
		return r.latencyBased(available), nil
	case "cost":
		return r.costBased(available, req), nil
	case "fallback":
		return r.fallback(available), nil
	default:
		return r.roundRobin(available), nil
	}
}

// getAvailableProviders returns circuit breakers whose circuit is closed or half-open.
func (r *Router) getAvailableProviders() []*CircuitBreaker {
	var available []*CircuitBreaker
	for _, cb := range r.providers {
		cb.mu.RLock()
		state := cb.state
		lastFail := cb.lastFailTime
		cb.mu.RUnlock()
		if state != StateOpen || (state == StateOpen && time.Since(lastFail) >= cb.resetTimeout) {
			available = append(available, cb)
		}
	}
	return available
}

// roundRobin returns the next provider in round-robin order.
func (r *Router) roundRobin(available []*CircuitBreaker) providers.Provider {
	idx := r.currentIndex.Add(1) % uint64(len(available))
	return available[idx]
}

// leastBusy returns the provider with the fewest inflight requests.
// This strategy distributes load evenly by always selecting the least-loaded
// provider, preventing hotspots and reducing tail latency.
func (r *Router) leastBusy(available []*CircuitBreaker) providers.Provider {
	var best *CircuitBreaker
	var minInflight int64 = 1<<63 - 1
	for _, cb := range available {
		inflight := cb.Inflight()
		if inflight < minInflight {
			minInflight = inflight
			best = cb
		}
	}
	if best != nil {
		return best
	}
	return available[0]
}

// usageBased returns the provider with the most remaining rate-limit headroom.
// It tracks real-time TPM/RPM usage against each provider's configured limits.
// If a provider is nearing its limit (e.g. >80% utilization), traffic shifts
// to the next available provider to prevent 429 errors.
func (r *Router) usageBased(available []*CircuitBreaker, rateLimits map[string]ProviderRateLimits, req *models.LLMRequest) providers.Provider {
	var best *CircuitBreaker
	var maxHeadroom float64 = -1.0

	for _, cb := range available {
		limits := rateLimits[cb.Name()]
		if limits.TPM <= 0 && limits.RPM <= 0 {
			// No rate limits configured — assume full headroom.
			if 1.0 > maxHeadroom {
				maxHeadroom = 1.0
				best = cb
			}
			continue
		}

		utilization := cb.CheckRateLimit(limits)
		headroom := 1.0 - utilization
		if headroom > maxHeadroom {
			maxHeadroom = headroom
			best = cb
		}
	}

	if best != nil {
		return best
	}
	return available[0]
}

// latencyBased returns the provider with the lowest latency.
func (r *Router) latencyBased(available []*CircuitBreaker) providers.Provider {
	var best providers.Provider
	var bestLatency float64 = 1<<63 - 1
	for _, cb := range available {
		health := cb.Health()
		if health.LatencyMs < bestLatency {
			bestLatency = health.LatencyMs
			best = cb
		}
	}
	return best
}

// costBased returns the provider with the lowest estimated cost for the given request.
func (r *Router) costBased(available []*CircuitBreaker, req *models.LLMRequest) providers.Provider {
	var best providers.Provider
	var lowestCost float64 = 1<<63 - 1
	for _, cb := range available {
		cost := estimateCost(cb, req)
		if cost < lowestCost {
			lowestCost = cost
			best = cb
		}
	}
	return best
}

// fallback returns the first available provider.
func (r *Router) fallback(available []*CircuitBreaker) providers.Provider {
	if len(available) > 0 {
		return available[0]
	}
	return nil
}

// estimateCost estimates the cost of a request.
func estimateCost(p providers.Provider, req *models.LLMRequest) float64 {
	totalTokens := 0
	for _, m := range req.Messages {
		if m.Content != nil {
			totalTokens += len(*m.Content) / 4
		}
	}
	_ = p
	return float64(totalTokens) * 0.001
}

// NoProviderError is returned when no provider is available.
type NoProviderError struct{}

// Error implements error.
func (e *NoProviderError) Error() string {
	return "no available provider"
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
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rateLimits = limits
}
