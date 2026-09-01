package router

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// Config holds router configuration.
type Config struct {
	Strategy     string
	BreakerConfig CircuitBreakerConfig
}

// CircuitBreakerConfig holds circuit breaker settings.
type CircuitBreakerConfig struct {
	MaxFailures     int
	ResetTimeout    time.Duration
	HalfOpenMaxCalls int
}

// CircuitBreakerState represents the state of a circuit breaker.
type CircuitBreakerState int

const (
	StateClosed CircuitBreakerState = iota
	StateHalfOpen
	StateOpen
)

// CircuitBreaker wraps a provider with circuit breaker logic.
type CircuitBreaker struct {
	provider     providers.Provider
	state        CircuitBreakerState
	failures     int64
	lastFailTime time.Time
	maxFailures  int
	resetTimeout time.Duration
	mu           sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker for a provider.
func NewCircuitBreaker(p providers.Provider, maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		provider:    p,
		maxFailures: maxFailures,
		resetTimeout: resetTimeout,
		state:       StateClosed,
	}
}

// Name returns the provider name.
func (c *CircuitBreaker) Name() string { return c.provider.Name() }

// Type returns the provider type string.
func (c *CircuitBreaker) Type() providers.ProviderType { return c.provider.Type() }

// ChatCompletions delegates to the underlying provider, recording success/failure.
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

func (e *CircuitBreakerOpenError) Error() string {
	return "circuit breaker open for provider: " + e.Provider
}

// Router routes requests to LLM providers with intelligent strategies.
type Router struct {
	providers    []*CircuitBreaker
	strategy     string
	breakerCfg   CircuitBreakerConfig
	currentIndex atomic.Uint64
	mu           sync.RWMutex
}

// New creates a new Router with the given configuration.
func New(cfg Config) *Router {
	return &Router{
		strategy:    cfg.Strategy,
		breakerCfg: cfg.BreakerConfig,
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
	r.mu.RUnlock()

	if len(available) == 0 {
		return nil, &NoProviderError{}
	}

	switch r.strategy {
	case "round_robin":
		return r.roundRobin(available), nil
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

// getAvailableProviders returns providers whose circuit breakers are closed or half-open.
func (r *Router) getAvailableProviders() []providers.Provider {
	var available []providers.Provider
	for _, cb := range r.providers {
		cb.mu.RLock()
		state := cb.state
		cb.mu.RUnlock()
		if state != StateOpen || (state == StateOpen && time.Since(cb.lastFailTime) >= cb.resetTimeout) {
			available = append(available, cb)
		}
	}
	return available
}

// roundRobin returns the next provider in round-robin order.
func (r *Router) roundRobin(available []providers.Provider) providers.Provider {
	idx := r.currentIndex.Add(1) % uint64(len(available))
	return available[idx]
}

// latencyBased returns the provider with the lowest latency.
func (r *Router) latencyBased(available []providers.Provider) providers.Provider {
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
func (r *Router) costBased(available []providers.Provider, req *models.LLMRequest) providers.Provider {
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
func (r *Router) fallback(available []providers.Provider) providers.Provider {
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

func (e *NoProviderError) Error() string {
	return "no available provider"
}

// Providers returns all registered circuit breakers.
func (r *Router) Providers() []*CircuitBreaker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out:=make([]*CircuitBreaker,len(r.providers))
	copy(out,r.providers)
	return out
}
