package aiops

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
)

// Built-in action names.
const (
	LatencyStrategyActionName = "latency_strategy"
	RateLimitActionName       = "rate_limit"
	CircuitBreakerActionName  = "provider_circuit"

	// DefaultLatencyStrategy is the routing strategy LatencyStrategyAction
	// switches to.
	DefaultLatencyStrategy = "latency"
	// DefaultMinRequests is the default minimum window sample size before an
	// error rate is trusted.
	DefaultMinRequests = 20
	// DefaultTightenFactor is the default RateLimitAction multiplier.
	DefaultTightenFactor = 0.5
	// DefaultRateLimitFloor is the default lowest limit RateLimitAction sets.
	DefaultRateLimitFloor = 1.0
	// DefaultMaxOpenCircuits is the default number of providers
	// CircuitBreakerAction may hold open at once.
	DefaultMaxOpenCircuits = 1
)

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// validateBand checks a trigger/recovery threshold pair: both finite and
// non-negative with recover < trigger (the hysteresis band).
func validateBand(what string, trigger, recover float64) error {
	if !finite(trigger) || !finite(recover) || trigger <= 0 || recover < 0 {
		return fmt.Errorf("%w: %s thresholds must be finite, trigger > 0 and recovery >= 0", ErrInvalidAction, what)
	}
	if recover >= trigger {
		return fmt.Errorf("%w: %s recovery threshold (%g) must be below the trigger threshold (%g)", ErrInvalidAction, what, recover, trigger)
	}
	return nil
}

// ---------------------------------------------------------------------------
// LatencyStrategyAction

// LatencyStrategyConfig configures LatencyStrategyAction.
type LatencyStrategyConfig struct {
	// HighLatencyMs: Signals.LatencyMs strictly above this is a breach.
	HighLatencyMs float64
	// RecoverLatencyMs: Signals.LatencyMs strictly below this is healthy.
	// Must be < HighLatencyMs.
	RecoverLatencyMs float64
	// Strategy is the routing strategy applied under sustained high latency
	// (default DefaultLatencyStrategy).
	Strategy string
	// Policy controls sustain/recovery evaluations and cooldown.
	Policy ActionPolicy
	// GetStrategy returns the current routing strategy (required).
	GetStrategy func() string
	// SetStrategy sets the routing strategy (required).
	SetStrategy func(ctx context.Context, strategy string) error
}

// LatencyStrategyAction switches the router to a latency-optimised strategy
// under sustained high latency and restores the previous strategy after
// sustained recovery. If the strategy was changed externally while applied,
// the revert leaves it alone (ErrSuperseded).
type LatencyStrategyAction struct {
	cfg LatencyStrategyConfig

	mu      sync.Mutex
	applied bool
	prev    string
}

// NewLatencyStrategyAction validates cfg and returns the action.
func NewLatencyStrategyAction(cfg LatencyStrategyConfig) (*LatencyStrategyAction, error) {
	if cfg.Strategy == "" {
		cfg.Strategy = DefaultLatencyStrategy
	}
	if !actionNamePattern.MatchString(cfg.Strategy) {
		return nil, fmt.Errorf("%w: strategy must match [A-Za-z0-9._:-]{1,64}", ErrInvalidAction)
	}
	if err := validateBand("latency", cfg.HighLatencyMs, cfg.RecoverLatencyMs); err != nil {
		return nil, err
	}
	if cfg.GetStrategy == nil || cfg.SetStrategy == nil {
		return nil, fmt.Errorf("%w: GetStrategy and SetStrategy callbacks are required", ErrInvalidAction)
	}
	if _, err := cfg.Policy.normalize(); err != nil {
		return nil, err
	}
	return &LatencyStrategyAction{cfg: cfg}, nil
}

// Name implements Action.
func (a *LatencyStrategyAction) Name() string { return LatencyStrategyActionName }

// Policy implements Action.
func (a *LatencyStrategyAction) Policy() ActionPolicy { return a.cfg.Policy }

// Observe implements Action. A window without traffic is reported as
// ConditionNoData because the latency signal is stale then.
func (a *LatencyStrategyAction) Observe(s Signals) []Observation {
	o := Observation{Reason: fmt.Sprintf("latency_ms=%.1f high=%.1f recover=%.1f", s.LatencyMs, a.cfg.HighLatencyMs, a.cfg.RecoverLatencyMs)}
	switch {
	case s.CountersAvailable && s.WindowRequests == 0:
		o.Condition = ConditionNoData
		o.Reason = "no traffic in window"
	case s.LatencyMs > a.cfg.HighLatencyMs:
		o.Condition = ConditionBreach
	case s.LatencyMs < a.cfg.RecoverLatencyMs:
		o.Condition = ConditionHealthy
	default:
		o.Condition = ConditionNeutral
	}
	return []Observation{o}
}

// Apply implements Action: it captures the current strategy and switches to
// the configured one.
func (a *LatencyStrategyAction) Apply(ctx context.Context, _ string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	prev := a.cfg.GetStrategy()
	if err := a.cfg.SetStrategy(ctx, a.cfg.Strategy); err != nil {
		return "", err
	}
	a.applied, a.prev = true, prev
	return fmt.Sprintf("strategy %q -> %q", prev, a.cfg.Strategy), nil
}

// Revert implements Action: it restores the captured strategy unless the
// strategy was changed externally meanwhile.
func (a *LatencyStrategyAction) Revert(ctx context.Context, _ string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.applied {
		return "nothing to revert", nil
	}
	cur := a.cfg.GetStrategy()
	if cur != a.cfg.Strategy {
		a.applied = false
		return fmt.Sprintf("strategy is %q (expected %q); keeping it", cur, a.cfg.Strategy), ErrSuperseded
	}
	if err := a.cfg.SetStrategy(ctx, a.prev); err != nil {
		return "", err
	}
	a.applied = false
	return fmt.Sprintf("strategy %q -> %q (restored)", cur, a.prev), nil
}

// ---------------------------------------------------------------------------
// RateLimitAction

// RateLimitConfig configures RateLimitAction.
type RateLimitConfig struct {
	// HighErrorRate: windowed error rate strictly above this is a breach.
	// In (0, 1].
	HighErrorRate float64
	// RecoverErrorRate: error rate strictly below this is healthy. Must be
	// < HighErrorRate.
	RecoverErrorRate float64
	// HighRPS (optional, 0 disables): request rate strictly above this is an
	// overload breach.
	HighRPS float64
	// RecoverRPS: request rate below this counts as recovered (required,
	// < HighRPS, when HighRPS > 0).
	RecoverRPS float64
	// MinRequests: the error rate is ignored when the window holds fewer
	// requests (default DefaultMinRequests; negative disables the check).
	MinRequests int64
	// Factor multiplies the current limit when tightening, in (0, 1)
	// (default DefaultTightenFactor).
	Factor float64
	// Floor is the lowest limit ever set, > 0 (default
	// DefaultRateLimitFloor).
	Floor float64
	// Policy controls sustain/recovery evaluations and cooldown.
	Policy ActionPolicy
	// GetLimit returns the current default rate limit (required).
	GetLimit func() float64
	// SetLimit sets the default rate limit (required).
	SetLimit func(ctx context.Context, limit float64) error
}

// RateLimitAction tightens the default rate limit under a sustained high
// error rate (or overload) and restores the original limit after sustained
// recovery. If the limit was changed externally while applied, the revert
// leaves it alone (ErrSuperseded).
type RateLimitAction struct {
	cfg RateLimitConfig

	mu       sync.Mutex
	applied  bool
	original float64
	set      float64
}

// NewRateLimitAction validates cfg and returns the action.
func NewRateLimitAction(cfg RateLimitConfig) (*RateLimitAction, error) {
	if cfg.MinRequests == 0 {
		cfg.MinRequests = DefaultMinRequests
	}
	if cfg.Factor == 0 {
		cfg.Factor = DefaultTightenFactor
	}
	if cfg.Floor == 0 {
		cfg.Floor = DefaultRateLimitFloor
	}
	if err := validateBand("error rate", cfg.HighErrorRate, cfg.RecoverErrorRate); err != nil {
		return nil, err
	}
	if cfg.HighErrorRate > 1 {
		return nil, fmt.Errorf("%w: error rate threshold must be <= 1", ErrInvalidAction)
	}
	if cfg.HighRPS != 0 {
		if err := validateBand("rps", cfg.HighRPS, cfg.RecoverRPS); err != nil {
			return nil, err
		}
	}
	if !finite(cfg.Factor) || cfg.Factor <= 0 || cfg.Factor >= 1 {
		return nil, fmt.Errorf("%w: factor must be in (0, 1)", ErrInvalidAction)
	}
	if !finite(cfg.Floor) || cfg.Floor <= 0 {
		return nil, fmt.Errorf("%w: floor must be a finite positive number", ErrInvalidAction)
	}
	if cfg.GetLimit == nil || cfg.SetLimit == nil {
		return nil, fmt.Errorf("%w: GetLimit and SetLimit callbacks are required", ErrInvalidAction)
	}
	if _, err := cfg.Policy.normalize(); err != nil {
		return nil, err
	}
	return &RateLimitAction{cfg: cfg}, nil
}

// Name implements Action.
func (a *RateLimitAction) Name() string { return RateLimitActionName }

// Policy implements Action.
func (a *RateLimitAction) Policy() ActionPolicy { return a.cfg.Policy }

// Observe implements Action.
func (a *RateLimitAction) Observe(s Signals) []Observation {
	c := a.cfg
	errKnown := !s.CountersAvailable || c.MinRequests < 0 || s.WindowRequests >= c.MinRequests
	errBreach := errKnown && s.ErrorRate > c.HighErrorRate
	rpsBreach := c.HighRPS > 0 && s.RPS > c.HighRPS
	errHealthy := errKnown && s.ErrorRate < c.RecoverErrorRate
	rpsHealthy := c.HighRPS == 0 || s.RPS < c.RecoverRPS
	o := Observation{Reason: fmt.Sprintf("error_rate=%.4f window_requests=%d rps=%.2f", s.ErrorRate, s.WindowRequests, s.RPS)}
	switch {
	case errBreach || rpsBreach:
		o.Condition = ConditionBreach
	case !errKnown && rpsHealthy:
		o.Condition = ConditionNoData
	case errHealthy && rpsHealthy:
		o.Condition = ConditionHealthy
	default:
		o.Condition = ConditionNeutral
	}
	return []Observation{o}
}

// Apply implements Action: limit = max(Floor, current*Factor), never above
// the current limit.
func (a *RateLimitAction) Apply(ctx context.Context, _ string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur := a.cfg.GetLimit()
	if !finite(cur) || cur <= 0 {
		return "", fmt.Errorf("aiops: current rate limit %v is not a positive number", cur)
	}
	next := math.Max(a.cfg.Floor, cur*a.cfg.Factor)
	if next > cur {
		next = cur
	}
	if err := a.cfg.SetLimit(ctx, next); err != nil {
		return "", err
	}
	a.applied, a.original, a.set = true, cur, next
	return fmt.Sprintf("rate limit %g -> %g", cur, next), nil
}

func sameLimit(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

// Revert implements Action: it restores the original limit unless the limit
// was changed externally meanwhile.
func (a *RateLimitAction) Revert(ctx context.Context, _ string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.applied {
		return "nothing to revert", nil
	}
	cur := a.cfg.GetLimit()
	if !sameLimit(cur, a.set) {
		a.applied = false
		return fmt.Sprintf("rate limit is %g (expected %g); keeping it", cur, a.set), ErrSuperseded
	}
	if err := a.cfg.SetLimit(ctx, a.original); err != nil {
		return "", err
	}
	a.applied = false
	return fmt.Sprintf("rate limit %g -> %g (restored)", cur, a.original), nil
}

// ---------------------------------------------------------------------------
// CircuitBreakerAction

// CircuitBreakerConfig configures CircuitBreakerAction.
type CircuitBreakerConfig struct {
	// HighErrorRate: a provider's windowed error rate strictly above this is
	// a breach. In (0, 1].
	HighErrorRate float64
	// RecoverErrorRate: error rate strictly below this is healthy. Must be
	// < HighErrorRate.
	RecoverErrorRate float64
	// MinRequests: providers with fewer window requests are reported as
	// ConditionNoData (default DefaultMinRequests).
	MinRequests int64
	// MaxOpen: maximum providers this action holds open at once (default
	// DefaultMaxOpenCircuits). The action also never opens the last
	// provider it can see.
	MaxOpen int
	// Policy controls sustain/recovery evaluations and cooldown. The
	// cooldown doubles as the minimum time a circuit stays open.
	Policy ActionPolicy
	// OpenCircuit forces the provider's circuit open (required).
	OpenCircuit func(ctx context.Context, provider string) error
	// CloseCircuit closes (or resets) the provider's circuit (required).
	CloseCircuit func(ctx context.Context, provider string) error
}

// CircuitBreakerAction opens the circuit of a provider with a sustained high
// error rate and closes it after sustained recovery. Targets are provider
// names, so each provider has independent state. While a circuit is open the
// provider usually receives no traffic, which is reported as ConditionNoData
// and counts as recovery evidence, so the circuit closes after the cooldown
// and RecoveryEvaluations; if the provider is still failing it is re-opened
// after another sustained breach.
type CircuitBreakerAction struct {
	cfg CircuitBreakerConfig

	mu        sync.Mutex
	providers int // providers seen in the latest Observe
}

// NewCircuitBreakerAction validates cfg and returns the action.
func NewCircuitBreakerAction(cfg CircuitBreakerConfig) (*CircuitBreakerAction, error) {
	if cfg.MinRequests == 0 {
		cfg.MinRequests = DefaultMinRequests
	}
	if cfg.MaxOpen == 0 {
		cfg.MaxOpen = DefaultMaxOpenCircuits
	}
	if err := validateBand("provider error rate", cfg.HighErrorRate, cfg.RecoverErrorRate); err != nil {
		return nil, err
	}
	if cfg.HighErrorRate > 1 {
		return nil, fmt.Errorf("%w: error rate threshold must be <= 1", ErrInvalidAction)
	}
	if cfg.MinRequests < 1 {
		return nil, fmt.Errorf("%w: min_requests must be >= 1", ErrInvalidAction)
	}
	if cfg.MaxOpen < 1 || cfg.MaxOpen > MaxTargetsPerAction {
		return nil, fmt.Errorf("%w: max_open must be in [1, %d]", ErrInvalidAction, MaxTargetsPerAction)
	}
	if cfg.OpenCircuit == nil || cfg.CloseCircuit == nil {
		return nil, fmt.Errorf("%w: OpenCircuit and CloseCircuit callbacks are required", ErrInvalidAction)
	}
	if _, err := cfg.Policy.normalize(); err != nil {
		return nil, err
	}
	return &CircuitBreakerAction{cfg: cfg}, nil
}

// Name implements Action.
func (a *CircuitBreakerAction) Name() string { return CircuitBreakerActionName }

// Policy implements Action.
func (a *CircuitBreakerAction) Policy() ActionPolicy { return a.cfg.Policy }

// Observe implements Action: one observation per provider in s.Providers.
func (a *CircuitBreakerAction) Observe(s Signals) []Observation {
	names := make([]string, 0, len(s.Providers))
	for name := range s.Providers {
		names = append(names, name)
	}
	sort.Strings(names)
	a.mu.Lock()
	a.providers = len(names)
	a.mu.Unlock()
	out := make([]Observation, 0, len(names))
	for _, name := range names {
		ps := s.Providers[name]
		o := Observation{Target: name, Reason: fmt.Sprintf("provider_error_rate=%.4f window_requests=%d", ps.ErrorRate, ps.Requests)}
		switch {
		case ps.Requests < a.cfg.MinRequests:
			o.Condition = ConditionNoData
		case ps.ErrorRate > a.cfg.HighErrorRate:
			o.Condition = ConditionBreach
		case ps.ErrorRate < a.cfg.RecoverErrorRate:
			o.Condition = ConditionHealthy
		default:
			o.Condition = ConditionNeutral
		}
		out = append(out, o)
	}
	return out
}

// CanApply implements ActionGuard: at most MaxOpen circuits, and never the
// last visible provider.
func (a *CircuitBreakerAction) CanApply(_ string, active []string) error {
	if len(active) >= a.cfg.MaxOpen {
		return fmt.Errorf("%w: %d circuit(s) already open (max %d)", ErrGuardrail, len(active), a.cfg.MaxOpen)
	}
	a.mu.Lock()
	providers := a.providers
	a.mu.Unlock()
	if len(active)+1 >= providers {
		return fmt.Errorf("%w: opening would leave no provider available (%d visible, %d open)", ErrGuardrail, providers, len(active))
	}
	return nil
}

// Apply implements Action.
func (a *CircuitBreakerAction) Apply(ctx context.Context, provider string) (string, error) {
	if err := a.cfg.OpenCircuit(ctx, provider); err != nil {
		return "", err
	}
	return "circuit opened", nil
}

// Revert implements Action.
func (a *CircuitBreakerAction) Revert(ctx context.Context, provider string) (string, error) {
	if err := a.cfg.CloseCircuit(ctx, provider); err != nil {
		return "", err
	}
	return "circuit closed", nil
}
