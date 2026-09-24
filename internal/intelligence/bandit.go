package intelligence

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// ErrNoCandidates is returned when a selection is requested over an empty
// candidate set.
var ErrNoCandidates = errors.New("intelligence: no candidates")

// BanditState tracks beliefs for one provider/model as a Beta(Alpha, Beta)
// posterior over the probability that a request is "rewarding".
type BanditState struct {
	Provider string
	Model    string
	Alpha    float64
	Beta     float64
}

// Score returns a sample from the state's Beta(Alpha, Beta) posterior
// (Thompson Sampling). It is NaN-safe: invalid parameters are treated as the
// uniform prior Beta(1, 1). The caller must serialize access to r.
func (b *BanditState) Score(r *rand.Rand) float64 {
	a, bb := sanitizeParam(b.Alpha), sanitizeParam(b.Beta)
	x := sampleGamma(r, a)
	y := sampleGamma(r, bb)
	sum := x + y
	if !(sum > 0) || math.IsInf(sum, 0) {
		// Degenerate draw: fall back to the posterior mean.
		return a / (a + bb)
	}
	return x / sum
}

// sanitizeParam maps NaN/Inf/non-positive Beta parameters to 1.
func sanitizeParam(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return 1
	}
	return v
}

// sampleGamma draws from Gamma(shape, 1) using Marsaglia–Tsang. shape > 0.
func sampleGamma(r *rand.Rand, shape float64) float64 {
	if shape < 1 {
		// Boost: Gamma(a) = Gamma(a+1) * U^(1/a).
		u := r.Float64()
		for u == 0 {
			u = r.Float64()
		}
		return sampleGamma(r, shape+1) * math.Pow(u, 1/shape)
	}
	d := shape - 1.0/3.0
	c := 1 / math.Sqrt(9*d)
	for i := 0; i < 1000; i++ {
		x := r.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := r.Float64()
		if u < 1-0.0331*x*x*x*x {
			return d * v
		}
		if u > 0 && math.Log(u) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
	return shape // mean, practically unreachable
}

// BanditRouter uses Thompson Sampling to route requests. It is safe for
// concurrent use.
type BanditRouter struct {
	mu     sync.Mutex
	states map[string]*BanditState
	rng    *rand.Rand // guarded by mu
}

// NewBanditRouter creates a new router.
func NewBanditRouter() *BanditRouter {
	return &BanditRouter{
		states: make(map[string]*BanditState),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func banditKey(provider, model string) string { return provider + "/" + model }

// getOrCreateLocked returns the state for provider/model; b.mu must be held.
func (b *BanditRouter) getOrCreateLocked(provider, model string) *BanditState {
	key := banditKey(provider, model)
	if s, ok := b.states[key]; ok {
		return s
	}
	s := &BanditState{Provider: provider, Model: model, Alpha: 1, Beta: 1}
	b.states[key] = s
	return s
}

// GetOrCreate returns a snapshot copy of the state keyed by provider/model,
// creating it with a uniform prior if needed. The returned value is a copy so
// callers cannot race with concurrent updates; use Update to change beliefs.
func (b *BanditRouter) GetOrCreate(provider, model string) *BanditState {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := *b.getOrCreateLocked(provider, model)
	return &s
}

// Route selects a model by Thompson Sampling. It returns ErrNoCandidates for
// an empty candidate list and ctx.Err() if ctx is already done.
func (b *BanditRouter) Route(ctx context.Context, candidates []ModelOption) (ModelOption, error) {
	if len(candidates) == 0 {
		return ModelOption{}, ErrNoCandidates
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return ModelOption{}, err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	best := candidates[0]
	bestScore := math.Inf(-1)
	for _, c := range candidates {
		score := b.getOrCreateLocked(c.Provider, c.Model).Score(b.rng)
		if math.IsNaN(score) {
			continue
		}
		if score > bestScore {
			bestScore = score
			best = c
		}
	}
	return best, nil
}

// Update updates beliefs with an observed outcome. The reward in [0,1] is
// 1/(1 + latencySeconds + cost), scaled down by 10x on failure; invalid
// (NaN, Inf, negative) latency or cost are treated as 0. The update is the
// standard fractional Bernoulli update Alpha += r, Beta += 1-r.
func (b *BanditRouter) Update(provider, model string, latencyMs, cost float64, success bool) {
	latencyMs = sanitizeNonNegative(latencyMs)
	cost = sanitizeNonNegative(cost)
	reward := 1.0 / (1.0 + latencyMs/1000.0 + cost)
	if !success {
		reward *= 0.1
	}
	if math.IsNaN(reward) || reward < 0 {
		reward = 0
	}
	if reward > 1 {
		reward = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.getOrCreateLocked(provider, model)
	state.Alpha += reward
	state.Beta += 1 - reward
}

func sanitizeNonNegative(v float64) float64 {
	if math.IsNaN(v) || v < 0 {
		return 0
	}
	if math.IsInf(v, 1) {
		return math.MaxFloat64 / 4
	}
	return v
}

// Snapshot returns current routing states.
func (b *BanditRouter) Snapshot() map[string]BanditState {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]BanditState, len(b.states))
	for k, v := range b.states {
		out[k] = *v
	}
	return out
}
