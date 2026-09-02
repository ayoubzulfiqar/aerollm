package intelligence

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// BanditState tracks beliefs for one provider/model.
type BanditState struct {
	Provider string
	Model    string
	Alpha    float64
	Beta     float64
}

// Score returns the current sampled score.
func (b *BanditState) Score(r *rand.Rand) float64 {
	mean := b.Alpha / (b.Alpha + b.Beta)
	variance := (b.Alpha * b.Beta) / ((b.Alpha + b.Beta) * (b.Alpha + b.Beta) * (b.Alpha + b.Beta + 1))
	std := math.Sqrt(variance)
	return mean + r.NormFloat64()*std
}

// BanditRouter uses Thompson Sampling to route requests.
type BanditRouter struct {
	mu      sync.RWMutex
	states  map[string]*BanditState
	rng     *rand.Rand
}

// NewBanditRouter creates a new router.
func NewBanditRouter() *BanditRouter {
	return &BanditRouter{
		states: make(map[string]*BanditState),
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// GetOrCreate returns a state keyed by provider/model.
func (b *BanditRouter) GetOrCreate(provider, model string) *BanditState {
	b.mu.RLock()
	s, ok := b.states[provider+"/"+model]
	b.mu.RUnlock()
	if ok {
		return s
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.states[provider+"/"+model]; ok {
		return s
	}
	s = &BanditState{Provider: provider, Model: model, Alpha: 1, Beta: 1}
	b.states[provider+"/"+model] = s
	return s
}

// Route selects a model by Thompson Sampling.
func (b *BanditRouter) Route(ctx context.Context, candidates []ModelOption) (ModelOption, error) {
	_ = ctx
	if len(candidates) == 0 {
		return ModelOption{}, contextDeadlineExceeded
	}
	best := candidates[0]
	bestScore := math.Inf(-1)
	for _, c := range candidates {
		state := b.GetOrCreate(c.Provider, c.Model)
		score := state.Score(b.rng)
		if score > bestScore {
			bestScore = score
			best = c
		}
	}
	return best, nil
}

// Update updates beliefs with observed latency and cost.
func (b *BanditRouter) Update(provider, model string, latencyMs, cost float64, success bool) {
	state := b.GetOrCreate(provider, model)
	reward := 1.0 / (1.0 + latencyMs/1000.0 + cost)
	if !success {
		reward *= 0.1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if reward > 0.5 {
		state.Alpha += reward
	} else {
		state.Beta += 1 - reward
	}
}

// Snapshot returns current routing states.
func (b *BanditRouter) Snapshot() map[string]BanditState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]BanditState, len(b.states))
	for k, v := range b.states {
		out[k] = *v
	}
	return out
}

var contextDeadlineExceeded = contextDeadlineExceededError{}

type contextDeadlineExceededError struct{}

func (contextDeadlineExceededError) Error() string { return "no candidates" }
