package intelligence

import (
	"context"
	"math"
	"sort"
)

// ModelOption represents a selectable model candidate.
type ModelOption struct {
	Provider string
	Model    string
	Cost     float64
	Latency  float64
	Quality  float64
}

// ModelSelector selects the best model for a request.
type ModelSelector interface {
	Select(ctx context.Context, opts []ModelOption, policy Policy) (ModelOption, error)
}

// Policy configures selection behavior. Zero or negative limits are unset.
type Policy struct {
	MaxCostPer1KTokens float64
	MaxLatencyMs       float64
	MinQuality         float64
	PreferCheapest     bool
}

// HeuristicSelector picks the cheapest (or highest-quality) model that meets
// the policy.
type HeuristicSelector struct{}

// NewHeuristicSelector creates a new selector.
func NewHeuristicSelector() *HeuristicSelector {
	return &HeuristicSelector{}
}

// meetsLimits reports whether an option satisfies the given limits. NaN
// metrics never satisfy a limit that is set.
func meetsLimits(o ModelOption, maxCost, maxLatency, minQuality float64) bool {
	if minQuality > 0 && !(o.Quality >= minQuality) {
		return false
	}
	if maxLatency > 0 && !(o.Latency <= maxLatency) {
		return false
	}
	if maxCost > 0 && !(o.Cost <= maxCost) {
		return false
	}
	return true
}

// costKey maps NaN cost to +Inf so it sorts last when ascending.
func costKey(v float64) float64 {
	if math.IsNaN(v) {
		return math.Inf(1)
	}
	return v
}

// qualityKey maps NaN quality to -Inf so it sorts last when descending.
func qualityKey(v float64) float64 {
	if math.IsNaN(v) {
		return math.Inf(-1)
	}
	return v
}

// rankOptions sorts options in place: cheapest first (ties: higher quality)
// when preferCheapest, otherwise highest quality first (ties: cheaper).
// NaN metrics always rank worst.
func rankOptions(opts []ModelOption, preferCheapest bool) {
	sort.SliceStable(opts, func(i, j int) bool {
		ci, cj := costKey(opts[i].Cost), costKey(opts[j].Cost)
		qi, qj := qualityKey(opts[i].Quality), qualityKey(opts[j].Quality)
		if preferCheapest {
			if ci != cj {
				return ci < cj
			}
			return qi > qj
		}
		if qi != qj {
			return qi > qj
		}
		return ci < cj
	})
}

// Select implements a simple cost-quality heuristic. It returns
// ErrNoCandidates for an empty option list. The policy is soft: when no
// option satisfies it, the best-ranked option overall is returned instead of
// an error (use SLASelector for hard requirements).
func (s *HeuristicSelector) Select(ctx context.Context, opts []ModelOption, policy Policy) (ModelOption, error) {
	if len(opts) == 0 {
		return ModelOption{}, ErrNoCandidates
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return ModelOption{}, err
		}
	}
	candidates := make([]ModelOption, 0, len(opts))
	for _, o := range opts {
		if meetsLimits(o, policy.MaxCostPer1KTokens, policy.MaxLatencyMs, policy.MinQuality) {
			candidates = append(candidates, o)
		}
	}
	if len(candidates) == 0 {
		candidates = append(candidates, opts...)
	}
	rankOptions(candidates, policy.PreferCheapest)
	return candidates[0], nil
}
