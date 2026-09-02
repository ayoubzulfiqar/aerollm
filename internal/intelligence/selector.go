package intelligence

import (
	"context"
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

// Policy configures selection behavior.
type Policy struct {
	MaxCostPer1KTokens float64
	MaxLatencyMs      float64
	MinQuality        float64
	PreferCheapest    bool
}

// HeuristicSelector picks the cheapest model that meets quality threshold.
type HeuristicSelector struct{}

// NewHeuristicSelector creates a new selector.
func NewHeuristicSelector() *HeuristicSelector {
	return &HeuristicSelector{}
}

// Select implements a simple cost-quality heuristic.
func (s *HeuristicSelector) Select(ctx context.Context, opts []ModelOption, policy Policy) (ModelOption, error) {
	_ = ctx
	if len(opts) == 0 {
		return ModelOption{}, context.DeadlineExceeded
	}
	var candidates []ModelOption
	for _, o := range opts {
		if policy.MinQuality > 0 && o.Quality < policy.MinQuality {
			continue
		}
		if policy.MaxLatencyMs > 0 && o.Latency > policy.MaxLatencyMs {
			continue
		}
		if policy.MaxCostPer1KTokens > 0 && o.Cost > policy.MaxCostPer1KTokens {
			continue
		}
		candidates = append(candidates, o)
	}
	if len(candidates) == 0 {
		return opts[0], nil
	}
	if policy.PreferCheapest {
		sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Cost < candidates[j].Cost })
		return candidates[0], nil
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Quality > candidates[j].Quality })
	return candidates[0], nil
}
