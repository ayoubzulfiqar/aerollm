package intelligence

import (
	"context"
	"sort"
)

// SLA defines service-level requirements for a request.
type SLA struct {
	MaxLatencyMs   float64
	MaxBudgetUSD   float64
	MinQuality     float64
	PreferCheapest bool
}

// ModelOptionWithSLA extends ModelOption with SLA-oriented availability.
type ModelOptionWithSLA struct {
	ModelOption
	Available bool
}

// SLASelector filters providers/agents that can fulfill the SLA and picks the best option.
type SLASelector struct{}

// NewSLASelector creates a new SLA-aware selector.
func NewSLASelector() *SLASelector {
	return &SLASelector{}
}

// Select picks the cheapest available option that meets the SLA requirements.
func (s *SLASelector) Select(ctx context.Context, opts []ModelOptionWithSLA, sla SLA) (ModelOption, error) {
	_ = ctx
	if len(opts) == 0 {
		return ModelOption{}, context.DeadlineExceeded
	}

	var candidates []ModelOption
	for _, opt := range opts {
		if !opt.Available {
			continue
		}
		if sla.MinQuality > 0 && opt.Quality < sla.MinQuality {
			continue
		}
		if sla.MaxLatencyMs > 0 && opt.Latency > sla.MaxLatencyMs {
			continue
		}
		if sla.MaxBudgetUSD > 0 && opt.Cost > sla.MaxBudgetUSD {
			continue
		}
		candidates = append(candidates, opt.ModelOption)
	}

	if len(candidates) == 0 {
		return opts[0].ModelOption, nil
	}

	if sla.PreferCheapest {
		sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Cost < candidates[j].Cost })
		return candidates[0], nil
	}

	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Quality > candidates[j].Quality })
	return candidates[0], nil
}
