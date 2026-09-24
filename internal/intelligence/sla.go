package intelligence

import (
	"context"
	"errors"
)

// ErrNoCandidateMeetsSLA is returned when no available option satisfies the
// SLA's hard requirements.
var ErrNoCandidateMeetsSLA = errors.New("intelligence: no candidate meets the SLA")

// SLA defines service-level requirements for a request. Zero or negative
// limits are unset.
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

// Select picks the best available option that meets the SLA requirements:
// the cheapest when sla.PreferCheapest, otherwise the highest quality. SLA
// limits are hard requirements: it returns ErrNoCandidates for an empty list
// and ErrNoCandidateMeetsSLA when no available option satisfies them.
func (s *SLASelector) Select(ctx context.Context, opts []ModelOptionWithSLA, sla SLA) (ModelOption, error) {
	if len(opts) == 0 {
		return ModelOption{}, ErrNoCandidates
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return ModelOption{}, err
		}
	}

	var candidates []ModelOption
	for _, opt := range opts {
		if !opt.Available {
			continue
		}
		if meetsLimits(opt.ModelOption, sla.MaxBudgetUSD, sla.MaxLatencyMs, sla.MinQuality) {
			candidates = append(candidates, opt.ModelOption)
		}
	}
	if len(candidates) == 0 {
		return ModelOption{}, ErrNoCandidateMeetsSLA
	}
	rankOptions(candidates, sla.PreferCheapest)
	return candidates[0], nil
}
