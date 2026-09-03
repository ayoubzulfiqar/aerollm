package hardware

import (
	"context"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
)

// HardwareAwareSelector extends model selection with local compute routing.
// It prefers local execution when privacy_mode=strict or cost_limit=0.
type HardwareAwareSelector struct {
	Base     intelligence.ModelSelector
	Detector Detector
}

// NewHardwareAwareSelector creates a selector with local hardware awareness.
func NewHardwareAwareSelector(base intelligence.ModelSelector, detector Detector) *HardwareAwareSelector {
	if detector == nil {
		detector = NewLocalDetector()
	}
	return &HardwareAwareSelector{Base: base, Detector: detector}
}

// Select routes to local hardware when requested by policy or detected capabilities.
func (s *HardwareAwareSelector) Select(ctx context.Context, opts []intelligence.ModelOption, policy intelligence.Policy) (intelligence.ModelOption, error) {
	if s.shouldUseLocal(policy) {
		caps := s.Detector.Detect()
		for _, c := range caps {
			if c.Available && c.Name != "cpu" {
				return intelligence.ModelOption{
					Provider: "edge",
					Model:    c.Name,
					Cost:     0,
					Latency:  0,
					Quality:  0.5,
				}, nil
			}
		}
	}
	if s.Base != nil {
		return s.Base.Select(ctx, opts, policy)
	}
	return intelligence.ModelOption{}, fmt.Errorf("hardware: no base selector configured")
}

func (s *HardwareAwareSelector) shouldUseLocal(policy intelligence.Policy) bool {
	if policy.MinQuality >= 1.0 && policy.MaxCostPer1KTokens == 0 {
		return true
	}
	if policy.MaxLatencyMs <= 1 {
		return true
	}
	return false
}
