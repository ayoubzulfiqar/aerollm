package hardware

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
)

// ErrNoLocalHardware is returned when local execution is required but no
// suitable local capability was detected.
var ErrNoLocalHardware = errors.New("hardware: no local compute capability available")

// DefaultLocalQuality is the quality score assigned to local execution when
// LocalQuality is unset.
const DefaultLocalQuality = 0.5

// localPriority orders local targets from most to least preferred.
var localPriority = []string{"cuda", "rocm", "metal", "vulkan", "ollama"}

// HardwareAwareSelector extends model selection with local compute routing.
//
// By default it delegates to Base (a zero-value intelligence.Policy never
// forces local routing). Local hardware is used when ForceLocal is set
// (privacy-strict: never falls back to remote), when PreferLocal is set and the
// local option satisfies the policy, or when remote selection is impossible
// (no Base, or Base fails) and a local option satisfies the policy.
type HardwareAwareSelector struct {
	Base     intelligence.ModelSelector
	Detector Detector

	// ForceLocal routes every request to local hardware and returns
	// ErrNoLocalHardware instead of falling back to a remote provider.
	ForceLocal bool
	// PreferLocal routes to local hardware whenever it satisfies the policy.
	PreferLocal bool
	// AllowCPU lets a CPU-only host count as a local target.
	AllowCPU bool
	// LocalProvider names the provider for local execution (default "edge").
	LocalProvider string
	// LocalModel names the local model; defaults to the capability name.
	LocalModel string
	// LocalQuality is the quality score of local execution, in (0,1];
	// unset or invalid values select DefaultLocalQuality.
	LocalQuality float64
}

// NewHardwareAwareSelector creates a selector with local hardware awareness.
func NewHardwareAwareSelector(base intelligence.ModelSelector, detector Detector) *HardwareAwareSelector {
	if detector == nil {
		detector = NewLocalDetector()
	}
	return &HardwareAwareSelector{Base: base, Detector: detector}
}

// Select routes to local hardware when configured to (see type docs),
// otherwise delegates to Base.
func (s *HardwareAwareSelector) Select(ctx context.Context, opts []intelligence.ModelOption, policy intelligence.Policy) (intelligence.ModelOption, error) {
	if s == nil {
		return intelligence.ModelOption{}, errors.New("hardware: nil selector")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return intelligence.ModelOption{}, err
	}
	if s.ForceLocal {
		if opt, ok := s.localOption(); ok {
			return opt, nil
		}
		return intelligence.ModelOption{}, ErrNoLocalHardware
	}
	if s.PreferLocal {
		if opt, ok := s.localOption(); ok && satisfies(opt, policy) {
			return opt, nil
		}
	}
	if s.Base != nil {
		sel, err := s.Base.Select(ctx, opts, policy)
		if err == nil {
			return sel, nil
		}
		if ctx.Err() == nil {
			if opt, ok := s.localOption(); ok && satisfies(opt, policy) {
				return opt, nil
			}
		}
		return intelligence.ModelOption{}, err
	}
	if opt, ok := s.localOption(); ok && satisfies(opt, policy) {
		return opt, nil
	}
	return intelligence.ModelOption{}, fmt.Errorf("hardware: no base selector configured and %w", ErrNoLocalHardware)
}

// localOption picks the preferred available local capability.
func (s *HardwareAwareSelector) localOption() (intelligence.ModelOption, bool) {
	if s.Detector == nil {
		return intelligence.ModelOption{}, false
	}
	caps := s.Detector.Detect()
	available := make(map[string]bool, len(caps))
	for _, c := range caps {
		if c.Available && c.Name != "" {
			available[c.Name] = true
		}
	}
	name := ""
	for _, n := range localPriority {
		if available[n] {
			name = n
			break
		}
	}
	if name == "" {
		// Unknown accelerators from custom detectors, in detection order.
		for _, c := range caps {
			if c.Available && c.Name != "" && c.Name != "cpu" {
				name = c.Name
				break
			}
		}
	}
	if name == "" && s.AllowCPU && available["cpu"] {
		name = "cpu"
	}
	if name == "" {
		return intelligence.ModelOption{}, false
	}
	provider := s.LocalProvider
	if provider == "" {
		provider = "edge"
	}
	model := s.LocalModel
	if model == "" {
		model = name
	}
	quality := s.LocalQuality
	if quality <= 0 || quality > 1 || math.IsNaN(quality) {
		quality = DefaultLocalQuality
	}
	return intelligence.ModelOption{Provider: provider, Model: model, Cost: 0, Latency: 0, Quality: quality}, true
}

// satisfies applies the same constraint semantics as the heuristic selector:
// zero-valued limits are unset.
func satisfies(opt intelligence.ModelOption, policy intelligence.Policy) bool {
	if policy.MinQuality > 0 && opt.Quality < policy.MinQuality {
		return false
	}
	if policy.MaxLatencyMs > 0 && opt.Latency > policy.MaxLatencyMs {
		return false
	}
	if policy.MaxCostPer1KTokens > 0 && opt.Cost > policy.MaxCostPer1KTokens {
		return false
	}
	return true
}
