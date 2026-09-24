package v1alpha1

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// Routing strategies understood by the gateway router (see internal/router).
const (
	StrategyRoundRobin = "round_robin"
	StrategyLeastBusy  = "least_busy"
	StrategyUsageBased = "usage_based"
	StrategyLatency    = "latency"
	StrategyCost       = "cost"
	StrategyFallback   = "fallback"
	StrategyWeighted   = "weighted"
)

var validStrategies = map[string]bool{
	"":                 true, // router default
	StrategyRoundRobin: true,
	StrategyLeastBusy:  true,
	StrategyUsageBased: true,
	StrategyLatency:    true,
	StrategyCost:       true,
	StrategyFallback:   true,
	StrategyWeighted:   true,
}

// AeroRouteSpec defines the desired routing configuration.
type AeroRouteSpec struct {
	Strategy      string                 `json:"strategy,omitempty"`
	Models        []string               `json:"models,omitempty"`
	Providers     []string               `json:"providers,omitempty"`
	Fallback      []string               `json:"fallback,omitempty"`
	BreakerConfig map[string]interface{} `json:"breaker_config,omitempty"`
	// Weights assigns a relative traffic weight to each provider. Required for
	// the "weighted" strategy; every key must appear in Providers.
	Weights map[string]float64 `json:"weights,omitempty"`
}

// Validate reports every problem with the spec, joined into one error.
func (s AeroRouteSpec) Validate() error {
	var errs []error
	if !validStrategies[s.Strategy] {
		errs = append(errs, fmt.Errorf("strategy %q: unsupported", s.Strategy))
	}
	errs = append(errs, validateEntries("models", s.Models)...)
	errs = append(errs, validateEntries("providers", s.Providers)...)
	errs = append(errs, validateEntries("fallback", s.Fallback)...)

	keys := make([]string, 0, len(s.BreakerConfig))
	for k := range s.BreakerConfig {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := validateBreakerValue(k, s.BreakerConfig[k]); err != nil {
			errs = append(errs, err)
		}
	}

	if s.Strategy == StrategyWeighted && len(s.Weights) == 0 {
		errs = append(errs, errors.New("weights: required for the weighted strategy"))
	}
	if len(s.Weights) > 0 {
		providers := make(map[string]bool, len(s.Providers))
		for _, p := range s.Providers {
			providers[p] = true
		}
		names := make([]string, 0, len(s.Weights))
		for p := range s.Weights {
			names = append(names, p)
		}
		sort.Strings(names)
		sum := 0.0
		weightsOK := true
		for _, p := range names {
			w := s.Weights[p]
			if math.IsNaN(w) || math.IsInf(w, 0) || w < 0 {
				errs = append(errs, fmt.Errorf("weights[%q]: must be a finite number >= 0", p))
				weightsOK = false
				continue
			}
			if !providers[p] {
				errs = append(errs, fmt.Errorf("weights[%q]: provider is not listed in providers", p))
			}
			sum += w
		}
		if weightsOK && sum <= 0 {
			errs = append(errs, errors.New("weights: at least one weight must be > 0"))
		}
	}
	return errors.Join(errs...)
}

// validateBreakerValue rejects negative or non-finite numeric breaker settings
// (thresholds, timeouts, ratios). Non-numeric values are left to the router.
func validateBreakerValue(key string, v interface{}) error {
	var f float64
	switch n := v.(type) {
	case float64:
		f = n
	case float32:
		f = float64(n)
	case int:
		f = float64(n)
	case int64:
		f = float64(n)
	case int32:
		f = float64(n)
	default:
		return nil
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
		return fmt.Errorf("breaker_config[%q]: must be a finite number >= 0", key)
	}
	return nil
}

// AeroRouteStatus reflects current routing health and circuit state.
type AeroRouteStatus struct {
	Healthy     bool              `json:"healthy,omitempty"`
	LatencyMs   float64           `json:"latency_ms,omitempty"`
	CircuitOpen bool              `json:"circuit_open,omitempty"`
	Providers   map[string]string `json:"providers,omitempty"`
	// ObservedGeneration is the metadata.generation last reconciled by the
	// operator; Conditions describe the outcome.
	ObservedGeneration int64       `json:"observedGeneration,omitempty"`
	Conditions         []Condition `json:"conditions,omitempty"`
}

// AeroRoute represents an externally managed routing policy.
// +kubebuilder:resource:path=aeroroutes
// +kubebuilder:printcolumn:name="Strategy",type="string",JSONPath=".spec.strategy"
// +kubebuilder:printcolumn:name="Healthy",type="bool",JSONPath=".status.healthy"
// +kubebuilder:printcolumn:name="CircuitOpen",type="bool",JSONPath=".status.circuit_open"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type AeroRoute struct {
	APIVersion string                 `json:"apiVersion,omitempty"`
	Kind       string                 `json:"kind,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	Spec       AeroRouteSpec          `json:"spec,omitempty"`
	Status     AeroRouteStatus        `json:"status,omitempty"`
}

// Validate checks the object's kind, metadata and spec.
func (r *AeroRoute) Validate() error {
	if r == nil {
		return errors.New("aeroroute: nil object")
	}
	return validateObject(KindAeroRoute, r.Kind, r.Metadata, r.Spec.Validate())
}
