package v1alpha1

// AeroRouteSpec defines the desired routing configuration.
type AeroRouteSpec struct {
	Strategy      string            `json:"strategy,omitempty"`
	Models        []string          `json:"models,omitempty"`
	Providers     []string          `json:"providers,omitempty"`
	Fallback      []string          `json:"fallback,omitempty"`
	BreakerConfig map[string]interface{} `json:"breaker_config,omitempty"`
}

// AeroRouteStatus reflects current routing health and circuit state.
type AeroRouteStatus struct {
	Healthy     bool              `json:"healthy,omitempty"`
	LatencyMs   float64           `json:"latency_ms,omitempty"`
	CircuitOpen bool              `json:"circuit_open,omitempty"`
	Providers   map[string]string `json:"providers,omitempty"`
}

// AeroRoute represents an externally managed routing policy.
// +kubebuilder:resource:path=aeroroutes
// +kubebuilder:printcolumn:name="Strategy",type="string",JSONPath=".spec.strategy"
// +kubebuilder:printcolumn:name="Healthy",type="bool",JSONPath=".status.healthy"
// +kubebuilder:printcolumn:name="CircuitOpen",type="bool",JSONPath=".status.circuit_open"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type AeroRoute struct {
	APIVersion string            `json:"apiVersion,omitempty"`
	Kind       string            `json:"kind,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	Spec       AeroRouteSpec     `json:"spec,omitempty"`
	Status     AeroRouteStatus   `json:"status,omitempty"`
}
