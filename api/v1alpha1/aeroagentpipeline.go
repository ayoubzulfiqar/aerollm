package v1alpha1

// AeroAgentPipelineSpec defines a DAG-based agent pipeline configuration.
type AeroAgentPipelineSpec struct {
	Name        string            `json:"name,omitempty"`
	Version     string            `json:"version,omitempty"`
	Nodes       []string          `json:"nodes,omitempty"`
	Edges       []string          `json:"edges,omitempty"`
	Tools       []string          `json:"tools,omitempty"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
}

// AeroAgentPipelineStatus reports last run state and errors.
type AeroAgentPipelineStatus struct {
	LastRun     string   `json:"last_run,omitempty"`
	State       string   `json:"state,omitempty"`
	ErrorCount  int      `json:"error_count,omitempty"`
	FailedNodes []string `json:"failed_nodes,omitempty"`
}

// AeroAgentPipeline represents an agentic DAG workflow resource.
// +kubebuilder:resource:path=aeroagentpipelines
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.version"
// +kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.state"
// +kubebuilder:printcolumn:name="Errors",type="int",JSONPath=".status.error_count"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type AeroAgentPipeline struct {
	APIVersion string                  `json:"apiVersion,omitempty"`
	Kind       string                  `json:"kind,omitempty"`
	Metadata   map[string]interface{}  `json:"metadata,omitempty"`
	Spec       AeroAgentPipelineSpec   `json:"spec,omitempty"`
	Status     AeroAgentPipelineStatus `json:"status,omitempty"`
}
