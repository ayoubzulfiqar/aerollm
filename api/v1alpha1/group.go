package v1alpha1

import "time"

// API group and version served by the AeroLLM CustomResourceDefinitions
// (see cmd/operator/deploy/crds.yaml).
const (
	GroupName  = "aerollm.io"
	Version    = "v1alpha1"
	APIVersion = GroupName + "/" + Version
)

// Plural resource names used in API paths
// (/apis/aerollm.io/v1alpha1/namespaces/{ns}/{plural}).
const (
	PluralAeroRoute         = "aeroroutes"
	PluralAeroBudget        = "aerobudgets"
	PluralAeroAgentPipeline = "aeroagentpipelines"
)

// PluralFor returns the plural resource name for a kind.
func PluralFor(kind string) (string, bool) {
	switch kind {
	case KindAeroRoute:
		return PluralAeroRoute, true
	case KindAeroBudget:
		return PluralAeroBudget, true
	case KindAeroAgentPipeline:
		return PluralAeroAgentPipeline, true
	}
	return "", false
}

// Condition types written to status.conditions by the operator.
const (
	// ConditionValid reports whether the spec passed validation.
	ConditionValid = "Valid"
	// ConditionReady reports whether the spec was applied to the gateway.
	ConditionReady = "Ready"
)

// Condition status values.
const (
	ConditionTrue    = "True"
	ConditionFalse   = "False"
	ConditionUnknown = "Unknown"
)

// Condition mirrors metav1.Condition: the latest observation of one aspect
// of a resource's state.
type Condition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	// ObservedGeneration is the metadata.generation the condition was set for.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// LastTransitionTime is when Status last changed (RFC 3339, UTC).
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
}

// FormatTime renders t the way Kubernetes expects in lastTransitionTime.
func FormatTime(t time.Time) string { return t.UTC().Format(time.RFC3339) }
