package v1alpha1

// AeroBudgetSpec defines allowed spend constraints.
type AeroBudgetSpec struct {
	APIKey       string  `json:"api_key,omitempty"`
	MaxUSD       float64 `json:"max_usd,omitempty"`
	MonthlyCap   float64 `json:"monthly_cap,omitempty"`
	AlertWebhook string  `json:"alert_webhook,omitempty"`
}

// AeroBudgetStatus reports current spend and remaining budget.
type AeroBudgetStatus struct {
	SpentUSD    float64 `json:"spent_usd,omitempty"`
	RemainingUSD float64 `json:"remaining_usd,omitempty"`
	AlertSent   bool    `json:"alert_sent,omitempty"`
}

// AeroBudget represents a budget policy tied to an API key or service account.
// +kubebuilder:resource:path=aerobudgets
// +kubebuilder:printcolumn:name="MaxUSD",type="float64",JSONPath=".spec.max_usd"
// +kubebuilder:printcolumn:name="RemainingUSD",type="float64",JSONPath=".status.remaining_usd"
// +kubebuilder:printcolumn:name="AlertSent",type="bool",JSONPath=".status.alert_sent"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type AeroBudget struct {
	APIVersion string            `json:"apiVersion,omitempty"`
	Kind       string            `json:"kind,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	Spec       AeroBudgetSpec    `json:"spec,omitempty"`
	Status     AeroBudgetStatus  `json:"status,omitempty"`
}
