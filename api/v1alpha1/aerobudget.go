package v1alpha1

import (
	"errors"
	"fmt"
)

// MaxAPIKeyLength bounds the inline API key accepted in an AeroBudget spec.
const MaxAPIKeyLength = 256

// AeroBudgetSpec defines allowed spend constraints.
type AeroBudgetSpec struct {
	// APIKey identifies the key the budget applies to.
	//
	// SECURITY: a CRD spec is readable by anyone with get/list access to the
	// resource and is stored unencrypted in etcd. Prefer APIKeySecretRef, which
	// points at a Kubernetes Secret, over an inline key.
	APIKey string `json:"api_key,omitempty"`
	// APIKeySecretRef references a Secret ("<secret-name>/<key>") holding the
	// API key. Takes precedence over APIKey when both are set.
	APIKeySecretRef string  `json:"api_key_secret_ref,omitempty"`
	MaxUSD          float64 `json:"max_usd,omitempty"`
	MonthlyCap      float64 `json:"monthly_cap,omitempty"`
	AlertWebhook    string  `json:"alert_webhook,omitempty"`
}

// Validate reports every problem with the spec, joined into one error.
func (s AeroBudgetSpec) Validate() error {
	var errs []error
	if err := validateMoney("max_usd", s.MaxUSD); err != nil {
		errs = append(errs, err)
	}
	if err := validateMoney("monthly_cap", s.MonthlyCap); err != nil {
		errs = append(errs, err)
	}
	if len(s.APIKey) > MaxAPIKeyLength {
		errs = append(errs, fmt.Errorf("api_key: longer than %d characters", MaxAPIKeyLength))
	}
	if hasControlChars(s.APIKey) {
		errs = append(errs, errors.New("api_key: contains control characters"))
	}
	if s.APIKeySecretRef != "" {
		if err := validateSecretRef(s.APIKeySecretRef); err != nil {
			errs = append(errs, err)
		}
	}
	if s.AlertWebhook != "" {
		if err := validateHTTPSURL("alert_webhook", s.AlertWebhook); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// AeroBudgetStatus reports current spend and remaining budget.
type AeroBudgetStatus struct {
	SpentUSD     float64 `json:"spent_usd,omitempty"`
	RemainingUSD float64 `json:"remaining_usd,omitempty"`
	AlertSent    bool    `json:"alert_sent,omitempty"`
}

// AeroBudget represents a budget policy tied to an API key or service account.
// +kubebuilder:resource:path=aerobudgets
// +kubebuilder:printcolumn:name="MaxUSD",type="float64",JSONPath=".spec.max_usd"
// +kubebuilder:printcolumn:name="RemainingUSD",type="float64",JSONPath=".status.remaining_usd"
// +kubebuilder:printcolumn:name="AlertSent",type="bool",JSONPath=".status.alert_sent"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type AeroBudget struct {
	APIVersion string                 `json:"apiVersion,omitempty"`
	Kind       string                 `json:"kind,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	Spec       AeroBudgetSpec         `json:"spec,omitempty"`
	Status     AeroBudgetStatus       `json:"status,omitempty"`
}

// Validate checks the object's kind, metadata and spec.
func (b *AeroBudget) Validate() error {
	if b == nil {
		return errors.New("aerobudget: nil object")
	}
	return validateObject(KindAeroBudget, b.Kind, b.Metadata, b.Spec.Validate())
}
