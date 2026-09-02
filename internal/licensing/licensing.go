package licensing

import (
	"fmt"
	"os"
	"strings"
)

// Feature identifies an enterprise gated capability.
type Feature string

const (
	FeatureZeroKnowledge      Feature = "zero_knowledge"
	FeatureAdvancedCRDTMesh   Feature = "advanced_crdt_mesh"
	FeatureMultiTenantSaaS    Feature = "multi_tenant_saas"
)

// LicenseChecker validates deployment licenses.
type LicenseChecker interface {
	// IsEnterprise returns true if the deployment is licensed for enterprise features.
	IsEnterprise() bool
	// IsFeatureEnabled returns true if the specific feature is available.
	IsFeatureEnabled(feature Feature) bool
}

// EnvLicenseChecker checks the AEROLLM_LICENSE_KEY environment variable.
type EnvLicenseChecker struct {
	licenseKey string
}

// NewEnvLicenseChecker creates a new environment-based license checker.
func NewEnvLicenseChecker() *EnvLicenseChecker {
	return &EnvLicenseChecker{
		licenseKey: os.Getenv("AEROLLM_LICENSE_KEY"),
	}
}

// IsEnterprise returns true if a non-empty license key is present.
func (e *EnvLicenseChecker) IsEnterprise() bool {
	return strings.TrimSpace(e.licenseKey) != ""
}

// IsFeatureEnabled returns true for enterprise features when a license key is present.
func (e *EnvLicenseChecker) IsFeatureEnabled(feature Feature) bool {
	switch feature {
	case FeatureZeroKnowledge, FeatureAdvancedCRDTMesh, FeatureMultiTenantSaaS:
		return e.IsEnterprise()
	default:
		return true
	}
}

// ErrFeatureGated is returned when an enterprise feature is used without a valid license.
type ErrFeatureGated struct {
	Feature string
}

func (e *ErrFeatureGated) Error() string {
	return fmt.Sprintf("feature %q requires an AeroLLM Enterprise license. Set AEROLLM_LICENSE_KEY or contact sales@aerollm.io", e.Feature)
}

// GateFeature returns an error if the feature is not enabled.
func GateFeature(checker LicenseChecker, feature Feature) error {
	if checker == nil || !checker.IsFeatureEnabled(feature) {
		return &ErrFeatureGated{Feature: string(feature)}
	}
	return nil
}
