package licensing

import (
	"os"
	"testing"
)

func TestEnvLicenseCheckerEnterprise(t *testing.T) {
	os.Setenv("AEROLLM_LICENSE_KEY", "enterprise-key")
	checker := NewEnvLicenseChecker()
	if !checker.IsEnterprise() {
		t.Fatalf("expected enterprise true")
	}
	if !checker.IsFeatureEnabled(FeatureZeroKnowledge) {
		t.Fatalf("expected zero knowledge enabled")
	}
}

func TestEnvLicenseCheckerCommunity(t *testing.T) {
	os.Setenv("AEROLLM_LICENSE_KEY", "")
	checker := NewEnvLicenseChecker()
	if checker.IsEnterprise() {
		t.Fatalf("expected enterprise false")
	}
	if checker.IsFeatureEnabled(FeatureAdvancedCRDTMesh) {
		t.Fatalf("expected mesh gated")
	}
	if !checker.IsFeatureEnabled("unknown") {
		t.Fatalf("expected unknown feature enabled")
	}
}

func TestGateFeature(t *testing.T) {
	if err := GateFeature(nil, FeatureZeroKnowledge); err == nil {
		t.Fatalf("expected gated error")
	}
	os.Setenv("AEROLLM_LICENSE_KEY", "key")
	if err := GateFeature(NewEnvLicenseChecker(), FeatureZeroKnowledge); err != nil {
		t.Fatalf("expected no error: %v", err)
	}
}
