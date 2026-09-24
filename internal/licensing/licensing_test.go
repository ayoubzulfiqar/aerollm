package licensing

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func newKeys(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func issue(t *testing.T, priv ed25519.PrivateKey, features []string, expires time.Time) string {
	t.Helper()
	lic, err := IssueLicense(priv, License{LicenseID: "L1", Licensee: "Acme", Tier: "enterprise", Features: features, IssuedAt: time.Now().Unix(), ExpiresAt: expires.Unix()})
	if err != nil {
		t.Fatal(err)
	}
	return lic
}

func setEnv(t *testing.T, pub ed25519.PublicKey, license string) {
	t.Helper()
	t.Setenv(EnvLicensePublicKey, base64.StdEncoding.EncodeToString(pub))
	t.Setenv(EnvLicenseKey, license)
}

func TestEnvLicenseCheckerEnterprise(t *testing.T) {
	pub, priv := newKeys(t)
	setEnv(t, pub, issue(t, priv, []string{"*"}, time.Now().Add(time.Hour)))
	checker := NewEnvLicenseChecker()
	if !checker.IsEnterprise() {
		t.Fatalf("expected enterprise true: %v", checker.Err())
	}
	if !checker.IsFeatureEnabled(FeatureZeroKnowledge) {
		t.Fatalf("expected zero knowledge enabled")
	}
}

func TestEnvLicenseCheckerCommunity(t *testing.T) {
	t.Setenv(EnvLicenseKey, "")
	checker := NewEnvLicenseChecker()
	if checker.IsEnterprise() {
		t.Fatalf("expected enterprise false")
	}
	if checker.IsFeatureEnabled(FeatureAdvancedCRDTMesh) {
		t.Fatalf("expected mesh gated")
	}
	if !checker.IsFeatureEnabled("unknown") {
		t.Fatalf("expected non-enterprise feature enabled")
	}
	if !errors.Is(checker.Err(), ErrNoLicense) {
		t.Fatalf("expected ErrNoLicense, got %v", checker.Err())
	}
}

func TestArbitraryLicenseStringIsNotEnough(t *testing.T) {
	pub, _ := newKeys(t)
	for _, bogus := range []string{"enterprise-key", "key", "a.b", strings.Repeat("x", 20000)} {
		setEnv(t, pub, bogus)
		c := NewEnvLicenseChecker()
		if c.IsEnterprise() || c.IsFeatureEnabled(FeatureZeroKnowledge) {
			t.Fatalf("bogus license %q must not enable enterprise features", bogus[:min(len(bogus), 20)])
		}
	}
	// A valid license without a configured verification key is rejected.
	_, priv := newKeys(t)
	t.Setenv(EnvLicensePublicKey, "")
	t.Setenv(EnvLicenseKey, issue(t, priv, []string{"*"}, time.Now().Add(time.Hour)))
	if c := NewEnvLicenseChecker(); c.IsEnterprise() || !errors.Is(c.Err(), ErrNoPublicKey) {
		t.Fatalf("license without verification key must be rejected: %v", c.Err())
	}
}

func TestLicenseSignatureExpiryAndFeatures(t *testing.T) {
	pub, priv := newKeys(t)
	_, otherPriv := newKeys(t)

	// Signed by the wrong key.
	c := NewLicenseChecker(issue(t, otherPriv, []string{"*"}, time.Now().Add(time.Hour)), pub)
	if c.IsEnterprise() || !errors.Is(c.Err(), ErrBadSignature) {
		t.Fatalf("wrong signer: %v", c.Err())
	}
	// Tampered payload (features widened) breaks the signature.
	lic := issue(t, priv, []string{string(FeatureZeroKnowledge)}, time.Now().Add(time.Hour))
	payload, sig, _ := strings.Cut(lic, ".")
	raw, _ := base64.RawURLEncoding.DecodeString(payload)
	tampered := strings.Replace(string(raw), `"zero_knowledge"`, `"*"`, 1)
	c = NewLicenseChecker(base64.RawURLEncoding.EncodeToString([]byte(tampered))+"."+sig, pub)
	if c.IsEnterprise() {
		t.Fatal("tampered license accepted")
	}
	// Feature list is enforced.
	c = NewLicenseChecker(lic, pub)
	if !c.IsFeatureEnabled(FeatureZeroKnowledge) || c.IsFeatureEnabled(FeatureAdvancedCRDTMesh) {
		t.Fatal("feature list not enforced")
	}
	// Expired license.
	c = NewLicenseChecker(issue(t, priv, []string{"*"}, time.Now().Add(-time.Minute)), pub)
	if c.IsEnterprise() || !errors.Is(c.Err(), ErrExpired) {
		t.Fatalf("expired: %v", c.Err())
	}
	// Expiry is evaluated at call time.
	c = NewLicenseChecker(issue(t, priv, []string{"*"}, time.Now().Add(time.Hour)), pub)
	c.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if c.IsEnterprise() {
		t.Fatal("license must expire while running")
	}
	// Not yet valid.
	nb, _ := IssueLicense(priv, License{Features: []string{"*"}, NotBefore: time.Now().Add(time.Hour).Unix(), ExpiresAt: time.Now().Add(2 * time.Hour).Unix()})
	if c := NewLicenseChecker(nb, pub); c.IsEnterprise() || !errors.Is(c.Err(), ErrNotYetValid) {
		t.Fatalf("not-before: %v", c.Err())
	}
	// Missing expiry is malformed.
	noExp, _ := IssueLicense(priv, License{Features: []string{"*"}})
	if c := NewLicenseChecker(noExp, pub); c.IsEnterprise() {
		t.Fatal("license without expiry must be rejected")
	}
}

func TestEmbeddedKeyTakesPrecedence(t *testing.T) {
	pub, priv := newKeys(t)
	envPub, envPriv := newKeys(t)
	old := embeddedPublicKey
	embeddedPublicKey = base64.StdEncoding.EncodeToString(pub)
	defer func() { embeddedPublicKey = old }()
	// A license signed with an attacker key set via env must not verify.
	setEnv(t, envPub, issue(t, envPriv, []string{"*"}, time.Now().Add(time.Hour)))
	if NewEnvLicenseChecker().IsEnterprise() {
		t.Fatal("env key must not override the embedded key")
	}
	t.Setenv(EnvLicenseKey, issue(t, priv, []string{"*"}, time.Now().Add(time.Hour)))
	if !NewEnvLicenseChecker().IsEnterprise() {
		t.Fatal("license signed by embedded key must verify")
	}
}

func TestGateFeature(t *testing.T) {
	if err := GateFeature(nil, FeatureZeroKnowledge); err == nil {
		t.Fatalf("expected gated error")
	}
	pub, priv := newKeys(t)
	if err := GateFeature(NewLicenseChecker(issue(t, priv, []string{"*"}, time.Now().Add(time.Hour)), pub), FeatureZeroKnowledge); err != nil {
		t.Fatalf("expected no error: %v", err)
	}
}
