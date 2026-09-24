// Package licensing gates enterprise features behind a signed license.
//
// License format (AEROLLM_LICENSE_KEY):
//
//	base64url(payload JSON) "." base64url(Ed25519 signature over the payload bytes)
//
// Payload: {"license_id","licensee","tier","features":[...],"issued_at",
// "not_before","expires_at"} with Unix-second timestamps. features may
// contain "*" for all enterprise features.
//
// Verification key: the key compiled in via
//
//	-ldflags "-X github.com/ayoubzulfiqar/aerollm/internal/licensing.embeddedPublicKey=<base64 or hex>"
//
// takes precedence; AEROLLM_LICENSE_PUBLIC_KEY is only honoured when no key
// is embedded (development builds).
//
// Behaviour with no (valid) license configured: the deployment runs in
// community mode. Enterprise features (FeatureZeroKnowledge,
// FeatureAdvancedCRDTMesh, FeatureMultiTenantSaaS) are disabled and
// Middleware answers 403; every other feature is enabled. An arbitrary
// non-empty AEROLLM_LICENSE_KEY no longer enables anything: the signature,
// validity window and feature list are all checked, and expiry is evaluated
// on every call.
package licensing

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
)

// Feature identifies an enterprise gated capability.
type Feature string

const (
	FeatureZeroKnowledge    Feature = "zero_knowledge"
	FeatureAdvancedCRDTMesh Feature = "advanced_crdt_mesh"
	FeatureMultiTenantSaaS  Feature = "multi_tenant_saas"
)

// EnterpriseFeatures lists the features that require a license.
var EnterpriseFeatures = []Feature{FeatureZeroKnowledge, FeatureAdvancedCRDTMesh, FeatureMultiTenantSaaS}

// IsEnterpriseFeature reports whether feature requires a license.
func IsEnterpriseFeature(f Feature) bool { return slices.Contains(EnterpriseFeatures, f) }

// Environment variables.
const (
	EnvLicenseKey       = "AEROLLM_LICENSE_KEY"
	EnvLicensePublicKey = "AEROLLM_LICENSE_PUBLIC_KEY"
)

// embeddedPublicKey is the vendor's license verification key (base64 or
// hex), set at build time with -ldflags -X.
var embeddedPublicKey = ""

// maxLicenseLen bounds the size of a license string.
const maxLicenseLen = 16 << 10

// clockSkew tolerates small clock differences for not_before.
const clockSkew = 5 * time.Minute

// LicenseChecker validates deployment licenses.
type LicenseChecker interface {
	// IsEnterprise returns true if the deployment is licensed for enterprise features.
	IsEnterprise() bool
	// IsFeatureEnabled returns true if the specific feature is available.
	IsFeatureEnabled(feature Feature) bool
}

// License is the verified license payload.
type License struct {
	LicenseID string   `json:"license_id"`
	Licensee  string   `json:"licensee"`
	Tier      string   `json:"tier"`
	Features  []string `json:"features"`
	IssuedAt  int64    `json:"issued_at"`
	NotBefore int64    `json:"not_before,omitempty"`
	ExpiresAt int64    `json:"expires_at"`
}

// Errors.
var (
	ErrNoLicense      = errors.New("licensing: no license configured")
	ErrNoPublicKey    = errors.New("licensing: no license verification key configured")
	ErrMalformed      = errors.New("licensing: malformed license")
	ErrBadSignature   = errors.New("licensing: invalid license signature")
	ErrExpired        = errors.New("licensing: license expired")
	ErrNotYetValid    = errors.New("licensing: license not yet valid")
	ErrFeatureMissing = errors.New("licensing: feature not included in license")
)

func decodeKey(s string) (ed25519.PublicKey, error) {
	s = strings.TrimSpace(s)
	if b, err := hex.DecodeString(s); err == nil && len(b) == ed25519.PublicKeySize {
		return ed25519.PublicKey(b), nil
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil && len(b) == ed25519.PublicKeySize {
			return ed25519.PublicKey(b), nil
		}
	}
	return nil, errors.New("licensing: public key must be 32 bytes (hex or base64)")
}

// VerificationKey returns the configured license verification key.
func VerificationKey() (ed25519.PublicKey, error) {
	if strings.TrimSpace(embeddedPublicKey) != "" {
		return decodeKey(embeddedPublicKey)
	}
	if env := strings.TrimSpace(os.Getenv(EnvLicensePublicKey)); env != "" {
		return decodeKey(env)
	}
	return nil, ErrNoPublicKey
}

// ParseLicense verifies a license string's signature and decodes it. It
// does not check the validity window; see (*License).Check.
func ParseLicense(license string, pub ed25519.PublicKey) (*License, error) {
	license = strings.TrimSpace(license)
	if license == "" {
		return nil, ErrNoLicense
	}
	if len(pub) != ed25519.PublicKeySize {
		return nil, ErrNoPublicKey
	}
	if len(license) > maxLicenseLen {
		return nil, ErrMalformed
	}
	payloadB64, sigB64, ok := strings.Cut(license, ".")
	if !ok || strings.Contains(sigB64, ".") {
		return nil, ErrMalformed
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(payloadB64, "="))
	if err != nil {
		return nil, ErrMalformed
	}
	sig, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(sigB64, "="))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return nil, ErrMalformed
	}
	if !ed25519.Verify(pub, payload, sig) {
		return nil, ErrBadSignature
	}
	var lic License
	if err := json.Unmarshal(payload, &lic); err != nil {
		return nil, ErrMalformed
	}
	if lic.ExpiresAt <= 0 {
		return nil, fmt.Errorf("%w: missing expires_at", ErrMalformed)
	}
	return &lic, nil
}

// Check returns nil if the license is within its validity window at now.
func (l *License) Check(now time.Time) error {
	if l == nil {
		return ErrNoLicense
	}
	if l.NotBefore > 0 && now.Add(clockSkew).Before(time.Unix(l.NotBefore, 0)) {
		return ErrNotYetValid
	}
	if !now.Before(time.Unix(l.ExpiresAt, 0)) {
		return ErrExpired
	}
	return nil
}

// HasFeature reports whether the license lists feature (or "*").
func (l *License) HasFeature(f Feature) bool {
	if l == nil {
		return false
	}
	for _, have := range l.Features {
		if have == "*" || have == string(f) {
			return true
		}
	}
	return false
}

// IssueLicense signs a license payload. It is intended for vendor tooling
// and tests; the private key must never ship with the gateway.
func IssueLicense(priv ed25519.PrivateKey, lic License) (string, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return "", errors.New("licensing: invalid private key")
	}
	payload, err := json.Marshal(lic)
	if err != nil {
		return "", err
	}
	sig := ed25519.Sign(priv, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// EnvLicenseChecker checks the license in AEROLLM_LICENSE_KEY against the
// verification key (see the package documentation).
type EnvLicenseChecker struct {
	license *License
	err     error
	now     func() time.Time
}

// NewEnvLicenseChecker creates a checker from the environment. Problems are
// reported by Err; the checker then runs in community mode.
func NewEnvLicenseChecker() *EnvLicenseChecker {
	raw := os.Getenv(EnvLicenseKey)
	if strings.TrimSpace(raw) == "" {
		return &EnvLicenseChecker{err: ErrNoLicense, now: time.Now}
	}
	pub, err := VerificationKey()
	if err != nil {
		return &EnvLicenseChecker{err: err, now: time.Now}
	}
	return NewLicenseChecker(raw, pub)
}

// NewLicenseChecker verifies license with pub.
func NewLicenseChecker(license string, pub ed25519.PublicKey) *EnvLicenseChecker {
	lic, err := ParseLicense(license, pub)
	return &EnvLicenseChecker{license: lic, err: err, now: time.Now}
}

// License returns the verified license (nil in community mode).
func (e *EnvLicenseChecker) License() *License {
	if e == nil {
		return nil
	}
	return e.license
}

// Err explains why no enterprise features are enabled (nil when a valid,
// unexpired license is loaded).
func (e *EnvLicenseChecker) Err() error {
	if e == nil {
		return ErrNoLicense
	}
	if e.err != nil {
		return e.err
	}
	return e.license.Check(e.clock())
}

func (e *EnvLicenseChecker) clock() time.Time {
	if e.now != nil {
		return e.now()
	}
	return time.Now()
}

// IsEnterprise returns true if a validly signed, unexpired license is loaded.
func (e *EnvLicenseChecker) IsEnterprise() bool {
	return e != nil && e.err == nil && e.license.Check(e.clock()) == nil
}

// IsFeatureEnabled reports whether feature is available: non-enterprise
// features always are; enterprise features need a valid license listing them.
func (e *EnvLicenseChecker) IsFeatureEnabled(feature Feature) bool {
	if !IsEnterpriseFeature(feature) {
		return true
	}
	return e.IsEnterprise() && e.license.HasFeature(feature)
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
