package federated

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// RegistrationTokenHeader carries the shared registration token on
	// RegisterNodeHandlerWithAuth requests. A dedicated header avoids
	// clashing with the gateway's own Authorization handling.
	RegistrationTokenHeader = "X-AeroLLM-Federation-Token"
	// MinRegistrationTokenLength is the minimum accepted token length.
	MinRegistrationTokenLength = 16
	// DefaultMaxAttestationValidity bounds ExpiresAt-IssuedAt of operator
	// attestations.
	DefaultMaxAttestationValidity = 24 * time.Hour

	registrationDomain = "aerollm/federated/register/v1"
)

var (
	// ErrRegistrationUnauthorized is returned when a registration carries no
	// valid credential. It deliberately does not say which check failed.
	ErrRegistrationUnauthorized = errors.New("federated: registration not authorized")
	// ErrRegistrationAuthNotConfigured is returned when no registration
	// credential source is configured (fail closed).
	ErrRegistrationAuthNotConfigured = errors.New("federated: registration authentication not configured")
)

// AuthMethod identifies how a registration was authorized.
type AuthMethod string

const (
	// AuthMethodToken means a configured registration token matched.
	AuthMethodToken AuthMethod = "token"
	// AuthMethodOperator means a trusted operator key signed the
	// registration (see RegistrationPayload).
	AuthMethodOperator AuthMethod = "operator_signature"
)

// RegistrationCredentials are the credentials presented with a registration.
type RegistrationCredentials struct {
	// Token is a shared registration token.
	Token string
	// OperatorSignature is an ed25519 signature by a trusted operator key
	// over RegistrationPayload(reg, IssuedAt, ExpiresAt).
	OperatorSignature []byte
	// IssuedAt and ExpiresAt are Unix seconds bound into the attestation.
	IssuedAt  int64
	ExpiresAt int64
}

// RegistrationAuthConfig configures a RegistrationAuth. At least one token or
// operator key is required.
type RegistrationAuthConfig struct {
	// Tokens are accepted shared registration tokens (each at least
	// MinRegistrationTokenLength bytes). Only their SHA-256 digests are kept.
	Tokens []string
	// OperatorKeys are trusted operator ed25519 public keys.
	OperatorKeys []ed25519.PublicKey
	// MaxAttestationValidity bounds ExpiresAt-IssuedAt
	// (default DefaultMaxAttestationValidity).
	MaxAttestationValidity time.Duration
	// MaxClockSkew tolerates operator clocks running ahead
	// (default DefaultMaxClockSkew).
	MaxClockSkew time.Duration
	// Now overrides the clock (tests).
	Now func() time.Time
}

// RegistrationAuth authorizes node registrations with either a shared
// registration token (compared in constant time) or an attestation signed by
// a pre-trusted operator key. It is immutable and safe for concurrent use.
type RegistrationAuth struct {
	tokenDigests [][sha256.Size]byte
	operatorKeys []ed25519.PublicKey
	maxValidity  time.Duration
	skew         time.Duration
	now          func() time.Time
}

// NewRegistrationAuth validates cfg and returns an authenticator.
func NewRegistrationAuth(cfg RegistrationAuthConfig) (*RegistrationAuth, error) {
	if len(cfg.Tokens) == 0 && len(cfg.OperatorKeys) == 0 {
		return nil, ErrRegistrationAuthNotConfigured
	}
	if cfg.MaxAttestationValidity < 0 || cfg.MaxClockSkew < 0 {
		return nil, fmt.Errorf("federated: negative registration auth durations")
	}
	a := &RegistrationAuth{maxValidity: cfg.MaxAttestationValidity, skew: cfg.MaxClockSkew, now: cfg.Now}
	for i, tok := range cfg.Tokens {
		if len(tok) < MinRegistrationTokenLength {
			return nil, fmt.Errorf("federated: registration token %d shorter than %d bytes", i, MinRegistrationTokenLength)
		}
		a.tokenDigests = append(a.tokenDigests, sha256.Sum256([]byte(tok)))
	}
	for i, k := range cfg.OperatorKeys {
		if len(k) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("federated: operator key %d is not a %d-byte ed25519 public key", i, ed25519.PublicKeySize)
		}
		a.operatorKeys = append(a.operatorKeys, append(ed25519.PublicKey(nil), k...))
	}
	if a.maxValidity == 0 {
		a.maxValidity = DefaultMaxAttestationValidity
	}
	if a.skew == 0 {
		a.skew = DefaultMaxClockSkew
	}
	if a.now == nil {
		a.now = time.Now
	}
	return a, nil
}

// RegistrationPayload returns the canonical bytes an operator signs to
// attest a registration. Every field is syntax-restricted by
// NodeRegistration.Validate (URLs cannot contain control characters), so the
// newline-separated encoding is unambiguous:
//
//	aerollm/federated/register/v1
//	node_id=<node_id>
//	endpoint=<endpoint>
//	public_key=<lowercase hex>
//	algorithms=<comma-separated>
//	issued_at=<unix seconds>
//	expires_at=<unix seconds>
func RegistrationPayload(reg *NodeRegistration, issuedAt, expiresAt int64) []byte {
	if reg == nil {
		return nil
	}
	var b strings.Builder
	b.WriteString(registrationDomain)
	b.WriteString("\nnode_id=")
	b.WriteString(reg.NodeID)
	b.WriteString("\nendpoint=")
	b.WriteString(reg.Endpoint)
	b.WriteString("\npublic_key=")
	b.WriteString(hex.EncodeToString(reg.PublicKey))
	b.WriteString("\nalgorithms=")
	b.WriteString(strings.Join(reg.Algorithms, ","))
	b.WriteString("\nissued_at=")
	b.WriteString(strconv.FormatInt(issuedAt, 10))
	b.WriteString("\nexpires_at=")
	b.WriteString(strconv.FormatInt(expiresAt, 10))
	return []byte(b.String())
}

// SignRegistration produces an operator attestation for reg valid from
// issuedAt until expiresAt (Unix-second precision). reg must carry the
// node's public key: the attestation's purpose is to bind NodeID to it.
func SignRegistration(operatorKey ed25519.PrivateKey, reg *NodeRegistration, issuedAt, expiresAt time.Time) ([]byte, error) {
	if len(operatorKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("federated: invalid operator private key")
	}
	if err := reg.Validate(); err != nil {
		return nil, err
	}
	if len(reg.PublicKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: attested registrations require a public key", ErrInvalidRegistration)
	}
	if !expiresAt.After(issuedAt) {
		return nil, fmt.Errorf("%w: expires_at must be after issued_at", ErrInvalidRegistration)
	}
	return ed25519.Sign(operatorKey, RegistrationPayload(reg, issuedAt.Unix(), expiresAt.Unix())), nil
}

// Authorize checks cred for reg. An operator attestation is checked first
// (when present and operator keys are configured), then the token. Any
// failure yields ErrRegistrationUnauthorized (or
// ErrRegistrationAuthNotConfigured for a nil receiver); malformed
// registrations yield ErrInvalidRegistration.
//
// Callers must pass AuthMethodOperator registrations to
// GatewayRegistry.RegisterAttested with time.Unix(cred.IssuedAt, 0) so that
// attestations issued before a deregistration are refused.
func (a *RegistrationAuth) Authorize(reg *NodeRegistration, cred RegistrationCredentials) (AuthMethod, error) {
	if a == nil {
		return "", ErrRegistrationAuthNotConfigured
	}
	if err := reg.Validate(); err != nil {
		return "", err
	}
	if len(cred.OperatorSignature) > 0 && len(a.operatorKeys) > 0 && a.attestationValid(reg, cred) {
		return AuthMethodOperator, nil
	}
	if cred.Token != "" && len(a.tokenDigests) > 0 && a.tokenValid(cred.Token) {
		return AuthMethodToken, nil
	}
	return "", ErrRegistrationUnauthorized
}

// tokenValid compares the token's digest with every configured digest in
// constant time (hashing first makes the comparison independent of the
// token length, and every digest is compared without early exit).
func (a *RegistrationAuth) tokenValid(token string) bool {
	d := sha256.Sum256([]byte(token))
	match := 0
	for i := range a.tokenDigests {
		match |= subtle.ConstantTimeCompare(d[:], a.tokenDigests[i][:])
	}
	return match == 1
}

func (a *RegistrationAuth) attestationValid(reg *NodeRegistration, cred RegistrationCredentials) bool {
	if len(reg.PublicKey) != ed25519.PublicKeySize || len(cred.OperatorSignature) != ed25519.SignatureSize {
		return false
	}
	if cred.IssuedAt <= 0 || cred.ExpiresAt <= cred.IssuedAt {
		return false
	}
	// Compare in seconds: converting the difference to a Duration could
	// overflow for hostile values. Both bounds are positive, so the
	// subtraction cannot overflow.
	if cred.ExpiresAt-cred.IssuedAt > int64(a.maxValidity/time.Second) {
		return false
	}
	now := a.now()
	issued := time.Unix(cred.IssuedAt, 0)
	expires := time.Unix(cred.ExpiresAt, 0)
	if issued.After(now.Add(a.skew)) || !now.Before(expires) {
		return false
	}
	payload := RegistrationPayload(reg, cred.IssuedAt, cred.ExpiresAt)
	for _, k := range a.operatorKeys {
		if ed25519.Verify(k, payload, cred.OperatorSignature) {
			return true
		}
	}
	return false
}
