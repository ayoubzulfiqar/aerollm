package marketplace

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// Size limits applied to untrusted marketplace input.
const (
	// MaxManifestBytes caps a signed plugin manifest document.
	MaxManifestBytes = 64 << 10
	// MaxWASMBytes caps a downloaded plugin module.
	MaxWASMBytes = 32 << 20

	maxNameRunes = 128

	// manifestSigDomain is prepended to the canonical manifest bytes before
	// signing so a manifest signature can never be confused with a signature
	// over any other message type produced with the same key.
	manifestSigDomain = "aerollm.marketplace.manifest.v1\n"
)

var (
	// ErrInvalidManifest reports a malformed or incomplete manifest.
	ErrInvalidManifest = errors.New("marketplace: invalid manifest")
	// ErrInvalidSignature reports a manifest whose Ed25519 signature does not
	// verify against its canonical encoding.
	ErrInvalidSignature = errors.New("marketplace: invalid manifest signature")
	// ErrUntrustedKey reports a validly signed manifest whose key is not pinned
	// for the claimed creator.
	ErrUntrustedKey = errors.New("marketplace: untrusted publisher key")
	// ErrNotFound reports a missing plugin.
	ErrNotFound = errors.New("marketplace: plugin not found")
	// ErrOwnership reports an attempt to publish over a plugin owned by another creator.
	ErrOwnership = errors.New("marketplace: plugin is owned by another creator")
	// ErrVersionConflict reports a publish that would downgrade or rewrite a version.
	ErrVersionConflict = errors.New("marketplace: version conflict")
	// ErrTooLarge reports an input document over its size cap.
	ErrTooLarge = errors.New("marketplace: document too large")
)

var (
	// Identifiers are restricted to a URL-, path- and Redis-key-safe charset
	// and may not start with a separator, so "..", "/" or ":" can never appear
	// as a path segment or key delimiter.
	idPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	semverPattern  = regexp.MustCompile(`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$`)
	sha256HexRegex = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// ValidatePluginID reports whether id is an acceptable plugin or creator identifier.
func ValidatePluginID(id string) error {
	if !idPattern.MatchString(id) {
		return fmt.Errorf("%w: identifier %q must match %s", ErrInvalidManifest, truncate(id, 64), idPattern.String())
	}
	return nil
}

// ValidateVersion reports whether v is a semantic version (optionally "v"-prefixed).
func ValidateVersion(v string) error {
	if len(v) > 64 || !semverPattern.MatchString(v) {
		return fmt.Errorf("%w: version %q is not a semantic version", ErrInvalidManifest, truncate(v, 64))
	}
	return nil
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: missing name", ErrInvalidManifest)
	}
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) > maxNameRunes {
		return fmt.Errorf("%w: name must be valid UTF-8 of at most %d characters", ErrInvalidManifest, maxNameRunes)
	}
	for _, r := range name {
		if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
			return fmt.Errorf("%w: name contains control characters", ErrInvalidManifest)
		}
	}
	return nil
}

// ValidateFields checks every field of a publish request / manifest document
// except the cryptographic material.
func (p PublishRequest) ValidateFields() error {
	if err := ValidatePluginID(p.ID); err != nil {
		return fmt.Errorf("id: %w", err)
	}
	if err := validateName(p.Name); err != nil {
		return err
	}
	if err := ValidateVersion(p.Version); err != nil {
		return err
	}
	if err := ValidatePluginID(p.CreatorID); err != nil {
		return fmt.Errorf("creator_id: %w", err)
	}
	if !sha256HexRegex.MatchString(p.WASMHash) {
		return fmt.Errorf("%w: wasm_hash must be a lowercase hex SHA-256 digest", ErrInvalidManifest)
	}
	return nil
}

// canonicalManifest fixes the field set and order (lexicographic) of the
// signed document. The signature itself is never part of the signed bytes.
type canonicalManifest struct {
	CreatorID string `json:"creator_id"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	PublicKey string `json:"public_key"`
	Version   string `json:"version"`
	WASMHash  string `json:"wasm_hash"`
}

// CanonicalBytes returns the canonical encoding of the manifest: a compact
// JSON object with lexicographically ordered keys (creator_id, id, name,
// public_key, version, wasm_hash), no insignificant whitespace and no HTML
// escaping. The signature field is excluded. Signers sign
// "aerollm.marketplace.manifest.v1\n" || CanonicalBytes().
func (p PublishRequest) CanonicalBytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(canonicalManifest{
		CreatorID: p.CreatorID,
		ID:        p.ID,
		Name:      p.Name,
		PublicKey: p.PublicKey,
		Version:   p.Version,
		WASMHash:  p.WASMHash,
	}); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte{'\n'}), nil
}

func signingMessage(canonical []byte) []byte {
	msg := make([]byte, 0, len(manifestSigDomain)+len(canonical))
	msg = append(msg, manifestSigDomain...)
	return append(msg, canonical...)
}

// SignManifest fills in PublicKey and Signature for req using priv. All other
// fields must already be set and valid.
func SignManifest(req PublishRequest, priv ed25519.PrivateKey) (PublishRequest, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return PublishRequest{}, fmt.Errorf("%w: private key must be %d bytes", ErrInvalidManifest, ed25519.PrivateKeySize)
	}
	if err := req.ValidateFields(); err != nil {
		return PublishRequest{}, err
	}
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return PublishRequest{}, fmt.Errorf("%w: unexpected public key type", ErrInvalidManifest)
	}
	req.PublicKey = base64.StdEncoding.EncodeToString(pub)
	req.Signature = ""
	canonical, err := req.CanonicalBytes()
	if err != nil {
		return PublishRequest{}, err
	}
	req.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(priv, signingMessage(canonical)))
	return req, nil
}

// HashWASM returns the lowercase hex SHA-256 digest used in wasm_hash.
func HashWASM(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// VerifyWASM checks b against the manifest's wasm_hash in constant time.
func (m *VerifiedManifest) VerifyWASM(b []byte) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	want, err := hex.DecodeString(m.WASMHash)
	if err != nil || len(want) != sha256.Size {
		return fmt.Errorf("%w: manifest wasm_hash is not a SHA-256 digest", ErrInvalidManifest)
	}
	got := sha256.Sum256(b)
	if subtle.ConstantTimeCompare(want, got[:]) != 1 {
		return fmt.Errorf("marketplace: wasm hash mismatch for plugin %q", m.ID)
	}
	return nil
}

// VerifyPublishRequest validates the fields of req and verifies its Ed25519
// signature over the canonical encoding. It does not consult any trust store;
// use TrustStore.Check to bind the key to the creator.
func VerifyPublishRequest(req PublishRequest) (*VerifiedManifest, error) {
	if err := req.ValidateFields(); err != nil {
		return nil, err
	}
	pub, err := decodeFixedB64("public_key", req.PublicKey, ed25519.PublicKeySize)
	if err != nil {
		return nil, err
	}
	sig, err := decodeFixedB64("signature", req.Signature, ed25519.SignatureSize)
	if err != nil {
		return nil, err
	}
	canonical, err := req.CanonicalBytes()
	if err != nil {
		return nil, err
	}
	if !ed25519.Verify(ed25519.PublicKey(pub), signingMessage(canonical), sig) {
		return nil, ErrInvalidSignature
	}
	return &VerifiedManifest{
		ID:        req.ID,
		Name:      req.Name,
		Version:   req.Version,
		CreatorID: req.CreatorID,
		WASMHash:  req.WASMHash,
		Signature: sig,
		PublicKey: pub,
		Payload:   canonical,
	}, nil
}

// Verify re-checks the stored signature of an already parsed manifest (for
// example one loaded back from a Store) against its fields.
func (m *VerifiedManifest) Verify() error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	_, err := VerifyPublishRequest(m.PublishRequest())
	return err
}

// PublishRequest converts the manifest back into its wire representation.
func (m *VerifiedManifest) PublishRequest() PublishRequest {
	return PublishRequest{
		ID:        m.ID,
		Name:      m.Name,
		Version:   m.Version,
		CreatorID: m.CreatorID,
		WASMHash:  m.WASMHash,
		PublicKey: base64.StdEncoding.EncodeToString(m.PublicKey),
		Signature: base64.StdEncoding.EncodeToString(m.Signature),
	}
}

// DecodePublishRequest strictly decodes a single manifest JSON document of at
// most MaxManifestBytes: unknown fields and trailing data are rejected so no
// alternate field can smuggle unsigned data past verification.
func DecodePublishRequest(r io.Reader) (PublishRequest, error) {
	var req PublishRequest
	if err := decodeStrictJSON(r, MaxManifestBytes, &req); err != nil {
		return PublishRequest{}, err
	}
	return req, nil
}

// decodeStrictJSON decodes exactly one JSON value of at most maxBytes into v.
func decodeStrictJSON(r io.Reader, maxBytes int64, v interface{}) error {
	if r == nil {
		return fmt.Errorf("%w: empty body", ErrInvalidManifest)
	}
	lr := &io.LimitedReader{R: r, N: maxBytes + 1}
	dec := json.NewDecoder(lr)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if lr.N <= 0 {
			return fmt.Errorf("%w: %w: exceeds %d bytes", ErrInvalidManifest, ErrTooLarge, maxBytes)
		}
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return err
		}
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: empty body", ErrInvalidManifest)
		}
		return fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if lr.N <= 0 {
		return fmt.Errorf("%w: %w: exceeds %d bytes", ErrInvalidManifest, ErrTooLarge, maxBytes)
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing data after JSON document", ErrInvalidManifest)
	}
	// dec.More only looks at the next token; make sure nothing but whitespace follows.
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("%w: trailing data after JSON document", ErrInvalidManifest)
	}
	return nil
}

func decodeFixedB64(field, s string, size int) ([]byte, error) {
	if s == "" {
		return nil, fmt.Errorf("%w: missing %s", ErrInvalidManifest, field)
	}
	if len(s) > base64.StdEncoding.EncodedLen(size)+4 {
		return nil, fmt.Errorf("%w: %s has wrong length", ErrInvalidManifest, field)
	}
	b, err := base64.StdEncoding.Strict().DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %s is not valid base64", ErrInvalidManifest, field)
	}
	if len(b) != size {
		return nil, fmt.Errorf("%w: %s must decode to %d bytes", ErrInvalidManifest, field, size)
	}
	return b, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// TrustStore pins Ed25519 publisher keys to creator IDs. A manifest is only
// trusted when its public key is pinned for its creator_id. In
// trust-on-first-use mode the first key seen for an unknown creator is pinned
// automatically; later manifests for that creator must use a pinned key.
type TrustStore struct {
	mu   sync.RWMutex
	keys map[string][]ed25519.PublicKey
	tofu bool
}

// NewTrustStore returns a strict trust store: only explicitly pinned keys are trusted.
func NewTrustStore() *TrustStore {
	return &TrustStore{keys: make(map[string][]ed25519.PublicKey)}
}

// NewTOFUTrustStore returns a trust store that pins the first key seen per creator.
func NewTOFUTrustStore() *TrustStore {
	t := NewTrustStore()
	t.tofu = true
	return t
}

// Pin trusts pub for creatorID. Multiple keys may be pinned (key rotation).
func (t *TrustStore) Pin(creatorID string, pub ed25519.PublicKey) error {
	if err := ValidatePluginID(creatorID); err != nil {
		return err
	}
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: public key must be %d bytes", ErrInvalidManifest, ed25519.PublicKeySize)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range t.keys[creatorID] {
		if subtle.ConstantTimeCompare(k, pub) == 1 {
			return nil
		}
	}
	t.keys[creatorID] = append(t.keys[creatorID], append(ed25519.PublicKey(nil), pub...))
	return nil
}

// Revoke removes a pinned key for creatorID.
func (t *TrustStore) Revoke(creatorID string, pub ed25519.PublicKey) {
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := t.keys[creatorID]
	out := keys[:0]
	for _, k := range keys {
		if subtle.ConstantTimeCompare(k, pub) != 1 {
			out = append(out, k)
		}
	}
	if len(out) == 0 {
		delete(t.keys, creatorID)
		return
	}
	t.keys[creatorID] = out
}

// Keys returns a copy of the keys pinned for creatorID.
func (t *TrustStore) Keys(creatorID string) []ed25519.PublicKey {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]ed25519.PublicKey, 0, len(t.keys[creatorID]))
	for _, k := range t.keys[creatorID] {
		out = append(out, append(ed25519.PublicKey(nil), k...))
	}
	return out
}

// Check returns nil when pub is trusted for creatorID. In TOFU mode an unknown
// creator's first key is pinned atomically.
func (t *TrustStore) Check(creatorID string, pub []byte) error {
	if t == nil {
		return fmt.Errorf("%w: no trust store configured", ErrUntrustedKey)
	}
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: malformed key", ErrUntrustedKey)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	keys, known := t.keys[creatorID]
	match := 0
	for _, k := range keys {
		// Accumulate so the comparison time does not depend on which key matched.
		match |= subtle.ConstantTimeCompare(k, pub)
	}
	if match == 1 {
		return nil
	}
	if !known && t.tofu {
		if err := ValidatePluginID(creatorID); err != nil {
			return err
		}
		t.keys[creatorID] = []ed25519.PublicKey{append(ed25519.PublicKey(nil), pub...)}
		return nil
	}
	return fmt.Errorf("%w: key is not pinned for creator %q", ErrUntrustedKey, creatorID)
}

// lookup reports, without pinning anything, whether pub is pinned for
// creatorID (trusted) and whether the creator has any pinned key (known).
func (t *TrustStore) lookup(creatorID string, pub []byte) (trusted, known bool) {
	if t == nil {
		return false, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	keys, known := t.keys[creatorID]
	match := 0
	for _, k := range keys {
		match |= subtle.ConstantTimeCompare(k, pub)
	}
	return match == 1, known
}

// ParseTrustedKeys parses "creator:base64key[,creator:base64key...]" (for
// example from an environment variable) into a strict TrustStore.
func ParseTrustedKeys(spec string) (*TrustStore, error) {
	t := NewTrustStore()
	for _, entry := range strings.FieldsFunc(spec, func(r rune) bool { return r == ',' || r == ';' || unicode.IsSpace(r) }) {
		creator, key, ok := strings.Cut(entry, ":")
		if !ok {
			return nil, fmt.Errorf("marketplace: trusted key entry %q must be creator:base64key", truncate(entry, 32))
		}
		pub, err := decodeFixedB64("trusted key for "+strconv.Quote(creator), key, ed25519.PublicKeySize)
		if err != nil {
			return nil, err
		}
		if err := t.Pin(creator, pub); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// CompareVersions compares two semantic versions using SemVer 2.0 precedence
// (build metadata ignored). ok is false when either version is not valid.
func CompareVersions(a, b string) (cmp int, ok bool) {
	pa, oka := parseSemver(a)
	pb, okb := parseSemver(b)
	if !oka || !okb {
		return 0, false
	}
	for i := 0; i < 3; i++ {
		if c := compareNumeric(pa.core[i], pb.core[i]); c != 0 {
			return c, true
		}
	}
	switch {
	case pa.pre == "" && pb.pre == "":
		return 0, true
	case pa.pre == "":
		return 1, true
	case pb.pre == "":
		return -1, true
	}
	ia, ib := strings.Split(pa.pre, "."), strings.Split(pb.pre, ".")
	for i := 0; i < len(ia) && i < len(ib); i++ {
		na, nb := isNumeric(ia[i]), isNumeric(ib[i])
		var c int
		switch {
		case na && nb:
			c = compareNumeric(ia[i], ib[i])
		case na:
			c = -1
		case nb:
			c = 1
		default:
			c = strings.Compare(ia[i], ib[i])
		}
		if c != 0 {
			return c, true
		}
	}
	switch {
	case len(ia) < len(ib):
		return -1, true
	case len(ia) > len(ib):
		return 1, true
	}
	return 0, true
}

type semver struct {
	core [3]string
	pre  string
}

func parseSemver(v string) (semver, bool) {
	m := semverPattern.FindStringSubmatch(v)
	if m == nil {
		return semver{}, false
	}
	return semver{core: [3]string{m[1], m[2], m[3]}, pre: m[4]}, true
}

func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// compareNumeric compares non-negative decimal strings of arbitrary length
// without overflow (leading zeros are rejected by the version grammar except
// in pre-release identifiers, where they are stripped).
func compareNumeric(a, b string) int {
	a = strings.TrimLeft(a, "0")
	b = strings.TrimLeft(b, "0")
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return strings.Compare(a, b)
}
