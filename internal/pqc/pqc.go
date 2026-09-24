// Package pqc provides key management, key encapsulation and peer
// attestation with post-quantum key exchange.
//
// What is (and is not) post-quantum here — read this before relying on it:
//
//   - Key encapsulation is real post-quantum cryptography from the Go
//     standard library: ML-KEM-768 (FIPS 203, crypto/mlkem) for
//     AlgorithmPQCMLKEM768, and the hybrid X-Wing KEM (ML-KEM-768 + X25519,
//     via crypto/hpke MLKEM768X25519) for the hybrid suites.
//   - Signatures are Ed25519 only. ML-DSA (FIPS 204) is NOT available in the
//     Go standard library of this build, so no post-quantum signature is
//     produced. AlgorithmPQCMLDSA65 therefore fails with ErrMLDSAUnavailable,
//     and the legacy identifier AlgorithmHybridEd25519MLDSA65 is accepted
//     only for wire compatibility: it selects exactly the same suite as
//     AlgorithmHybridMLKEM768X25519Ed25519 (X-Wing KEM + Ed25519 signatures).
//     Suite() reports the real primitives.
//   - Stream encryption uses AES-256-GCM in a chunked STREAM construction with
//     per-stream HKDF-derived keys and counter nonces (see stream.go).
package pqc

import (
	"context"
	"crypto/ed25519"
	"crypto/hpke"
	"crypto/mlkem"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// AlgorithmPQCMLKEM768 is ML-KEM-768 key encapsulation only (no signatures).
	AlgorithmPQCMLKEM768 = "mlkem-768"
	// AlgorithmPQCMLDSA65 is ML-DSA-65 signatures. It is not available in this
	// build: every operation returns ErrMLDSAUnavailable.
	AlgorithmPQCMLDSA65 = "mldsa-65"
	// AlgorithmHybridEd25519MLDSA65 is a legacy identifier kept for wire
	// compatibility.
	//
	// Deprecated: despite its name it does NOT use ML-DSA. It is an alias of
	// AlgorithmHybridMLKEM768X25519Ed25519 (X-Wing KEM + Ed25519 signatures).
	AlgorithmHybridEd25519MLDSA65 = "hybrid-ed25519+mldsa-65"
	// AlgorithmHybridMLKEM768X25519Ed25519 is the honest name of the hybrid
	// suite: X-Wing (ML-KEM-768 + X25519) key encapsulation and Ed25519
	// signatures.
	AlgorithmHybridMLKEM768X25519Ed25519 = "hybrid-mlkem768x25519+ed25519"
)

// Primitive names reported by Suite.
const (
	KEMNameMLKEM768 = "ML-KEM-768"
	KEMNameXWing    = "X-Wing (ML-KEM-768+X25519)"
	SigNameEd25519  = "Ed25519"
)

// Errors.
var (
	ErrMLDSAUnavailable     = errors.New("pqc: ML-DSA is not available in this build (no post-quantum signature implementation in the Go standard library)")
	ErrUnsupportedAlgorithm = errors.New("pqc: unsupported algorithm")
	ErrSignatureUnsupported = errors.New("pqc: this suite does not support signatures")
	ErrInvalidKey           = errors.New("pqc: invalid key material")
	ErrBadSignature         = errors.New("pqc: bad signature")
	ErrDecapsulation        = errors.New("pqc: decapsulation failed")
)

// PeerAttestation carries signed peer identity material.
type PeerAttestation struct {
	PeerID       string
	Algorithm    string
	PublicKey    []byte
	AttestedAt   int64
	Capabilities []string
	// Payload is the exact signed message; Signature is over Payload.
	Payload   []byte
	Signature []byte
}

// KeyManager handles post-quantum key lifecycle.
type KeyManager interface {
	GenerateKeyPair(ctx context.Context) (PublicKey, PrivateKey, error)
	Encapsulate(ctx context.Context, peerPublicKey []byte) (ciphertext, sharedSecret []byte, err error)
	Decapsulate(ctx context.Context, ciphertext, privateKey []byte) (sharedSecret []byte, err error)
	Sign(ctx context.Context, privateKey, message []byte) ([]byte, error)
	Verify(ctx context.Context, publicKey, message, signature []byte) error
}

// PublicKeyDeriver is implemented by key managers that can derive the public
// key from a private key (used by AttestPeer).
type PublicKeyDeriver interface {
	PublicKeyFromPrivate(priv PrivateKey) (PublicKey, error)
}

// PublicKey is a PQ-safe public key.
type PublicKey []byte

// PrivateKey is a PQ-safe private key.
type PrivateKey []byte

// EncodedKey bundles serialized key material.
type EncodedKey struct {
	PublicKey  []byte
	PrivateKey []byte
	Algorithm  string
}

// Suite describes the real primitives behind an algorithm identifier.
type Suite struct {
	Algorithm            string `json:"algorithm"`
	KEM                  string `json:"kem,omitempty"`
	Signature            string `json:"signature,omitempty"`
	PostQuantumKEM       bool   `json:"post_quantum_kem"`
	PostQuantumSignature bool   `json:"post_quantum_signature"`
}

// SuiteFor returns the suite for an algorithm identifier.
func SuiteFor(algorithm string) (Suite, error) {
	switch algorithm {
	case AlgorithmPQCMLKEM768:
		return Suite{Algorithm: algorithm, KEM: KEMNameMLKEM768, PostQuantumKEM: true}, nil
	case AlgorithmHybridEd25519MLDSA65, AlgorithmHybridMLKEM768X25519Ed25519:
		return Suite{Algorithm: algorithm, KEM: KEMNameXWing, Signature: SigNameEd25519, PostQuantumKEM: true}, nil
	case AlgorithmPQCMLDSA65:
		return Suite{}, ErrMLDSAUnavailable
	default:
		return Suite{}, ErrUnsupportedAlgorithm
	}
}

// QuantumSafeKeyManager implements KeyManager for the suites above. It is
// safe for concurrent use.
type QuantumSafeKeyManager struct {
	algorithm string

	idMu     sync.Mutex
	identity *serverIdentity

	sessMu   sync.Mutex
	sessions map[string]session
}

// NewQuantumSafeKeyManager creates a new key manager for the given algorithm.
// Unsupported algorithms are reported by the first operation.
func NewQuantumSafeKeyManager(algorithm string) *QuantumSafeKeyManager {
	return &QuantumSafeKeyManager{algorithm: algorithm, sessions: map[string]session{}}
}

// Algorithm returns the configured algorithm identifier.
func (k *QuantumSafeKeyManager) Algorithm() string { return k.algorithm }

// Suite reports the real primitives used by this manager.
func (k *QuantumSafeKeyManager) Suite() (Suite, error) { return SuiteFor(k.algorithm) }

func (k *QuantumSafeKeyManager) isHybrid() bool {
	return k.algorithm == AlgorithmHybridEd25519MLDSA65 || k.algorithm == AlgorithmHybridMLKEM768X25519Ed25519
}

// ---------------------------------------------------------------------------
// Composite key encoding for hybrid suites:
//   magic(4) | u16 len | ed25519 part | u16 len | X-Wing part
// Public:  "PQP1", ed25519 public key (32),  X-Wing public key (1216)
// Private: "PQS1", ed25519 seed (32),        X-Wing private seed (32)
// ---------------------------------------------------------------------------

var (
	magicPublic  = []byte("PQP1")
	magicPrivate = []byte("PQS1")
)

func encodeComposite(magic, a, b []byte) []byte {
	out := make([]byte, 0, len(magic)+4+len(a)+len(b))
	out = append(out, magic...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(a)))
	out = append(out, a...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(b)))
	return append(out, b...)
}

func decodeComposite(magic, in []byte) (a, b []byte, ok bool) {
	if len(in) < len(magic)+4 || string(in[:len(magic)]) != string(magic) {
		return nil, nil, false
	}
	rest := in[len(magic):]
	la := int(binary.BigEndian.Uint16(rest))
	rest = rest[2:]
	if len(rest) < la+2 {
		return nil, nil, false
	}
	a, rest = rest[:la], rest[la:]
	lb := int(binary.BigEndian.Uint16(rest))
	rest = rest[2:]
	if len(rest) != lb {
		return nil, nil, false
	}
	return a, rest, true
}

func xwing() hpke.KEM { return hpke.MLKEM768X25519() }

const (
	kemInfo         = "aerollm/pqc/kem/v1"
	kemExportLabel  = "aerollm/pqc/shared-secret/v1"
	sharedKeyLength = 32
)

// GenerateKeyPair generates a new key pair for the configured suite.
func (k *QuantumSafeKeyManager) GenerateKeyPair(ctx context.Context) (PublicKey, PrivateKey, error) {
	switch {
	case k.isHybrid():
		edPub, edPriv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, nil, err
		}
		sk, err := xwing().GenerateKey()
		if err != nil {
			return nil, nil, err
		}
		skBytes, err := sk.Bytes()
		if err != nil {
			return nil, nil, err
		}
		pub := encodeComposite(magicPublic, edPub, sk.PublicKey().Bytes())
		priv := encodeComposite(magicPrivate, edPriv.Seed(), skBytes)
		return PublicKey(pub), PrivateKey(priv), nil
	case k.algorithm == AlgorithmPQCMLKEM768:
		dk, err := mlkem.GenerateKey768()
		if err != nil {
			return nil, nil, err
		}
		return PublicKey(dk.EncapsulationKey().Bytes()), PrivateKey(dk.Bytes()), nil
	case k.algorithm == AlgorithmPQCMLDSA65:
		return nil, nil, ErrMLDSAUnavailable
	default:
		return nil, nil, ErrUnsupportedAlgorithm
	}
}

// PublicKeyFromPrivate derives the public key for a private key of this
// suite. Raw 64-byte Ed25519 private keys are also accepted.
func (k *QuantumSafeKeyManager) PublicKeyFromPrivate(priv PrivateKey) (PublicKey, error) {
	switch {
	case k.isHybrid():
		if seed, kemSeed, ok := decodeComposite(magicPrivate, priv); ok && len(seed) == ed25519.SeedSize {
			sk, err := xwing().NewPrivateKey(kemSeed)
			if err != nil {
				return nil, ErrInvalidKey
			}
			edPub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
			return PublicKey(encodeComposite(magicPublic, edPub, sk.PublicKey().Bytes())), nil
		}
		if len(priv) == ed25519.PrivateKeySize {
			return PublicKey(ed25519.PrivateKey(priv).Public().(ed25519.PublicKey)), nil
		}
		return nil, ErrInvalidKey
	case k.algorithm == AlgorithmPQCMLKEM768:
		dk, err := mlkem.NewDecapsulationKey768(priv)
		if err != nil {
			return nil, ErrInvalidKey
		}
		return PublicKey(dk.EncapsulationKey().Bytes()), nil
	case k.algorithm == AlgorithmPQCMLDSA65:
		return nil, ErrMLDSAUnavailable
	}
	return nil, ErrUnsupportedAlgorithm
}

// kemPublicPart extracts the KEM public key from a composite or raw key.
func kemPublicPart(pub []byte) []byte {
	if _, kemPub, ok := decodeComposite(magicPublic, pub); ok {
		return kemPub
	}
	return pub
}

// Encapsulate generates a fresh shared secret for peerPublicKey (a composite
// key from GenerateKeyPair, or a raw KEM public key) and returns the
// ciphertext to send to the peer.
func (k *QuantumSafeKeyManager) Encapsulate(ctx context.Context, peerPublicKey []byte) ([]byte, []byte, error) {
	if len(peerPublicKey) == 0 {
		return nil, nil, errors.New("pqc: empty peer public key")
	}
	switch {
	case k.isHybrid():
		pk, err := xwing().NewPublicKey(kemPublicPart(peerPublicKey))
		if err != nil {
			return nil, nil, ErrInvalidKey
		}
		enc, sender, err := hpke.NewSender(pk, hpke.HKDFSHA256(), hpke.ExportOnly(), []byte(kemInfo))
		if err != nil {
			return nil, nil, err
		}
		shared, err := sender.Export(kemExportLabel, sharedKeyLength)
		if err != nil {
			return nil, nil, err
		}
		return enc, shared, nil
	case k.algorithm == AlgorithmPQCMLKEM768:
		ek, err := mlkem.NewEncapsulationKey768(peerPublicKey)
		if err != nil {
			return nil, nil, ErrInvalidKey
		}
		shared, ct := ek.Encapsulate()
		return ct, shared, nil
	case k.algorithm == AlgorithmPQCMLDSA65:
		return nil, nil, ErrMLDSAUnavailable
	}
	return nil, nil, ErrUnsupportedAlgorithm
}

// Decapsulate recovers the shared secret from ciphertext with privateKey.
func (k *QuantumSafeKeyManager) Decapsulate(ctx context.Context, ciphertext, privateKey []byte) ([]byte, error) {
	if len(ciphertext) == 0 {
		return nil, errors.New("pqc: empty ciphertext")
	}
	if len(privateKey) == 0 {
		return nil, errors.New("pqc: empty private key")
	}
	switch {
	case k.isHybrid():
		_, kemSeed, ok := decodeComposite(magicPrivate, privateKey)
		if !ok {
			kemSeed = privateKey
		}
		sk, err := xwing().NewPrivateKey(kemSeed)
		if err != nil {
			return nil, ErrInvalidKey
		}
		r, err := hpke.NewRecipient(ciphertext, sk, hpke.HKDFSHA256(), hpke.ExportOnly(), []byte(kemInfo))
		if err != nil {
			return nil, ErrDecapsulation
		}
		return r.Export(kemExportLabel, sharedKeyLength)
	case k.algorithm == AlgorithmPQCMLKEM768:
		dk, err := mlkem.NewDecapsulationKey768(privateKey)
		if err != nil {
			return nil, ErrInvalidKey
		}
		shared, err := dk.Decapsulate(ciphertext)
		if err != nil {
			return nil, ErrDecapsulation
		}
		return shared, nil
	case k.algorithm == AlgorithmPQCMLDSA65:
		return nil, ErrMLDSAUnavailable
	}
	return nil, ErrUnsupportedAlgorithm
}

func (k *QuantumSafeKeyManager) edPrivate(priv []byte) (ed25519.PrivateKey, error) {
	if seed, _, ok := decodeComposite(magicPrivate, priv); ok && len(seed) == ed25519.SeedSize {
		return ed25519.NewKeyFromSeed(seed), nil
	}
	if len(priv) == ed25519.PrivateKeySize {
		return ed25519.PrivateKey(priv), nil
	}
	return nil, ErrInvalidKey
}

func edPublic(pub []byte) (ed25519.PublicKey, error) {
	if edPub, _, ok := decodeComposite(magicPublic, pub); ok && len(edPub) == ed25519.PublicKeySize {
		return ed25519.PublicKey(edPub), nil
	}
	if len(pub) == ed25519.PublicKeySize {
		return ed25519.PublicKey(pub), nil
	}
	return nil, ErrInvalidKey
}

// Sign signs message with the Ed25519 component of privateKey. (Classical
// signature: see the package documentation.)
func (k *QuantumSafeKeyManager) Sign(ctx context.Context, privateKey, message []byte) ([]byte, error) {
	switch {
	case k.isHybrid():
		sk, err := k.edPrivate(privateKey)
		if err != nil {
			return nil, err
		}
		return ed25519.Sign(sk, message), nil
	case k.algorithm == AlgorithmPQCMLDSA65:
		return nil, ErrMLDSAUnavailable
	case k.algorithm == AlgorithmPQCMLKEM768:
		return nil, ErrSignatureUnsupported
	}
	return nil, ErrUnsupportedAlgorithm
}

// Verify verifies an Ed25519 signature made by Sign.
func (k *QuantumSafeKeyManager) Verify(ctx context.Context, publicKey, message, signature []byte) error {
	switch {
	case k.isHybrid():
		pk, err := edPublic(publicKey)
		if err != nil {
			return err
		}
		if !ed25519.Verify(pk, message, signature) {
			return ErrBadSignature
		}
		return nil
	case k.algorithm == AlgorithmPQCMLDSA65:
		return ErrMLDSAUnavailable
	case k.algorithm == AlgorithmPQCMLKEM768:
		return ErrSignatureUnsupported
	}
	return ErrUnsupportedAlgorithm
}

// ---------------------------------------------------------------------------
// Attestation
// ---------------------------------------------------------------------------

type attestationPayload struct {
	Domain       string   `json:"domain"`
	PeerID       string   `json:"peer_id"`
	Algorithm    string   `json:"alg"`
	PublicKeyFP  string   `json:"pk_sha256"`
	Timestamp    int64    `json:"ts"`
	Nonce        []byte   `json:"nonce"`
	Capabilities []string `json:"caps"`
}

const attestationDomain = "aerollm-pqc-attestation-v1"

// AttestPeer creates a signed attestation binding peerID to the public key
// of priv. The signature (Ed25519 for the hybrid suites) covers Payload,
// which includes the peer ID, algorithm, public-key fingerprint, timestamp
// and a random nonce. Verify it with VerifyAttestation.
func AttestPeer(ctx context.Context, km KeyManager, peerID string, priv PrivateKey) (*PeerAttestation, error) {
	if km == nil || len(priv) == 0 {
		return nil, errors.New("invalid key material")
	}
	var pub PublicKey
	var err error
	if d, ok := km.(PublicKeyDeriver); ok {
		pub, err = d.PublicKeyFromPrivate(priv)
	} else if len(priv) == ed25519.PrivateKeySize {
		pub = PublicKey(ed25519.PrivateKey(priv).Public().(ed25519.PublicKey))
	} else {
		err = ErrInvalidKey
	}
	if err != nil {
		return nil, err
	}
	alg := AlgorithmHybridMLKEM768X25519Ed25519
	if a, ok := km.(interface{ Algorithm() string }); ok {
		alg = a.Algorithm()
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	fp := sha256.Sum256(pub)
	caps := []string{"chat", "stream"}
	now := time.Now().Unix()
	payload, err := json.Marshal(attestationPayload{
		Domain: attestationDomain, PeerID: peerID, Algorithm: alg,
		PublicKeyFP: fmt.Sprintf("%x", fp[:]), Timestamp: now, Nonce: nonce, Capabilities: caps,
	})
	if err != nil {
		return nil, err
	}
	sig, err := km.Sign(ctx, priv, payload)
	if err != nil {
		return nil, err
	}
	return &PeerAttestation{
		PeerID:       peerID,
		Algorithm:    alg,
		PublicKey:    pub,
		AttestedAt:   now,
		Capabilities: caps,
		Payload:      payload,
		Signature:    sig,
	}, nil
}

// VerifyAttestation checks an attestation's signature and that its payload
// matches its fields. maxAge > 0 additionally rejects stale attestations.
func VerifyAttestation(ctx context.Context, km KeyManager, att *PeerAttestation, maxAge time.Duration) error {
	if km == nil || att == nil || len(att.Payload) == 0 || len(att.Signature) == 0 {
		return errors.New("pqc: incomplete attestation")
	}
	if err := km.Verify(ctx, att.PublicKey, att.Payload, att.Signature); err != nil {
		return err
	}
	var p attestationPayload
	if err := json.Unmarshal(att.Payload, &p); err != nil {
		return errors.New("pqc: malformed attestation payload")
	}
	fp := sha256.Sum256(att.PublicKey)
	if p.Domain != attestationDomain || p.PeerID != att.PeerID || p.Timestamp != att.AttestedAt ||
		p.PublicKeyFP != fmt.Sprintf("%x", fp[:]) {
		return errors.New("pqc: attestation fields do not match signed payload")
	}
	if maxAge > 0 && time.Since(time.Unix(p.Timestamp, 0)) > maxAge {
		return errors.New("pqc: attestation expired")
	}
	return nil
}

// CapabilityDiscoveryRequest asks a peer to reveal supported PQ algorithms.
type CapabilityDiscoveryRequest struct {
	PeerID     string                 `json:"peer_id"`
	Algorithms []string               `json:"algorithms"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// CapabilityDiscoveryResponse captures peer capabilities.
type CapabilityDiscoveryResponse struct {
	PeerID     string   `json:"peer_id"`
	Algorithms []string `json:"algorithms"`
	PublicKey  []byte   `json:"public_key,omitempty"`
	Error      string   `json:"error,omitempty"`
}

// SupportedAlgorithms lists the algorithm identifiers that work in this
// build.
func SupportedAlgorithms() []string {
	return []string{AlgorithmHybridMLKEM768X25519Ed25519, AlgorithmHybridEd25519MLDSA65, AlgorithmPQCMLKEM768}
}
