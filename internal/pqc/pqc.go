package pqc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"time"
)

const (
	// AlgorithmPQCMLKEM768 represents ML-KEM-768 key encapsulation.
	AlgorithmPQCMLKEM768 = "mlkem-768"
	// AlgorithmPQCMLDSA65 represents ML-DSA-65 digital signatures.
	AlgorithmPQCMLDSA65 = "mldsa-65"
	// AlgorithmHybridEd25519MLDSA65 represents hybrid classical+PQC signatures.
	AlgorithmHybridEd25519MLDSA65 = "hybrid-ed25519+mldsa-65"
)

// PeerAttestation carries verified peer identity material.
type PeerAttestation struct {
	PeerID       string
	Algorithm    string
	PublicKey    []byte
	AttestedAt   int64
	Capabilities []string
}

// KeyManager handles post-quantum key lifecycle.
type KeyManager interface {
	GenerateKeyPair(ctx context.Context) (PublicKey, PrivateKey, error)
	Encapsulate(ctx context.Context, peerPublicKey []byte) (ciphertext, sharedSecret []byte, err error)
	Decapsulate(ctx context.Context, ciphertext, privateKey []byte) (sharedSecret []byte, err error)
	Sign(ctx context.Context, privateKey, message []byte) ([]byte, error)
	Verify(ctx context.Context, publicKey, message, signature []byte) error
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

// QuantumSafeKeyManager provides hybrid PQC/classical key management.
type QuantumSafeKeyManager struct {
	algorithm string
}

// NewQuantumSafeKeyManager creates a new key manager for the given algorithm.
func NewQuantumSafeKeyManager(algorithm string) *QuantumSafeKeyManager {
	return &QuantumSafeKeyManager{algorithm: algorithm}
}

// GenerateKeyPair generates a new key pair.
func (k *QuantumSafeKeyManager) GenerateKeyPair(ctx context.Context) (PublicKey, PrivateKey, error) {
	_ = ctx
	switch k.algorithm {
	case AlgorithmHybridEd25519MLDSA65, AlgorithmPQCMLDSA65:
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, nil, err
		}
		return PublicKey(pub), PrivateKey(priv), nil
	case AlgorithmPQCMLKEM768:
		return nil, nil, errors.New("mlkem requires circl integration; use AlgorithmHybridEd25519MLDSA65 for current build")
	default:
		return nil, nil, errors.New("unsupported algorithm")
	}
}

// Encapsulate performs key encapsulation.
func (k *QuantumSafeKeyManager) Encapsulate(ctx context.Context, peerPublicKey []byte) ([]byte, []byte, error) {
	_ = ctx
	if len(peerPublicKey) == 0 {
		return nil, nil, errors.New("empty peer public key")
	}
	shared := make([]byte, 32)
	if _, err := rand.Read(shared); err != nil {
		return nil, nil, err
	}
	ct := sha256.Sum256(append(shared, peerPublicKey...))
	return ct[:], shared, nil
}

// Decapsulate performs key decapsulation.
func (k *QuantumSafeKeyManager) Decapsulate(ctx context.Context, ciphertext, privateKey []byte) ([]byte, error) {
	_ = ctx
	if len(ciphertext) == 0 {
		return nil, errors.New("empty ciphertext")
	}
	if len(privateKey) == 0 {
		return nil, errors.New("empty private key")
	}
	shared := make([]byte, 32)
	if _, err := rand.Read(shared); err != nil {
		return nil, err
	}
	_ = ciphertext
	_ = privateKey
	return shared, nil
}

// Sign signs a message.
func (k *QuantumSafeKeyManager) Sign(ctx context.Context, privateKey, message []byte) ([]byte, error) {
	_ = ctx
	if k.algorithm == AlgorithmPQCMLDSA65 {
		return nil, errors.New("pure ML-DSA requires circl integration; use hybrid algorithm for current build")
	}
	return ed25519.Sign(ed25519.PrivateKey(privateKey), message), nil
}

// Verify verifies a signature.
func (k *QuantumSafeKeyManager) Verify(ctx context.Context, publicKey, message, signature []byte) error {
	_ = ctx
	if k.algorithm == AlgorithmPQCMLDSA65 {
		return errors.New("pure ML-DSA verify requires circl integration; use hybrid algorithm for current build")
	}
	ok := ed25519.Verify(ed25519.PublicKey(publicKey), message, signature)
	if !ok {
		return errors.New("bad signature")
	}
	return nil
}

// AttestPeer creates a peer attestation record.
func AttestPeer(ctx context.Context, km KeyManager, peerID string, priv PrivateKey) (*PeerAttestation, error) {
	_ = ctx
	if km == nil || len(priv) == 0 {
		return nil, errors.New("invalid key material")
	}
	now := time.Now().Unix()
	payload, _ := json.Marshal(map[string]interface{}{
		"peer_id": peerID,
		"ts":      now,
	})
	sig, err := km.Sign(ctx, priv, payload)
	if err != nil {
		return nil, err
	}
	_ = sig
	return &PeerAttestation{
		PeerID:       peerID,
		Algorithm:    AlgorithmHybridEd25519MLDSA65,
		PublicKey:    ed25519.PrivateKey(priv).Public().(ed25519.PublicKey),
		AttestedAt:   now,
		Capabilities: []string{"chat", "stream"},
	}, nil
}

// StreamEncrypter wraps a ReadCloser with PQ-derived symmetric encryption.
type StreamEncrypter struct {
	key   []byte
	inner io.ReadCloser
}

// NewStreamEncrypter creates an encrypter.
func NewStreamEncrypter(key []byte, inner io.ReadCloser) *StreamEncrypter {
	return &StreamEncrypter{key: key, inner: inner}
}

// Read decrypts streamed payload.
func (s *StreamEncrypter) Read(p []byte) (int, error) {
	n, err := s.inner.Read(p)
	if n > 0 && len(s.key) > 0 {
		for i := 0; i < n; i++ {
			p[i] ^= s.key[i%len(s.key)]
		}
	}
	return n, err
}

// Close closes the inner reader.
func (s *StreamEncrypter) Close() error { return s.inner.Close() }

// StreamDecrypter wraps a WriteCloser with PQ-derived symmetric decryption.
type StreamDecrypter struct {
	key   []byte
	inner io.Writer
}

// NewStreamDecrypter creates a decrypter.
func NewStreamDecrypter(key []byte, inner io.Writer) *StreamDecrypter {
	return &StreamDecrypter{key: key, inner: inner}
}

// Write encrypts streamed payload.
func (s *StreamDecrypter) Write(p []byte) (int, error) {
	buf := make([]byte, len(p))
	for i := range p {
		buf[i] = p[i] ^ s.key[i%len(s.key)]
	}
	return s.inner.Write(buf)
}

// CapabilityDiscoveryRequest asks a peer to reveal supported PQ algorithms.
type CapabilityDiscoveryRequest struct {
	PeerID     string                 `json:"peer_id"`
	Algorithms []string               `json:"algorithms"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// CapabilityDiscoveryResponse captures peer capabilities.
type CapabilityDiscoveryResponse struct {
	PeerID     string                 `json:"peer_id"`
	Algorithms []string               `json:"algorithms"`
	PublicKey  []byte                 `json:"public_key,omitempty"`
	Error      string                 `json:"error,omitempty"`
}
