package pqc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

var hybridAlgs = []string{AlgorithmHybridEd25519MLDSA65, AlgorithmHybridMLKEM768X25519Ed25519}

func TestQuantumSafeKeyManagerHybridGenerate(t *testing.T) {
	for _, alg := range hybridAlgs {
		km := NewQuantumSafeKeyManager(alg)
		pub, priv, err := km.GenerateKeyPair(context.Background())
		if err != nil {
			t.Fatalf("%s: generate failed: %v", alg, err)
		}
		if len(pub) == 0 || len(priv) == 0 || bytes.Equal(pub, priv) {
			t.Fatalf("%s: bad keys", alg)
		}
		derived, err := km.PublicKeyFromPrivate(priv)
		if err != nil || !bytes.Equal(derived, pub) {
			t.Fatalf("%s: derived public key mismatch: %v", alg, err)
		}
	}
}

func TestEncapsulateDecapsulateRoundTrip(t *testing.T) {
	for _, alg := range append(hybridAlgs, AlgorithmPQCMLKEM768) {
		km := NewQuantumSafeKeyManager(alg)
		ctx := context.Background()
		peerPub, peerPriv, err := km.GenerateKeyPair(ctx)
		if err != nil {
			t.Fatalf("%s: %v", alg, err)
		}
		ct, shared, err := km.Encapsulate(ctx, peerPub)
		if err != nil {
			t.Fatalf("%s: encapsulate: %v", alg, err)
		}
		if len(shared) != 32 || len(ct) == 0 {
			t.Fatalf("%s: unexpected sizes ct=%d ss=%d", alg, len(ct), len(shared))
		}
		got, err := km.Decapsulate(ctx, ct, peerPriv)
		if err != nil {
			t.Fatalf("%s: decapsulate: %v", alg, err)
		}
		if !bytes.Equal(got, shared) {
			t.Fatalf("%s: shared secrets differ (fake KEM?)", alg)
		}
		// A second encapsulation yields a different secret.
		_, shared2, _ := km.Encapsulate(ctx, peerPub)
		if bytes.Equal(shared, shared2) {
			t.Fatalf("%s: encapsulation not randomized", alg)
		}
		// Wrong private key does not recover the secret.
		_, otherPriv, _ := km.GenerateKeyPair(ctx)
		if wrong, err := km.Decapsulate(ctx, ct, otherPriv); err == nil && bytes.Equal(wrong, shared) {
			t.Fatalf("%s: wrong key recovered the secret", alg)
		}
		// Tampered ciphertext does not recover the secret.
		bad := append([]byte(nil), ct...)
		bad[len(bad)/2] ^= 0xff
		if tampered, err := km.Decapsulate(ctx, bad, peerPriv); err == nil && bytes.Equal(tampered, shared) {
			t.Fatalf("%s: tampered ciphertext accepted", alg)
		}
		if _, _, err := km.Encapsulate(ctx, []byte("short")); err == nil {
			t.Fatalf("%s: invalid public key accepted", alg)
		}
	}
}

func TestQuantumSafeKeyManagerSignVerify(t *testing.T) {
	km := NewQuantumSafeKeyManager(AlgorithmHybridEd25519MLDSA65)
	ctx := context.Background()
	pub, priv, err := km.GenerateKeyPair(ctx)
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	msg := []byte("hello pqc")
	sig, err := km.Sign(ctx, priv, msg)
	if err != nil || len(sig) == 0 {
		t.Fatalf("sign failed: %v", err)
	}
	if err := km.Verify(ctx, pub, msg, sig); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
	if err := km.Verify(ctx, pub, []byte("tampered"), sig); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("expected bad signature, got %v", err)
	}
}

func TestHonestAlgorithmReporting(t *testing.T) {
	ctx := context.Background()
	mldsa := NewQuantumSafeKeyManager(AlgorithmPQCMLDSA65)
	if _, _, err := mldsa.GenerateKeyPair(ctx); !errors.Is(err, ErrMLDSAUnavailable) {
		t.Fatalf("ML-DSA must fail honestly, got %v", err)
	}
	if _, err := mldsa.Sign(ctx, make([]byte, 64), []byte("m")); !errors.Is(err, ErrMLDSAUnavailable) {
		t.Fatalf("ML-DSA sign must fail, got %v", err)
	}
	suite, err := NewQuantumSafeKeyManager(AlgorithmHybridEd25519MLDSA65).Suite()
	if err != nil || suite.PostQuantumSignature || !suite.PostQuantumKEM || suite.Signature != SigNameEd25519 {
		t.Fatalf("hybrid suite must report Ed25519 (non-PQ) signatures: %+v %v", suite, err)
	}
	kem := NewQuantumSafeKeyManager(AlgorithmPQCMLKEM768)
	_, priv, _ := kem.GenerateKeyPair(ctx)
	if _, err := kem.Sign(ctx, priv, []byte("m")); !errors.Is(err, ErrSignatureUnsupported) {
		t.Fatalf("mlkem suite cannot sign, got %v", err)
	}
	if _, _, err := NewQuantumSafeKeyManager("rot13").GenerateKeyPair(ctx); !errors.Is(err, ErrUnsupportedAlgorithm) {
		t.Fatalf("unknown algorithm: %v", err)
	}
}

func TestAttestPeer(t *testing.T) {
	km := NewQuantumSafeKeyManager(AlgorithmHybridEd25519MLDSA65)
	ctx := context.Background()
	pub, priv, err := km.GenerateKeyPair(ctx)
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	att, err := AttestPeer(ctx, km, "peer-1", priv)
	if err != nil {
		t.Fatalf("attest failed: %v", err)
	}
	if att.PeerID != "peer-1" || att.Algorithm != AlgorithmHybridEd25519MLDSA65 {
		t.Fatalf("unexpected attestation: %+v", att)
	}
	if !bytes.Equal(att.PublicKey, pub) {
		t.Fatalf("public key mismatch")
	}
	if len(att.Signature) == 0 {
		t.Fatal("attestation must carry a signature")
	}
	if err := VerifyAttestation(ctx, km, att, time.Minute); err != nil {
		t.Fatalf("verify attestation: %v", err)
	}
	forged := *att
	forged.PeerID = "peer-2"
	if err := VerifyAttestation(ctx, km, &forged, 0); err == nil {
		t.Fatal("forged peer id must fail")
	}
	otherPub, _, _ := km.GenerateKeyPair(ctx)
	forged = *att
	forged.PublicKey = otherPub
	if err := VerifyAttestation(ctx, km, &forged, 0); err == nil {
		t.Fatal("swapped public key must fail")
	}
}
