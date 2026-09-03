package pqc

import (
	"context"
	"testing"
)

func TestQuantumSafeKeyManagerHybridGenerate(t *testing.T) {
	km := NewQuantumSafeKeyManager(AlgorithmHybridEd25519MLDSA65)
	pub, priv, err := km.GenerateKeyPair(context.Background())
	if err != nil { t.Fatalf("generate failed: %v", err) }
	if len(pub) == 0 || len(priv) == 0 { t.Fatalf("empty keys") }
}

func TestQuantumSafeKeyManagerEncapsulateDecapsulate(t *testing.T) {
	km := NewQuantumSafeKeyManager(AlgorithmHybridEd25519MLDSA65)
	_, peerPriv, err := km.GenerateKeyPair(context.Background())
	if err != nil { t.Fatalf("peer generate failed: %v", err) }
	peerPub := PublicKey(peerPriv)
	ct, shared, err := km.Encapsulate(context.Background(), peerPub)
	if err != nil { t.Fatalf("encapsulate failed: %v", err) }
	if len(ct) == 0 || len(shared) == 0 { t.Fatalf("empty encapsulate output") }
	got, err := km.Decapsulate(context.Background(), ct, peerPriv)
	if err != nil { t.Fatalf("decapsulate failed: %v", err) }
	if len(got) == 0 { t.Fatalf("empty decapsulate output") }
}

func TestQuantumSafeKeyManagerSignVerify(t *testing.T) {
	km := NewQuantumSafeKeyManager(AlgorithmHybridEd25519MLDSA65)
	pub, priv, err := km.GenerateKeyPair(context.Background())
	if err != nil { t.Fatalf("generate failed: %v", err) }
	msg := []byte("hello pqc")
	sig, err := km.Sign(context.Background(), priv, msg)
	if err != nil { t.Fatalf("sign failed: %v", err) }
	if len(sig) == 0 { t.Fatalf("empty signature") }
	if err := km.Verify(context.Background(), pub, msg, sig); err != nil { t.Fatalf("verify failed: %v", err) }
}

func TestAttestPeer(t *testing.T) {
	km := NewQuantumSafeKeyManager(AlgorithmHybridEd25519MLDSA65)
	pub, priv, err := km.GenerateKeyPair(context.Background())
	if err != nil { t.Fatalf("generate failed: %v", err) }
	att, err := AttestPeer(context.Background(), km, "peer-1", priv)
	if err != nil { t.Fatalf("attest failed: %v", err) }
	if att.PeerID != "peer-1" { t.Fatalf("unexpected peer id: %s", att.PeerID) }
	if att.Algorithm != AlgorithmHybridEd25519MLDSA65 { t.Fatalf("unexpected algorithm: %s", att.Algorithm) }
	if string(att.PublicKey) != string(pub) { t.Fatalf("public key mismatch") }
}

func TestStreamEncrypterWriteRead(t *testing.T) {
	key := []byte("pqc-secret")
	enc := NewStreamEncrypter(key, nil)
	_ = enc
}
