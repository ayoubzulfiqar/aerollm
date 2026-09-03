package federated

import (
	"context"
	"crypto/ed25519"
	"fmt"
)

// FedAvgAggregatorWithVerify implements Federated Aggregator with real signature verification.
type FedAvgAggregatorWithVerify struct {
	FedAvgAggregator
	signingKey ed25519.PrivateKey
}

// NewFedAvgAggregatorWithVerify creates a new aggregator with signing key.
func NewFedAvgAggregatorWithVerify(signingKey ed25519.PrivateKey) *FedAvgAggregatorWithVerify {
	return &FedAvgAggregatorWithVerify{signingKey: signingKey}
}

// Verify verifies a federated update signature using the stored public key.
func (a *FedAvgAggregatorWithVerify) Verify(_ context.Context, update *LoRAMatrix, signature []byte) error {
	if a.signingKey == nil || len(a.signingKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("federated: missing signing key")
	}
	if update == nil {
		return fmt.Errorf("federated: missing update")
	}
	if len(signature) == 0 {
		return fmt.Errorf("federated: missing signature")
	}
	payload := []byte(fmt.Sprintf("%s:%d:%s", update.Owner, update.Rows, update.Checksum()))
	pub := ed25519.PrivateKey(a.signingKey).Public().(ed25519.PublicKey)
	if !ed25519.Verify(pub, payload, signature) {
		return fmt.Errorf("federated: invalid signature")
	}
	return nil
}
