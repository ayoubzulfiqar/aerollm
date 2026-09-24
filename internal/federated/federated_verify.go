package federated

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
)

var (
	// ErrInvalidSignature is returned when a signature does not verify.
	ErrInvalidSignature = errors.New("federated: invalid signature")
	// ErrUnknownOwner is returned when no public key is known for an update's owner.
	ErrUnknownOwner = errors.New("federated: unknown update owner")
)

// PublicKeyResolver resolves the ed25519 public key of an update owner.
type PublicKeyResolver interface {
	PublicKey(owner string) (ed25519.PublicKey, bool)
}

// FedAvgAggregatorWithVerify implements FedAvg with ed25519 signature
// verification of individual updates.
//
// Keys are resolved in this order:
//  1. a PublicKeyResolver (per-owner public keys, e.g. the GatewayRegistry),
//  2. the legacy shared signing key passed to NewFedAvgAggregatorWithVerify.
//
// The legacy mode requires every signer to hold the same private key and
// therefore cannot distinguish nodes; prefer NewFedAvgAggregatorWithPublicKeys
// or NewFedAvgAggregatorWithRegistry.
//
// Note: the embedded Aggregate method does NOT verify anything; use
// AggregateVerified to aggregate only authenticated updates.
type FedAvgAggregatorWithVerify struct {
	FedAvgAggregator
	signingKey ed25519.PrivateKey
	resolver   PublicKeyResolver
}

// NewFedAvgAggregatorWithVerify creates an aggregator that verifies updates
// against the public half of a shared signing key (legacy mode).
func NewFedAvgAggregatorWithVerify(signingKey ed25519.PrivateKey) *FedAvgAggregatorWithVerify {
	return &FedAvgAggregatorWithVerify{signingKey: signingKey}
}

// NewFedAvgAggregatorWithPublicKeys creates an aggregator that verifies each
// update with the public key registered for its Owner. Every key must be a
// valid-length ed25519 public key and every owner must be non-empty.
func NewFedAvgAggregatorWithPublicKeys(keys map[string]ed25519.PublicKey) (*FedAvgAggregatorWithVerify, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("federated: no public keys provided")
	}
	copied := make(staticKeys, len(keys))
	for owner, k := range keys {
		if owner == "" {
			return nil, fmt.Errorf("federated: empty owner in key set")
		}
		if len(k) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("federated: public key for %q has invalid length %d", owner, len(k))
		}
		copied[owner] = append(ed25519.PublicKey(nil), k...)
	}
	return &FedAvgAggregatorWithVerify{resolver: copied}, nil
}

// NewFedAvgAggregatorWithRegistry creates an aggregator that resolves each
// update's Owner as a NodeID in the registry and verifies with that node's
// registered ed25519 public key.
func NewFedAvgAggregatorWithRegistry(registry *GatewayRegistry) *FedAvgAggregatorWithVerify {
	if registry == nil {
		return &FedAvgAggregatorWithVerify{}
	}
	return &FedAvgAggregatorWithVerify{resolver: registry}
}

type staticKeys map[string]ed25519.PublicKey

func (s staticKeys) PublicKey(owner string) (ed25519.PublicKey, bool) {
	k, ok := s[owner]
	return k, ok
}

// SignaturePayload returns the canonical bytes that an update owner signs:
// "Owner:Rows:Checksum()". Cols is bound implicitly because verification also
// requires Rows*Cols == len(Data).
func SignaturePayload(update *LoRAMatrix) []byte {
	if update == nil {
		return nil
	}
	return []byte(fmt.Sprintf("%s:%d:%s", update.Owner, update.Rows, update.Checksum()))
}

// SignUpdate signs an update with the owner's private key.
func SignUpdate(priv ed25519.PrivateKey, update *LoRAMatrix) ([]byte, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("federated: invalid private key")
	}
	if err := validateSignable(update); err != nil {
		return nil, err
	}
	return ed25519.Sign(priv, SignaturePayload(update)), nil
}

func validateSignable(update *LoRAMatrix) error {
	if update == nil {
		return fmt.Errorf("federated: missing update")
	}
	if update.Owner == "" {
		return fmt.Errorf("%w: missing owner", ErrInvalidMatrix)
	}
	return update.Validate()
}

func (a *FedAvgAggregatorWithVerify) publicKeyFor(owner string) (ed25519.PublicKey, error) {
	if a.resolver != nil {
		k, ok := a.resolver.PublicKey(owner)
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownOwner, owner)
		}
		if len(k) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("%w: key for %q is not an ed25519 public key", ErrUnknownOwner, owner)
		}
		return k, nil
	}
	if len(a.signingKey) == ed25519.PrivateKeySize {
		return a.signingKey.Public().(ed25519.PublicKey), nil
	}
	return nil, ErrVerificationNotConfigured
}

// Verify verifies a federated update signature. The update must be a valid
// matrix with a non-empty Owner, and the signature must be a valid ed25519
// signature over SignaturePayload(update) by the owner's key.
func (a *FedAvgAggregatorWithVerify) Verify(_ context.Context, update *LoRAMatrix, signature []byte) error {
	if a == nil {
		return ErrVerificationNotConfigured
	}
	if a.resolver == nil && len(a.signingKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("%w: missing signing key", ErrVerificationNotConfigured)
	}
	if err := validateSignable(update); err != nil {
		return err
	}
	if len(signature) == 0 {
		return fmt.Errorf("federated: missing signature")
	}
	if len(signature) != ed25519.SignatureSize {
		return ErrInvalidSignature
	}
	pub, err := a.publicKeyFor(update.Owner)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, SignaturePayload(update), signature) {
		return ErrInvalidSignature
	}
	return nil
}

// AggregateVerified verifies every update (signatures[i] belongs to
// updates[i]) and aggregates them with equal weights. It fails closed: any nil
// update, missing or invalid signature, or duplicate owner aborts the whole
// aggregation.
func (a *FedAvgAggregatorWithVerify) AggregateVerified(ctx context.Context, updates []*LoRAMatrix, signatures [][]byte) (*LoRAMatrix, error) {
	if a == nil {
		return nil, ErrVerificationNotConfigured
	}
	if len(updates) == 0 {
		return nil, ErrNoUpdates
	}
	if len(updates) > MaxUpdates {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyUpdates, len(updates), MaxUpdates)
	}
	if len(signatures) != len(updates) {
		return nil, fmt.Errorf("federated: got %d signatures for %d updates", len(signatures), len(updates))
	}
	seen := make(map[string]struct{}, len(updates))
	for i, u := range updates {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if u == nil {
			return nil, fmt.Errorf("update %d: %w: nil matrix", i, ErrInvalidMatrix)
		}
		if _, dup := seen[u.Owner]; dup {
			return nil, fmt.Errorf("update %d: duplicate owner %q", i, u.Owner)
		}
		seen[u.Owner] = struct{}{}
		if err := a.Verify(ctx, u, signatures[i]); err != nil {
			return nil, fmt.Errorf("update %d: %w", i, err)
		}
	}
	return a.Aggregate(ctx, updates)
}
