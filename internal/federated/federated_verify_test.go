package federated

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFedAvgAggregatorWithVerifyRejectsBadSignature(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	agg := NewFedAvgAggregatorWithVerify(priv)
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "e1"}
	err = agg.Verify(nil, m, []byte("bad"))
	require.Error(t, err)
}

func TestFedAvgAggregatorWithVerifyAcceptsValidSignature(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	agg := NewFedAvgAggregatorWithVerify(priv)
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "e1"}
	payload := []byte(m.Owner + ":" + "1" + ":" + m.Checksum())
	sig := ed25519.Sign(priv, payload)
	err = agg.Verify(nil, m, sig)
	require.NoError(t, err)
	// SignaturePayload matches the legacy format.
	require.Equal(t, payload, SignaturePayload(m))
}

func TestFedAvgAggregatorWithVerifyMissingKey(t *testing.T) {
	agg := NewFedAvgAggregatorWithVerify(nil)
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "e1"}
	err := agg.Verify(context.Background(), m, make([]byte, ed25519.SignatureSize))
	require.ErrorIs(t, err, ErrVerificationNotConfigured)
}

func TestFedAvgAggregatorWithVerifyValidatesMatrix(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	agg := NewFedAvgAggregatorWithVerify(priv)

	// A correctly signed payload does not help if the matrix itself is
	// malformed (Cols is bound through Rows*Cols == len(Data)).
	bad := &LoRAMatrix{Rows: 1, Cols: 5, Data: []float64{1}, Owner: "e1"}
	sig := ed25519.Sign(priv, SignaturePayload(bad))
	require.ErrorIs(t, agg.Verify(nil, bad, sig), ErrInvalidMatrix)

	nan := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{math.NaN()}, Owner: "e1"}
	sig = ed25519.Sign(priv, SignaturePayload(nan))
	require.ErrorIs(t, agg.Verify(nil, nan, sig), ErrInvalidMatrix)

	noOwner := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}}
	sig = ed25519.Sign(priv, SignaturePayload(noOwner))
	require.ErrorIs(t, agg.Verify(nil, noOwner, sig), ErrInvalidMatrix)

	require.Error(t, agg.Verify(nil, nil, sig))
}

func TestFedAvgAggregatorWithPublicKeys(t *testing.T) {
	pub1, priv1, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pub2, priv2, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	agg, err := NewFedAvgAggregatorWithPublicKeys(map[string]ed25519.PublicKey{"e1": pub1, "e2": pub2})
	require.NoError(t, err)

	m1 := &LoRAMatrix{Rows: 1, Cols: 2, Data: []float64{1, 2}, Owner: "e1"}
	sig1, err := SignUpdate(priv1, m1)
	require.NoError(t, err)
	require.NoError(t, agg.Verify(context.Background(), m1, sig1))

	// e2 cannot sign on behalf of e1.
	forged, err := SignUpdate(priv2, m1)
	require.NoError(t, err)
	require.ErrorIs(t, agg.Verify(context.Background(), m1, forged), ErrInvalidSignature)

	// Unknown owner fails closed.
	m3 := &LoRAMatrix{Rows: 1, Cols: 2, Data: []float64{1, 2}, Owner: "e3"}
	sig3, err := SignUpdate(priv1, m3)
	require.NoError(t, err)
	require.ErrorIs(t, agg.Verify(context.Background(), m3, sig3), ErrUnknownOwner)

	// Tampering with the data invalidates the signature.
	tampered := m1.Clone()
	tampered.Data[0] = 1000
	require.ErrorIs(t, agg.Verify(context.Background(), tampered, sig1), ErrInvalidSignature)
}

func TestNewFedAvgAggregatorWithPublicKeysValidation(t *testing.T) {
	_, err := NewFedAvgAggregatorWithPublicKeys(nil)
	require.Error(t, err)
	_, err = NewFedAvgAggregatorWithPublicKeys(map[string]ed25519.PublicKey{"e1": []byte("short")})
	require.Error(t, err)
	pub, _, _ := ed25519.GenerateKey(nil)
	_, err = NewFedAvgAggregatorWithPublicKeys(map[string]ed25519.PublicKey{"": pub})
	require.Error(t, err)
}

func TestAggregateVerified(t *testing.T) {
	pub1, priv1, _ := ed25519.GenerateKey(nil)
	pub2, priv2, _ := ed25519.GenerateKey(nil)
	agg, err := NewFedAvgAggregatorWithPublicKeys(map[string]ed25519.PublicKey{"e1": pub1, "e2": pub2})
	require.NoError(t, err)

	m1 := &LoRAMatrix{Rows: 1, Cols: 2, Data: []float64{1, 2}, Owner: "e1"}
	m2 := &LoRAMatrix{Rows: 1, Cols: 2, Data: []float64{3, 4}, Owner: "e2"}
	s1, _ := SignUpdate(priv1, m1)
	s2, _ := SignUpdate(priv2, m2)

	out, err := agg.AggregateVerified(context.Background(), []*LoRAMatrix{m1, m2}, [][]byte{s1, s2})
	require.NoError(t, err)
	require.Equal(t, []float64{2, 3}, out.Data)

	// One bad signature aborts everything.
	_, err = agg.AggregateVerified(context.Background(), []*LoRAMatrix{m1, m2}, [][]byte{s1, s1})
	require.ErrorIs(t, err, ErrInvalidSignature)

	// Signature count mismatch.
	_, err = agg.AggregateVerified(context.Background(), []*LoRAMatrix{m1, m2}, [][]byte{s1})
	require.Error(t, err)

	// Nil update is rejected rather than skipped.
	_, err = agg.AggregateVerified(context.Background(), []*LoRAMatrix{m1, nil}, [][]byte{s1, nil})
	require.ErrorIs(t, err, ErrInvalidMatrix)

	// The same owner cannot contribute twice.
	_, err = agg.AggregateVerified(context.Background(), []*LoRAMatrix{m1, m1}, [][]byte{s1, s1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate owner")
}

func TestAggregatorWithRegistry(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	reg := NewGatewayRegistry()
	require.NoError(t, reg.Register(context.Background(), &NodeRegistration{NodeID: "node-1", PublicKey: pub}))
	require.NoError(t, reg.Register(context.Background(), &NodeRegistration{NodeID: "keyless"}))

	agg := NewFedAvgAggregatorWithRegistry(reg)
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{7}, Owner: "node-1"}
	sig, _ := SignUpdate(priv, m)
	require.NoError(t, agg.Verify(context.Background(), m, sig))

	// A node registered without a key cannot have its updates verified.
	k := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{7}, Owner: "keyless"}
	ksig, _ := SignUpdate(priv, k)
	require.ErrorIs(t, agg.Verify(context.Background(), k, ksig), ErrUnknownOwner)

	// Nil registry fails closed.
	require.True(t, errors.Is(NewFedAvgAggregatorWithRegistry(nil).Verify(nil, m, sig), ErrVerificationNotConfigured))
}

func TestSignUpdateValidation(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	_, err := SignUpdate(priv, nil)
	require.Error(t, err)
	_, err = SignUpdate(nil, &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "o"})
	require.Error(t, err)
	_, err = SignUpdate(priv, &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: fmt.Sprint("o")})
	require.NoError(t, err)
}
