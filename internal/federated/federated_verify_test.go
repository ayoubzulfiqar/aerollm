package federated

import (
	"crypto/ed25519"
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
}
