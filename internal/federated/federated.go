package federated

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
)

// LoRAMatrix represents a simplified LoRA weight matrix.
type LoRAMatrix struct {
	Rows  int
	Cols  int
	Data  []float64
	Owner string
}

// Clone returns a deep copy.
func (m *LoRAMatrix) Clone() *LoRAMatrix {
	out := &LoRAMatrix{Rows: m.Rows, Cols: m.Cols, Owner: m.Owner}
	out.Data = make([]float64, len(m.Data))
	copy(out.Data, m.Data)
	return out
}

// Checksum returns a sha256 checksum.
func (m *LoRAMatrix) Checksum() string {
	sum := sha256.Sum256([]byte(fmt.Sprint(m.Data)))
	return hex.EncodeToString(sum[:])
}

// FederatedAggregator defines the contract for secure aggregation.
type FederatedAggregator interface {
	Aggregate(ctx context.Context, updates []*LoRAMatrix) (*LoRAMatrix, error)
	Verify(ctx context.Context, update *LoRAMatrix, signature []byte) error
}

// FedAvgAggregator implements Federated Averaging.
type FedAvgAggregator struct {
	mu sync.Mutex
}

// NewFedAvgAggregator creates a new aggregator.
func NewFedAvgAggregator() *FedAvgAggregator {
	return &FedAvgAggregator{}
}

// Aggregate performs FedAvg over the provided matrices.
func (a *FedAvgAggregator) Aggregate(ctx context.Context, updates []*LoRAMatrix) (*LoRAMatrix, error) {
	_ = ctx
	if len(updates) == 0 {
		return nil, fmt.Errorf("no updates")
	}
	ref := updates[0]
	if ref == nil || len(ref.Data) == 0 {
		return nil, fmt.Errorf("invalid reference matrix")
	}
	out := &LoRAMatrix{Rows: ref.Rows, Cols: ref.Cols, Data: make([]float64, len(ref.Data))}
	count := 0
	for _, u := range updates {
		if u == nil || len(u.Data) != len(out.Data) {
			continue
		}
		for i := range out.Data {
			out.Data[i] += u.Data[i]
		}
		count++
	}
	if count == 0 {
		return nil, fmt.Errorf("no valid updates")
	}
	for i := range out.Data {
		out.Data[i] /= float64(count)
	}
	// clamp to safe bounds
	for i := range out.Data {
		if math.IsInf(out.Data[i], 0) || math.IsNaN(out.Data[i]) {
			out.Data[i] = 0
		}
	}
	return out, nil
}

// Verify is a stub for signature verification.
func (a *FedAvgAggregator) Verify(ctx context.Context, update *LoRAMatrix, signature []byte) error {
	_ = ctx
	_ = update
	_ = signature
	return nil
}
