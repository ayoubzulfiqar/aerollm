package federated

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/bits"
)

// Limits applied to every aggregation input. They bound the memory and CPU an
// untrusted caller (e.g. the HTTP aggregate endpoint) can make the gateway
// spend on a single request.
const (
	// MaxUpdates is the maximum number of updates accepted by one aggregation.
	MaxUpdates = 1024
	// MaxMatrixElements is the maximum number of elements (Rows*Cols) in a
	// single LoRA matrix.
	MaxMatrixElements = 1 << 20
)

var (
	// ErrNoUpdates is returned when an aggregation receives no usable update.
	ErrNoUpdates = errors.New("federated: no updates")
	// ErrTooManyUpdates is returned when more than MaxUpdates are submitted.
	ErrTooManyUpdates = errors.New("federated: too many updates")
	// ErrInvalidMatrix is returned for malformed matrices (bad shape, size or
	// non-finite values).
	ErrInvalidMatrix = errors.New("federated: invalid matrix")
	// ErrDimensionMismatch is returned when updates have different shapes.
	ErrDimensionMismatch = errors.New("federated: dimension mismatch between updates")
	// ErrInvalidWeights is returned for malformed aggregation weights.
	ErrInvalidWeights = errors.New("federated: invalid weights")
	// ErrVerificationNotConfigured is returned by aggregators that have no
	// verification key material. Verification fails closed.
	ErrVerificationNotConfigured = errors.New("federated: signature verification not configured")
)

// LoRAMatrix represents a simplified LoRA weight matrix.
type LoRAMatrix struct {
	Rows  int
	Cols  int
	Data  []float64
	Owner string
}

// Clone returns a deep copy. A nil receiver returns nil.
func (m *LoRAMatrix) Clone() *LoRAMatrix {
	if m == nil {
		return nil
	}
	out := &LoRAMatrix{Rows: m.Rows, Cols: m.Cols, Owner: m.Owner}
	out.Data = make([]float64, len(m.Data))
	copy(out.Data, m.Data)
	return out
}

// Checksum returns a sha256 checksum of the matrix data. fmt.Sprint formats
// float64 values with the shortest representation that round-trips, so the
// checksum is exact. A nil receiver hashes an empty data slice.
func (m *LoRAMatrix) Checksum() string {
	var data []float64
	if m != nil {
		data = m.Data
	}
	sum := sha256.Sum256([]byte(fmt.Sprint(data)))
	return hex.EncodeToString(sum[:])
}

// Validate checks that the matrix is well formed: positive dimensions,
// Rows*Cols == len(Data) (overflow-safe), at most MaxMatrixElements elements
// and only finite values.
func (m *LoRAMatrix) Validate() error {
	if m == nil {
		return fmt.Errorf("%w: nil matrix", ErrInvalidMatrix)
	}
	if m.Rows <= 0 || m.Cols <= 0 {
		return fmt.Errorf("%w: rows and cols must be positive (got %dx%d)", ErrInvalidMatrix, m.Rows, m.Cols)
	}
	hi, lo := bits.Mul64(uint64(m.Rows), uint64(m.Cols))
	if hi != 0 || lo > MaxMatrixElements {
		return fmt.Errorf("%w: %dx%d exceeds %d elements", ErrInvalidMatrix, m.Rows, m.Cols, MaxMatrixElements)
	}
	if uint64(len(m.Data)) != lo {
		return fmt.Errorf("%w: data length %d does not match %dx%d", ErrInvalidMatrix, len(m.Data), m.Rows, m.Cols)
	}
	for i, v := range m.Data {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("%w: non-finite value at index %d", ErrInvalidMatrix, i)
		}
	}
	return nil
}

// FederatedAggregator defines the contract for secure aggregation.
type FederatedAggregator interface {
	Aggregate(ctx context.Context, updates []*LoRAMatrix) (*LoRAMatrix, error)
	Verify(ctx context.Context, update *LoRAMatrix, signature []byte) error
}

// FedAvgAggregator implements Federated Averaging. It is stateless and safe
// for concurrent use. It has no key material, so Verify always fails with
// ErrVerificationNotConfigured; use NewFedAvgAggregatorWithPublicKeys or
// NewFedAvgAggregatorWithRegistry for authenticated aggregation.
type FedAvgAggregator struct{}

// NewFedAvgAggregator creates a new aggregator.
func NewFedAvgAggregator() *FedAvgAggregator {
	return &FedAvgAggregator{}
}

// Aggregate performs unweighted FedAvg over the provided matrices. Nil entries
// are skipped; every other entry must be valid and share the same shape.
func (a *FedAvgAggregator) Aggregate(ctx context.Context, updates []*LoRAMatrix) (*LoRAMatrix, error) {
	weights := make([]float64, len(updates))
	for i := range weights {
		weights[i] = 1
	}
	return aggregateWeighted(ctx, updates, weights)
}

// AggregateWeighted performs weighted FedAvg: out = sum_i (w_i/W) * x_i where
// W is the sum of the weights of the non-nil updates. Weights must be finite,
// non-negative and not all zero; len(weights) must equal len(updates).
func (a *FedAvgAggregator) AggregateWeighted(ctx context.Context, updates []*LoRAMatrix, weights []float64) (*LoRAMatrix, error) {
	return aggregateWeighted(ctx, updates, weights)
}

// Verify always fails: the plain FedAvg aggregator has no verification keys.
// Previously this returned nil, which made verification trivially bypassable.
func (a *FedAvgAggregator) Verify(ctx context.Context, update *LoRAMatrix, signature []byte) error {
	_ = ctx
	_ = update
	_ = signature
	return ErrVerificationNotConfigured
}

// validateUpdates checks count, per-matrix validity and shape agreement. It
// returns the reference (first non-nil) matrix.
func validateUpdates(ctx context.Context, updates []*LoRAMatrix) (*LoRAMatrix, error) {
	if len(updates) == 0 {
		return nil, ErrNoUpdates
	}
	if len(updates) > MaxUpdates {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyUpdates, len(updates), MaxUpdates)
	}
	var ref *LoRAMatrix
	for i, u := range updates {
		if u == nil {
			continue
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if err := u.Validate(); err != nil {
			return nil, fmt.Errorf("update %d: %w", i, err)
		}
		if ref == nil {
			ref = u
			continue
		}
		if u.Rows != ref.Rows || u.Cols != ref.Cols {
			return nil, fmt.Errorf("%w: update %d is %dx%d, expected %dx%d", ErrDimensionMismatch, i, u.Rows, u.Cols, ref.Rows, ref.Cols)
		}
	}
	if ref == nil {
		return nil, fmt.Errorf("%w: no valid updates", ErrNoUpdates)
	}
	return ref, nil
}

func aggregateWeighted(ctx context.Context, updates []*LoRAMatrix, weights []float64) (*LoRAMatrix, error) {
	ref, err := validateUpdates(ctx, updates)
	if err != nil {
		return nil, err
	}
	if len(weights) != len(updates) {
		return nil, fmt.Errorf("%w: got %d weights for %d updates", ErrInvalidWeights, len(weights), len(updates))
	}
	var total float64
	for i, w := range weights {
		if math.IsNaN(w) || math.IsInf(w, 0) || w < 0 {
			return nil, fmt.Errorf("%w: weight %d must be finite and non-negative", ErrInvalidWeights, i)
		}
		if updates[i] != nil {
			total += w
		}
	}
	if total <= 0 || math.IsInf(total, 0) {
		return nil, fmt.Errorf("%w: weights of non-nil updates must sum to a positive finite value", ErrInvalidWeights)
	}

	out := &LoRAMatrix{Rows: ref.Rows, Cols: ref.Cols, Data: make([]float64, len(ref.Data))}
	for i, u := range updates {
		if u == nil || weights[i] == 0 {
			continue
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		// Dividing first keeps every partial sum bounded by max|x|, so
		// finite inputs cannot overflow.
		f := weights[i] / total
		for j, v := range u.Data {
			out.Data[j] += f * v
		}
	}
	for j, v := range out.Data {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("%w: non-finite aggregate at index %d", ErrInvalidMatrix, j)
		}
	}
	return out, nil
}
