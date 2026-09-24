package federated

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestFedAvgAggregate(t *testing.T) {
	a := NewFedAvgAggregator()
	m1 := &LoRAMatrix{Rows: 2, Cols: 2, Data: []float64{1, 2, 3, 4}, Owner: "edge1"}
	m2 := &LoRAMatrix{Rows: 2, Cols: 2, Data: []float64{3, 4, 5, 6}, Owner: "edge2"}
	out, err := a.Aggregate(context.Background(), []*LoRAMatrix{m1, m2})
	if err != nil {
		t.Fatalf("aggregate failed: %v", err)
	}
	if len(out.Data) != 4 {
		t.Fatalf("unexpected output size")
	}
	if out.Data[0] != 2 || out.Data[1] != 3 || out.Data[2] != 4 || out.Data[3] != 5 {
		t.Fatalf("unexpected avg: %v", out.Data)
	}
	if out.Rows != 2 || out.Cols != 2 {
		t.Fatalf("unexpected shape %dx%d", out.Rows, out.Cols)
	}
}

func TestFedAvgAggregateSkipsNil(t *testing.T) {
	a := NewFedAvgAggregator()
	valid := &LoRAMatrix{Rows: 2, Cols: 2, Data: []float64{1, 2, 3, 4}, Owner: "edge1"}
	out, err := a.Aggregate(context.Background(), []*LoRAMatrix{nil, valid, nil})
	if err != nil {
		t.Fatalf("aggregate failed: %v", err)
	}
	if len(out.Data) != 4 || out.Data[3] != 4 {
		t.Fatalf("unexpected output: %v", out.Data)
	}
}

func TestFedAvgAggregateEmpty(t *testing.T) {
	a := NewFedAvgAggregator()
	if _, err := a.Aggregate(context.Background(), nil); !errors.Is(err, ErrNoUpdates) {
		t.Fatalf("expected ErrNoUpdates, got %v", err)
	}
	if _, err := a.Aggregate(context.Background(), []*LoRAMatrix{nil, nil}); !errors.Is(err, ErrNoUpdates) {
		t.Fatalf("expected ErrNoUpdates for all-nil input, got %v", err)
	}
}

func TestFedAvgAggregateNilContext(t *testing.T) {
	a := NewFedAvgAggregator()
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{3}}
	//nolint:staticcheck // nil ctx is used by the CLI and must not panic.
	out, err := a.Aggregate(nil, []*LoRAMatrix{m})
	if err != nil || out.Data[0] != 3 {
		t.Fatalf("unexpected result %v, %v", out, err)
	}
}

// A wrong-shaped first update used to become the reference and cause every
// legitimate update to be silently skipped.
func TestFedAvgAggregateRejectsDimensionMismatch(t *testing.T) {
	a := NewFedAvgAggregator()
	attacker := &LoRAMatrix{Rows: 1, Cols: 2, Data: []float64{1000, 1000}, Owner: "evil"}
	legit := &LoRAMatrix{Rows: 2, Cols: 2, Data: []float64{1, 2, 3, 4}, Owner: "edge1"}
	_, err := a.Aggregate(context.Background(), []*LoRAMatrix{attacker, legit})
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("expected ErrDimensionMismatch, got %v", err)
	}
}

func TestFedAvgAggregateRejectsInvalidShapes(t *testing.T) {
	a := NewFedAvgAggregator()
	cases := map[string]*LoRAMatrix{
		"zero rows":        {Rows: 0, Cols: 1, Data: []float64{}},
		"negative cols":    {Rows: 1, Cols: -1, Data: []float64{1}},
		"length mismatch":  {Rows: 2, Cols: 2, Data: []float64{1, 2, 3}},
		"empty data":       {Rows: 1, Cols: 1},
		"overflowing dims": {Rows: math.MaxInt, Cols: math.MaxInt, Data: []float64{1}},
		"too many elems":   {Rows: MaxMatrixElements + 1, Cols: 1, Data: []float64{1}},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := a.Aggregate(context.Background(), []*LoRAMatrix{m}); !errors.Is(err, ErrInvalidMatrix) {
				t.Fatalf("expected ErrInvalidMatrix, got %v", err)
			}
		})
	}
}

func TestFedAvgAggregateRejectsNonFinite(t *testing.T) {
	a := NewFedAvgAggregator()
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		m := &LoRAMatrix{Rows: 1, Cols: 3, Data: []float64{1, v, 2}, Owner: "e1"}
		if _, err := a.Aggregate(context.Background(), []*LoRAMatrix{m}); !errors.Is(err, ErrInvalidMatrix) {
			t.Fatalf("expected ErrInvalidMatrix for %v, got %v", v, err)
		}
	}
}

func TestFedAvgAggregateTooManyUpdates(t *testing.T) {
	a := NewFedAvgAggregator()
	updates := make([]*LoRAMatrix, MaxUpdates+1)
	for i := range updates {
		updates[i] = &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}}
	}
	if _, err := a.Aggregate(context.Background(), updates); !errors.Is(err, ErrTooManyUpdates) {
		t.Fatalf("expected ErrTooManyUpdates, got %v", err)
	}
}

func TestFedAvgAggregateNoOverflow(t *testing.T) {
	a := NewFedAvgAggregator()
	big := math.MaxFloat64
	m1 := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{big}}
	m2 := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{big}}
	out, err := a.Aggregate(context.Background(), []*LoRAMatrix{m1, m2})
	if err != nil {
		t.Fatalf("aggregate failed: %v", err)
	}
	if math.IsInf(out.Data[0], 0) || out.Data[0] != big {
		t.Fatalf("expected %v without overflow, got %v", big, out.Data[0])
	}
}

func TestFedAvgAggregateCancelledContext(t *testing.T) {
	a := NewFedAvgAggregator()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}}
	if _, err := a.Aggregate(ctx, []*LoRAMatrix{m}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestFedAvgAggregateWeighted(t *testing.T) {
	a := NewFedAvgAggregator()
	m1 := &LoRAMatrix{Rows: 1, Cols: 2, Data: []float64{0, 10}}
	m2 := &LoRAMatrix{Rows: 1, Cols: 2, Data: []float64{4, 2}}
	out, err := a.AggregateWeighted(context.Background(), []*LoRAMatrix{m1, m2}, []float64{1, 3})
	if err != nil {
		t.Fatalf("weighted aggregate failed: %v", err)
	}
	if out.Data[0] != 3 || out.Data[1] != 4 {
		t.Fatalf("unexpected weighted avg: %v", out.Data)
	}

	// Zero weight excludes an update.
	out, err = a.AggregateWeighted(context.Background(), []*LoRAMatrix{m1, m2}, []float64{0, 1})
	if err != nil || out.Data[0] != 4 || out.Data[1] != 2 {
		t.Fatalf("zero weight should exclude update: %v, %v", out, err)
	}
}

func TestFedAvgAggregateWeightedInvalid(t *testing.T) {
	a := NewFedAvgAggregator()
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}}
	cases := map[string][]float64{
		"length mismatch": {1, 1},
		"negative":        {-1},
		"nan":             {math.NaN()},
		"inf":             {math.Inf(1)},
		"all zero":        {0},
	}
	for name, w := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := a.AggregateWeighted(context.Background(), []*LoRAMatrix{m}, w); !errors.Is(err, ErrInvalidWeights) {
				t.Fatalf("expected ErrInvalidWeights, got %v", err)
			}
		})
	}
	// Weight only on a nil update counts as all zero.
	if _, err := a.AggregateWeighted(context.Background(), []*LoRAMatrix{nil, m}, []float64{5, 0}); !errors.Is(err, ErrInvalidWeights) {
		t.Fatalf("expected ErrInvalidWeights when only nil updates carry weight, got %v", err)
	}
}

func TestLoRAMatrixClone(t *testing.T) {
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{42}, Owner: "e1"}
	c := m.Clone()
	if &c.Data[0] == &m.Data[0] {
		t.Fatalf("expected deep copy")
	}
	if c.Owner != m.Owner {
		t.Fatalf("owner mismatch")
	}
	var nilM *LoRAMatrix
	if nilM.Clone() != nil {
		t.Fatalf("nil clone should be nil")
	}
}

func TestLoRAMatrixChecksum(t *testing.T) {
	m := &LoRAMatrix{Data: []float64{1, 2}}
	first := m.Checksum()
	if first == "" {
		t.Fatalf("expected non-empty checksum")
	}
	if m.Checksum() != first {
		t.Fatalf("checksum not stable")
	}
	other := &LoRAMatrix{Data: []float64{1, 2.0000000000000004}}
	if other.Checksum() == first {
		t.Fatalf("checksum must distinguish nearby floats")
	}
	var nilM *LoRAMatrix
	if nilM.Checksum() == "" {
		t.Fatalf("nil checksum should not panic or be empty")
	}
}

// The plain aggregator has no keys, so Verify must fail closed instead of
// silently accepting any signature.
func TestFederatedAggregatorVerifyFailsClosed(t *testing.T) {
	a := NewFedAvgAggregator()
	err := a.Verify(context.Background(), &LoRAMatrix{}, []byte("sig"))
	if !errors.Is(err, ErrVerificationNotConfigured) {
		t.Fatalf("expected ErrVerificationNotConfigured, got %v", err)
	}
}
