package federated

import (
	"context"
	"math"
	"testing"
)

func TestFedAvgAggregate(t *testing.T) {
	a := NewFedAvgAggregator()
	m1 := &LoRAMatrix{Rows: 2, Cols: 2, Data: []float64{1, 2, 3, 4}, Owner: "edge1"}
	m2 := &LoRAMatrix{Rows: 2, Cols: 2, Data: []float64{3, 4, 5, 6}, Owner: "edge2"}
	out, err := a.Aggregate(context.Background(), []*LoRAMatrix{m1, m2})
	if err != nil { t.Fatalf("aggregate failed: %v", err) }
	if len(out.Data) != 4 { t.Fatalf("unexpected output size") }
	if out.Data[0] != 2 || out.Data[1] != 3 || out.Data[2] != 4 || out.Data[3] != 5 { t.Fatalf("unexpected avg: %v", out.Data) }
}

func TestFedAvgAggregateSkipsInvalid(t *testing.T) {
	a := NewFedAvgAggregator()
	valid := &LoRAMatrix{Rows: 2, Cols: 2, Data: []float64{1, 2, 3, 4}, Owner: "edge1"}
	out, err := a.Aggregate(context.Background(), []*LoRAMatrix{valid, nil})
	if err != nil { t.Fatalf("aggregate failed: %v", err) }
	if len(out.Data) != 4 { t.Fatalf("unexpected output size") }
}

func TestFedAvgAggregateEmpty(t *testing.T) {
	a := NewFedAvgAggregator()
	_, err := a.Aggregate(context.Background(), nil)
	if err == nil { t.Fatalf("expected error on empty input") }
}

func TestLoRAMatrixClone(t *testing.T) {
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{42}, Owner: "e1"}
	c := m.Clone()
	if &c.Data[0] == &m.Data[0] { t.Fatalf("expected deep copy") }
	if c.Owner != m.Owner { t.Fatalf("owner mismatch") }
}

func TestLoRAMatrixChecksum(t *testing.T) {
	m := &LoRAMatrix{Data: []float64{1, 2}}
	first := m.Checksum()
	if first == "" { t.Fatalf("expected non-empty checksum") }
	if m.Checksum() != first { t.Fatalf("checksum not stable") }
}

func TestFedAvgAggregateClampsInfNaN(t *testing.T) {
	a := NewFedAvgAggregator()
	m := &LoRAMatrix{Rows: 1, Cols: 3, Data: []float64{1, math.Inf(1), math.NaN()}, Owner: "e1"}
	out, err := a.Aggregate(context.Background(), []*LoRAMatrix{m})
	if err != nil { t.Fatalf("aggregate failed: %v", err) }
	if out.Data[1] != 0 || out.Data[2] != 0 { t.Fatalf("expected clamped values, got %v", out.Data) }
}

func TestFederatedAggregatorVerifyStub(t *testing.T) {
	a := NewFedAvgAggregator()
	if err := a.Verify(context.Background(), &LoRAMatrix{}, []byte("sig")); err != nil {
		t.Fatalf("stub verify should not error: %v", err)
	}
}
