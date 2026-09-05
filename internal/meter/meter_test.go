package meter

import (
	"testing"
)

func TestRecordAndRecords(t *testing.T) {
	r := NewRecorder()
	r.Record(UsageRecord{APIKey: "k1", Provider: "p1", Model: "m1", TokensIn: 10, TokensOut: 20, LatencyMs: 100})
	out := r.Records()
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
	if out[0].APIKey != "k1" || out[0].TokensOut != 20 {
		t.Fatalf("unexpected record: %+v", out[0])
	}
}

func TestClearRemovesRecords(t *testing.T) {
	r := NewRecorder()
	r.Record(UsageRecord{APIKey: "k1"})
	r.Clear()
	if len(r.Records()) != 0 {
		t.Fatalf("expected cleared records")
	}
}

func TestRecordSetsTimestamp(t *testing.T) {
	r := NewRecorder()
	r.Record(UsageRecord{APIKey: "k1"})
	out := r.Records()
	if out[0].Timestamp.IsZero() {
		t.Fatalf("expected timestamp to be set")
	}
}
