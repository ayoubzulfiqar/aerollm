package meter

import (
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestRingBufferBounded(t *testing.T) {
	r := NewRecorderWithCapacity(3)
	for i := 0; i < 5; i++ {
		r.Record(UsageRecord{APIKey: "k", TokensIn: int64(i)})
	}
	out := r.Records()
	if len(out) != 3 || out[0].TokensIn != 2 || out[2].TokensIn != 4 || r.Dropped() != 2 {
		t.Fatalf("unexpected ring contents %+v dropped=%d", out, r.Dropped())
	}
	r.Clear()
	r.Record(UsageRecord{TokensIn: 9})
	if out := r.Records(); len(out) != 1 || out[0].TokensIn != 9 {
		t.Fatalf("clear/reuse broken: %+v", out)
	}
}

func TestSanitizeAndAggregate(t *testing.T) {
	r := NewRecorder()
	r.Record(UsageRecord{Model: "a", TokensIn: -5, LatencyMs: math.NaN()})
	r.Record(UsageRecord{Model: "a", TokensIn: 10, TokensOut: 2, LatencyMs: 100})
	r.Record(UsageRecord{Model: "b", TokensIn: 1, LatencyMs: 50})
	if out := r.Records(); out[0].TokensIn != 0 || out[0].LatencyMs != 0 {
		t.Fatalf("invalid values not clamped: %+v", out[0])
	}
	agg, err := r.Aggregate("model", time.Time{})
	if err != nil || len(agg) != 2 || agg[0].Key != "a" || agg[0].Requests != 2 || agg[0].TokensIn != 10 || agg[0].AvgLatencyMs != 50 || agg[0].MaxLatencyMs != 100 {
		t.Fatalf("unexpected aggregate %+v %v", agg, err)
	}
	if _, err := r.Aggregate("nope", time.Time{}); !errors.Is(err, ErrInvalidGroupBy) {
		t.Fatal("expected ErrInvalidGroupBy")
	}
	if agg, _ := r.Aggregate("model", time.Now().Add(time.Hour)); len(agg) != 0 {
		t.Fatal("since filter ignored")
	}
}

func TestConcurrentRecord(t *testing.T) {
	r := NewRecorderWithCapacity(100)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				r.Record(UsageRecord{Model: "m", TokensIn: 1})
				_ = r.Records()
				_, _ = r.Aggregate("model", time.Time{})
			}
		}()
	}
	wg.Wait()
	if r.Len() != 100 || r.Dropped() != 700 {
		t.Fatalf("len=%d dropped=%d", r.Len(), r.Dropped())
	}
}

func TestHandler(t *testing.T) {
	r := NewRecorder()
	h := r.Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"api_key":"sk-secret-abcdef","model":"m","tokens_in":3}`)))
	if rec.Code != http.StatusCreated || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("POST: %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"tokens_in":"x"}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad body: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"model":"`+strings.Repeat("x", 70<<10)+`"}`)))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/?group_by=api_key", nil))
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("GET leaked key or failed: %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") == "" {
		t.Fatalf("DELETE: %d allow=%q", rec.Code, rec.Header().Get("Allow"))
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/?limit=-1", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad limit: %d", rec.Code)
	}
}
