package flywheel

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

type fakeLedger struct {
	records []ledger.LedgerRecord
	err     error
}

func (f *fakeLedger) Append(ctx context.Context, record ledger.LedgerRecord) error {
	_ = ctx
	f.records = append(f.records, record)
	return nil
}

func (f *fakeLedger) Latest(ctx context.Context) (*ledger.LedgerRecord, error) {
	_ = ctx
	if len(f.records) == 0 {
		return nil, nil
	}
	out := f.records[len(f.records)-1]
	return &out, nil
}

func (f *fakeLedger) All(ctx context.Context) ([]ledger.LedgerRecord, error) {
	_ = ctx
	if f.err != nil {
		return nil, f.err
	}
	out := make([]ledger.LedgerRecord, len(f.records))
	copy(out, f.records)
	return out, nil
}

func strPtr(s string) *string { return &s }

func postFeedback(t *testing.T, f *FeedbackExporter, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/feedback", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.FeedbackHandler(w, req)
	return w
}

func TestFeedbackHandler(t *testing.T) {
	exporter := NewFeedbackExporter(nil)
	w := postFeedback(t, exporter, `{"request_id":"abc","rating":"up"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	// The record must actually be stored (it used to be silently dropped
	// because FeedbackRecord had no json tags).
	if r, ok := exporter.Rating("abc"); !ok || r != "up" {
		t.Fatalf("expected stored rating up, got %q %v", r, ok)
	}
	snap := exporter.Snapshot()
	if len(snap) != 1 || snap[0].CreatedAt.IsZero() {
		t.Fatalf("expected one record with server-side timestamp, got %+v", snap)
	}
}

func TestFeedbackHandlerValidation(t *testing.T) {
	exporter := NewFeedbackExporter(nil)
	cases := map[string]string{
		"missing request_id": `{"rating":"up"}`,
		"missing rating":     `{"request_id":"abc"}`,
		"blank request_id":   `{"request_id":"   ","rating":"up"}`,
		"bad rating":         `{"request_id":"abc","rating":"excellent"}`,
		"long request_id":    `{"request_id":"` + strings.Repeat("a", MaxRequestIDLength+1) + `","rating":"up"}`,
		"long comment":       `{"request_id":"abc","rating":"up","comment":"` + strings.Repeat("c", MaxCommentLength+1) + `"}`,
		"invalid json":       `{"request_id":`,
		"legacy field name":  `{"RequestID":"abc","Rating":"up"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			w := postFeedback(t, exporter, body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("expected JSON content type, got %q", ct)
			}
			var payload map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil || payload["error"] == "" {
				t.Fatalf("expected JSON error body, got %s", w.Body.String())
			}
		})
	}
	if exporter.Len() != 0 {
		t.Fatalf("invalid feedback must not be stored, have %d", exporter.Len())
	}
}

func TestFeedbackHandlerMethodAndSize(t *testing.T) {
	exporter := NewFeedbackExporter(nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/feedback", nil)
	w := httptest.NewRecorder()
	exporter.FeedbackHandler(w, req)
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("expected 405 with Allow: POST, got %d %q", w.Code, w.Header().Get("Allow"))
	}

	big := `{"request_id":"abc","rating":"up","comment":"` + strings.Repeat("x", MaxFeedbackBodyBytes) + `"}`
	w = postFeedback(t, exporter, big)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}

	var nilExporter *FeedbackExporter
	w = postFeedback(t, nilExporter, `{"request_id":"abc","rating":"up"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for nil exporter, got %d", w.Code)
	}
}

func TestFeedbackNormalizationAndDedupe(t *testing.T) {
	f := NewFeedbackExporter(nil)
	if err := f.Add(FeedbackRecord{RequestID: " abc ", Rating: " UP "}); err != nil {
		t.Fatal(err)
	}
	if err := f.Add(FeedbackRecord{RequestID: "abc", Rating: "down"}); err != nil {
		t.Fatal(err)
	}
	if f.Len() != 1 {
		t.Fatalf("expected dedupe by request_id, got %d records", f.Len())
	}
	if r, _ := f.Rating("abc"); r != "down" {
		t.Fatalf("latest rating should win, got %q", r)
	}
	if err := f.Add(FeedbackRecord{RequestID: "n", Rating: "5"}); err != nil {
		t.Fatalf("numeric rating should be accepted: %v", err)
	}
	if err := f.Add(FeedbackRecord{RequestID: "n", Rating: "6"}); !errors.Is(err, ErrInvalidFeedback) {
		t.Fatalf("rating 6 should be rejected, got %v", err)
	}
}

func TestFeedbackBoundedMemory(t *testing.T) {
	f := NewFeedbackExporter(nil)
	f.max = 3
	for i := 0; i < 10; i++ {
		if err := f.Add(FeedbackRecord{RequestID: fmt.Sprintf("r%d", i), Rating: "up"}); err != nil {
			t.Fatal(err)
		}
	}
	if f.Len() != 3 {
		t.Fatalf("expected 3 records, got %d", f.Len())
	}
	if _, ok := f.Rating("r0"); ok {
		t.Fatal("oldest record should have been evicted")
	}
	snap := f.Snapshot()
	if snap[0].RequestID != "r7" || snap[2].RequestID != "r9" {
		t.Fatalf("unexpected retained records: %+v", snap)
	}
	// Re-rating an old entry refreshes it so it is not evicted next.
	if err := f.Add(FeedbackRecord{RequestID: "r7", Rating: "down"}); err != nil {
		t.Fatal(err)
	}
	_ = f.Add(FeedbackRecord{RequestID: "r10", Rating: "up"})
	if _, ok := f.Rating("r7"); !ok {
		t.Fatal("refreshed record should survive eviction")
	}
	if _, ok := f.Rating("r8"); ok {
		t.Fatal("r8 should now be the evicted oldest")
	}
}

func TestFeedbackConcurrent(t *testing.T) {
	f := NewFeedbackExporter(nil)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = f.Add(FeedbackRecord{RequestID: fmt.Sprintf("r%d", i), Rating: "up"})
				_, _ = f.Rating("r1")
				_ = f.Snapshot()
			}
		}(g)
	}
	wg.Wait()
	if f.Len() != 100 {
		t.Fatalf("expected 100 unique records, got %d", f.Len())
	}
}

func TestIngestNilRequest(t *testing.T) {
	f := NewFeedbackExporter(nil)
	if err := f.Ingest(context.Background(), nil); !errors.Is(err, ErrInvalidFeedback) {
		t.Fatalf("expected ErrInvalidFeedback, got %v", err)
	}
}

func TestExportJSONLAll(t *testing.T) {
	store := &fakeLedger{
		records: []ledger.LedgerRecord{
			{RequestPayload: `{"prompt":"hi"}`, ResponsePayload: `{"text":"hello"}`},
			{RequestPayload: "", ResponsePayload: "skipped"},
		},
	}
	exporter := &DatasetExporter{Ledger: store}
	out, err := exporter.ExportJSONL(context.Background(), "")
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	if !strings.Contains(out, `"request"`) || !strings.Contains(out, `"response"`) {
		t.Fatalf("expected request/response fields in JSONL: %s", out)
	}
	if strings.Contains(out, `"rating"`) {
		t.Fatalf("unrated export must not claim a rating: %s", out)
	}
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("expected exactly one line, got %q", out)
	}
}

// A rating filter without a feedback source used to label every ledger
// record with the requested rating.
func TestExportJSONLRequiresFeedbackSource(t *testing.T) {
	store := &fakeLedger{records: []ledger.LedgerRecord{{RequestPayload: "a", ResponsePayload: "b"}}}
	_, err := (&DatasetExporter{Ledger: store}).ExportJSONL(context.Background(), "up")
	if !errors.Is(err, ErrNoFeedbackSource) {
		t.Fatalf("expected ErrNoFeedbackSource, got %v", err)
	}
}

func TestExportJSONLFiltersByRating(t *testing.T) {
	store := &fakeLedger{
		records: []ledger.LedgerRecord{
			{RequestPayload: `{"q":"one"}`, ResponsePayload: `{"id":"chatcmpl-1","text":"a"}`},
			{RequestPayload: `{"q":"two"}`, ResponsePayload: `{"id":"chatcmpl-2","text":"b"}`},
			{RequestPayload: `{"q":"three"}`, ResponsePayload: `not json`, Metadata: map[string]interface{}{"request_id": "meta-3"}},
			{RequestPayload: `{"q":"four"}`, ResponsePayload: `x`, ChainHash: "hash-4"},
			{RequestPayload: `{"q":"five"}`, ResponsePayload: `{"id":"chatcmpl-5"}`},
		},
	}
	fb := NewFeedbackExporter(store)
	for id, rating := range map[string]string{"chatcmpl-1": "up", "chatcmpl-2": "down", "meta-3": "up", "hash-4": "up"} {
		if err := fb.Add(FeedbackRecord{RequestID: id, Rating: rating}); err != nil {
			t.Fatal(err)
		}
	}
	out, err := (&DatasetExporter{Ledger: store, Feedback: fb}).ExportJSONL(context.Background(), "UP")
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	var lines []datasetLine
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		var l datasetLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			t.Fatalf("each line must be valid JSON: %v (%q)", err, sc.Text())
		}
		lines = append(lines, l)
	}
	if len(lines) != 3 {
		t.Fatalf("expected 3 up-rated records, got %d: %s", len(lines), out)
	}
	for _, l := range lines {
		if l.Rating != "up" {
			t.Fatalf("unexpected rating %q", l.Rating)
		}
		if strings.Contains(l.Request, "two") || strings.Contains(l.Request, "five") {
			t.Fatalf("non-matching record exported: %+v", l)
		}
	}
}

func TestExportJSONLEscaping(t *testing.T) {
	tricky := "line1\nline2 \"quoted\"\t<tag>"
	store := &fakeLedger{records: []ledger.LedgerRecord{{RequestPayload: tricky, ResponsePayload: tricky}}}
	out, err := (&DatasetExporter{Ledger: store}).ExportJSONL(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(out, "\n") != 1 || !strings.HasSuffix(out, "\n") {
		t.Fatalf("payload newlines must be escaped, got %q", out)
	}
	var l datasetLine
	if err := json.Unmarshal([]byte(strings.TrimSuffix(out, "\n")), &l); err != nil || l.Request != tricky {
		t.Fatalf("round trip failed: %v %q", err, l.Request)
	}
}

func TestExportJSONLErrors(t *testing.T) {
	var nilExporter *DatasetExporter
	if _, err := nilExporter.ExportJSONL(context.Background(), ""); !errors.Is(err, ErrNoLedger) {
		t.Fatalf("expected ErrNoLedger, got %v", err)
	}
	if _, err := (&DatasetExporter{}).ExportJSONL(context.Background(), ""); !errors.Is(err, ErrNoLedger) {
		t.Fatalf("expected ErrNoLedger, got %v", err)
	}
	boom := errors.New("boom")
	if _, err := (&DatasetExporter{Ledger: &fakeLedger{err: boom}}).ExportJSONL(context.Background(), ""); !errors.Is(err, boom) {
		t.Fatalf("expected ledger error, got %v", err)
	}
	fb := NewFeedbackExporter(nil)
	if _, err := (&DatasetExporter{Ledger: &fakeLedger{}, Feedback: fb}).ExportJSONL(context.Background(), "great"); !errors.Is(err, ErrInvalidFeedback) {
		t.Fatalf("expected ErrInvalidFeedback for unknown rating filter, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &fakeLedger{records: []ledger.LedgerRecord{{RequestPayload: "a", ResponsePayload: "b"}}}
	if _, err := (&DatasetExporter{Ledger: store}).ExportJSONL(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestJSONLReadCloser(t *testing.T) {
	store := &fakeLedger{
		records: []ledger.LedgerRecord{
			{RequestPayload: "a", ResponsePayload: "b"},
		},
	}
	rc, err := (&DatasetExporter{Ledger: store}).JSONLReadCloser(context.Background(), "")
	if err != nil {
		t.Fatalf("read closer failed: %v", err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	if !strings.Contains(string(b), "a") || !strings.Contains(string(b), "b") {
		t.Fatalf("unexpected JSONL contents: %s", string(b))
	}
}

func TestBackgroundExportWorker(t *testing.T) {
	store := &fakeLedger{records: []ledger.LedgerRecord{{RequestPayload: "a", ResponsePayload: `{"id":"r1"}`}}}
	fb := NewFeedbackExporter(store)
	_ = fb.Add(FeedbackRecord{RequestID: "r1", Rating: "up"})

	var uploads atomic.Int32
	var errs atomic.Int32
	w := &BackgroundExportWorker{
		Exporter: fb,
		Dataset:  &DatasetExporter{Ledger: store},
		Interval: 5 * time.Millisecond,
		UploadFunc: func(ctx context.Context, payload string) error {
			if !strings.Contains(payload, `"rating":"up"`) {
				t.Errorf("unexpected payload %q", payload)
			}
			uploads.Add(1)
			return errors.New("upload down")
		},
		OnError: func(error) { errs.Add(1) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Start(ctx)
		close(done)
	}()
	deadline := time.After(2 * time.Second)
	for uploads.Load() == 0 || errs.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("worker did not upload/report errors in time")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop on context cancellation")
	}
}

func TestBackgroundExportWorkerZeroInterval(t *testing.T) {
	w := &BackgroundExportWorker{Exporter: NewFeedbackExporter(nil), Dataset: &DatasetExporter{Ledger: &fakeLedger{}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Must not panic (time.NewTicker panics on non-positive durations).
	w.Start(ctx)
}

var _ = strPtr
