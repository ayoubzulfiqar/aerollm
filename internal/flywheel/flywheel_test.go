package flywheel

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

type fakeLedger struct {
	records []ledger.LedgerRecord
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
	out := make([]ledger.LedgerRecord, len(f.records))
	copy(out, f.records)
	return out, nil
}

func strPtr(s string) *string { return &s }

func TestFeedbackHandler(t *testing.T) {
	exporter := NewFeedbackExporter(nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/feedback", strings.NewReader(`{"request_id":"abc","rating":"up"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	exporter.FeedbackHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestExportJSONL(t *testing.T) {
	store := &fakeLedger{
		records: []ledger.LedgerRecord{
			{RequestPayload: `{"prompt":"hi"}`, ResponsePayload: `{"text":"hello"}`},
		},
	}
	exporter := &DatasetExporter{Ledger: store}
	out, err := exporter.ExportJSONL(context.Background(), "up")
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	if !strings.Contains(out, `"request"`) {
		t.Fatalf("expected request field in JSONL: %s", out)
	}
	if !strings.Contains(out, `"response"`) {
		t.Fatalf("expected response field in JSONL: %s", out)
	}
}

func TestJSONLReadCloser(t *testing.T) {
	store := &fakeLedger{
		records: []ledger.LedgerRecord{
			{RequestPayload: "a", ResponsePayload: "b"},
		},
	}
	rc, err := (&DatasetExporter{Ledger: store}).JSONLReadCloser(context.Background(), "up")
	if err != nil {
		t.Fatalf("read closer failed: %v", err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	if !strings.Contains(string(b), "a") || !strings.Contains(string(b), "b") {
		t.Fatalf("unexpected JSONL contents: %s", string(b))
	}
}
