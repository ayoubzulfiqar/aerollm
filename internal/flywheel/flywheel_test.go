package flywheel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

type fakeLedger struct {
	last *ledger.LedgerRecord
}

func (f *fakeLedger) Append(ctx context.Context, record ledger.LedgerRecord) error { return nil }
func (f *fakeLedger) Latest(ctx context.Context) (*ledger.LedgerRecord, error) {
	if f.last == nil {
		return nil, nil
	}
	out := *f.last
	return &out, nil
}

func TestFeedbackExporterIngest(t *testing.T) {
	f := NewFeedbackExporter(&fakeLedger{})
	req := httptest.NewRequest(http.MethodPost, "/v1/feedback", strings.NewReader(`{"request_id":"req-1","rating":"up"}`))
	req.Header.Set("Content-Type", "application/json")
	if err := f.Ingest(context.Background(), req); err != nil {
		t.Fatalf("ingest failed: %v", err)
	}
}

func TestDatasetExporterExportJSONLNoLedger(t *testing.T) {
	exporter := &DatasetExporter{Ledger: &fakeLedger{}}
	out, err := exporter.ExportJSONL(context.Background(), "up")
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	if out != "" {
		t.Fatalf("expected empty export when ledger is empty, got %q", out)
	}
}
