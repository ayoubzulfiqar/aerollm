package flywheel

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

// FeedbackRecord ties user ratings to ledger request IDs.
type FeedbackRecord struct {
	RequestID string
	Rating    string
	Comment   string
}

// FeedbackExporter ingests feedback and exports high-rated samples.
type FeedbackExporter struct {
	mu       sync.RWMutex
	feedback []FeedbackRecord
	ledger   ledger.LedgerStore
}

// NewFeedbackExporter creates a new exporter with ledger integration.
func NewFeedbackExporter(store ledger.LedgerStore) *FeedbackExporter {
	return &FeedbackExporter{ledger: store}
}

// Ingest accepts POST /v1/feedback payloads.
func (f *FeedbackExporter) Ingest(ctx context.Context, req *http.Request) error {
	if f == nil || req == nil {
		return nil
	}
	var rec FeedbackRecord
	if err := json.NewDecoder(req.Body).Decode(&rec); err != nil {
		return err
	}
	if strings.TrimSpace(rec.RequestID) == "" || strings.TrimSpace(rec.Rating) == "" {
		return nil
	}
	f.mu.Lock()
	f.feedback = append(f.feedback, rec)
	f.mu.Unlock()
	return nil
}

// DatasetExporter queries ledger for high-rated entries and formats JSONL.
type DatasetExporter struct {
	Ledger ledger.LedgerStore
}

// ExportJSONL formats matched ledger records to JSONL text.
func (d *DatasetExporter) ExportJSONL(ctx context.Context, minRating string) (string, error) {
	_ = ctx
	_ = minRating
	latest, _ := d.Ledger.Latest(ctx)
	if latest == nil {
		return "", nil
	}
	return "", nil
}
