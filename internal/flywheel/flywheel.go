package flywheel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

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
	records, err := d.Ledger.All(ctx)
	if err != nil || len(records) == 0 {
		return "", err
	}
	var sb strings.Builder
	for _, rec := range records {
		if rec.RequestPayload == "" || rec.ResponsePayload == "" {
			continue
		}
		item := map[string]string{
			"request":  rec.RequestPayload,
			"response": rec.ResponsePayload,
			"rating":   minRating,
		}
		b, _ := json.Marshal(item)
		sb.WriteString(string(b))
		sb.WriteString("\n")
	}
	return sb.String(), nil
}

// FeedbackHandler serves POST /v1/feedback.
func (f *FeedbackExporter) FeedbackHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	if err := f.Ingest(r.Context(), r); err != nil {
		http.Error(w, `{"error":"invalid feedback"}`, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

// BackgroundExportWorker periodically exports high-rated samples.
type BackgroundExportWorker struct {
	Exporter   *FeedbackExporter
	Dataset    *DatasetExporter
	Interval   time.Duration
	UploadFunc func(ctx context.Context, payload string) error
}

// Start runs the worker until the context is canceled.
func (w *BackgroundExportWorker) Start(ctx context.Context) {
	if w == nil || w.Exporter == nil || w.Dataset == nil {
		return
	}
	ticker := time.NewTicker(w.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			payload, _ := w.Dataset.ExportJSONL(ctx, "up")
			if payload == "" {
				continue
			}
			if w.UploadFunc != nil {
				_ = w.UploadFunc(ctx, payload)
			}
		}
	}
}

// JSONLReadCloser returns exported payload as an io.ReadCloser for upload targets.
func (d *DatasetExporter) JSONLReadCloser(ctx context.Context, minRating string) (io.ReadCloser, error) {
	payload, err := d.ExportJSONL(ctx, minRating)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader(payload)), nil
}
