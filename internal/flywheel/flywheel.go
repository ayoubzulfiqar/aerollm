package flywheel

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

// Feedback limits.
const (
	// MaxFeedbackBodyBytes caps the POST /v1/feedback request body.
	MaxFeedbackBodyBytes = 64 << 10
	// MaxRequestIDLength is the maximum accepted request_id length.
	MaxRequestIDLength = 128
	// MaxCommentLength is the maximum accepted comment length.
	MaxCommentLength = 4096
	// MaxStoredFeedback bounds the number of retained feedback records
	// (deduplicated by request_id; the oldest are evicted first).
	MaxStoredFeedback = 100000
	// DefaultExportInterval is used when BackgroundExportWorker.Interval <= 0.
	DefaultExportInterval = 5 * time.Minute
)

var (
	// ErrInvalidFeedback is returned for malformed or incomplete feedback.
	ErrInvalidFeedback = errors.New("flywheel: invalid feedback")
	// ErrNoFeedbackSource is returned when a rating filter is requested but
	// the DatasetExporter has no feedback source to join against.
	ErrNoFeedbackSource = errors.New("flywheel: rating filter requested but no feedback source configured")
	// ErrNoLedger is returned when the DatasetExporter has no ledger.
	ErrNoLedger = errors.New("flywheel: ledger not configured")
)

// allowedRatings is the set of accepted (normalized) rating values.
var allowedRatings = map[string]struct{}{
	"up": {}, "down": {}, "neutral": {},
	"1": {}, "2": {}, "3": {}, "4": {}, "5": {},
}

// NormalizeRating lowercases and trims a rating and reports whether it is one
// of the accepted values: "up", "down", "neutral" or "1".."5".
func NormalizeRating(r string) (string, bool) {
	r = strings.ToLower(strings.TrimSpace(r))
	_, ok := allowedRatings[r]
	return r, ok
}

// FeedbackRecord ties user ratings to ledger request IDs. RequestID is the
// completion id returned to the client (response "id"), a ledger
// Metadata["request_id"] or a ledger chain hash.
type FeedbackRecord struct {
	RequestID string    `json:"request_id"`
	Rating    string    `json:"rating"`
	Comment   string    `json:"comment,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Validate normalizes and validates the record in place.
func (r *FeedbackRecord) Validate() error {
	if r == nil {
		return fmt.Errorf("%w: missing record", ErrInvalidFeedback)
	}
	r.RequestID = strings.TrimSpace(r.RequestID)
	if r.RequestID == "" {
		return fmt.Errorf("%w: request_id is required", ErrInvalidFeedback)
	}
	if len(r.RequestID) > MaxRequestIDLength {
		return fmt.Errorf("%w: request_id exceeds %d characters", ErrInvalidFeedback, MaxRequestIDLength)
	}
	if strings.TrimSpace(r.Rating) == "" {
		return fmt.Errorf("%w: rating is required", ErrInvalidFeedback)
	}
	rating, ok := NormalizeRating(r.Rating)
	if !ok {
		return fmt.Errorf("%w: rating must be one of up, down, neutral, 1-5", ErrInvalidFeedback)
	}
	r.Rating = rating
	if len(r.Comment) > MaxCommentLength {
		return fmt.Errorf("%w: comment exceeds %d characters", ErrInvalidFeedback, MaxCommentLength)
	}
	return nil
}

// FeedbackExporter ingests feedback and exports high-rated samples. Feedback
// is deduplicated by request_id (latest rating wins) and bounded to
// MaxStoredFeedback records.
type FeedbackExporter struct {
	mu       sync.RWMutex
	feedback *list.List               // of FeedbackRecord, oldest at the front
	index    map[string]*list.Element // request_id -> element in feedback
	ledger   ledger.LedgerStore
	max      int
}

// NewFeedbackExporter creates a new exporter with ledger integration.
func NewFeedbackExporter(store ledger.LedgerStore) *FeedbackExporter {
	return &FeedbackExporter{ledger: store}
}

// Ingest decodes and stores one feedback record from a POST /v1/feedback
// request. It returns an error wrapping ErrInvalidFeedback for malformed
// input; the request body is expected to be size-limited by the caller
// (FeedbackHandler does this).
func (f *FeedbackExporter) Ingest(ctx context.Context, req *http.Request) error {
	if f == nil {
		return errors.New("flywheel: feedback exporter not initialized")
	}
	if req == nil || req.Body == nil {
		return fmt.Errorf("%w: missing body", ErrInvalidFeedback)
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	var rec FeedbackRecord
	dec := json.NewDecoder(req.Body)
	if err := dec.Decode(&rec); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return err
		}
		return fmt.Errorf("%w: invalid JSON", ErrInvalidFeedback)
	}
	return f.Add(rec)
}

// Add validates and stores a feedback record. CreatedAt is always set
// server-side.
func (f *FeedbackExporter) Add(rec FeedbackRecord) error {
	if f == nil {
		return errors.New("flywheel: feedback exporter not initialized")
	}
	if err := rec.Validate(); err != nil {
		return err
	}
	rec.CreatedAt = time.Now().UTC()

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.feedback == nil {
		f.feedback = list.New()
		f.index = make(map[string]*list.Element)
	}
	limit := f.max
	if limit <= 0 {
		limit = MaxStoredFeedback
	}
	if el, ok := f.index[rec.RequestID]; ok {
		// Latest rating wins; the record moves to the newest position.
		f.feedback.Remove(el)
		delete(f.index, rec.RequestID)
	}
	for f.feedback.Len() >= limit {
		oldest := f.feedback.Front()
		delete(f.index, oldest.Value.(FeedbackRecord).RequestID)
		f.feedback.Remove(oldest)
	}
	f.index[rec.RequestID] = f.feedback.PushBack(rec)
	return nil
}

// Rating returns the latest rating recorded for requestID.
func (f *FeedbackExporter) Rating(requestID string) (string, bool) {
	if f == nil {
		return "", false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	el, ok := f.index[strings.TrimSpace(requestID)]
	if !ok {
		return "", false
	}
	return el.Value.(FeedbackRecord).Rating, true
}

// Snapshot returns a copy of the stored feedback, oldest first.
func (f *FeedbackExporter) Snapshot() []FeedbackRecord {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.feedback == nil {
		return []FeedbackRecord{}
	}
	out := make([]FeedbackRecord, 0, f.feedback.Len())
	for el := f.feedback.Front(); el != nil; el = el.Next() {
		out = append(out, el.Value.(FeedbackRecord))
	}
	return out
}

// Len returns the number of stored feedback records.
func (f *FeedbackExporter) Len() int {
	if f == nil {
		return 0
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.feedback == nil {
		return 0
	}
	return f.feedback.Len()
}

// DatasetExporter queries the ledger and formats records as JSONL.
//
// When Feedback is set, ExportJSONL(minRating) exports only ledger records
// whose recorded rating equals minRating. Records are joined to feedback by
// the response "id" field, Metadata["request_id"] or the ledger chain hash.
type DatasetExporter struct {
	Ledger   ledger.LedgerStore
	Feedback *FeedbackExporter
}

type datasetLine struct {
	Request  string `json:"request"`
	Response string `json:"response"`
	Rating   string `json:"rating,omitempty"`
}

// ExportJSONL formats matched ledger records to JSONL text (one JSON object
// per line with "request", "response" and, when known, "rating").
//
// If minRating is empty, every record with a request and response is
// exported without a rating. If minRating is set, a Feedback source is
// required (ErrNoFeedbackSource otherwise) so records are never mislabeled.
func (d *DatasetExporter) ExportJSONL(ctx context.Context, minRating string) (string, error) {
	if d == nil || d.Ledger == nil {
		return "", ErrNoLedger
	}
	if ctx == nil {
		ctx = context.Background()
	}
	wantRating := ""
	if strings.TrimSpace(minRating) != "" {
		r, ok := NormalizeRating(minRating)
		if !ok {
			return "", fmt.Errorf("%w: unsupported rating filter %q", ErrInvalidFeedback, minRating)
		}
		if d.Feedback == nil {
			return "", ErrNoFeedbackSource
		}
		wantRating = r
	}
	records, err := d.Ledger.All(ctx)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	for i := range records {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		rec := &records[i]
		if rec.RequestPayload == "" || rec.ResponsePayload == "" {
			continue
		}
		line := datasetLine{Request: rec.RequestPayload, Response: rec.ResponsePayload}
		if wantRating != "" {
			rating, ok := d.ratingFor(rec)
			if !ok || rating != wantRating {
				continue
			}
			line.Rating = rating
		}
		b, err := json.Marshal(line)
		if err != nil {
			return "", fmt.Errorf("flywheel: encoding dataset line: %w", err)
		}
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return sb.String(), nil
}

// ratingFor looks up the feedback rating for a ledger record.
func (d *DatasetExporter) ratingFor(rec *ledger.LedgerRecord) (string, bool) {
	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(rec.ResponsePayload), &resp); err == nil && resp.ID != "" {
		if r, ok := d.Feedback.Rating(resp.ID); ok {
			return r, true
		}
	}
	if rec.Metadata != nil {
		if id, ok := rec.Metadata["request_id"].(string); ok && id != "" {
			if r, ok := d.Feedback.Rating(id); ok {
				return r, true
			}
		}
	}
	if rec.ChainHash != "" {
		if r, ok := d.Feedback.Rating(rec.ChainHash); ok {
			return r, true
		}
	}
	return "", false
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// FeedbackHandler serves POST /v1/feedback.
func (f *FeedbackExporter) FeedbackHandler(w http.ResponseWriter, r *http.Request) {
	if r == nil || r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if f == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "feedback not available")
		return
	}
	if r.Body == nil {
		writeJSONError(w, http.StatusBadRequest, "missing body")
		return
	}
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, MaxFeedbackBodyBytes)
	if err := f.Ingest(r.Context(), r); err != nil {
		var mbe *http.MaxBytesError
		switch {
		case errors.As(err, &mbe):
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
		case errors.Is(err, ErrInvalidFeedback):
			// Validation messages are generated here and safe to return.
			writeJSONError(w, http.StatusBadRequest, err.Error())
		default:
			writeJSONError(w, http.StatusBadRequest, "invalid feedback")
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

// BackgroundExportWorker periodically exports high-rated samples.
type BackgroundExportWorker struct {
	Exporter   *FeedbackExporter
	Dataset    *DatasetExporter
	Interval   time.Duration
	UploadFunc func(ctx context.Context, payload string) error
	// MinRating is the rating filter passed to ExportJSONL (default "up").
	MinRating string
	// OnError, if set, receives export and upload errors.
	OnError func(error)
}

// Start runs the worker until the context is canceled. A non-positive
// Interval falls back to DefaultExportInterval.
func (w *BackgroundExportWorker) Start(ctx context.Context) {
	if w == nil || w.Exporter == nil || w.Dataset == nil || ctx == nil {
		return
	}
	interval := w.Interval
	if interval <= 0 {
		interval = DefaultExportInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnce(ctx)
		}
	}
}

func (w *BackgroundExportWorker) runOnce(ctx context.Context) {
	rating := w.MinRating
	if rating == "" {
		rating = "up"
	}
	ds := w.Dataset
	if ds.Feedback == nil {
		// Join against the worker's own feedback source without mutating
		// the caller's exporter.
		ds = &DatasetExporter{Ledger: ds.Ledger, Feedback: w.Exporter}
	}
	payload, err := ds.ExportJSONL(ctx, rating)
	if err != nil {
		w.reportError(err)
		return
	}
	if payload == "" || w.UploadFunc == nil {
		return
	}
	if err := w.UploadFunc(ctx, payload); err != nil {
		w.reportError(fmt.Errorf("flywheel: upload failed: %w", err))
	}
}

func (w *BackgroundExportWorker) reportError(err error) {
	if w.OnError != nil && err != nil {
		w.OnError(err)
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
