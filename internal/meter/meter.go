package meter

import (
	"sync"
	"time"
)

// UsageRecord captures a single usage event.
type UsageRecord struct {
	Timestamp time.Time `json:"timestamp"`
	APIKey    string     `json:"api_key"`
	Provider  string     `json:"provider"`
	Model     string     `json:"model"`
	TokensIn  int64      `json:"tokens_in"`
	TokensOut int64      `json:"tokens_out"`
	LatencyMs float64    `json:"latency_ms"`
}

// Recorder records usage events in memory.
type Recorder struct {
	mu      sync.RWMutex
	records []UsageRecord
}

// NewRecorder creates a new usage recorder.
func NewRecorder() *Recorder {
	return &Recorder{records: make([]UsageRecord, 0, 256)}
}

// Record appends a usage record.
func (r *Recorder) Record(record UsageRecord) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}
	r.records = append(r.records, record)
}

// Records returns a copy of all recorded usage.
func (r *Recorder) Records() []UsageRecord {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]UsageRecord, len(r.records))
	copy(out, r.records)
	return out
}

// Clear removes all recorded usage.
func (r *Recorder) Clear() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = r.records[:0]
}
