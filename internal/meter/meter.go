package meter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// DefaultMaxRecords is the default retention of a Recorder; older records
// are overwritten (ring buffer) so memory stays bounded.
const DefaultMaxRecords = 100_000

// UsageRecord captures a single usage event.
type UsageRecord struct {
	Timestamp time.Time `json:"timestamp"`
	APIKey    string    `json:"api_key"`
	Provider  string    `json:"provider"`
	Model     string    `json:"model"`
	TokensIn  int64     `json:"tokens_in"`
	TokensOut int64     `json:"tokens_out"`
	LatencyMs float64   `json:"latency_ms"`
}

// UsageAggregate summarises records sharing one dimension value.
type UsageAggregate struct {
	Key          string  `json:"key"`
	Requests     int64   `json:"requests"`
	TokensIn     int64   `json:"tokens_in"`
	TokensOut    int64   `json:"tokens_out"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	MaxLatencyMs float64 `json:"max_latency_ms"`
}

// ErrInvalidGroupBy is returned by Aggregate for unknown dimensions.
var ErrInvalidGroupBy = errors.New("meter: group by must be api_key, provider or model")

// Recorder records usage events in a bounded in-memory ring buffer. It is
// safe for concurrent use.
type Recorder struct {
	mu      sync.RWMutex
	records []UsageRecord
	head    int
	size    int
	max     int
	dropped int64
}

// NewRecorder creates a usage recorder retaining DefaultMaxRecords records.
func NewRecorder() *Recorder {
	return NewRecorderWithCapacity(DefaultMaxRecords)
}

// NewRecorderWithCapacity creates a recorder retaining at most n records
// (n <= 0 selects DefaultMaxRecords).
func NewRecorderWithCapacity(n int) *Recorder {
	if n <= 0 {
		n = DefaultMaxRecords
	}
	return &Recorder{records: make([]UsageRecord, 0, min(n, 256)), max: n}
}

func sanitize(record UsageRecord) UsageRecord {
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}
	record.TokensIn = max(record.TokensIn, 0)
	record.TokensOut = max(record.TokensOut, 0)
	if math.IsNaN(record.LatencyMs) || math.IsInf(record.LatencyMs, 0) || record.LatencyMs < 0 {
		record.LatencyMs = 0
	}
	return record
}

// Record appends a usage record. Missing timestamps are set to now;
// negative token counts and invalid latencies are clamped to zero.
func (r *Recorder) Record(record UsageRecord) {
	if r == nil {
		return
	}
	record = sanitize(record)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.max <= 0 {
		r.max = DefaultMaxRecords
	}
	if r.size < r.max {
		r.records = append(r.records, record)
		r.size++
		return
	}
	r.records[r.head] = record
	r.head = (r.head + 1) % r.max
	r.dropped++
}

// Records returns a copy of all retained usage, oldest first.
func (r *Recorder) Records() []UsageRecord {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]UsageRecord, r.size)
	for i := 0; i < r.size; i++ {
		out[i] = r.records[(r.head+i)%len(r.records)]
	}
	return out
}

// Len returns the number of retained records.
func (r *Recorder) Len() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.size
}

// Dropped returns how many records were overwritten due to the cap.
func (r *Recorder) Dropped() int64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.dropped
}

// Clear removes all recorded usage.
func (r *Recorder) Clear() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = r.records[:0]
	r.head, r.size = 0, 0
}

// Aggregate groups retained records at or after since (zero = all) by
// "api_key", "provider" or "model", sorted by descending request count.
func (r *Recorder) Aggregate(groupBy string, since time.Time) ([]UsageAggregate, error) {
	var keyOf func(UsageRecord) string
	switch groupBy {
	case "api_key":
		keyOf = func(u UsageRecord) string { return u.APIKey }
	case "provider":
		keyOf = func(u UsageRecord) string { return u.Provider }
	case "model":
		keyOf = func(u UsageRecord) string { return u.Model }
	default:
		return nil, ErrInvalidGroupBy
	}
	if r == nil {
		return []UsageAggregate{}, nil
	}
	type acc struct {
		UsageAggregate
		latSum float64
	}
	groups := map[string]*acc{}
	r.mu.RLock()
	for i := 0; i < r.size; i++ {
		u := r.records[(r.head+i)%len(r.records)]
		if !since.IsZero() && u.Timestamp.Before(since) {
			continue
		}
		k := keyOf(u)
		a := groups[k]
		if a == nil {
			a = &acc{UsageAggregate: UsageAggregate{Key: k}}
			groups[k] = a
		}
		a.Requests++
		a.TokensIn += u.TokensIn
		a.TokensOut += u.TokensOut
		a.latSum += u.LatencyMs
		if u.LatencyMs > a.MaxLatencyMs {
			a.MaxLatencyMs = u.LatencyMs
		}
	}
	r.mu.RUnlock()
	out := make([]UsageAggregate, 0, len(groups))
	for _, a := range groups {
		a.AvgLatencyMs = a.latSum / float64(a.Requests)
		out = append(out, a.UsageAggregate)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Requests != out[j].Requests {
			return out[i].Requests > out[j].Requests
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

// redactKey hides all but the last four characters of an API key.
func redactKey(k string) string {
	if len(k) <= 8 {
		if k == "" {
			return ""
		}
		return "***"
	}
	return k[:3] + "..." + k[len(k)-4:]
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Handler serves the recorder over HTTP:
//
//	POST: record one UsageRecord (JSON body, max 64 KiB) -> 201 with the stored record
//	GET:  {"records": [...newest first, ?limit (default 100, max 1000)],
//	       "summary": aggregate by ?group_by (default "model")}; API keys are redacted.
func (r *Recorder) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			req.Body = http.MaxBytesReader(w, req.Body, 64<<10)
			dec := json.NewDecoder(req.Body)
			dec.DisallowUnknownFields()
			var rec UsageRecord
			if err := dec.Decode(&rec); err != nil {
				var mbe *http.MaxBytesError
				if errors.As(err, &mbe) {
					writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
					return
				}
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid usage record"})
				return
			}
			if _, err := dec.Token(); !errors.Is(err, io.EOF) {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid usage record"})
				return
			}
			rec = sanitize(rec)
			r.Record(rec)
			writeJSON(w, http.StatusCreated, rec)
		case http.MethodGet, http.MethodHead:
			limit := 100
			if s := req.URL.Query().Get("limit"); s != "" {
				n, err := strconv.Atoi(s)
				if err != nil || n <= 0 {
					writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
					return
				}
				limit = min(n, 1000)
			}
			groupBy := req.URL.Query().Get("group_by")
			if groupBy == "" {
				groupBy = "model"
			}
			summary, err := r.Aggregate(groupBy, time.Time{})
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			if groupBy == "api_key" {
				for i := range summary {
					summary[i].Key = redactKey(summary[i].Key)
				}
			}
			all := r.Records()
			out := make([]UsageRecord, 0, min(limit, len(all)))
			for i := len(all) - 1; i >= 0 && len(out) < limit; i-- {
				rec := all[i]
				rec.APIKey = redactKey(rec.APIKey)
				out = append(out, rec)
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{"records": out, "summary": summary, "retained": r.Len(), "dropped": r.Dropped()})
		default:
			w.Header().Set("Allow", "GET, HEAD, POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": fmt.Sprintf("method %s not allowed", req.Method)})
		}
	})
}
