// Package trace provides lightweight in-process tracing: W3C-compatible
// trace/span IDs with parent propagation (context and the traceparent
// header), bounded in-memory storage of finished spans, and request/error/
// latency metrics.
package trace

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds tracing configuration.
type Config struct {
	ServiceName string
	// SampleRate is the fraction (0,1] of traces whose finished spans are
	// stored; 0/unset/invalid means 1 (store all). Metrics count every span.
	SampleRate float64
	// MaxSpans bounds stored finished spans (ring buffer). 0 means
	// DefaultMaxSpans; negative disables span storage.
	MaxSpans int
}

// Limits.
const (
	DefaultMaxSpans   = 1000
	MaxAttributes     = 64
	MaxEvents         = 128
	maxAttrLen        = 256
	maxNameLen        = 256
	defaultSpansLimit = 100
)

// Span is a lightweight trace span handle. The zero value is a no-op span.
type Span struct {
	TraceID  string
	SpanID   string
	ParentID string
	rec      *spanRecord
}

type spanRecord struct {
	mu      sync.Mutex
	name    string
	start   time.Time
	attrs   map[string]string
	events  []SpanEvent
	sampled bool
	ended   atomic.Bool
}

// SpanEvent is a timestamped annotation on a span.
type SpanEvent struct {
	Name string    `json:"name"`
	Time time.Time `json:"time"`
}

// SpanData is a finished, stored span.
type SpanData struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	ParentID   string            `json:"parent_id,omitempty"`
	Name       string            `json:"name"`
	Start      time.Time         `json:"start"`
	End        time.Time         `json:"end"`
	DurationMs float64           `json:"duration_ms"`
	Error      bool              `json:"error"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Events     []SpanEvent       `json:"events,omitempty"`
}

// Provider manages trace spans and in-memory metrics.
type Provider struct {
	mu           sync.Mutex
	serviceName  string
	sampleRate   float64
	requestCount int64
	errorCount   int64
	totalLatency int64 // nanoseconds
	latencyCount int64

	spans  []SpanData // ring buffer
	next   int
	stored int
}

// NewProvider creates a new trace provider.
func NewProvider(cfg Config) *Provider {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "aerollm"
	}
	rate := cfg.SampleRate
	if math.IsNaN(rate) || rate <= 0 || rate > 1 {
		rate = 1
	}
	max := cfg.MaxSpans
	if max == 0 {
		max = DefaultMaxSpans
	}
	if max < 0 {
		max = 0
	}
	return &Provider{serviceName: cfg.ServiceName, sampleRate: rate, spans: make([]SpanData, max)}
}

type ctxKey string

const spanKey ctxKey = "trace-span"

// SpanFromContext returns the span stored in ctx, if any.
func SpanFromContext(ctx context.Context) (Span, bool) {
	if ctx == nil {
		return Span{}, false
	}
	s, ok := ctx.Value(spanKey).(Span)
	return s, ok && s.TraceID != ""
}

// StartSpan starts a new span, as a child of the span in ctx when present,
// and returns a context carrying it.
func (p *Provider) StartSpan(ctx context.Context, operation string) (context.Context, Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	parent, hasParent := SpanFromContext(ctx)
	span := Span{SpanID: newSpanID()}
	sampled := false
	if hasParent {
		span.TraceID = parent.TraceID
		span.ParentID = parent.SpanID
		if parent.rec != nil {
			sampled = parent.rec.sampled
		} else {
			sampled = p.sampled(span.TraceID)
		}
	} else {
		span.TraceID = newTraceID()
		sampled = p.sampled(span.TraceID)
	}
	span.rec = &spanRecord{name: truncate(operation, maxNameLen), start: time.Now(), sampled: sampled}
	return context.WithValue(ctx, spanKey, span), span
}

// sampled makes a deterministic per-trace sampling decision.
func (p *Provider) sampled(traceID string) bool {
	if p.sampleRate >= 1 {
		return true
	}
	b, err := hex.DecodeString(traceID)
	if err != nil || len(b) < 8 {
		return true
	}
	v := binary.BigEndian.Uint64(b[len(b)-8:])
	return float64(v)/float64(math.MaxUint64) < p.sampleRate
}

// End ends a span, recording metrics and (if sampled) storing it. Ending the
// same span twice is a no-op.
func (p *Provider) End(ctx context.Context, span Span, latency time.Duration, isError bool) {
	if span.TraceID == "" {
		return
	}
	var data *SpanData
	if span.rec != nil {
		if !span.rec.ended.CompareAndSwap(false, true) {
			return
		}
		if span.rec.sampled && len(p.spans) > 0 {
			end := time.Now()
			span.rec.mu.Lock()
			d := SpanData{
				TraceID:    span.TraceID,
				SpanID:     span.SpanID,
				ParentID:   span.ParentID,
				Name:       span.rec.name,
				Start:      span.rec.start,
				End:        end,
				DurationMs: float64(latency) / float64(time.Millisecond),
				Error:      isError,
			}
			if len(span.rec.events) > 0 {
				d.Events = append([]SpanEvent(nil), span.rec.events...)
			}
			if len(span.rec.attrs) > 0 {
				d.Attributes = make(map[string]string, len(span.rec.attrs))
				for k, v := range span.rec.attrs {
					d.Attributes[k] = v
				}
			}
			span.rec.mu.Unlock()
			data = &d
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requestCount++
	if isError {
		p.errorCount++
	}
	if latency > 0 {
		p.totalLatency += int64(latency)
		p.latencyCount++
	}
	if data != nil {
		p.spans[p.next] = *data
		p.next = (p.next + 1) % len(p.spans)
		if p.stored < len(p.spans) {
			p.stored++
		}
	}
}

// AddEvent adds a timestamped event to an active span (bounded).
func (p *Provider) AddEvent(_ context.Context, span Span, name string) {
	if span.rec == nil || span.rec.ended.Load() {
		return
	}
	span.rec.mu.Lock()
	defer span.rec.mu.Unlock()
	if !span.rec.ended.Load() && len(span.rec.events) < MaxEvents {
		span.rec.events = append(span.rec.events, SpanEvent{Name: truncate(name, maxNameLen), Time: time.Now()})
	}
}

// SetAttributes sets key/value attributes on an active span, given as
// alternating keys and values (a trailing key gets an empty value).
func (p *Provider) SetAttributes(_ context.Context, span Span, kv ...string) {
	if span.rec == nil || span.rec.ended.Load() {
		return
	}
	span.rec.mu.Lock()
	defer span.rec.mu.Unlock()
	for i := 0; i < len(kv); i += 2 {
		k := truncate(kv[i], maxAttrLen)
		v := ""
		if i+1 < len(kv) {
			v = truncate(kv[i+1], maxAttrLen)
		}
		if span.rec.attrs == nil {
			span.rec.attrs = make(map[string]string)
		}
		if _, exists := span.rec.attrs[k]; !exists && len(span.rec.attrs) >= MaxAttributes {
			continue
		}
		span.rec.attrs[k] = v
	}
}

// ParseTraceparent parses a W3C traceparent header value into a remote
// parent span (no record attached).
func ParseTraceparent(h string) (Span, bool) {
	parts := strings.Split(strings.TrimSpace(h), "-")
	if len(parts) < 4 || len(parts[0]) != 2 || parts[0] == "ff" {
		return Span{}, false
	}
	if parts[0] == "00" && len(parts) != 4 {
		return Span{}, false
	}
	traceID, spanID, flags := parts[1], parts[2], parts[3]
	if !isLowerHex(parts[0]) || len(traceID) != 32 || !isLowerHex(traceID) || allZero(traceID) ||
		len(spanID) != 16 || !isLowerHex(spanID) || allZero(spanID) || len(flags) != 2 || !isLowerHex(flags) {
		return Span{}, false
	}
	return Span{TraceID: traceID, SpanID: spanID}, true
}

// Traceparent formats span as a W3C traceparent header value.
func (s Span) Traceparent() string {
	if s.TraceID == "" || s.SpanID == "" {
		return ""
	}
	flags := "01"
	if s.rec != nil && !s.rec.sampled {
		flags = "00"
	}
	return "00-" + s.TraceID + "-" + s.SpanID + "-" + flags
}

// TraceMiddleware returns middleware that starts spans (continuing an
// incoming traceparent), sets X-Trace-Id / X-Span-Id response headers, and
// records latency and 5xx/panic errors.
func (p *Provider) TraceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if parent, ok := ParseTraceparent(r.Header.Get("traceparent")); ok {
			ctx = context.WithValue(ctx, spanKey, parent)
		}
		ctx, span := p.StartSpan(ctx, r.Method+" "+r.URL.Path)
		p.SetAttributes(ctx, span, "http.method", r.Method, "http.path", r.URL.Path)
		// Headers must be set before the handler writes the response.
		setHeader(w, "X-Trace-Id", span.TraceID)
		setHeader(w, "X-Span-Id", span.SpanID)
		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w}
		defer func() {
			if v := recover(); v != nil {
				p.SetAttributes(ctx, span, "http.status_code", "panic")
				p.End(ctx, span, time.Since(start), true)
				panic(v)
			}
			status := rec.status()
			p.SetAttributes(ctx, span, "http.status_code", strconv.Itoa(status))
			p.End(ctx, span, time.Since(start), status >= http.StatusInternalServerError)
		}()
		next.ServeHTTP(rec, r.WithContext(ctx))
	})
}

// RequestCount returns total requests.
func (p *Provider) RequestCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.requestCount
}

// ErrorCount returns total errors.
func (p *Provider) ErrorCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.errorCount
}

// AvgLatency returns average latency in ms.
func (p *Provider) AvgLatency() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.latencyCount == 0 {
		return 0
	}
	return float64(p.totalLatency) / float64(p.latencyCount) / float64(time.Millisecond)
}

// Spans returns stored finished spans, newest first.
func (p *Provider) Spans() []SpanData {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]SpanData, 0, p.stored)
	for i := 1; i <= p.stored; i++ {
		idx := (p.next - i + len(p.spans)) % len(p.spans)
		out = append(out, p.spans[idx])
	}
	return out
}

// Trace returns the stored spans of one trace ordered by start time.
func (p *Provider) Trace(traceID string) []SpanData {
	var out []SpanData
	for _, s := range p.Spans() {
		if s.TraceID == traceID {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

// MetricsSnapshot is the JSON metrics payload.
type MetricsSnapshot struct {
	Service      string  `json:"service"`
	Requests     int64   `json:"requests"`
	Errors       int64   `json:"errors"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	ErrorRate    float64 `json:"error_rate"`
	SpansStored  int     `json:"spans_stored"`
}

// Snapshot returns the current metrics.
func (p *Provider) Snapshot() MetricsSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := MetricsSnapshot{Service: p.serviceName, Requests: p.requestCount, Errors: p.errorCount, SpansStored: p.stored}
	if p.latencyCount > 0 {
		s.AvgLatencyMs = float64(p.totalLatency) / float64(p.latencyCount) / float64(time.Millisecond)
	}
	if p.requestCount > 0 {
		s.ErrorRate = float64(p.errorCount) / float64(p.requestCount)
	}
	return s
}

// MetricsHandler returns an HTTP handler that serves metrics JSON.
func (p *Provider) MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowGet(w, r) {
			return
		}
		writeJSON(w, http.StatusOK, p.Snapshot())
	}
}

// SpansHandler serves stored spans, newest first. Query: trace_id (filter),
// limit (default 100, max = storage capacity).
func (p *Provider) SpansHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowGet(w, r) {
			return
		}
		q := r.URL.Query()
		limit := defaultSpansLimit
		if l := q.Get("limit"); l != "" {
			n, err := strconv.Atoi(l)
			if err != nil || n <= 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
				return
			}
			limit = n
		}
		var spans []SpanData
		if id := q.Get("trace_id"); id != "" {
			spans = p.Trace(id)
		} else {
			spans = p.Spans()
		}
		if len(spans) > limit {
			spans = spans[:limit]
		}
		if spans == nil {
			spans = []SpanData{}
		}
		writeJSON(w, http.StatusOK, spans)
	}
}

func allowGet(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	w.Header().Set("Allow", "GET, HEAD")
	writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	return false
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *responseRecorder) WriteHeader(code int) {
	if r.statusCode == 0 {
		r.statusCode = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if r.statusCode == 0 {
		r.statusCode = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer so streaming (SSE) keeps working.
func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *responseRecorder) status() int {
	if r.statusCode == 0 {
		return http.StatusOK
	}
	return r.statusCode
}

func setHeader(w http.ResponseWriter, k, v string) {
	if w != nil {
		w.Header().Set(k, v)
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	for {
		_, _ = rand.Read(b) // crypto/rand.Read never fails on supported platforms
		for _, c := range b {
			if c != 0 {
				return hex.EncodeToString(b)
			}
		}
	}
}

func newTraceID() string { return randomHex(16) }
func newSpanID() string  { return randomHex(8) }

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func allZero(s string) bool { return strings.Trim(s, "0") == "" }

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
