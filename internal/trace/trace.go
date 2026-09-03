package trace

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Config holds tracing configuration.
type Config struct {
	ServiceName string
	SampleRate  float64
}

// Span is a lightweight trace span.
type Span struct {
	TraceID string
	SpanID  string
}

// Provider manages trace spans and in-memory metrics.
type Provider struct {
	mu           sync.Mutex
	serviceName  string
	requestCount int64
	errorCount   int64
	totalLatency int64
	latencyCount int64
}

// NewProvider creates a new trace provider.
func NewProvider(cfg Config) *Provider {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "aerollm"
	}
	return &Provider{serviceName: cfg.ServiceName}
}

// StartSpan starts a new span and returns context with span.
func (p *Provider) StartSpan(ctx context.Context, operation string) (context.Context, Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	span := Span{TraceID: newID(), SpanID: newID()}
	return context.WithValue(ctx, spanKey, span), span
}

// End ends a span.
func (p *Provider) End(ctx context.Context, span Span, latency time.Duration, isError bool) {
	if span.TraceID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requestCount++
	if isError {
		p.errorCount++
	}
	if latency > 0 {
		p.totalLatency += latency.Milliseconds()
		p.latencyCount++
	}
}

// AddEvent adds an event to the span.
func (p *Provider) AddEvent(_ context.Context, _ Span, _ string) {}

// SetAttributes sets attributes on the span.
func (p *Provider) SetAttributes(_ context.Context, _ Span, _ ...string) {}

type ctxKey string

const spanKey ctxKey = "trace-span"

// TraceMiddleware returns middleware that starts spans and instruments latency/errors.
func (p *Provider) TraceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := p.StartSpan(r.Context(), r.URL.Path)
		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))
		latency := time.Since(start)
		p.End(ctx, span, latency, rec.statusCode >= http.StatusInternalServerError)
		setHeader(w, "X-Trace-Id", span.TraceID)
		setHeader(w, "X-Span-Id", span.SpanID)
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
	return float64(p.totalLatency) / float64(p.latencyCount)
}

// MetricsSnapshot is the JSON metrics payload.
type MetricsSnapshot struct {
	Service     string  `json:"service"`
	Requests    int64   `json:"requests"`
	Errors      int64   `json:"errors"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
}

// MetricsHandler returns an HTTP handler that serves metrics JSON.
func (p *Provider) MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap := MetricsSnapshot{
			Service:      p.serviceName,
			Requests:     p.RequestCount(),
			Errors:       p.ErrorCount(),
			AvgLatencyMs: p.AvgLatency(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	}
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func setHeader(w http.ResponseWriter, k, v string) {
	if w != nil {
		w.Header().Set(k, v)
	}
}

func newID() string {
	var b [16]byte
	_ = b
	return "trace-" + time.Now().Format("20060102150405.999999999")
}
