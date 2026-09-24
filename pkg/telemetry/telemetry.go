package telemetry

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// ProviderMetric holds per-provider telemetry aggregates.
type ProviderMetric struct {
	Name      string  `json:"name"`
	Requests  int64   `json:"requests"`
	LatencyMs float64 `json:"avg_latency_ms"`
}

// Provider holds the OpenTelemetry provider instance.
type Provider struct {
	TracerProvider *sdktrace.TracerProvider
	tracer         trace.Tracer
}

// Config holds telemetry configuration.
type Config struct {
	ServiceName string
	Exporter    string
	OTLPAddr    string
	SampleRate  float64
}

// NewProvider creates a new telemetry provider. When cfg.Exporter is "otlp"
// and an address is set, spans are exported over OTLP/gRPC; otherwise spans
// are recorded in-process only. SampleRate in (0,1) enables ratio sampling.
func NewProvider(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "aerollm"
	}

	var opts []sdktrace.TracerProviderOption
	if cfg.Exporter == "otlp" && cfg.OTLPAddr != "" {
		exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(cfg.OTLPAddr), otlptracegrpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	if cfg.SampleRate > 0 && cfg.SampleRate < 1 {
		opts = append(opts, sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRate))))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)

	return &Provider{
		TracerProvider: tp,
		tracer:         tp.Tracer(cfg.ServiceName),
	}, nil
}

// Start initializes the telemetry provider.
func (p *Provider) Start() {}

// Stop shuts down the telemetry provider gracefully, flushing pending spans.
func (p *Provider) Stop(ctx context.Context) {
	if p != nil && p.TracerProvider != nil {
		_ = p.TracerProvider.Shutdown(ctx)
	}
}

// Tracer returns a tracer for the given name.
func (p *Provider) Tracer(name string) trace.Tracer {
	if p != nil && p.tracer != nil {
		return p.tracer
	}
	return otel.Tracer(name)
}

// TraceSpan represents an active trace span.
type TraceSpan struct {
	span trace.Span
}

// StartSpan starts a new traced span with context propagation. It is safe to
// call on a nil Provider, in which case the global tracer is used.
func (p *Provider) StartSpan(ctx context.Context, operation string) (context.Context, TraceSpan) {
	ctx, s := p.Tracer("aerollm").Start(ctx, operation)
	return ctx, TraceSpan{span: s}
}

// End ends the trace span.
func (t *TraceSpan) End() {
	if t.span != nil {
		t.span.End()
	}
}

// AddEvent adds an event to the current span.
func (t *TraceSpan) AddEvent(name string, attrs ...attribute.KeyValue) {
	if t.span != nil {
		t.span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

// SetAttributes sets attributes on the current span.
func (t *TraceSpan) SetAttributes(attrs ...attribute.KeyValue) {
	if t.span != nil {
		t.span.SetAttributes(attrs...)
	}
}

// RecordError marks the span as failed with the given error.
func (t *TraceSpan) RecordError(err error) {
	if t.span != nil && err != nil {
		t.span.RecordError(err)
	}
}

// Process-wide counters. All access goes through atomics or providerMu so the
// recorders are safe to call from any goroutine.
var (
	requestCount  atomic.Int64
	cacheHits     atomic.Int64
	cacheMisses   atomic.Int64
	errorCount    atomic.Int64
	totalLatency  atomic.Int64 // microseconds
	latencyCount  atomic.Int64
	tokensIn      atomic.Int64
	tokensOut     atomic.Int64
	costMicroUSD  atomic.Int64
	inflightCount atomic.Int64

	providerMu      sync.Mutex
	providerStats   = map[string]*providerStat{}
	latencyBuckets  = []float64{50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000}
	latencyHist     = make([]atomic.Int64, len(latencyBuckets)+1)
	latencySumMicro atomic.Int64
)

type providerStat struct {
	requests  int64
	errors    int64
	latencyMs float64
	samples   int64
}

func statFor(name string) *providerStat {
	if name == "" {
		name = "unknown"
	}
	s, ok := providerStats[name]
	if !ok {
		s = &providerStat{}
		providerStats[name] = s
	}
	return s
}

// RecordProviderMetrics records request/latency for a named provider.
func RecordProviderMetrics(name string, latencyMs float64) {
	providerMu.Lock()
	s := statFor(name)
	s.requests++
	s.latencyMs += latencyMs
	s.samples++
	providerMu.Unlock()
}

// RecordProviderError records a failed call against a named provider.
func RecordProviderError(name string) {
	providerMu.Lock()
	statFor(name).errors++
	providerMu.Unlock()
}

// ProviderMetrics returns per-provider request/latency aggregates, sorted by name.
func ProviderMetrics() []ProviderMetric {
	providerMu.Lock()
	defer providerMu.Unlock()
	out := make([]ProviderMetric, 0, len(providerStats))
	for name, s := range providerStats {
		avg := 0.0
		if s.samples > 0 {
			avg = s.latencyMs / float64(s.samples)
		}
		out = append(out, ProviderMetric{Name: name, Requests: s.requests, LatencyMs: avg})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// RecordRequestCount increments the total request counter.
func RecordRequestCount(provider string, count int64) {
	requestCount.Add(count)
}

// RequestCount returns total requests.
func RequestCount() int64 { return requestCount.Load() }

// RecordCacheHit increments the cache hit (or miss) counter.
func RecordCacheHit(hit bool) {
	if hit {
		cacheHits.Add(1)
	} else {
		cacheMisses.Add(1)
	}
}

// CacheHits returns cache hit count.
func CacheHits() int64 { return cacheHits.Load() }

// CacheMisses returns cache miss count.
func CacheMisses() int64 { return cacheMisses.Load() }

// RecordError increments error counter.
func RecordError() { errorCount.Add(1) }

// ErrorCount returns error count.
func ErrorCount() int64 { return errorCount.Load() }

// RecordLatency records a latency observation.
func RecordLatency(name string, latency time.Duration) {
	RecordLatencyMs(float64(latency) / float64(time.Millisecond))
}

// RecordLatencyMs records latency in milliseconds.
func RecordLatencyMs(latency float64) {
	if latency < 0 || math.IsNaN(latency) {
		return
	}
	micros := int64(latency * 1000)
	totalLatency.Add(micros)
	latencyCount.Add(1)
	latencySumMicro.Add(micros)
	idx := sort.SearchFloat64s(latencyBuckets, latency)
	latencyHist[idx].Add(1)
}

// AvgLatency returns average latency in ms.
func AvgLatency() float64 {
	n := latencyCount.Load()
	if n == 0 {
		return 0
	}
	return float64(totalLatency.Load()) / 1000 / float64(n)
}

// RecordTokens adds prompt/completion token counts.
func RecordTokens(prompt, completion int) {
	if prompt > 0 {
		tokensIn.Add(int64(prompt))
	}
	if completion > 0 {
		tokensOut.Add(int64(completion))
	}
}

// RecordCostUSD adds spend in USD.
func RecordCostUSD(usd float64) {
	if usd > 0 && !math.IsInf(usd, 0) {
		costMicroUSD.Add(int64(usd * 1e6))
	}
}

// IncInflight / DecInflight track concurrently served requests.
func IncInflight() { inflightCount.Add(1) }

// DecInflight decrements the in-flight gauge.
func DecInflight() { inflightCount.Add(-1) }

// Inflight returns the current in-flight request count.
func Inflight() int64 { return inflightCount.Load() }

// WritePrometheus writes all counters in the Prometheus text exposition format.
func WritePrometheus(w io.Writer) {
	var b strings.Builder
	counter := func(name, help string, v float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s %g\n", name, help, name, name, v)
	}
	gauge := func(name, help string, v float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", name, help, name, name, v)
	}
	counter("aerollm_requests_total", "Total completed gateway requests.", float64(requestCount.Load()))
	counter("aerollm_errors_total", "Total gateway errors.", float64(errorCount.Load()))
	counter("aerollm_cache_hits_total", "Total cache hits.", float64(cacheHits.Load()))
	counter("aerollm_cache_misses_total", "Total cache misses.", float64(cacheMisses.Load()))
	counter("aerollm_prompt_tokens_total", "Total prompt tokens processed.", float64(tokensIn.Load()))
	counter("aerollm_completion_tokens_total", "Total completion tokens generated.", float64(tokensOut.Load()))
	counter("aerollm_cost_usd_total", "Total estimated spend in USD.", float64(costMicroUSD.Load())/1e6)
	gauge("aerollm_inflight_requests", "Requests currently being served.", float64(inflightCount.Load()))

	b.WriteString("# HELP aerollm_request_latency_ms Gateway request latency in milliseconds.\n# TYPE aerollm_request_latency_ms histogram\n")
	var cum int64
	for i, le := range latencyBuckets {
		cum += latencyHist[i].Load()
		fmt.Fprintf(&b, "aerollm_request_latency_ms_bucket{le=\"%g\"} %d\n", le, cum)
	}
	cum += latencyHist[len(latencyBuckets)].Load()
	fmt.Fprintf(&b, "aerollm_request_latency_ms_bucket{le=\"+Inf\"} %d\n", cum)
	fmt.Fprintf(&b, "aerollm_request_latency_ms_sum %g\n", float64(latencySumMicro.Load())/1000)
	fmt.Fprintf(&b, "aerollm_request_latency_ms_count %d\n", latencyCount.Load())

	providerMu.Lock()
	names := make([]string, 0, len(providerStats))
	for n := range providerStats {
		names = append(names, n)
	}
	sort.Strings(names)
	if len(names) > 0 {
		b.WriteString("# HELP aerollm_provider_requests_total Requests per upstream provider.\n# TYPE aerollm_provider_requests_total counter\n")
		for _, n := range names {
			fmt.Fprintf(&b, "aerollm_provider_requests_total{provider=%q} %d\n", n, providerStats[n].requests)
		}
		b.WriteString("# HELP aerollm_provider_errors_total Errors per upstream provider.\n# TYPE aerollm_provider_errors_total counter\n")
		for _, n := range names {
			fmt.Fprintf(&b, "aerollm_provider_errors_total{provider=%q} %d\n", n, providerStats[n].errors)
		}
	}
	providerMu.Unlock()

	_, _ = io.WriteString(w, b.String())
}

// MetricsHandler serves WritePrometheus output at a /metrics endpoint.
func MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		WritePrometheus(w)
	}
}
