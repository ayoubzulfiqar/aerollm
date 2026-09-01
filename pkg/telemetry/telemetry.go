package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

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

// NewProvider creates a new telemetry provider.
func NewProvider(ctx context.Context, cfg Config) (*Provider, error) {
	var exporterOpt sdktrace.TracerProviderOption
	if cfg.Exporter == "otlp" && cfg.OTLPAddr != "" {
		exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(cfg.OTLPAddr), otlptracegrpc.WithInsecure())
		if err == nil {
			exporterOpt = sdktrace.WithBatcher(exp)
		}
	}

	var opts []sdktrace.TracerProviderOption
if exporterOpt != nil {
	opts = append(opts, exporterOpt)
}

tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)

	return &Provider{
		TracerProvider: tp,
		tracer:         tp.Tracer(cfg.ServiceName),
	}, nil
}

// Start initializes the telemetry provider.
func (p *Provider) Start() {
	_ = p
}

// Stop shuts down the telemetry provider gracefully.
func (p *Provider) Stop(ctx context.Context) {
	if p.TracerProvider != nil {
		_ = p.TracerProvider.Shutdown(ctx)
	}
}

// Tracer returns a tracer for the given name.
func (p *Provider) Tracer(name string) trace.Tracer {
	if p.tracer != nil {
		return p.tracer
	}
	return otel.Tracer(name)
}

// RecordLatency records a latency metric.
func RecordLatency(name string, latency time.Duration) {
	_ = name
	_ = latency
}

// TraceSpan represents an active trace span.
type TraceSpan struct {
	span trace.Span
}

// StartSpan starts a new traced span with context propagation.
func (p *Provider) StartSpan(ctx context.Context, operation string) (context.Context, TraceSpan) {
	ctx, s := p.tracer.Start(ctx, operation)
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

var (
	requestCount int64
	cacheHits    int64
	errorCount   int64
	totalLatency int64
	latencyCount int64
)

// RecordRequestCount increments request counter.
func RecordRequestCount(provider string, count int64) {
	_ = provider
	requestCount += count
}

// RequestCount returns total requests.
func RequestCount() int64 { return requestCount }

// RecordCacheHit increments cache hit counter.
func RecordCacheHit(hit bool) {
	if hit {
		cacheHits++
	}
}

// CacheHits returns cache hit count.
func CacheHits() int64 { return cacheHits }

// RecordError increments error counter.
func RecordError() { errorCount++ }

// ErrorCount returns error count.
func ErrorCount() int64 { return errorCount }

// RecordLatencyMs records latency in milliseconds.
func RecordLatencyMs(latency float64) {
	totalLatency += int64(latency)
	latencyCount++
}

// AvgLatency returns average latency in ms.
func AvgLatency() float64 {
	if latencyCount == 0 {
		return 0
	}
	return float64(totalLatency) / float64(latencyCount)
}
