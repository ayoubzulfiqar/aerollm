package router

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// mockProvider is a mock implementation of providers.Provider.
type mockProvider struct {
	name       string
	providerType providers.ProviderType
	available  bool
	latencyMs  float64
}

func (m *mockProvider) Name() string                          { return m.name }
func (m *mockProvider) Type() providers.ProviderType         { return m.providerType }
func (m *mockProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	return &models.LLMResponse{}, nil
}
func (m *mockProvider) Health() providers.ProviderHealth {
	return providers.ProviderHealth{Name: m.name, Healthy: m.available, LatencyMs: m.latencyMs}
}
func (m *mockProvider) Close() error { return nil }

func TestNewRouter(t *testing.T) {
	r := New(Config{Strategy: "round_robin"})
	if r == nil {
		t.Fatal("expected non-nil router")
	}
}

func TestRegisterProvider(t *testing.T) {
	r := New(Config{Strategy: "round_robin"})
	p := &mockProvider{name: "test", providerType: providers.ProviderOpenAI, available: true}
	r.RegisterProvider(p)

	got, ok := r.GetProvider("test")
	if !ok || got == nil {
		t.Fatal("expected provider to be registered")
	}
}

func TestRouteRoundRobin(t *testing.T) {
	r := New(Config{Strategy: "round_robin"})
	r.RegisterProvider(&mockProvider{name: "p1", providerType: providers.ProviderOpenAI, available: true})
	r.RegisterProvider(&mockProvider{name: "p2", providerType: providers.ProviderAnthropic, available: true})

	_, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRouteFallback(t *testing.T) {
	r := New(Config{Strategy: "fallback"})
	r.RegisterProvider(&mockProvider{name: "p1", providerType: providers.ProviderOpenAI, available: true})

	p, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Name() != "p1" {
		t.Fatalf("expected p1, got %s", p.Name())
	}
}

func TestRouteNoProvider(t *testing.T) {
	r := New(Config{Strategy: "round_robin"})
	_, err := r.Route(context.Background(), &models.LLMRequest{})
	if err == nil {
		t.Fatal("expected error when no providers available")
	}
}

func TestRouteLatencyBased(t *testing.T) {
	r := New(Config{Strategy: "latency"})
	r.RegisterProvider(&mockProvider{name: "slow", providerType: providers.ProviderOpenAI, available: true, latencyMs: 500})
	r.RegisterProvider(&mockProvider{name: "fast", providerType: providers.ProviderOpenAI, available: true, latencyMs: 50})

	p, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Name() != "fast" {
		t.Fatalf("expected fast provider, got %s", p.Name())
	}
}

func TestRouteCostBased(t *testing.T) {
	r := New(Config{Strategy: "round_robin"})
	r.RegisterProvider(&mockProvider{name: "expensive", providerType: providers.ProviderOpenAI, available: true})
	r.RegisterProvider(&mockProvider{name: "cheap", providerType: providers.ProviderAnthropic, available: true})

	p, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
}
func TestRouteLeastBusy(t *testing.T) {
	r := New(Config{Strategy: "least_busy"})
	r.RegisterProvider(&mockProvider{name: "p1", providerType: providers.ProviderOpenAI, available: true})
	r.RegisterProvider(&mockProvider{name: "p2", providerType: providers.ProviderAnthropic, available: true})

	// Get the circuit breakers to manipulate inflight counters.
	cbs := r.Providers()

	// Simulate p1 having 5 inflight requests.
	atomic.AddInt64(&cbs[0].usage.InflightRequests, 5)

	p, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// p2 should be selected since it has fewer inflight requests (0 vs 5).
	if p.Name() != "p2" {
		t.Fatalf("expected p2 (least busy), got %s", p.Name())
	}
}
func TestRouteLeastBusyAllZero(t *testing.T) {
	r := New(Config{Strategy: "least_busy"})
	r.RegisterProvider(&mockProvider{name: "p1", providerType: providers.ProviderOpenAI, available: true})
	r.RegisterProvider(&mockProvider{name: "p2", providerType: providers.ProviderAnthropic, available: true})

	// Both have 0 inflight — should return one of them without error.
	p, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
}
func TestRouteUsageBased(t *testing.T) {
	r := New(Config{
		Strategy: "usage_based",
		ProviderRateLimits: map[string]ProviderRateLimits{
			"p1": {TPM: 100, RPM: 10},
			"p2": {TPM: 100, RPM: 10},
		},
	})
	r.RegisterProvider(&mockProvider{name: "p1", providerType: providers.ProviderOpenAI, available: true})
	r.RegisterProvider(&mockProvider{name: "p2", providerType: providers.ProviderAnthropic, available: true})

	cbs := r.Providers()

	// Simulate p1 at 90% RPM (9/10 requests used).
	cbs[0].usage.RequestCountMinute = 9
	cbs[0].usage.LastReset = time.Now()

	p, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// p2 should be selected since it has more headroom.
	if p.Name() != "p2" {
		t.Fatalf("expected p2 (more headroom), got %s", p.Name())
	}
}
func TestRouteUsageBasedEqual(t *testing.T) {
	r := New(Config{
		Strategy: "usage_based",
		ProviderRateLimits: map[string]ProviderRateLimits{
			"p1": {TPM: 100, RPM: 10},
			"p2": {TPM: 100, RPM: 10},
		},
	})
	r.RegisterProvider(&mockProvider{name: "p1", providerType: providers.ProviderOpenAI, available: true})
	r.RegisterProvider(&mockProvider{name: "p2", providerType: providers.ProviderAnthropic, available: true})

	// Both have 0 usage — should return one without error.
	p, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
}
func TestRouteUsageBasedNoLimits(t *testing.T) {
	// When no rate limits are configured, usage_based should fall back to
	// selecting any available provider.
	r := New(Config{Strategy: "usage_based"})
	r.RegisterProvider(&mockProvider{name: "p1", providerType: providers.ProviderOpenAI, available: true})
	r.RegisterProvider(&mockProvider{name: "p2", providerType: providers.ProviderAnthropic, available: true})

	p, err := r.Route(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
}
func TestSetStrategy(t *testing.T) {
	r := New(Config{Strategy: "round_robin"})
	r.SetStrategy("least_busy")

	r.mu.RLock()
	s := r.strategy
	r.mu.RUnlock()

	if s != "least_busy" {
		t.Fatalf("expected strategy 'least_busy', got '%s'", s)
	}
}
func TestSetRateLimits(t *testing.T) {
	r := New(Config{Strategy: "round_robin"})
	limits := map[string]ProviderRateLimits{
		"p1": {TPM: 1000, RPM: 100},
	}
	r.SetRateLimits(limits)

	r.mu.RLock()
	got := r.rateLimits["p1"]
	r.mu.RUnlock()

	if got.TPM != 1000 || got.RPM != 100 {
		t.Fatalf("expected TPM=1000, RPM=100; got TPM=%d, RPM=%d", got.TPM, got.RPM)
	}
}
