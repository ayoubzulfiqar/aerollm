package router

import (
	"context"
	"testing"

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
	r := New(Config{Strategy: "cost"})
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
