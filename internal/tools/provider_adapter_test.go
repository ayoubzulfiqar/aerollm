package tools

import (
	"context"
	"errors"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
)

type adapterTestProvider struct {
	name  string
	calls int
	resp  *models.LLMResponse
	err   error
}

func (p *adapterTestProvider) Name() string                 { return p.name }
func (p *adapterTestProvider) Type() providers.ProviderType { return providers.ProviderOpenAI }
func (p *adapterTestProvider) Health() providers.ProviderHealth {
	return providers.ProviderHealth{Name: p.name, Healthy: true}
}
func (p *adapterTestProvider) Close() error { return nil }
func (p *adapterTestProvider) ChatCompletions(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
	p.calls++
	return p.resp, p.err
}

func TestRouterAdapterPreservesUpstreamModel(t *testing.T) {
	r := router.New(router.Config{Strategy: "fallback"})
	r.RegisterProvider(&adapterTestProvider{name: "openai", resp: &models.LLMResponse{Model: "gpt-4o-2024-08-06"}})
	resp, err := NewRouterAdapter(r).CallLLM(context.Background(), &models.LLMRequest{Model: "gpt-4o"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Model != "gpt-4o-2024-08-06" {
		t.Fatalf("upstream model must be preserved, got %q", resp.Model)
	}
}

func TestRouterAdapterFillsEmptyModel(t *testing.T) {
	r := router.New(router.Config{Strategy: "fallback"})
	r.RegisterProvider(&adapterTestProvider{name: "local", resp: &models.LLMResponse{}})
	resp, err := NewRouterAdapter(r).CallLLM(context.Background(), &models.LLMRequest{Model: "llama-3"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Model != "llama-3" {
		t.Fatalf("expected request model, got %q", resp.Model)
	}
}

func TestRouterAdapterFallsBack(t *testing.T) {
	bad := &adapterTestProvider{name: "bad", err: &providers.UpstreamError{Provider: "bad", StatusCode: 503}}
	good := &adapterTestProvider{name: "good", resp: &models.LLMResponse{Model: "m"}}
	r := router.New(router.Config{Strategy: "fallback"})
	r.RegisterProvider(bad)
	r.RegisterProvider(good)
	if _, err := NewRouterAdapter(r).CallLLM(context.Background(), &models.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	if bad.calls != 1 || good.calls != 1 {
		t.Fatalf("expected fallback bad->good, calls %d/%d", bad.calls, good.calls)
	}
}

func TestRouterAdapterErrors(t *testing.T) {
	if _, err := NewRouterAdapter(nil).CallLLM(context.Background(), &models.LLMRequest{}); err == nil {
		t.Fatal("nil router must error")
	}
	var nilAdapter *RouterAdapter
	if _, err := nilAdapter.CallLLM(context.Background(), &models.LLMRequest{}); err == nil {
		t.Fatal("nil adapter must error")
	}
	r := router.New(router.Config{})
	a := NewRouterAdapter(r)
	if _, err := a.CallLLM(context.Background(), nil); err == nil {
		t.Fatal("nil request must error")
	}
	_, err := a.CallLLM(context.Background(), &models.LLMRequest{})
	var npe *router.NoProviderError
	if !errors.As(err, &npe) {
		t.Fatalf("expected NoProviderError, got %v", err)
	}
	r.RegisterProvider(&adapterTestProvider{name: "p", err: &providers.UpstreamError{Provider: "p", StatusCode: 401}})
	_, err = a.CallLLM(context.Background(), &models.LLMRequest{})
	if providers.StatusCode(err) != 401 {
		t.Fatalf("expected 401 preserved, got %v", err)
	}
}
