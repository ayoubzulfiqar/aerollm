package universal

import (
	"context"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// LegacyProviderAdapter bridges a legacy providers.Provider into the universal registry.
type LegacyProviderAdapter struct {
	inner providers.Provider
}

// NewLegacyProviderAdapter wraps a legacy provider.
func NewLegacyProviderAdapter(p providers.Provider) *LegacyProviderAdapter {
	return &LegacyProviderAdapter{inner: p}
}

func (a *LegacyProviderAdapter) Name() string              { return a.inner.Name() }
func (a *LegacyProviderAdapter) Type() string              { return string(a.inner.Type()) }
func (a *LegacyProviderAdapter) Health() map[string]interface{} {
	return map[string]interface{}{
		"name": a.inner.Name(),
		"type": a.inner.Type(),
	}
}
func (a *LegacyProviderAdapter) Close() error { return a.inner.Close() }

func (a *LegacyProviderAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	return a.inner.ChatCompletions(ctx, req)
}

func (a *LegacyProviderAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	_ = ctx
	_ = req
	ch := make(chan AeroStreamChunk)
	close(ch)
	return ch, fmt.Errorf("stream not implemented for legacy adapter %s", a.inner.Name())
}

// AsProvider exposes the legacy provider interface for router compatibility.
func (a *LegacyProviderAdapter) AsProvider() providers.Provider {
	return a.inner
}

// ResolveProvider retrieves a legacy-compatible provider from the registry by name.
func ResolveProvider(ctx context.Context, reg *ProviderRegistry, name string) (providers.Provider, bool) {
	_ = ctx
	adapter, ok := reg.Get(name)
	if !ok {
		return nil, false
	}
	if lp, ok := adapter.(*LegacyProviderAdapter); ok {
		return lp.AsProvider(), true
	}
	return nil, false
}
