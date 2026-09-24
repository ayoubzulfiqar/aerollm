package universal

import (
	"context"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// LegacyProviderAdapter bridges a legacy providers.Provider into the universal registry.
type LegacyProviderAdapter struct {
	inner      providers.Provider
	StreamFunc func(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error)
}

// NewLegacyProviderAdapter wraps a legacy provider.
func NewLegacyProviderAdapter(p providers.Provider) *LegacyProviderAdapter {
	return &LegacyProviderAdapter{inner: p}
}

func (a *LegacyProviderAdapter) Name() string { return a.inner.Name() }
func (a *LegacyProviderAdapter) Type() string { return string(a.inner.Type()) }

// Health reports the wrapped provider's health.
func (a *LegacyProviderAdapter) Health() map[string]interface{} {
	h := a.inner.Health()
	return map[string]interface{}{
		"name":         a.inner.Name(),
		"type":         string(a.inner.Type()),
		"healthy":      h.Healthy && !h.CircuitOpen,
		"latency_ms":   h.LatencyMs,
		"failures":     h.Failures,
		"last_checked": h.LastChecked,
	}
}

func (a *LegacyProviderAdapter) Close() error { return a.inner.Close() }

func (a *LegacyProviderAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	return a.inner.ChatCompletions(ctx, req)
}

// StreamChatCompletions delegates to the wrapped provider when it implements
// providers.StreamingProvider; otherwise it returns
// providers.ErrStreamingNotSupported so callers fall back to non-streaming.
func (a *LegacyProviderAdapter) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	if sp, ok := a.inner.(providers.StreamingProvider); ok {
		return sp.StreamChatCompletions(ctx, req)
	}
	return nil, providers.ErrStreamingNotSupported
}

// Stream streams in the legacy chunk format, using StreamFunc when set.
func (a *LegacyProviderAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	if a.StreamFunc != nil {
		return a.StreamFunc(ctx, req)
	}
	if sp, ok := a.inner.(providers.StreamingProvider); ok {
		ch, err := sp.StreamChatCompletions(ctx, req)
		if err != nil {
			return nil, err
		}
		return toAeroStream(ctx, a.inner.Name(), ch), nil
	}
	return nil, fmt.Errorf("streaming unavailable for legacy adapter %s: %w", a.inner.Name(), providers.ErrStreamingNotSupported)
}

// AsProvider exposes the legacy provider interface for router compatibility.
func (a *LegacyProviderAdapter) AsProvider() providers.Provider {
	return a.inner
}

// ResolveProvider retrieves a legacy-compatible provider from the registry by name.
func ResolveProvider(ctx context.Context, reg *ProviderRegistry, name string) (providers.Provider, bool) {
	_ = ctx
	if reg == nil {
		return nil, false
	}
	adapter, ok := reg.Get(name)
	if !ok {
		return nil, false
	}
	if lp, ok := adapter.(*LegacyProviderAdapter); ok {
		return lp.AsProvider(), true
	}
	return nil, false
}
