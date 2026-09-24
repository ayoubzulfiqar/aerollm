package main

import (
	"context"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/providers/universal"
)

// universalAdapterBridge wraps any universal.ProviderAdapter and exposes it as
// a providers.Provider + providers.MultiEndpointProvider (+ StreamingProvider
// when the adapter can stream) so it can be used by the API handler. This
// avoids the Type()/Health() signature conflict between the universal and
// legacy provider interfaces.
type universalAdapterBridge struct {
	inner universal.ProviderAdapter
}

var (
	_ providers.MultiEndpointProvider = (*universalAdapterBridge)(nil)
	_ providers.StreamingProvider     = (*universalAdapterBridge)(nil)
)

func (b *universalAdapterBridge) Name() string { return b.inner.Name() }
func (b *universalAdapterBridge) Type() providers.ProviderType {
	return providers.ProviderType(b.inner.Type())
}
func (b *universalAdapterBridge) ProviderType() providers.ProviderType {
	return providers.ProviderType(b.inner.Type())
}
func (b *universalAdapterBridge) Health() providers.ProviderHealth {
	healthy := true
	if hv, ok := b.inner.Health()["healthy"].(bool); ok {
		healthy = hv
	}
	return providers.ProviderHealth{Name: b.inner.Name(), Type: b.ProviderType(), Healthy: healthy}
}
func (b *universalAdapterBridge) Close() error { return b.inner.Close() }

func (b *universalAdapterBridge) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	return b.inner.ChatCompletions(ctx, req)
}

// StreamChatCompletions streams when the wrapped adapter supports it and
// otherwise reports providers.ErrStreamingNotSupported so the gateway can
// fall back to a non-streaming call.
func (b *universalAdapterBridge) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	if sp, ok := b.inner.(providers.StreamingProvider); ok {
		return sp.StreamChatCompletions(ctx, req)
	}
	return nil, providers.ErrStreamingNotSupported
}

func (b *universalAdapterBridge) Embeddings(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error) {
	if mp, ok := b.inner.(interface {
		Embeddings(context.Context, *models.EmbeddingRequest) (*models.EmbeddingResponse, error)
	}); ok {
		return mp.Embeddings(ctx, req)
	}
	return nil, fmt.Errorf("embeddings not supported by provider %s", b.inner.Name())
}

func (b *universalAdapterBridge) ImageGenerations(ctx context.Context, req *models.ImageRequest) (*models.ImageResponse, error) {
	if mp, ok := b.inner.(interface {
		ImageGenerations(context.Context, *models.ImageRequest) (*models.ImageResponse, error)
	}); ok {
		return mp.ImageGenerations(ctx, req)
	}
	return nil, fmt.Errorf("image generation not supported by provider %s", b.inner.Name())
}

func (b *universalAdapterBridge) AudioTranscriptions(ctx context.Context, req *models.AudioRequest) (*models.AudioResponse, error) {
	if mp, ok := b.inner.(interface {
		AudioTranscriptions(context.Context, *models.AudioRequest) (*models.AudioResponse, error)
	}); ok {
		return mp.AudioTranscriptions(ctx, req)
	}
	return nil, fmt.Errorf("audio transcription not supported by provider %s", b.inner.Name())
}

func (b *universalAdapterBridge) Responses(ctx context.Context, req *models.ResponsesRequest) (*models.ResponsesResponse, error) {
	if mp, ok := b.inner.(interface {
		Responses(context.Context, *models.ResponsesRequest) (*models.ResponsesResponse, error)
	}); ok {
		return mp.Responses(ctx, req)
	}
	return nil, fmt.Errorf("responses not supported by provider %s", b.inner.Name())
}
