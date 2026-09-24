package router

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// fnProvider is a configurable providers.Provider for tests.
type fnProvider struct {
	name    string
	latency float64
	calls   atomic.Int64
	fn      func(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error)
}

func (f *fnProvider) Name() string                 { return f.name }
func (f *fnProvider) Type() providers.ProviderType { return providers.ProviderOpenAI }
func (f *fnProvider) Close() error                 { return nil }
func (f *fnProvider) Health() providers.ProviderHealth {
	return providers.ProviderHealth{Name: f.name, Healthy: true, LatencyMs: f.latency}
}
func (f *fnProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	f.calls.Add(1)
	if f.fn != nil {
		return f.fn(ctx, req)
	}
	model := ""
	if req != nil {
		model = req.Model
	}
	return &models.LLMResponse{Model: model, Usage: &models.Usage{TotalTokens: 10}}, nil
}

// modelProvider declares its models and a default model.
type modelProvider struct {
	*fnProvider
	models []string
	def    string
}

func (m *modelProvider) SupportsModel(model string) bool {
	for _, s := range m.models {
		if s == model {
			return true
		}
	}
	return false
}

func (m *modelProvider) DefaultModel() string { return m.def }

// defaultOnly has a default model but does not declare supported models.
type defaultOnly struct {
	*fnProvider
	def string
}

func (d *defaultOnly) DefaultModel() string { return d.def }

// streamProvider adds streaming to fnProvider.
type streamProvider struct {
	*fnProvider
	streamCalls atomic.Int64
	stream      func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error)
}

func (s *streamProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	s.streamCalls.Add(1)
	return s.stream(ctx, req)
}

// chunksStream returns a stream func that emits the given chunks, honoring ctx.
func chunksStream(chunks ...models.StreamChunk) func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	return func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
		ch := make(chan models.StreamChunk)
		go func() {
			defer close(ch)
			for _, c := range chunks {
				select {
				case ch <- c:
				case <-ctx.Done():
					return
				}
			}
		}()
		return ch, nil
	}
}

func upstreamErr(status int) error {
	return &providers.UpstreamError{Provider: "test", StatusCode: status, Message: "boom"}
}

func failWith(err error) func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
	return func(context.Context, *models.LLMRequest) (*models.LLMResponse, error) { return nil, err }
}

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", msg)
		}
		time.Sleep(time.Millisecond)
	}
}

func strp(s string) *string { return &s }
