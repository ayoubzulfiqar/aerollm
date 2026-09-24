package providers

import (
	"context"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// StreamingProvider is implemented by providers that can stream chat
// completions token-by-token.
//
// Contract:
//   - On success the returned channel yields chunks in order and is closed by
//     the provider when the upstream stream ends.
//   - A mid-stream failure is reported as a final chunk with Err set, after
//     which the channel is closed.
//   - When ctx is cancelled the provider stops reading upstream, closes the
//     channel and releases its goroutine; it must never block forever on send
//     (select on ctx.Done() when sending).
//   - An error returned directly (nil channel) means the stream never started,
//     so the caller may safely fall back to another provider.
type StreamingProvider interface {
	StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error)
}
