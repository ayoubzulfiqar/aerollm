package main

import (
	"context"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/realtime"
)

// edgeRealtimeProvider is the local realtime streamer exposed on
// /v1/edge/realtime/ws. It emits a content delta followed by a separate
// finish marker. realtime.ServeWS stops forwarding at the first chunk whose
// Finish flag is set and drops that chunk's Delta, so the delta and the finish
// marker must be sent as two chunks or the content is never delivered.
type edgeRealtimeProvider struct{}

func (e *edgeRealtimeProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan realtime.StreamChunk, error) {
	out := make(chan realtime.StreamChunk)
	go func() {
		defer close(out)
		for _, chunk := range []realtime.StreamChunk{
			{Delta: "hi", Provider: e.Name()},
			{Finish: true, Provider: e.Name()},
		} {
			// Never block forever: if the consumer has gone away (client
			// disconnected, session cancelled) the context is cancelled and
			// the goroutine exits instead of leaking on an unbuffered send.
			select {
			case out <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (e *edgeRealtimeProvider) Name() string { return "edge-realtime" }

func newEdgeRealtimeProvider() *edgeRealtimeProvider { return &edgeRealtimeProvider{} }
