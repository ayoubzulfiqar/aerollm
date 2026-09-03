package main

import (
	"context"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/realtime"
)

type edgeRealtimeProvider struct{}

func (e *edgeRealtimeProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan realtime.StreamChunk, error) {
	out := make(chan realtime.StreamChunk)
	go func() {
		defer close(out)
		out <- realtime.StreamChunk{Delta: "hi", Finish: true}
	}()
	return out, nil
}

func (e *edgeRealtimeProvider) Name() string { return "edge-realtime" }

func newEdgeRealtimeProvider() *edgeRealtimeProvider { return &edgeRealtimeProvider{} }
