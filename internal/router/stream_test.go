package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

func TestStreamForwardsChunksAndRecordsUsage(t *testing.T) {
	sp := &streamProvider{fnProvider: &fnProvider{name: "s"}, stream: chunksStream(
		models.StreamChunk{ID: "1", Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "he"}}}},
		models.StreamChunk{ID: "2", Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "llo"}}}},
		models.StreamChunk{ID: "3", Usage: &models.Usage{TotalTokens: 42}},
	)}
	cb := NewCircuitBreaker(sp, 1, time.Minute)
	ch, err := cb.StreamChatCompletions(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for c := range ch {
		ids = append(ids, c.ID)
	}
	if len(ids) != 3 || ids[0] != "1" || ids[2] != "3" {
		t.Fatalf("chunks not forwarded in order: %v", ids)
	}
	waitFor(t, time.Second, func() bool { return cb.Inflight() == 0 }, "inflight released")
	cb.usage.mu.Lock()
	toks := cb.usage.TokenCountMinute
	cb.usage.mu.Unlock()
	if toks != 42 {
		t.Fatalf("expected 42 tokens recorded, got %d", toks)
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected closed, got %v", cb.State())
	}
}

func TestStreamErrChunkCountsFailure(t *testing.T) {
	sp := &streamProvider{fnProvider: &fnProvider{name: "s"}, stream: chunksStream(
		models.StreamChunk{ID: "1"},
		models.StreamChunk{Err: upstreamErr(500)},
	)}
	cb := NewCircuitBreaker(sp, 1, time.Minute)
	ch, err := cb.StreamChatCompletions(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var sawErr bool
	for c := range ch {
		if c.Err != nil {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatal("error chunk must be forwarded")
	}
	// State is recorded before the channel is closed.
	if cb.State() != StateOpen {
		t.Fatalf("retryable mid-stream error must count as failure, got %v", cb.State())
	}
}

func TestStreamNonRetryableErrChunkIsNeutral(t *testing.T) {
	sp := &streamProvider{fnProvider: &fnProvider{name: "s"}, stream: chunksStream(models.StreamChunk{Err: upstreamErr(400)})}
	cb := NewCircuitBreaker(sp, 1, time.Minute)
	ch, err := cb.StreamChatCompletions(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected closed, got %v", cb.State())
	}
}

func TestStreamStartErrorCountsFailure(t *testing.T) {
	sp := &streamProvider{fnProvider: &fnProvider{name: "s"}, stream: func(context.Context, *models.LLMRequest) (<-chan models.StreamChunk, error) {
		return nil, upstreamErr(503)
	}}
	cb := NewCircuitBreaker(sp, 1, time.Minute)
	if _, err := cb.StreamChatCompletions(context.Background(), &models.LLMRequest{}); providers.StatusCode(err) != 503 {
		t.Fatalf("expected 503, got %v", err)
	}
	if cb.State() != StateOpen || cb.Inflight() != 0 {
		t.Fatalf("expected open with no inflight, got %v / %d", cb.State(), cb.Inflight())
	}
	if _, err := cb.StreamChatCompletions(context.Background(), &models.LLMRequest{}); !errors.Is(err, providers.ErrCircuitOpen) {
		t.Fatalf("expected circuit open, got %v", err)
	}
}

func TestStreamNotSupported(t *testing.T) {
	cb := NewCircuitBreaker(&fnProvider{name: "plain"}, 1, time.Minute)
	ch, err := cb.StreamChatCompletions(context.Background(), &models.LLMRequest{})
	if ch != nil || !errors.Is(err, providers.ErrStreamingNotSupported) {
		t.Fatalf("expected ErrStreamingNotSupported, got %v", err)
	}
	if cb.State() != StateClosed || cb.Inflight() != 0 {
		t.Fatal("unsupported streaming must not affect breaker")
	}
}

func TestStreamCtxCancelNoLeak(t *testing.T) {
	producerDone := make(chan struct{})
	sp := &streamProvider{fnProvider: &fnProvider{name: "s"}, stream: func(ctx context.Context, _ *models.LLMRequest) (<-chan models.StreamChunk, error) {
		ch := make(chan models.StreamChunk)
		go func() {
			defer close(producerDone)
			defer close(ch)
			for {
				select {
				case ch <- models.StreamChunk{ID: "x"}:
				case <-ctx.Done():
					return
				}
			}
		}()
		return ch, nil
	}}
	cb := NewCircuitBreaker(sp, 1, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := cb.StreamChatCompletions(ctx, &models.LLMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	<-ch // read one chunk, then stop consuming and cancel
	cancel()

	closed := make(chan struct{})
	go func() {
		for range ch {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("wrapper did not close its channel after cancellation")
	}
	select {
	case <-producerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("producer goroutine leaked")
	}
	waitFor(t, time.Second, func() bool { return cb.Inflight() == 0 }, "inflight released")
	if cb.State() != StateClosed {
		t.Fatalf("cancellation must not count as failure, got %v", cb.State())
	}
}

func TestStreamConsumerStopsReadingAndCancels(t *testing.T) {
	// Consumer never reads again after cancel: wrapper must not block forever.
	producerDone := make(chan struct{})
	sp := &streamProvider{fnProvider: &fnProvider{name: "s"}, stream: func(ctx context.Context, _ *models.LLMRequest) (<-chan models.StreamChunk, error) {
		ch := make(chan models.StreamChunk)
		go func() {
			defer close(producerDone)
			defer close(ch)
			for {
				select {
				case ch <- models.StreamChunk{ID: "x"}:
				case <-ctx.Done():
					return
				}
			}
		}()
		return ch, nil
	}}
	cb := NewCircuitBreaker(sp, 1, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := cb.StreamChatCompletions(ctx, &models.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond) // wrapper is now blocked sending to us
	cancel()
	select {
	case <-producerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("producer goroutine leaked")
	}
	waitFor(t, 2*time.Second, func() bool { return cb.Inflight() == 0 }, "wrapper goroutine exit")
}
