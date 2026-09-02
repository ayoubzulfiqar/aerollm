package realtime

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

type fakeProvider struct {
	chunks []StreamChunk
}

func (f *fakeProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan StreamChunk, error) {
	out := make(chan StreamChunk)
	go func() {
		defer close(out)
		for _, c := range f.chunks {
			select {
			case out <- c:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (f *fakeProvider) Name() string { return "fake" }

func TestNewHubAndRegisterUnregister(t *testing.T) {
	hub := NewHub()
	if hub.ActiveCount() != 0 {
		t.Fatalf("expected 0 sessions, got %d", hub.ActiveCount())
	}
	s := &StreamSession{SessionID: "s1"}
	hub.Register(s)
	if hub.ActiveCount() != 1 {
		t.Fatalf("expected 1 session, got %d", hub.ActiveCount())
	}
	hub.Unregister("s1")
	if hub.ActiveCount() != 0 {
		t.Fatalf("expected 0 sessions after unregister, got %d", hub.ActiveCount())
	}
}

func TestBargeInDetectorAnalyzeChunk(t *testing.T) {
	called := false
	det := NewBargeInDetector(1.0, 1, func(e BargeInEvent) { called = true })
	// High energy chunk should trigger barge-in.
	det.AnalyzeChunk(context.Background(), "s1", []byte{0x7F, 0x80})
	if !called {
		t.Fatal("expected barge-in callback for high energy chunk")
	}
	// Low energy should not trigger.
	called = false
	det.AnalyzeChunk(context.Background(), "s1", []byte{0x00, 0x00})
	if called {
		t.Fatal("did not expect barge-in for silent chunk")
	}
}

func TestAveragePCMEnergy(t *testing.T) {
	if averagePCMEnergy(nil) != 0 {
		t.Fatal("expected 0 energy for nil slice")
	}
	// Constant max amplitude should give max energy.
	energy := averagePCMEnergy([]byte{0x80, 0x80})
	if energy == 0 {
		t.Fatal("expected non-zero energy")
	}
}

func TestStreamSessionCancel(t *testing.T) {
	s := &StreamSession{}
	if s.Cancel != nil {
		t.Fatal("expected nil cancel for empty session")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.Ctx = ctx
	s.Cancel = cancel
	s.Cancel()
	select {
	case <-ctx.Done():
		// ok
	default:
		t.Fatal("expected context to be canceled")
	}
}

func TestHubCancelAll(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	hub.Register(&StreamSession{SessionID: "x", Ctx: ctx, Cancel: cancel})
	hub.CancelAll()
	select {
	case <-ctx.Done():
		// ok
	default:
		t.Fatal("expected session context to be canceled")
	}
}
