package mesh_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
)

func TestInMemoryTransportDialAndClose(t *testing.T) {
	transport := mesh.NewInMemoryTransport(mesh.PeerID("local"))

	conn, err := transport.Dial(context.Background(), mesh.PeerDescriptor{ID: "remote"})
	if err != nil {
		t.Fatalf("unexpected dial error: %v", err)
	}
	if conn == nil {
		t.Fatal("expected conn, got nil")
	}

	if err := transport.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	if _, err := transport.Dial(context.Background(), mesh.PeerDescriptor{ID: "other"}); err == nil {
		t.Fatal("expected error after close, got nil")
	}
}

func TestInMemoryTransportSendDoesNotPanic(t *testing.T) {
	transport := mesh.NewInMemoryTransport(mesh.PeerID("local"))
	conn, err := transport.Dial(context.Background(), mesh.PeerDescriptor{ID: "remote"})
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	msg := mesh.Envelope{StateType: "test", Payload: json.RawMessage(`"payload"`)}
	if err := conn.Send(context.Background(), msg); err != nil {
		t.Fatalf("send error: %v", err)
	}

	if _, err := conn.Receive(context.Background()); err != nil {
		t.Fatalf("receive error: %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("transport close error: %v", err)
	}
}

func TestStreamConnRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	client := &mesh.StreamConn{Reader: bufio.NewReader(bytes.NewReader([]byte{})), Writer: &buf}

	payload, err := json.Marshal("hello")
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if err := client.WriteEnvelope(mesh.Envelope{StateType: "ping", Payload: payload}); err != nil {
		t.Fatalf("write error: %v", err)
	}

	server := &mesh.StreamConn{Reader: bufio.NewReader(&buf), Writer: bytes.NewBuffer(nil)}
	got, err := server.ReadEnvelope()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	if got.StateType != "ping" || string(got.Payload) != "\"hello\"" {
		t.Fatalf("unexpected envelope: %+v", got)
	}
}

func TestStreamConnReadEnvelopeError(t *testing.T) {
	server := &mesh.StreamConn{Reader: bufio.NewReader(strings.NewReader("not-json")), Writer: bytes.NewBuffer(nil)}

	if _, err := server.ReadEnvelope(); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestInMemoryTransportDialCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	transport := mesh.NewInMemoryTransport(mesh.PeerID("node"))

	if _, err := transport.Dial(ctx, mesh.PeerDescriptor{ID: "other"}); err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestInMemoryConnSendAfterClose(t *testing.T) {
	transport := mesh.NewInMemoryTransport(mesh.PeerID("a"))
	conn, err := transport.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"})
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	if err := conn.Send(context.Background(), mesh.Envelope{}); err == nil {
		t.Fatal("expected send error after close")
	}
}

func TestMeshGossipWorkerLifecycle(t *testing.T) {
	state := &stubMeshState{
		snapshot: []byte(`{}`),
	}
	transport := mesh.NewInMemoryTransport(mesh.PeerID("local"))

	worker := mesh.NewGossipWorker(mesh.GossipWorkerConfig{
		State:     state,
		Peers:     []mesh.PeerDescriptor{{ID: "peer"}},
		Transport: transport,
		Interval:  50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	worker.Start(ctx)

	<-ctx.Done()
	worker.Stop()

	if state.merged {
		t.Fatal("expected no merge attempt with empty peers")
	}
}

func TestMeshGossipWorkerSkipsNilTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	state := &stubMeshState{
		snapshot: []byte(`{}`),
	}

	worker := mesh.NewGossipWorker(mesh.GossipWorkerConfig{
		State:    state,
		Peers:    []mesh.PeerDescriptor{{ID: "peer"}},
		Interval: 50 * time.Millisecond,
	})

	worker.Start(ctx)

	<-ctx.Done()
	worker.Stop()

	if state.merged {
		t.Fatal("expected no merge attempt with nil transport")
	}
}

func TestInMemoryListenerAcceptContextCancelled(t *testing.T) {
	transport := mesh.NewInMemoryTransport(mesh.PeerID("local"))
	listener, err := transport.Listen(context.Background(), "")
	if err != nil {
		t.Fatalf("listen error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err = listener.Accept(ctx)
	if err == nil || err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("listener close error: %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("transport close error: %v", err)
	}
}

func TestMeshSyncWorkerStopsQuicklyWithNilDiscovery(t *testing.T) {
	state := &stubMeshState{}
	worker := mesh.NewSyncWorker(mesh.SyncWorkerConfig{State: state})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	worker.Start(ctx)

	<-ctx.Done()
	worker.Stop()
}

type stubMeshState struct {
	mu       sync.RWMutex
	snapshot []byte
	merged   bool
	msg      mesh.Envelope
}

func (s *stubMeshState) Merge(_ context.Context, _ json.RawMessage) error {
	s.mu.Lock()
	s.merged = true
	s.mu.Unlock()
	return nil
}

func (s *stubMeshState) LocalSnapshot(_ context.Context) (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot, nil
}

func (s *stubMeshState) Type() string { return "stub" }
