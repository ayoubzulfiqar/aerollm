package mesh_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
)

func acceptOne(t *testing.T, l mesh.PeerListener) mesh.PeerConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	return c
}

func recvOne(t *testing.T, c mesh.PeerConn) (mesh.Envelope, bool) {
	t.Helper()
	ch, err := c.Receive(context.Background())
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	select {
	case env, ok := <-ch:
		return env, ok
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for envelope")
		return mesh.Envelope{}, false
	}
}

func TestInMemoryNetworkRoundTripAndSenderIdentity(t *testing.T) {
	a, _, l := newListeningPair(t, "a", "b")

	conn, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	server := acceptOne(t, l)
	defer server.Close()

	// A peer cannot spoof From: the transport stamps the real sender.
	if err := conn.Send(context.Background(), mesh.Envelope{From: "mallory", StateType: "t", Payload: json.RawMessage(`1`)}); err != nil {
		t.Fatalf("send: %v", err)
	}
	env, ok := recvOne(t, server)
	if !ok || env.From != "a" || env.StateType != "t" || string(env.Payload) != "1" {
		t.Fatalf("unexpected envelope %+v ok=%v", env, ok)
	}

	if err := server.Send(context.Background(), mesh.Envelope{StateType: "reply"}); err != nil {
		t.Fatalf("reply: %v", err)
	}
	env, ok = recvOne(t, conn)
	if !ok || env.From != "b" || env.StateType != "reply" {
		t.Fatalf("unexpected reply %+v ok=%v", env, ok)
	}
}

func TestInMemoryTransportUnreachablePeers(t *testing.T) {
	standalone := mesh.NewInMemoryTransport("solo")
	defer standalone.Close()
	if _, err := standalone.Dial(context.Background(), mesh.PeerDescriptor{ID: "elsewhere"}); !errors.Is(err, mesh.ErrPeerUnreachable) {
		t.Fatalf("expected ErrPeerUnreachable, got %v", err)
	}
	if _, err := standalone.Dial(context.Background(), mesh.PeerDescriptor{}); err == nil {
		t.Fatal("expected error for empty peer id")
	}

	network := mesh.NewInMemoryNetwork()
	a, _ := network.Transport("a")
	b, _ := network.Transport("b")
	defer a.Close()
	defer b.Close()
	if _, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"}); !errors.Is(err, mesh.ErrPeerUnreachable) {
		t.Fatalf("expected not-listening peer to be unreachable, got %v", err)
	}
	if _, err := network.Transport("a"); err == nil {
		t.Fatal("expected duplicate attach to fail")
	}
	if _, err := network.Transport(""); err == nil {
		t.Fatal("expected empty id attach to fail")
	}
	_ = b.Close()
	if _, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"}); !errors.Is(err, mesh.ErrPeerUnreachable) {
		t.Fatalf("expected closed peer to be unreachable, got %v", err)
	}
}

func TestInMemoryTransportLoopback(t *testing.T) {
	tr := mesh.NewInMemoryTransport("self")
	defer tr.Close()
	l, err := tr.Listen(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Listen(context.Background(), ""); !errors.Is(err, mesh.ErrAlreadyListening) {
		t.Fatalf("expected ErrAlreadyListening, got %v", err)
	}
	c, err := tr.Dial(context.Background(), mesh.PeerDescriptor{ID: "self"})
	if err != nil {
		t.Fatalf("loopback dial: %v", err)
	}
	defer c.Close()
	s := acceptOne(t, l)
	defer s.Close()
	if err := c.Send(context.Background(), mesh.Envelope{StateType: "x"}); err != nil {
		t.Fatal(err)
	}
	if env, ok := recvOne(t, s); !ok || env.StateType != "x" {
		t.Fatalf("unexpected %+v", env)
	}
}

func TestInMemoryConnCloseDrainsThenCloses(t *testing.T) {
	a, _, l := newListeningPair(t, "a", "b")
	conn, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := conn.Send(context.Background(), mesh.Envelope{StateType: "m"}); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close()
	_ = conn.Close() // idempotent

	server := acceptOne(t, l)
	ch, _ := server.Receive(context.Background())
	n := 0
	for range ch {
		n++
	}
	if n != 3 {
		t.Fatalf("expected 3 buffered envelopes before close, got %d", n)
	}
	if err := server.Send(context.Background(), mesh.Envelope{}); err == nil {
		t.Fatal("expected send on closed pipe to fail")
	}
}

func TestInMemoryConnConcurrentSendAndCloseNoPanic(t *testing.T) {
	for round := 0; round < 20; round++ {
		a, _, l := newListeningPair(t, "a", "b")
		conn, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"})
		if err != nil {
			t.Fatal(err)
		}
		server := acceptOne(t, l)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
				for j := 0; j < 50; j++ {
					if conn.Send(ctx, mesh.Envelope{}) != nil {
						return
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = server.Close()
		}()
		wg.Wait()
		_ = conn.Close()
		_ = a.Close()
	}
}

func TestInMemoryConnSendRespectsContextWhenFull(t *testing.T) {
	a, _, _ := newListeningPair(t, "a", "b")
	conn, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var sendErr error
	for i := 0; i < 1000 && sendErr == nil; i++ {
		sendErr = conn.Send(ctx, mesh.Envelope{})
	}
	if !errors.Is(sendErr, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded once inbox is full, got %v", sendErr)
	}
}

func TestInMemoryTransportCloseClosesConnsAndListener(t *testing.T) {
	a, b, l := newListeningPair(t, "a", "b")
	conn, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	server := acceptOne(t, l)
	_ = b.Close()
	if _, err := l.Accept(context.Background()); !errors.Is(err, mesh.ErrListenerClosed) {
		t.Fatalf("expected ErrListenerClosed, got %v", err)
	}
	ch, _ := conn.Receive(context.Background())
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel")
		}
	case <-time.After(time.Second):
		t.Fatal("dialer conn not closed when remote transport closed")
	}
	_ = server.Close()
	if _, err := b.Listen(context.Background(), ""); !errors.Is(err, mesh.ErrTransportClosed) {
		t.Fatalf("expected ErrTransportClosed, got %v", err)
	}
}

func TestInMemoryListenerCloseClosesPendingConns(t *testing.T) {
	a, _, l := newListeningPair(t, "a", "b")
	conn, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	if err := conn.Send(context.Background(), mesh.Envelope{}); err == nil {
		t.Fatal("expected send on never-accepted conn to fail after listener close")
	}
	if _, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "b"}); !errors.Is(err, mesh.ErrPeerUnreachable) {
		t.Fatalf("expected unreachable after listener close, got %v", err)
	}
}

func TestStreamConnRejectsOversizedEnvelope(t *testing.T) {
	line := `{"StateType":"x","Payload":"` + strings.Repeat("a", 5000) + `"}` + "\n"
	s := &mesh.StreamConn{Reader: bufio.NewReaderSize(strings.NewReader(line), 16), MaxBytes: 1024}
	if _, err := s.ReadEnvelope(); !errors.Is(err, mesh.ErrEnvelopeTooLarge) {
		t.Fatalf("expected ErrEnvelopeTooLarge, got %v", err)
	}

	var buf bytes.Buffer
	w := &mesh.StreamConn{Writer: &buf, MaxBytes: 64}
	if err := w.WriteEnvelope(mesh.Envelope{Payload: json.RawMessage(`"` + strings.Repeat("b", 100) + `"`)}); !errors.Is(err, mesh.ErrEnvelopeTooLarge) {
		t.Fatalf("expected ErrEnvelopeTooLarge on write, got %v", err)
	}

	// Lines longer than the bufio buffer but within the cap are still read.
	small := &mesh.StreamConn{Reader: bufio.NewReaderSize(strings.NewReader(line), 16)}
	env, err := small.ReadEnvelope()
	if err != nil || env.StateType != "x" {
		t.Fatalf("expected long line to be read, got %+v %v", env, err)
	}
}

func TestStreamConnPartialLine(t *testing.T) {
	s := &mesh.StreamConn{Reader: bufio.NewReader(strings.NewReader(`{"StateType":"x"`))}
	if _, err := s.ReadEnvelope(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
	empty := &mesh.StreamConn{Reader: bufio.NewReader(strings.NewReader(""))}
	if _, err := empty.ReadEnvelope(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	var nilConn *mesh.StreamConn
	if _, err := nilConn.ReadEnvelope(); err == nil {
		t.Fatal("expected error for nil conn")
	}
}
