package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/realtime"
	"github.com/gorilla/websocket"
)

// TestEdgeRealtimeWSStreamsChunk reads the realtime stream on the test
// goroutine (no shared state with a background reader) with a hard read
// deadline, and requires both the content delta and the finish event.
func TestEdgeRealtimeWSStreamsChunk(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(realtime.ServeWS(realtime.NewHub(), newEdgeRealtimeProvider())))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/edge/realtime/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v", err)
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	var msgs []string
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read failed after %d messages %q: %v", len(msgs), msgs, err)
		}
		msgs = append(msgs, string(msg))
		if strings.Contains(string(msg), `"event":"finish"`) {
			break
		}
	}

	if len(msgs) < 2 {
		t.Fatalf("expected a delta chunk before finish, got %q", msgs)
	}
	if !strings.Contains(msgs[0], `"Delta":"hi"`) {
		t.Fatalf("expected first message to carry the delta, got %q", msgs[0])
	}
}

// TestEdgeRealtimeRejectsCrossSiteOrigin checks the edge route refuses a
// WebSocket handshake from a foreign browser origin (CSWSH) but accepts
// same-host and non-browser clients.
func TestEdgeRealtimeRejectsCrossSiteOrigin(t *testing.T) {
	_, srv := newTestEdge(t, func(c *edgeConfig) { c.allowedOrigins = []string{"https://app.example"} })
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/edge/realtime/ws"

	hdr := http.Header{"Origin": []string{"https://evil.example"}}
	if conn, resp, err := websocket.DefaultDialer.Dial(wsURL, hdr); err == nil {
		conn.Close()
		t.Fatal("expected cross-site handshake to be rejected")
	} else if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %v", resp)
	}

	for _, origin := range []string{"", srv.URL, "https://app.example"} {
		h := http.Header{}
		if origin != "" {
			h.Set("Origin", origin)
		}
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, h)
		if err != nil {
			t.Fatalf("origin %q rejected: %v", origin, err)
		}
		conn.Close()
	}
}

// TestEdgeRealtimeProviderStopsOnCancel verifies the provider goroutine exits
// when the consumer abandons the stream (previously it blocked forever on an
// unbuffered send and leaked).
func TestEdgeRealtimeProviderStopsOnCancel(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := newEdgeRealtimeProvider().StreamChatCompletions(ctx, nil)
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		cancel() // consumer never reads
		// Drain until closed; must terminate promptly.
		deadline := time.After(2 * time.Second)
	drain:
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					break drain
				}
			case <-deadline:
				t.Fatal("provider channel not closed after cancel")
			}
		}
	}
	// Allow the scheduler to reap exited goroutines.
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+5 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}
