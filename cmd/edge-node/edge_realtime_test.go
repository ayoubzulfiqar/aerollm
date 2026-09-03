package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/realtime"
	"github.com/gorilla/websocket"
)

func TestEdgeRealtimeWSStreamsChunk(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(realtime.ServeWS(realtime.NewHub(), newEdgeRealtimeProvider())))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/edge/realtime/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v", err)
	}
	defer conn.Close()

	done := make(chan bool)
	var last string
	go func() {
		defer close(done)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			last = string(msg)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}

	if last == "" {
		t.Fatalf("expected websocket response, got none")
	}
	if !strings.Contains(last, "hi") && !strings.Contains(last, `"event":"finish"`) {
		t.Fatalf("expected streamed chunk or finish, got last=%q", last)
	}
}
