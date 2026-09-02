package realtime

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/gorilla/websocket"
)

// isBargeIn reports whether an inbound WebSocket frame should cancel the current provider stream.
// It accepts JSON events (`type=barge-in` or `action=cancel`) and non-empty binary frames.
func isBargeIn(msg []byte) bool {
	if len(msg) == 0 {
		return false
	}
	if msg[0] == '{' {
		var evt map[string]interface{}
		if json.Unmarshal(msg, &evt) == nil {
			if t, ok := evt["type"].(string); ok && t == "barge-in" {
				return true
			}
			if action, ok := evt["action"].(string); ok && action == "cancel" {
				return true
			}
		}
		return false
	}
	return true
}

// ServeWS upgrades HTTP connections to WebSocket and manages the bidirectional streaming lifecycle.
func ServeWS(hub *Hub, provider ProviderStreamer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		ctx, cancel := context.WithCancel(r.Context())
		session := &StreamSession{
			Conn:      conn,
			Ctx:       ctx,
			Cancel:    cancel,
			Provider:  provider,
			SessionID: time.Now().UTC().Format("20060102T150405.000Z"),
			StartedAt: time.Now().UTC(),
		}
		hub.Register(session)
		defer hub.Unregister(session.SessionID)
		defer cancel()

		// Pump provider streaming output to the client.
		errCh := make(chan error, 1)
		go func() {
			chunks, err := provider.StreamChatCompletions(ctx, &models.LLMRequest{
				Model:    session.Model,
				Messages: []models.Message{},
			})
			if err != nil {
				errCh <- err
				return
			}
			for chunk := range chunks {
				if chunk.Finish {
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"finish"}`))
					return
				}
				msg, _ := json.Marshal(chunk)
				if writeErr := conn.WriteMessage(websocket.TextMessage, msg); writeErr != nil {
					errCh <- writeErr
					return
				}
			}
		}()

		// Handle incoming client messages for barge-in / control.
		for {
			msgType, msg, readErr := conn.ReadMessage()
			if readErr != nil {
				return
			}
			if msgType == websocket.TextMessage {
				var evt map[string]interface{}
				if json.Unmarshal(msg, &evt) != nil {
					continue
				}
				if t, ok := evt["type"].(string); ok && t == "ping" {
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"pong"}`))
				}
				if isBargeIn(msg) {
					cancel()
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"barge-in"}`))
					return
				}
			}
		}
	}
}
