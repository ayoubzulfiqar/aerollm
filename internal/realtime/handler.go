package realtime

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
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

// clientEvent is an inbound control/request frame.
//
//	{"type":"request","model":"gpt-4o","messages":[...]}      start a stream
//	{"type":"request","request":{...LLMRequest...}}           start a stream
//	{"type":"chat","prompt":"hello"}                           start a stream
//	{"type":"barge-in"} or {"action":"cancel"}                 cancel in-flight stream
//	{"type":"ping"}                                            -> {"event":"pong"}
type clientEvent struct {
	Type    string             `json:"type"`
	Action  string             `json:"action"`
	Prompt  string             `json:"prompt"`
	Request *models.LLMRequest `json:"request"`
}

type serverEvent struct {
	Event     string `json:"event"`
	Message   string `json:"message,omitempty"`
	SessionID string `json:"session_id,omitempty"`
}

// ServeWS upgrades HTTP connections to WebSocket and manages the bidirectional
// streaming lifecycle using DefaultConfig (allowed cross-origin browser
// origins come from AEROLLM_WS_ALLOWED_ORIGINS).
func ServeWS(hub *Hub, provider ProviderStreamer) http.HandlerFunc {
	return ServeWSWithConfig(hub, provider, DefaultConfig())
}

// ServeWSWithConfig is ServeWS with explicit configuration. Zero-valued
// durations/limits in cfg are replaced by safe defaults; AutoStart and
// AllowedOrigins are used as given.
func ServeWSWithConfig(hub *Hub, provider ProviderStreamer, cfg Config) http.HandlerFunc {
	cfg = cfg.withDefaults()
	if hub == nil {
		hub = NewHub()
	}
	upgrader := websocket.Upgrader{
		HandshakeTimeout: 10 * time.Second,
		CheckOrigin:      OriginChecker(cfg.AllowedOrigins),
		Error: func(w http.ResponseWriter, r *http.Request, status int, reason error) {
			writeHTTPError(w, status, http.StatusText(status))
		},
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if provider == nil {
			writeHTTPError(w, http.StatusServiceUnavailable, "realtime provider not configured")
			return
		}
		if cfg.MaxSessions > 0 && hub.ActiveCount() >= cfg.MaxSessions {
			writeHTTPError(w, http.StatusServiceUnavailable, "too many realtime sessions")
			return
		}
		id, err := newSessionID()
		if err != nil {
			writeHTTPError(w, http.StatusInternalServerError, "internal error")
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return // the upgrader already replied
		}
		defer conn.Close()

		// The request context is not reliably cancelled for hijacked
		// connections; the read loop detects disconnects instead.
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		session := &StreamSession{
			Conn:      conn,
			Ctx:       ctx,
			Cancel:    cancel,
			Provider:  provider,
			SessionID: id,
			Model:     r.URL.Query().Get("model"),
			StartedAt: time.Now().UTC(),
			cfg:       cfg,
		}
		if !hub.tryRegister(session, cfg.MaxSessions) {
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "too many sessions"),
				time.Now().Add(cfg.WriteWait))
			return
		}
		defer hub.unregisterSession(session)

		conn.SetReadLimit(cfg.MaxMessageBytes)
		_ = conn.SetReadDeadline(time.Now().Add(cfg.PongWait))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(cfg.PongWait))
		})

		// Teardown: cancelling the session context (client gone, write
		// failure, Hub.CancelAll) closes the connection, which unblocks the
		// read loop below; then wait for background goroutines.
		defer session.waitBackground()
		defer cancel()

		session.wg.Add(2)
		go session.closeOnCancel()
		go session.pingLoop()

		if cfg.AutoStart {
			prompt := r.URL.Query().Get("prompt")
			if prompt != "" || !requiresMessages(provider) {
				req := &models.LLMRequest{Model: session.Model, Messages: []models.Message{}}
				if prompt != "" {
					req.Messages = append(req.Messages, models.Message{Role: models.RoleUser, Content: &prompt})
				}
				session.startStream(req)
			}
		}

		session.readLoop()
	}
}

func requiresMessages(p ProviderStreamer) bool {
	rm, ok := p.(interface{ RequiresMessages() bool })
	return ok && rm.RequiresMessages()
}

func (s *StreamSession) readLoop() {
	for {
		msgType, msg, err := s.Conn.ReadMessage()
		if err != nil {
			return
		}
		_ = s.Conn.SetReadDeadline(time.Now().Add(s.cfg.PongWait))
		switch msgType {
		case websocket.BinaryMessage:
			// Inbound audio while generating = barge-in.
			if isBargeIn(msg) {
				s.bargeIn()
			}
		case websocket.TextMessage:
			s.handleText(msg)
		}
		if s.Ctx.Err() != nil {
			return
		}
	}
}

func (s *StreamSession) handleText(msg []byte) {
	var evt clientEvent
	if err := json.Unmarshal(msg, &evt); err != nil {
		s.writeEvent(serverEvent{Event: "error", Message: "invalid message: expected a JSON object"})
		return
	}
	if evt.Type == "barge-in" || evt.Action == "cancel" {
		s.bargeIn()
		return
	}
	switch evt.Type {
	case "ping":
		s.writeEvent(serverEvent{Event: "pong"})
	case "request", "response.create", "chat":
		req := evt.Request
		if req == nil {
			// Top-level LLMRequest fields (model, messages, temperature, ...).
			var top models.LLMRequest
			if err := json.Unmarshal(msg, &top); err != nil {
				s.writeEvent(serverEvent{Event: "error", Message: "invalid request"})
				return
			}
			req = &top
		}
		if strings.TrimSpace(evt.Prompt) != "" {
			p := evt.Prompt
			req.Messages = append(req.Messages, models.Message{Role: models.RoleUser, Content: &p})
		}
		if len(req.Messages) == 0 {
			s.writeEvent(serverEvent{Event: "error", Message: "request has no messages"})
			return
		}
		s.mu.Lock()
		if req.Model == "" {
			req.Model = s.Model
		} else {
			s.Model = req.Model
		}
		s.mu.Unlock()
		s.startStream(req)
	case "":
		s.writeEvent(serverEvent{Event: "error", Message: "missing event type"})
	default:
		s.writeEvent(serverEvent{Event: "error", Message: "unsupported event type"})
	}
}

// bargeIn cancels the in-flight stream (if any) and acknowledges it. The
// connection stays open for the next request.
func (s *StreamSession) bargeIn() {
	s.stopStream()
	s.writeEvent(serverEvent{Event: "barge-in"})
}

// startStream cancels any in-flight stream, waits (bounded) for it to exit and
// then starts a new provider stream for req.
func (s *StreamSession) startStream(req *models.LLMRequest) {
	s.stopStream()
	if s.Ctx.Err() != nil {
		return
	}
	streamCtx, streamCancel := context.WithCancel(s.Ctx)
	done := make(chan struct{})
	s.streamMu.Lock()
	s.streamCancel = streamCancel
	s.streamDone = done
	s.streamMu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(done)
		defer streamCancel()
		s.pump(streamCtx, req)
	}()
}

// stopStream cancels the in-flight stream and waits (bounded) for its
// goroutine to exit.
func (s *StreamSession) stopStream() {
	s.streamMu.Lock()
	cancel, done := s.streamCancel, s.streamDone
	s.streamCancel, s.streamDone = nil, nil
	s.streamMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(s.cfg.ShutdownWait):
		}
	}
}

// pump forwards provider chunks to the client until the stream finishes, the
// provider fails or ctx is cancelled (barge-in / disconnect).
func (s *StreamSession) pump(ctx context.Context, req *models.LLMRequest) {
	chunks, err := s.Provider.StreamChatCompletions(ctx, req)
	if err != nil || chunks == nil {
		if ctx.Err() == nil {
			s.writeStreamEvent(ctx, serverEvent{Event: "error", Message: "stream failed"})
		}
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-chunks:
			if !ok {
				// Upstream ended without an explicit finish marker.
				s.writeStreamEvent(ctx, serverEvent{Event: "finish"})
				return
			}
			if chunk.Err != nil {
				s.writeStreamEvent(ctx, serverEvent{Event: "error", Message: "stream failed"})
				return
			}
			if chunk.Finish {
				if chunk.Delta != "" || len(chunk.Data) > 0 {
					if !s.writeStreamJSON(ctx, chunk) {
						return
					}
				}
				s.writeStreamEvent(ctx, serverEvent{Event: "finish"})
				return
			}
			if !s.writeStreamJSON(ctx, chunk) {
				return
			}
		}
	}
}

// writeStreamJSON writes v on behalf of a stream. The stream context is
// re-checked under the write lock so nothing from a cancelled stream is sent
// after a barge-in acknowledgement.
func (s *StreamSession) writeStreamJSON(ctx context.Context, v interface{}) bool {
	data, err := json.Marshal(v)
	if err != nil {
		return false
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	return s.writeLocked(data)
}

func (s *StreamSession) writeStreamEvent(ctx context.Context, evt serverEvent) bool {
	return s.writeStreamJSON(ctx, evt)
}

// writeEvent writes a control event on behalf of the read loop.
func (s *StreamSession) writeEvent(evt serverEvent) bool {
	data, err := json.Marshal(evt)
	if err != nil {
		return false
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeLocked(data)
}

// writeLocked performs one deadline-bounded text write; the caller holds
// writeMu (gorilla/websocket supports a single concurrent writer). A failed
// write tears the session down.
func (s *StreamSession) writeLocked(data []byte) bool {
	_ = s.Conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteWait))
	if err := s.Conn.WriteMessage(websocket.TextMessage, data); err != nil {
		s.cancelSession()
		return false
	}
	return true
}

// pingLoop keeps the connection alive and detects dead peers. WriteControl is
// safe to call concurrently with other writes.
func (s *StreamSession) pingLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.PingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-s.Ctx.Done():
			return
		case <-ticker.C:
			if err := s.Conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.cfg.WriteWait)); err != nil {
				s.cancelSession()
				return
			}
		}
	}
}

// closeOnCancel closes the connection once the session context ends so the
// blocking read loop returns (e.g. after Hub.CancelAll or a write failure).
func (s *StreamSession) closeOnCancel() {
	defer s.wg.Done()
	<-s.Ctx.Done()
	_ = s.Conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(s.cfg.WriteWait))
	_ = s.Conn.Close()
}

// waitBackground waits (bounded) for the stream, ping and close goroutines.
func (s *StreamSession) waitBackground() {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(s.cfg.ShutdownWait):
	}
}

func writeHTTPError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{"message": msg, "code": status},
	})
}
