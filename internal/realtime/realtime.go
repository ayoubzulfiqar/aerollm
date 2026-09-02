package realtime

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// StreamSession represents a single WebSocket streaming session.
type StreamSession struct {
	Conn      *websocket.Conn
	Ctx       context.Context
	Cancel    context.CancelFunc
	Provider  ProviderStreamer
	SessionID string
	Model     string
	StartedAt time.Time
	mu        sync.RWMutex
}

// ProviderStreamer defines the contract for a provider that can stream responses.
type ProviderStreamer interface {
	StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan StreamChunk, error)
	Name() string
}

// StreamChunk represents a single chunk in a streaming response.
type StreamChunk struct {
	Delta       string
	Finish      bool
	Provider    string
	ContentType string
	Data        []byte
}

// Hub tracks active realtime sessions and supports broadcast/kill-all for control-plane events.
type Hub struct {
	sessions map[string]*StreamSession
	mu       sync.RWMutex
}

// NewHub creates a new realtime hub.
func NewHub() *Hub {
	return &Hub{sessions: make(map[string]*StreamSession)}
}

// Register adds a session.
func (h *Hub) Register(s *StreamSession) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sessions[s.SessionID] = s
}

// Unregister removes a session.
func (h *Hub) Unregister(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.sessions, id)
}

// CancelAll sends a cancel signal to all active provider streams. Use for graceful shutdown or broadcast interrupts.
func (h *Hub) CancelAll() {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, s := range h.sessions {
		s.mu.RLock()
		cancel := s.Cancel
		s.mu.RUnlock()
		if cancel != nil {
			cancel()
		}
	}
}

// ActiveCount returns the number of active sessions.
func (h *Hub) ActiveCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.sessions)
}

// BargeInEvent is emitted when VAD detects speech during generation.
type BargeInEvent struct {
	SessionID string
	Timestamp time.Time
	Action    string
}

// BargeInDetector analyzes incoming audio chunks and triggers context cancellation when voice activity is detected during generation.
// The implementation uses a naive energy-based VAD: non-silent audio while the provider is streaming => barge-in.
type BargeInDetector struct {
	Threshold  float64
	MinFrames  int
	OnBargeIn  func(BargeInEvent)
}

// NewBargeInDetector creates a new detector.
func NewBargeInDetector(threshold float64, minFrames int, onBargeIn func(BargeInEvent)) *BargeInDetector {
	return &BargeInDetector{Threshold: threshold, MinFrames: minFrames, OnBargeIn: onBargeIn}
}

// AnalyzeChunk decides whether the incoming audio chunk should trigger barge-in.
// It returns true if a barge-in action should be sent to the provider context.
func (d *BargeInDetector) AnalyzeChunk(ctx context.Context, sessionID string, pcm []byte) bool {
	if d == nil || len(pcm) == 0 {
		return false
	}
	energy := averagePCMEnergy(pcm)
	if energy < d.Threshold {
		return false
	}
	if d.OnBargeIn != nil {
		d.OnBargeIn(BargeInEvent{SessionID: sessionID, Timestamp: time.Now().UTC(), Action: "cancel"})
	}
	return true
}

func averagePCMEnergy(pcm []byte) float64 {
	if len(pcm) == 0 {
		return 0
	}
	var sum float64
	for _, b := range pcm {
		v := float64(int8(b))
		sum += v * v
	}
	return sum / float64(len(pcm))
}
