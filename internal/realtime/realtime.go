package realtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/gorilla/websocket"
)

// EnvAllowedOrigins names the environment variable holding a comma-separated
// list of browser origins (e.g. "https://app.example.com") that may open
// realtime WebSocket sessions cross-origin. "*" explicitly allows any origin.
// Same-origin requests and non-browser clients (no Origin header) are always
// accepted.
const EnvAllowedOrigins = "AEROLLM_WS_ALLOWED_ORIGINS"

// Default connection tuning values used by DefaultConfig and to fill zero
// fields of a Config.
const (
	DefaultMaxMessageBytes int64 = 1 << 20
	DefaultPongWait              = 60 * time.Second
	DefaultWriteWait             = 10 * time.Second
	DefaultShutdownWait          = 5 * time.Second
)

// ToolCallsContentType is the StreamChunk.ContentType used for chunks whose
// Data carries JSON-encoded tool call deltas.
const ToolCallsContentType = "application/vnd.aerollm.tool_calls+json"

// Config tunes the realtime WebSocket endpoint.
type Config struct {
	// AllowedOrigins lists cross-origin browser origins permitted to connect
	// ("scheme://host[:port]" or bare "host[:port]"; "*" allows all).
	AllowedOrigins []string
	// MaxMessageBytes caps a single inbound WebSocket message.
	MaxMessageBytes int64
	// PongWait is how long the server waits for any inbound frame (including
	// pong replies to its pings) before considering the peer dead.
	PongWait time.Duration
	// PingPeriod is the interval between server pings; it must be shorter
	// than PongWait (defaults to 90% of PongWait).
	PingPeriod time.Duration
	// WriteWait bounds every single write to the peer.
	WriteWait time.Duration
	// ShutdownWait bounds how long session teardown waits for the provider
	// stream goroutine to exit.
	ShutdownWait time.Duration
	// MaxSessions caps concurrently registered sessions on the hub (0 = no cap).
	MaxSessions int
	// AutoStart starts a stream right after the upgrade using the "model" and
	// "prompt" query parameters (legacy behaviour). Providers implementing
	// RequiresMessages() == true are only auto-started when a prompt is given.
	AutoStart bool
}

// DefaultConfig returns the configuration used by ServeWS: safe timeouts, a
// 1 MiB message cap, auto-start enabled and allowed origins read from
// AEROLLM_WS_ALLOWED_ORIGINS.
func DefaultConfig() Config {
	return Config{
		AllowedOrigins:  ParseOrigins(os.Getenv(EnvAllowedOrigins)),
		MaxMessageBytes: DefaultMaxMessageBytes,
		PongWait:        DefaultPongWait,
		WriteWait:       DefaultWriteWait,
		ShutdownWait:    DefaultShutdownWait,
		AutoStart:       true,
	}
}

func (c Config) withDefaults() Config {
	if c.MaxMessageBytes <= 0 {
		c.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if c.PongWait <= 0 {
		c.PongWait = DefaultPongWait
	}
	if c.PingPeriod <= 0 || c.PingPeriod >= c.PongWait {
		c.PingPeriod = c.PongWait * 9 / 10
	}
	if c.WriteWait <= 0 {
		c.WriteWait = DefaultWriteWait
	}
	if c.ShutdownWait <= 0 {
		c.ShutdownWait = DefaultShutdownWait
	}
	return c
}

// ParseOrigins splits a comma-separated origin list, dropping blanks.
func ParseOrigins(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// OriginChecker returns a websocket CheckOrigin function. Requests without an
// Origin header (non-browser clients) and same-origin requests are accepted;
// cross-origin requests are accepted only when listed in allowed (or when
// allowed contains "*"). This prevents Cross-Site WebSocket Hijacking.
func OriginChecker(allowed []string) func(r *http.Request) bool {
	allowAll := false
	set := make(map[string]struct{}, len(allowed))
	for _, o := range allowed {
		o = strings.ToLower(strings.TrimRight(strings.TrimSpace(o), "/"))
		if o == "" {
			continue
		}
		if o == "*" {
			allowAll = true
			continue
		}
		set[o] = struct{}{}
	}
	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		if allowAll {
			return true
		}
		u, err := url.Parse(origin)
		if err != nil || u.Host == "" {
			return false
		}
		host := strings.ToLower(u.Host)
		if host == strings.ToLower(r.Host) {
			return true
		}
		if _, ok := set[strings.ToLower(u.Scheme)+"://"+host]; ok {
			return true
		}
		_, ok := set[host]
		return ok
	}
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

	cfg     Config
	writeMu sync.Mutex
	// streamMu guards the in-flight stream handle.
	streamMu     sync.Mutex
	streamCancel context.CancelFunc
	streamDone   chan struct{}
	wg           sync.WaitGroup
}

// ProviderStreamer defines the contract for a provider that can stream responses.
//
// Implementations must close the returned channel when the stream ends and
// must never block forever on a send: select on ctx.Done() when sending.
type ProviderStreamer interface {
	StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan StreamChunk, error)
	Name() string
}

// StreamChunk represents a single chunk in a streaming response. It is sent to
// WebSocket clients as JSON using the Go field names (e.g. "Delta").
type StreamChunk struct {
	Delta       string
	Finish      bool
	Provider    string
	ContentType string
	Data        []byte
	// Err reports a mid-stream failure; it is never serialized. A chunk with
	// Err set ends the stream.
	Err error `json:"-"`
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
	if s == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sessions[s.SessionID] = s
}

// tryRegister adds a session unless the hub already holds max sessions
// (max <= 0 means unlimited). It reports whether the session was added.
func (h *Hub) tryRegister(s *StreamSession, max int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if max > 0 && len(h.sessions) >= max {
		return false
	}
	h.sessions[s.SessionID] = s
	return true
}

// Unregister removes a session.
func (h *Hub) Unregister(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.sessions, id)
}

// unregisterSession removes s only if it is still the session registered
// under its ID.
func (h *Hub) unregisterSession(s *StreamSession) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.sessions[s.SessionID]; ok && cur == s {
		delete(h.sessions, s.SessionID)
	}
}

// Get returns the session registered under id.
func (h *Hub) Get(id string) (*StreamSession, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s, ok := h.sessions[id]
	return s, ok
}

func (h *Hub) snapshot() []*StreamSession {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*StreamSession, 0, len(h.sessions))
	for _, s := range h.sessions {
		out = append(out, s)
	}
	return out
}

// CancelAll cancels every active session (terminating its provider stream
// and closing its connection). Use for graceful shutdown.
func (h *Hub) CancelAll() {
	for _, s := range h.snapshot() {
		s.cancelSession()
	}
}

// CancelSession cancels (terminates) one session and reports whether it existed.
func (h *Hub) CancelSession(id string) bool {
	s, ok := h.Get(id)
	if ok {
		s.cancelSession()
	}
	return ok
}

// InterruptAll cancels only the in-flight provider stream of every session
// (a broadcast barge-in); connections stay open. It returns how many streams
// were interrupted.
func (h *Hub) InterruptAll() int {
	n := 0
	for _, s := range h.snapshot() {
		if s.InterruptStream() {
			n++
		}
	}
	return n
}

// ActiveCount returns the number of active sessions.
func (h *Hub) ActiveCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.sessions)
}

func (s *StreamSession) cancelSession() {
	s.mu.RLock()
	cancel := s.Cancel
	s.mu.RUnlock()
	if cancel != nil {
		cancel()
	}
}

// InterruptStream cancels the session's in-flight provider stream, if any,
// without closing the connection. It reports whether a stream was running.
func (s *StreamSession) InterruptStream() bool {
	s.streamMu.Lock()
	cancel := s.streamCancel
	s.streamCancel = nil
	s.streamMu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func newSessionID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
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
	Threshold float64
	MinFrames int
	OnBargeIn func(BargeInEvent)
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

// StreamFunc streams an OpenAI-compatible chat completion as
// models.StreamChunk values (the providers.StreamingProvider contract).
type StreamFunc func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error)

// ErrEmptyRequest is returned by NewStreamingProvider adapters when the
// request carries no messages.
var ErrEmptyRequest = errors.New("realtime: request has no messages")

// NewStreamingProvider adapts a models.StreamChunk producer (for example a
// providers.StreamingProvider's StreamChatCompletions method) into a
// ProviderStreamer usable by ServeWS. Content deltas become StreamChunk.Delta,
// tool call deltas become JSON Data with ContentType ToolCallsContentType, a
// finish_reason or the end of the upstream channel yields a Finish chunk, and
// an upstream Err is forwarded as an Err chunk.
//
// The adapter reports RequiresMessages() == true, so ServeWS waits for a
// client "request" frame (or a ?prompt= query parameter) instead of starting
// an empty stream on connect.
func NewStreamingProvider(name string, fn StreamFunc) ProviderStreamer {
	return &streamingProvider{name: name, fn: fn}
}

type streamingProvider struct {
	name string
	fn   StreamFunc
}

func (p *streamingProvider) Name() string { return p.name }

// RequiresMessages reports that this provider needs a non-empty request.
func (p *streamingProvider) RequiresMessages() bool { return true }

func (p *streamingProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan StreamChunk, error) {
	if p.fn == nil {
		return nil, errors.New("realtime: streaming function not configured")
	}
	if req == nil || len(req.Messages) == 0 {
		return nil, ErrEmptyRequest
	}
	in, err := p.fn(ctx, req)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, errors.New("realtime: provider returned no stream")
	}
	out := make(chan StreamChunk)
	go func() {
		defer close(out)
		send := func(c StreamChunk) bool {
			select {
			case out <- c:
				return true
			case <-ctx.Done():
				return false
			}
		}
		finished := false
		for {
			select {
			case <-ctx.Done():
				return
			case mc, ok := <-in:
				if !ok {
					if !finished {
						send(StreamChunk{Finish: true, Provider: p.name})
					}
					return
				}
				if finished {
					// Keep draining so the upstream producer can finish
					// (e.g. a trailing usage chunk) without blocking.
					continue
				}
				if mc.Err != nil {
					send(StreamChunk{Err: mc.Err, Provider: p.name})
					return
				}
				for _, ch := range mc.Choices {
					if ch.Index != 0 {
						continue
					}
					if ch.Delta.Content != "" {
						if !send(StreamChunk{Delta: ch.Delta.Content, Provider: p.name}) {
							return
						}
					}
					if len(ch.Delta.ToolCalls) > 0 {
						data, err := json.Marshal(ch.Delta.ToolCalls)
						if err == nil {
							if !send(StreamChunk{Provider: p.name, ContentType: ToolCallsContentType, Data: data}) {
								return
							}
						}
					}
					if ch.FinishReason != nil {
						if !send(StreamChunk{Finish: true, Provider: p.name}) {
							return
						}
						finished = true
					}
				}
			}
		}
	}()
	return out, nil
}
