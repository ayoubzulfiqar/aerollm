package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/gorilla/websocket"
)

// scriptedProvider streams "tick" deltas forever when the last user message is
// "infinite"; otherwise it streams the prompt back followed by a finish chunk.
type scriptedProvider struct {
	mu        sync.Mutex
	requests  []*models.LLMRequest
	cancelled atomic.Int32
	needMsgs  bool
	burst     int
}

func (p *scriptedProvider) Name() string           { return "scripted" }
func (p *scriptedProvider) RequiresMessages() bool { return p.needMsgs }

func (p *scriptedProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan StreamChunk, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	last := ""
	if n := len(req.Messages); n > 0 && req.Messages[n-1].Content != nil {
		last = *req.Messages[n-1].Content
	}
	out := make(chan StreamChunk)
	go func() {
		defer close(out)
		send := func(c StreamChunk) bool {
			select {
			case out <- c:
				return true
			case <-ctx.Done():
				p.cancelled.Add(1)
				return false
			}
		}
		switch {
		case last == "infinite":
			for {
				if !send(StreamChunk{Delta: "tick", Provider: p.Name()}) {
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		case last == "fail":
			send(StreamChunk{Err: errors.New("upstream exploded: secret-host:9999")})
		case p.burst > 0:
			for i := 0; i < p.burst; i++ {
				if !send(StreamChunk{Delta: "b", Provider: p.Name()}) {
					return
				}
			}
			send(StreamChunk{Finish: true})
		default:
			if !send(StreamChunk{Delta: "echo:" + last, Provider: p.Name()}) {
				return
			}
			send(StreamChunk{Finish: true, Delta: "!"})
		}
	}()
	return out, nil
}

func (p *scriptedProvider) req(i int) *models.LLMRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.requests[i]
}

func (p *scriptedProvider) requestCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.requests)
}

func newWSServer(t *testing.T, hub *Hub, p ProviderStreamer, cfg Config) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(ServeWSWithConfig(hub, p, cfg))
	t.Cleanup(srv.Close)
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

func dial(t *testing.T, url string, hdr http.Header) *websocket.Conn {
	t.Helper()
	conn, resp, err := websocket.DefaultDialer.Dial(url, hdr)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial failed (status %d): %v", status, err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	return conn
}

func sendJSON(t *testing.T, c *websocket.Conn, v interface{}) {
	t.Helper()
	if err := c.WriteJSON(v); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// readUntil reads messages until one contains needle, returning all read.
func readUntil(t *testing.T, c *websocket.Conn, needle string) []string {
	t.Helper()
	var msgs []string
	for {
		_, msg, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read failed waiting for %q after %d msgs (last %q): %v", needle, len(msgs), tail(msgs), err)
		}
		msgs = append(msgs, string(msg))
		if strings.Contains(string(msg), needle) {
			return msgs
		}
	}
}

func tail(m []string) string {
	if len(m) == 0 {
		return ""
	}
	return m[len(m)-1]
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func testConfig() Config {
	return Config{PongWait: 5 * time.Second, WriteWait: 2 * time.Second, ShutdownWait: 2 * time.Second}
}

func TestOriginChecker(t *testing.T) {
	check := OriginChecker([]string{"https://app.example.com", "partner.example.org:8443"})
	cases := []struct {
		origin, host string
		want         bool
	}{
		{"", "api.example.com", true},                         // non-browser
		{"https://api.example.com", "api.example.com", true},  // same origin
		{"https://API.example.com", "api.example.com", true},  // case-insensitive
		{"https://app.example.com", "api.example.com", true},  // allow-listed
		{"https://app.example.com/", "api.example.com", true}, // trailing slash irrelevant
		{"http://app.example.com", "api.example.com", false},  // scheme mismatch
		{"https://partner.example.org:8443", "api.example.com", true},
		{"https://evil.example.net", "api.example.com", false},
		{"null", "api.example.com", false},
		{"://bad", "api.example.com", false},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(http.MethodGet, "/ws", nil)
		r.Host = tc.host
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		if got := check(r); got != tc.want {
			t.Errorf("origin %q host %q: got %v want %v", tc.origin, tc.host, got, tc.want)
		}
	}
	all := OriginChecker([]string{"*"})
	r := httptest.NewRequest(http.MethodGet, "/ws", nil)
	r.Header.Set("Origin", "https://anything.example")
	if !all(r) {
		t.Fatal("expected * to allow any origin")
	}
}

func TestDefaultConfigReadsEnvOrigins(t *testing.T) {
	t.Setenv(EnvAllowedOrigins, " https://a.example , ,https://b.example")
	cfg := DefaultConfig()
	if len(cfg.AllowedOrigins) != 2 || cfg.AllowedOrigins[1] != "https://b.example" {
		t.Fatalf("unexpected origins: %v", cfg.AllowedOrigins)
	}
	if !cfg.AutoStart || cfg.MaxMessageBytes != DefaultMaxMessageBytes {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestServeWSRejectsCrossOriginHijack(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	cfg := testConfig()
	cfg.AllowedOrigins = []string{"https://good.example"}
	srv, url := newWSServer(t, NewHub(), p, cfg)

	hdr := http.Header{"Origin": []string{"https://evil.example"}}
	_, resp, err := websocket.DefaultDialer.Dial(url, hdr)
	if err == nil {
		t.Fatal("expected cross-origin dial to be rejected")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %+v", resp)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected JSON error, got %q", ct)
	}

	dial(t, url, http.Header{"Origin": []string{"https://good.example"}})
	dial(t, url, http.Header{"Origin": []string{srv.URL}}) // same origin
}

func TestServeWSNilProvider(t *testing.T) {
	srv := httptest.NewServer(ServeWS(nil, nil))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
}

func TestServeWSRequestStreamsAndFinishDeltaNotDropped(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	_, url := newWSServer(t, NewHub(), p, testConfig())
	c := dial(t, url, nil)

	sendJSON(t, c, map[string]interface{}{"type": "request", "model": "m1", "messages": []map[string]string{{"role": "user", "content": "hello"}}})
	msgs := readUntil(t, c, `"event":"finish"`)
	joined := strings.Join(msgs, "\n")
	if !strings.Contains(joined, `"Delta":"echo:hello"`) || !strings.Contains(joined, `"Delta":"!"`) {
		t.Fatalf("expected both deltas (incl. the finish chunk's), got %q", msgs)
	}
	if p.req(0).Model != "m1" {
		t.Fatalf("model not propagated: %+v", p.req(0))
	}

	// Nested request + prompt form.
	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "again"})
	msgs = readUntil(t, c, `"event":"finish"`)
	if !strings.Contains(strings.Join(msgs, ""), "echo:again") {
		t.Fatalf("unexpected: %q", msgs)
	}
	if got := p.req(1).Model; got != "m1" {
		t.Fatalf("expected sticky session model m1, got %q", got)
	}
}

func TestServeWSErrorsDoNotLeakInternals(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	_, url := newWSServer(t, NewHub(), p, testConfig())
	c := dial(t, url, nil)

	_ = c.WriteMessage(websocket.TextMessage, []byte("not json"))
	readUntil(t, c, `"event":"error"`)

	sendJSON(t, c, map[string]interface{}{"type": "request", "messages": []interface{}{}})
	msgs := readUntil(t, c, `"event":"error"`)
	if !strings.Contains(tail(msgs), "no messages") {
		t.Fatalf("unexpected error: %q", msgs)
	}

	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "fail"})
	msgs = readUntil(t, c, `"event":"error"`)
	if strings.Contains(tail(msgs), "secret-host") {
		t.Fatalf("internal error leaked: %q", tail(msgs))
	}

	sendJSON(t, c, map[string]interface{}{"type": "bogus"})
	readUntil(t, c, `"event":"error"`)
	sendJSON(t, c, map[string]interface{}{"type": "ping"})
	readUntil(t, c, `"event":"pong"`)
}

func TestBargeInCancelsStreamButKeepsConnection(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	hub := NewHub()
	_, url := newWSServer(t, hub, p, testConfig())
	c := dial(t, url, nil)

	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "infinite"})
	readUntil(t, c, `"Delta":"tick"`)

	sendJSON(t, c, map[string]string{"type": "barge-in"})
	readUntil(t, c, `"event":"barge-in"`)
	waitFor(t, func() bool { return p.cancelled.Load() >= 1 }, "provider stream cancellation")

	// Connection still usable: no further ticks after barge-in ack, and a new
	// request streams normally.
	sendJSON(t, c, map[string]string{"type": "ping"})
	msgs := readUntil(t, c, `"event":"pong"`)
	for _, m := range msgs {
		if strings.Contains(m, "tick") {
			t.Fatalf("stream chunk delivered after barge-in ack: %q", msgs)
		}
	}
	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "second"})
	readUntil(t, c, "echo:second")
	readUntil(t, c, `"event":"finish"`)

	// Binary frame (audio) is also a barge-in.
	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "infinite"})
	readUntil(t, c, `"Delta":"tick"`)
	if err := c.WriteMessage(websocket.BinaryMessage, []byte{0x7f, 0x80}); err != nil {
		t.Fatal(err)
	}
	readUntil(t, c, `"event":"barge-in"`)
	waitFor(t, func() bool { return p.cancelled.Load() >= 2 }, "second cancellation")
	if hub.ActiveCount() != 1 {
		t.Fatalf("expected session still active, got %d", hub.ActiveCount())
	}
}

func TestNewRequestReplacesInflightStream(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	_, url := newWSServer(t, NewHub(), p, testConfig())
	c := dial(t, url, nil)
	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "infinite"})
	readUntil(t, c, "tick")
	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "next"})
	readUntil(t, c, "echo:next")
	waitFor(t, func() bool { return p.cancelled.Load() >= 1 }, "old stream cancellation")
}

func TestConcurrentWritesAreSerialized(t *testing.T) {
	p := &scriptedProvider{needMsgs: true, burst: 500}
	_, url := newWSServer(t, NewHub(), p, testConfig())
	c := dial(t, url, nil)

	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "go"})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if err := c.WriteJSON(map[string]string{"type": "ping"}); err != nil {
				return
			}
		}
	}()
	pongs, finished := 0, false
	for !finished || pongs < 50 {
		_, msg, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v (pongs=%d finished=%v)", err, pongs, finished)
		}
		var evt map[string]interface{}
		if err := json.Unmarshal(msg, &evt); err != nil {
			t.Fatalf("corrupted frame %q: %v", msg, err)
		}
		switch evt["event"] {
		case "pong":
			pongs++
		case "finish":
			finished = true
		}
	}
	wg.Wait()
}

func TestNoGoroutineLeakAfterDisconnect(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	hub := NewHub()
	_, url := newWSServer(t, hub, p, testConfig())

	// Warm up the HTTP server/client machinery.
	c := dial(t, url, nil)
	c.Close()
	waitFor(t, func() bool { return hub.ActiveCount() == 0 }, "warm-up session teardown")
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_ = conn.WriteJSON(map[string]string{"type": "chat", "prompt": "infinite"})
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatal(err)
		}
		conn.Close() // abrupt disconnect mid-stream
	}
	waitFor(t, func() bool { return hub.ActiveCount() == 0 }, "all sessions unregistered")
	waitFor(t, func() bool { return p.cancelled.Load() >= 10 }, "all provider streams cancelled")
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before+3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+3 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestReadLimitEnforced(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	hub := NewHub()
	cfg := testConfig()
	cfg.MaxMessageBytes = 128
	_, url := newWSServer(t, hub, p, cfg)
	c := dial(t, url, nil)
	waitFor(t, func() bool { return hub.ActiveCount() == 1 }, "session registration")

	big := `{"type":"chat","prompt":"` + strings.Repeat("x", 4096) + `"}`
	_ = c.WriteMessage(websocket.TextMessage, []byte(big))
	_, _, err := c.ReadMessage()
	if err == nil {
		t.Fatal("expected connection to be closed for oversized message")
	}
	if ce, ok := err.(*websocket.CloseError); ok && ce.Code != websocket.CloseMessageTooBig {
		t.Fatalf("expected close 1009, got %d", ce.Code)
	}
	waitFor(t, func() bool { return hub.ActiveCount() == 0 }, "session teardown")
	if p.requestCount() != 0 {
		t.Fatal("oversized request must not reach the provider")
	}
}

func TestHubCancelAllClosesLiveSessions(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	hub := NewHub()
	_, url := newWSServer(t, hub, p, testConfig())
	c := dial(t, url, nil)
	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "infinite"})
	readUntil(t, c, "tick")

	hub.CancelAll()
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			break
		}
	}
	waitFor(t, func() bool { return hub.ActiveCount() == 0 }, "session teardown")
	waitFor(t, func() bool { return p.cancelled.Load() >= 1 }, "stream cancellation")
}

func TestHubInterruptAllKeepsSessions(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	hub := NewHub()
	_, url := newWSServer(t, hub, p, testConfig())
	c := dial(t, url, nil)
	sendJSON(t, c, map[string]interface{}{"type": "chat", "prompt": "infinite"})
	readUntil(t, c, "tick")
	if n := hub.InterruptAll(); n != 1 {
		t.Fatalf("expected 1 interrupted stream, got %d", n)
	}
	waitFor(t, func() bool { return p.cancelled.Load() >= 1 }, "stream cancellation")
	sendJSON(t, c, map[string]string{"type": "ping"})
	readUntil(t, c, "pong")
	if hub.ActiveCount() != 1 {
		t.Fatalf("session should survive interrupt, got %d", hub.ActiveCount())
	}
}

func TestMaxSessions(t *testing.T) {
	p := &scriptedProvider{needMsgs: true}
	hub := NewHub()
	cfg := testConfig()
	cfg.MaxSessions = 1
	_, url := newWSServer(t, hub, p, cfg)
	dial(t, url, nil)
	waitFor(t, func() bool { return hub.ActiveCount() == 1 }, "first session")
	_, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err == nil {
		t.Fatal("expected second session to be refused")
	}
	if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %+v", resp)
	}
}

func TestAutoStartLegacyAndPromptQuery(t *testing.T) {
	// Legacy provider (no RequiresMessages): stream starts on connect.
	legacy := &scriptedProvider{}
	_, url := newWSServer(t, NewHub(), legacy, DefaultConfig())
	c := dial(t, url, nil)
	msgs := readUntil(t, c, `"event":"finish"`)
	if !strings.Contains(msgs[0], `"Delta":"echo:"`) {
		t.Fatalf("expected first message to be a delta chunk, got %q", msgs)
	}

	// Request-driven provider: nothing on connect without a prompt...
	driven := &scriptedProvider{needMsgs: true}
	_, url2 := newWSServer(t, NewHub(), driven, DefaultConfig())
	c2 := dial(t, url2, nil)
	sendJSON(t, c2, map[string]string{"type": "ping"})
	msgs = readUntil(t, c2, "pong")
	if len(msgs) != 1 || driven.requestCount() != 0 {
		t.Fatalf("expected no auto-started stream, got %q (requests=%d)", msgs, driven.requestCount())
	}
	// ...but ?prompt= auto-starts it.
	c3 := dial(t, url2+"?model=m9&prompt=hi", nil)
	readUntil(t, c3, "echo:hi")
	if driven.req(0).Model != "m9" {
		t.Fatalf("expected model from query, got %q", driven.req(0).Model)
	}
}

func TestSessionIDsAreUniqueAndRandom(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id, err := newSessionID()
		if err != nil || len(id) != 32 {
			t.Fatalf("bad id %q: %v", id, err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

// --- NewStreamingProvider adapter ---

func strp(s string) *string { return &s }

func collect(t *testing.T, ch <-chan StreamChunk) []StreamChunk {
	t.Helper()
	var out []StreamChunk
	timeout := time.After(5 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, c)
		case <-timeout:
			t.Fatal("adapter channel not closed")
		}
	}
}

func userReq(s string) *models.LLMRequest {
	return &models.LLMRequest{Model: "m", Messages: []models.Message{{Role: models.RoleUser, Content: strp(s)}}}
}

func TestStreamingProviderConvertsChunks(t *testing.T) {
	stop := "stop"
	fn := func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
		ch := make(chan models.StreamChunk, 8)
		ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Role: models.RoleAssistant}}}}
		ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "Hel"}}}}
		ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "lo"}}}}
		ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{ToolCalls: []models.ToolCallDelta{{Index: 0, ID: "c1", Function: models.ToolFunctionDelta{Name: "calc"}}}}}}}
		ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "!"}, FinishReason: &stop}}}
		ch <- models.StreamChunk{Usage: &models.Usage{TotalTokens: 3}} // trailing usage chunk
		close(ch)
		return ch, nil
	}
	p := NewStreamingProvider("up", fn)
	if p.Name() != "up" || !requiresMessages(p) {
		t.Fatal("adapter must be named and request-driven")
	}
	ch, err := p.StreamChatCompletions(context.Background(), userReq("x"))
	if err != nil {
		t.Fatal(err)
	}
	got := collect(t, ch)
	if len(got) != 5 {
		t.Fatalf("expected 5 chunks, got %d: %+v", len(got), got)
	}
	if got[0].Delta != "Hel" || got[1].Delta != "lo" || got[3].Delta != "!" {
		t.Fatalf("unexpected deltas: %+v", got)
	}
	if got[2].ContentType != ToolCallsContentType || !strings.Contains(string(got[2].Data), `"calc"`) {
		t.Fatalf("unexpected tool call chunk: %+v", got[2])
	}
	if !got[4].Finish || got[4].Provider != "up" {
		t.Fatalf("expected final finish chunk, got %+v", got[4])
	}
}

func TestStreamingProviderErrorsAndValidation(t *testing.T) {
	boom := errors.New("boom")
	p := NewStreamingProvider("up", func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
		ch := make(chan models.StreamChunk, 2)
		ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "a"}}}}
		ch <- models.StreamChunk{Err: boom}
		close(ch)
		return ch, nil
	})
	if _, err := p.StreamChatCompletions(context.Background(), &models.LLMRequest{}); !errors.Is(err, ErrEmptyRequest) {
		t.Fatalf("expected ErrEmptyRequest, got %v", err)
	}
	if _, err := p.StreamChatCompletions(context.Background(), nil); !errors.Is(err, ErrEmptyRequest) {
		t.Fatalf("expected ErrEmptyRequest for nil, got %v", err)
	}
	ch, err := p.StreamChatCompletions(context.Background(), userReq("x"))
	if err != nil {
		t.Fatal(err)
	}
	got := collect(t, ch)
	if len(got) != 2 || !errors.Is(got[1].Err, boom) {
		t.Fatalf("expected delta then error chunk, got %+v", got)
	}

	startErr := errors.New("no stream")
	p2 := NewStreamingProvider("up", func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
		return nil, startErr
	})
	if _, err := p2.StreamChatCompletions(context.Background(), userReq("x")); !errors.Is(err, startErr) {
		t.Fatalf("expected start error, got %v", err)
	}
	if _, err := NewStreamingProvider("x", nil).StreamChatCompletions(context.Background(), userReq("x")); err == nil {
		t.Fatal("expected error for nil stream func")
	}

	// Upstream closing without a finish_reason still yields a Finish chunk.
	p3 := NewStreamingProvider("up", func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
		ch := make(chan models.StreamChunk)
		close(ch)
		return ch, nil
	})
	ch, _ = p3.StreamChatCompletions(context.Background(), userReq("x"))
	got = collect(t, ch)
	if len(got) != 1 || !got[0].Finish {
		t.Fatalf("expected synthetic finish, got %+v", got)
	}
}

func TestStreamingProviderStopsOnCancel(t *testing.T) {
	upstreamDone := make(chan struct{})
	p := NewStreamingProvider("up", func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
		ch := make(chan models.StreamChunk)
		go func() {
			defer close(upstreamDone)
			defer close(ch)
			for {
				select {
				case ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "x"}}}}:
				case <-ctx.Done():
					return
				}
			}
		}()
		return ch, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.StreamChatCompletions(ctx, userReq("x"))
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	cancel() // consumer walks away without draining
	collect(t, ch)
	select {
	case <-upstreamDone:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream goroutine not released after cancel")
	}
}

func TestStreamingProviderOverWebSocket(t *testing.T) {
	stop := "stop"
	p := NewStreamingProvider("up", func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
		ch := make(chan models.StreamChunk, 2)
		ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "from-upstream:" + *req.Messages[0].Content}}}}
		ch <- models.StreamChunk{Choices: []models.StreamChoice{{FinishReason: &stop}}}
		close(ch)
		return ch, nil
	})
	_, url := newWSServer(t, NewHub(), p, DefaultConfig())
	c := dial(t, url, nil)
	sendJSON(t, c, map[string]interface{}{"type": "request", "request": map[string]interface{}{"model": "gpt-4o", "messages": []map[string]string{{"role": "user", "content": "q"}}}})
	msgs := readUntil(t, c, `"event":"finish"`)
	if !strings.Contains(msgs[0], "from-upstream:q") {
		t.Fatalf("unexpected: %q", msgs)
	}
}
