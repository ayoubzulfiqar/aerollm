package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/keymanager"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
	"github.com/redis/go-redis/v9"
)

// scriptedProvider returns canned responses/errors and can stream.
type scriptedProvider struct {
	name    string
	err     error
	reply   string
	usage   *models.Usage
	chunks  []string
	mu      sync.Mutex
	calls   int
	lastReq *models.LLMRequest
}

func (p *scriptedProvider) Name() string                 { return p.name }
func (p *scriptedProvider) Type() providers.ProviderType { return providers.ProviderOpenAI }
func (p *scriptedProvider) Health() providers.ProviderHealth {
	return providers.ProviderHealth{Name: p.name, Healthy: true}
}
func (p *scriptedProvider) Close() error { return nil }

func (p *scriptedProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	p.mu.Lock()
	p.calls++
	cp := *req
	p.lastReq = &cp
	p.mu.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	text := p.reply
	return &models.LLMResponse{
		ID: "chatcmpl-up", Object: "chat.completion", Created: 1, Model: req.Model + "-2024",
		Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: &text}, FinishReason: "stop"}},
		Usage:   p.usage,
	}, nil
}

type streamingProvider struct{ *scriptedProvider }

func (p streamingProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	ch := make(chan models.StreamChunk)
	go func() {
		defer close(ch)
		send := func(c models.StreamChunk) bool {
			select {
			case ch <- c:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !send(models.StreamChunk{ID: "s1", Model: req.Model, Choices: []models.StreamChoice{{Delta: models.MessageDelta{Role: models.RoleAssistant}}}}) {
			return
		}
		for _, c := range p.chunks {
			if !send(models.StreamChunk{ID: "s1", Model: req.Model, Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: c}}}}) {
				return
			}
		}
		stop := "stop"
		send(models.StreamChunk{ID: "s1", Model: req.Model, Choices: []models.StreamChoice{{FinishReason: &stop}}, Usage: p.usage})
	}()
	return ch, nil
}

// memRedis is a tiny in-memory stand-in for the cache's Redis client.
type memRedis struct {
	mu   sync.Mutex
	data map[string]string
}

func newMemRedis() *memRedis { return &memRedis{data: map[string]string{}} }

func (m *memRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	m.mu.Lock()
	defer m.mu.Unlock()
	cmd := redis.NewStringCmd(ctx)
	if v, ok := m.data[key]; ok {
		cmd.SetVal(v)
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}
func (m *memRedis) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch v := value.(type) {
	case []byte:
		m.data[key] = string(v)
	case string:
		m.data[key] = v
	}
	return redis.NewStatusCmd(ctx)
}
func (m *memRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.data, k)
	}
	return redis.NewIntCmd(ctx)
}
func (m *memRedis) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	return redis.NewStringSliceCmd(ctx)
}
func (m *memRedis) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return redis.NewScanCmdResult(nil, 0, nil)
}
func (m *memRedis) Close() error { return nil }

func newChatHandler(t *testing.T, provs ...providers.Provider) *Handler {
	t.Helper()
	r := router.New(router.Config{Strategy: "fallback"})
	for _, p := range provs {
		r.RegisterProvider(p)
	}
	return NewHandler(r, nil, nil, nil, nil, nil)
}

func postChat(h http.HandlerFunc, body string, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	if key != "" {
		req = req.WithContext(middleware.WithPrincipal(req.Context(), &middleware.Principal{Key: key, KeyID: middleware.KeyID(key)}))
	}
	w := httptest.NewRecorder()
	h(w, req)
	return w
}

const simpleChat = `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`

func TestChatPreservesUpstreamModelAndUsage(t *testing.T) {
	p := &scriptedProvider{name: "openai", reply: "hello", usage: &models.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}}
	h := newChatHandler(t, p)
	var events []UsageEvent
	h.OnUsage = func(_ context.Context, ev UsageEvent) { events = append(events, ev) }
	h.CostFunc = func(model string, u *models.Usage) float64 { return float64(u.TotalTokens) * 0.001 }

	w := postChat(h.ChatCompletions, simpleChat, "sk-a")
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var resp models.LLMResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Model != "gpt-4o-2024" {
		t.Fatalf("upstream model must be preserved, got %q", resp.Model)
	}
	if w.Header().Get("X-AeroLLM-Provider") != "openai" {
		t.Fatalf("provider header: %q", w.Header().Get("X-AeroLLM-Provider"))
	}
	if len(events) != 1 || events[0].CostUSD != 0.005 || events[0].KeyID != middleware.KeyID("sk-a") || events[0].UsageEstimated {
		t.Fatalf("usage event: %+v", events)
	}
}

func TestChatValidation(t *testing.T) {
	h := newChatHandler(t, &scriptedProvider{name: "p", reply: "x"})
	cases := map[string]string{
		"no model":    `{"messages":[{"role":"user","content":"hi"}]}`,
		"no messages": `{"model":"m","messages":[]}`,
		"bad role":    `{"model":"m","messages":[{"role":"wizard","content":"hi"}]}`,
		"bad temp":    `{"model":"m","temperature":5,"messages":[{"role":"user","content":"hi"}]}`,
		"trailing":    simpleChat + `{}`,
		"not json":    `nope`,
	}
	for name, body := range cases {
		if w := postChat(h.ChatCompletions, body, ""); w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d %s", name, w.Code, w.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	h.ChatCompletions(w, req)
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "POST" {
		t.Fatalf("GET: %d", w.Code)
	}
}

func TestChatFallsBackOnRetryableError(t *testing.T) {
	bad := &scriptedProvider{name: "bad", err: &providers.UpstreamError{Provider: "bad", StatusCode: 503, Message: "overloaded"}}
	good := &scriptedProvider{name: "good", reply: "ok"}
	h := newChatHandler(t, bad, good)
	w := postChat(h.ChatCompletions, simpleChat, "")
	if w.Code != 200 || w.Header().Get("X-AeroLLM-Provider") != "good" {
		t.Fatalf("expected fallback to good provider: %d %s", w.Code, w.Body.String())
	}
}

func TestChatMapsUpstreamErrors(t *testing.T) {
	cases := []struct {
		err    error
		status int
	}{
		{&providers.UpstreamError{Provider: "p", StatusCode: 400, Message: "bad param"}, 400},
		{&providers.UpstreamError{Provider: "p", StatusCode: 401, Message: "bad key"}, 502},
		{&providers.UpstreamError{Provider: "p", StatusCode: 429, Message: "slow down", RetryAfter: 2 * time.Second}, 429},
		{&providers.UpstreamError{Provider: "p", StatusCode: 500, Message: "boom"}, 502},
		{errors.New("weird"), 500},
	}
	for _, c := range cases {
		h := newChatHandler(t, &scriptedProvider{name: "p", err: c.err})
		w := postChat(h.ChatCompletions, simpleChat, "")
		if w.Code != c.status {
			t.Errorf("%v: expected %d got %d (%s)", c.err, c.status, w.Code, w.Body.String())
		}
		if c.status == 429 && w.Header().Get("Retry-After") != "2" {
			t.Errorf("Retry-After not forwarded: %q", w.Header().Get("Retry-After"))
		}
		if strings.Contains(w.Body.String(), "bad key") {
			t.Errorf("upstream auth details must not leak: %s", w.Body.String())
		}
	}
	h := NewHandler(router.New(router.Config{}), nil, nil, nil, nil, nil)
	if w := postChat(h.ChatCompletions, simpleChat, ""); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("no providers: %d", w.Code)
	}
}

func readSSE(t *testing.T, body string) (events []string) {
	t.Helper()
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			events = append(events, strings.TrimPrefix(line, "data: "))
		}
	}
	return events
}

func TestChatStreamsNatively(t *testing.T) {
	p := streamingProvider{&scriptedProvider{name: "s", chunks: []string{"Hel", "lo"}, usage: &models.Usage{PromptTokens: 2, CompletionTokens: 2, TotalTokens: 4}}}
	h := newChatHandler(t, p)
	var ev UsageEvent
	h.OnUsage = func(_ context.Context, e UsageEvent) { ev = e }
	w := postChat(h.ChatCompletions, `{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`, "")
	if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status %d ct %q", w.Code, w.Header().Get("Content-Type"))
	}
	events := readSSE(t, w.Body.String())
	if len(events) < 3 || events[len(events)-1] != "[DONE]" {
		t.Fatalf("unexpected events %v", events)
	}
	var text strings.Builder
	for _, e := range events[:len(events)-1] {
		var c models.StreamChunk
		if err := json.Unmarshal([]byte(e), &c); err != nil {
			t.Fatalf("bad chunk %q: %v", e, err)
		}
		if c.Object != "chat.completion.chunk" {
			t.Fatalf("object: %q", c.Object)
		}
		for _, ch := range c.Choices {
			text.WriteString(ch.Delta.Content)
		}
	}
	if text.String() != "Hello" {
		t.Fatalf("streamed text %q", text.String())
	}
	if !ev.Stream || ev.Usage == nil || ev.Usage.TotalTokens != 4 || ev.Response.Choices[0].Message.TextContent() != "Hello" {
		t.Fatalf("stream usage event: %+v", ev)
	}
}

func TestChatStreamFallsBackToNonStreamingProvider(t *testing.T) {
	p := &scriptedProvider{name: "plain", reply: "full answer"}
	h := newChatHandler(t, p)
	w := postChat(h.ChatCompletions, `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "")
	events := readSSE(t, w.Body.String())
	if w.Code != 200 || len(events) == 0 || events[len(events)-1] != "[DONE]" || !strings.Contains(w.Body.String(), "full answer") {
		t.Fatalf("synthesized stream: %d %v", w.Code, events)
	}
	if strings.Contains(w.Body.String(), `"usage"`) {
		t.Fatalf("usage must not be sent unless requested: %s", w.Body.String())
	}
}

func TestChatStreamUpstreamErrorBeforeStart(t *testing.T) {
	p := streamingProvider{&scriptedProvider{name: "s", err: &providers.UpstreamError{Provider: "s", StatusCode: 400, Message: "nope"}}}
	h := newChatHandler(t, p)
	w := postChat(h.ChatCompletions, `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "")
	if w.Code != 400 || !strings.Contains(w.Header().Get("Content-Type"), "json") {
		t.Fatalf("pre-stream error should be a JSON error: %d %q", w.Code, w.Header().Get("Content-Type"))
	}
}

func TestChatCacheIsNamespacedPerKey(t *testing.T) {
	p := &scriptedProvider{name: "p", reply: "fresh"}
	h := newChatHandler(t, p)
	h.Cache = cache.NewRedisCache(newMemRedis(), time.Hour)

	if w := postChat(h.ChatCompletions, simpleChat, "sk-a"); w.Header().Get("X-AeroLLM-Cache") != "MISS" {
		t.Fatalf("first call should miss")
	}
	if w := postChat(h.ChatCompletions, simpleChat, "sk-a"); w.Header().Get("X-AeroLLM-Cache") != "HIT" {
		t.Fatalf("same key should hit: %v", w.Header())
	}
	if w := postChat(h.ChatCompletions, simpleChat, "sk-b"); w.Header().Get("X-AeroLLM-Cache") != "MISS" {
		t.Fatal("a different key must not see another key's cached response")
	}
	// Cache hits replay as a stream when requested.
	w := postChat(h.ChatCompletions, `{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "sk-a")
	if w.Header().Get("X-AeroLLM-Cache") != "HIT" || !strings.Contains(w.Body.String(), "[DONE]") {
		t.Fatalf("stream cache hit: %v %s", w.Header(), w.Body.String())
	}
	// Cache-Control: no-cache bypasses the lookup.
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(simpleChat))
	req = req.WithContext(middleware.WithPrincipal(req.Context(), &middleware.Principal{KeyID: middleware.KeyID("sk-a")}))
	req.Header.Set("Cache-Control", "no-cache")
	rec := httptest.NewRecorder()
	h.ChatCompletions(rec, req)
	if rec.Header().Get("X-AeroLLM-Cache") != "MISS" {
		t.Fatal("no-cache must bypass the cache")
	}
	if p.calls != 3 {
		t.Fatalf("expected 3 upstream calls, got %d", p.calls)
	}
}

func TestChatVirtualKeyModelAllowList(t *testing.T) {
	h := newChatHandler(t, &scriptedProvider{name: "p", reply: "x"})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(simpleChat))
	vk := &keymanager.VirtualKey{Models: []string{"claude-*"}}
	req = req.WithContext(middleware.WithPrincipal(req.Context(), &middleware.Principal{KeyID: "k", Virtual: vk}))
	w := httptest.NewRecorder()
	h.ChatCompletions(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("model outside allow-list: %d", w.Code)
	}
}

func TestChatResolverTakesPrecedenceAndHotSwaps(t *testing.T) {
	routed := &scriptedProvider{name: "routed", reply: "r"}
	a := &scriptedProvider{name: "a", reply: "a"}
	b := &scriptedProvider{name: "b", reply: "b"}
	h := newChatHandler(t, routed)
	h.SetModelResolver(func(model string) (providers.Provider, bool) { return a, model == "gpt-4o" })
	if w := postChat(h.ChatCompletions, simpleChat, ""); w.Header().Get("X-AeroLLM-Provider") != "a" {
		t.Fatalf("resolver not used: %v", w.Header())
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			postChat(h.ChatCompletions, simpleChat, "")
		}()
	}
	h.SetModelResolver(func(model string) (providers.Provider, bool) { return b, true })
	wg.Wait()
	if w := postChat(h.ChatCompletions, simpleChat, ""); w.Header().Get("X-AeroLLM-Provider") != "b" {
		t.Fatalf("hot swap not applied: %v", w.Header())
	}
}

func TestModelsEndpoint(t *testing.T) {
	h := newChatHandler(t)
	h.ModelLister = func() []ModelEntry {
		return []ModelEntry{{ID: "gpt-4o", OwnedBy: "openai"}, {ID: "claude-3", OwnedBy: "anthropic"}, {ID: "gpt-4o"}}
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	h.Models(w, req)
	var list ModelList
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || list.Object != "list" || len(list.Data) != 2 || list.Data[0].ID != "claude-3" {
		t.Fatalf("list: %s", w.Body.String())
	}
	w = httptest.NewRecorder()
	h.Models(w, httptest.NewRequest(http.MethodGet, "/v1/models/gpt-4o", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"id":"gpt-4o"`) {
		t.Fatalf("get: %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.Models(w, httptest.NewRequest(http.MethodGet, "/v1/models/nope", nil))
	if w.Code != 404 {
		t.Fatalf("missing model: %d", w.Code)
	}
}

func TestAnthropicMessagesTranslation(t *testing.T) {
	p := &scriptedProvider{name: "p", reply: "Bonjour", usage: &models.Usage{PromptTokens: 10, CompletionTokens: 3, TotalTokens: 13}}
	h := newChatHandler(t, p)
	body := `{"model":"claude-3","max_tokens":100,"system":"be brief",
	  "messages":[{"role":"user","content":[{"type":"text","text":"hello"}]},
	    {"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"calc","input":{"x":1}}]},
	    {"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"2"},{"type":"text","text":"thanks"}]}],
	  "tools":[{"name":"calc","description":"adds","input_schema":{"type":"object"}}],
	  "stop_sequences":["END"]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Messages(w, req)
	if w.Code != 200 {
		t.Fatalf("status %d %s", w.Code, w.Body.String())
	}
	var out anthropicResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Type != "message" || out.Content[0].Text != "Bonjour" || *out.StopReason != "end_turn" || out.Usage.InputTokens != 10 || !strings.HasPrefix(out.ID, "msg_") {
		t.Fatalf("translated response: %+v", out)
	}
	up := p.lastReq
	if up.Messages[0].Role != models.RoleSystem || up.Messages[0].TextContent() != "be brief" {
		t.Fatalf("system prompt lost: %+v", up.Messages[0])
	}
	if len(up.Messages) != 5 || up.Messages[2].ToolCalls[0].Function.Arguments != `{"x":1}` || up.Messages[3].Role != models.RoleTool || *up.Messages[3].ToolCallID != "t1" {
		t.Fatalf("messages not translated: %+v", up.Messages)
	}
	if *up.MaxTokens != 100 || up.Stop[0] != "END" || up.Tools[0].Name != "calc" {
		t.Fatalf("params not translated: %+v", up)
	}

	// Errors use the Anthropic envelope.
	w = httptest.NewRecorder()
	h.Messages(w, httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-3","messages":[]}`)))
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"type":"error"`) {
		t.Fatalf("anthropic error envelope: %d %s", w.Code, w.Body.String())
	}
}

func TestAnthropicMessagesStreaming(t *testing.T) {
	p := streamingProvider{&scriptedProvider{name: "s", chunks: []string{"Hi", " there"}, usage: &models.Usage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7}}}
	h := newChatHandler(t, p)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-3","max_tokens":50,"stream":true,"messages":[{"role":"user","content":"hey"}]}`))
	w := httptest.NewRecorder()
	h.Messages(w, req)
	body := w.Body.String()
	var events []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "event: ") {
			events = append(events, strings.TrimPrefix(line, "event: "))
		}
	}
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("events %v\n%s", events, body)
	}
	if !strings.Contains(body, `"text":" there"`) || !strings.Contains(body, `"output_tokens":2`) {
		t.Fatalf("stream content: %s", body)
	}
}

func TestResumeApprovalValidation(t *testing.T) {
	h := newChatHandler(t)
	w := httptest.NewRecorder()
	h.ResumeApproval(w, httptest.NewRequest(http.MethodGet, "/v1/agents/approvals/x", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ResumeApproval(w, httptest.NewRequest(http.MethodPost, "/v1/agents/approvals/x", bytes.NewBufferString(`{}`)))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("no advanced agent: %d", w.Code)
	}
}

// blockingStream sends one chunk and then waits for the client to go away.
type blockingStream struct{ *scriptedProvider }

func (p blockingStream) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	ch := make(chan models.StreamChunk)
	go func() {
		defer close(ch)
		select {
		case ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "partial answer text"}}}}:
		case <-ctx.Done():
			return
		}
		<-ctx.Done()
	}()
	return ch, nil
}

func TestChatStreamClientDisconnectStillBillsAndSkipsCache(t *testing.T) {
	p := blockingStream{&scriptedProvider{name: "s"}}
	h := newChatHandler(t, p)
	mem := newMemRedis()
	h.Cache = cache.NewRedisCache(mem, time.Hour)
	events := make(chan UsageEvent, 1)
	h.OnUsage = func(_ context.Context, ev UsageEvent) { events <- ev }

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)).WithContext(ctx)
	req = req.WithContext(middleware.WithPrincipal(req.Context(), &middleware.Principal{KeyID: "k"}))
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ChatCompletions(w, req); close(done) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	select {
	case ev := <-events:
		if !ev.UsageEstimated || ev.Usage.CompletionTokens == 0 {
			t.Fatalf("partial stream must be billed with estimated usage: %+v", ev.Usage)
		}
	case <-time.After(time.Second):
		t.Fatal("no usage recorded for interrupted stream")
	}
	mem.mu.Lock()
	n := len(mem.data)
	mem.mu.Unlock()
	if n != 0 {
		t.Fatalf("partial answer must not be cached, cache has %d entries", n)
	}
}

func TestModelsFilteredForVirtualKey(t *testing.T) {
	h := newChatHandler(t)
	h.ModelLister = func() []ModelEntry { return []ModelEntry{{ID: "gpt-4o"}, {ID: "claude-3"}} }
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	vk := &keymanager.VirtualKey{Models: []string{"gpt-4o"}}
	req = req.WithContext(middleware.WithPrincipal(req.Context(), &middleware.Principal{KeyID: "k", Virtual: vk}))
	w := httptest.NewRecorder()
	h.Models(w, req)
	if strings.Contains(w.Body.String(), "claude-3") || !strings.Contains(w.Body.String(), "gpt-4o") {
		t.Fatalf("virtual key should only see allowed models: %s", w.Body.String())
	}
}
