package providers

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func strp(s string) *string { return &s }

func collect(t *testing.T, ch <-chan models.StreamChunk) []models.StreamChunk {
	t.Helper()
	var out []models.StreamChunk
	timeout := time.After(5 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, c)
		case <-timeout:
			t.Fatal("stream did not finish")
		}
	}
}

func sse(w http.ResponseWriter, lines ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	f, _ := w.(http.Flusher)
	for _, l := range lines {
		fmt.Fprint(w, l)
		if f != nil {
			f.Flush()
		}
	}
}

// ---------------------------------------------------------------- errors

func TestNewUpstreamErrorParsesBodies(t *testing.T) {
	cases := []struct {
		body, msg, typ string
	}{
		{`{"error":{"message":"Rate limit reached","type":"requests","code":"rate_limit_exceeded"}}`, "Rate limit reached", "requests"},
		{`{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, "Overloaded", "overloaded_error"},
		{`[{"error":{"code":429,"message":"Resource exhausted","status":"RESOURCE_EXHAUSTED"}}]`, "Resource exhausted", "RESOURCE_EXHAUSTED"},
		{`{"message":"invalid api token"}`, "invalid api token", ""},
		{`plain text failure`, "plain text failure", ""},
		{`<html><body>502 Bad Gateway</body></html>`, "Too Many Requests", ""},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		rec.Header().Set("Retry-After", "7")
		rec.WriteHeader(http.StatusTooManyRequests)
		rec.WriteString(c.body)
		ue := NewUpstreamError("p", rec.Result())
		if ue.Message != c.msg || ue.Type != c.typ {
			t.Fatalf("%s: got msg=%q typ=%q", c.body, ue.Message, ue.Type)
		}
		if ue.RetryAfter != 7*time.Second || ue.StatusCode != 429 || !ue.Retryable() {
			t.Fatalf("%s: bad error %+v", c.body, ue)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h := http.Header{}
	h.Set("Retry-After", now.Add(30*time.Second).Format(http.TimeFormat))
	if d := parseRetryAfter(h, now); d != 30*time.Second {
		t.Fatalf("http date: %v", d)
	}
	h = http.Header{}
	h.Set("retry-after-ms", "1500")
	if d := parseRetryAfter(h, now); d != 1500*time.Millisecond {
		t.Fatalf("ms: %v", d)
	}
	h = http.Header{}
	h.Set("Retry-After", "99999999")
	if d := parseRetryAfter(h, now); d != 10*time.Minute {
		t.Fatalf("clamp: %v", d)
	}
	h.Set("Retry-After", "-5")
	if d := parseRetryAfter(h, now); d != 10*time.Minute && d != 0 {
		t.Fatalf("negative: %v", d)
	}
}

type retryableErr struct{ r bool }

func (e retryableErr) Error() string   { return "x" }
func (e retryableErr) Retryable() bool { return e.r }

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{&UpstreamError{StatusCode: 429}, true},
		{&UpstreamError{StatusCode: 408}, true},
		{&UpstreamError{StatusCode: 500}, true},
		{&UpstreamError{StatusCode: 503}, true},
		{&UpstreamError{StatusCode: 400}, false},
		{&UpstreamError{StatusCode: 401}, false},
		{&UpstreamError{StatusCode: 404}, false},
		{fmt.Errorf("wrapped: %w", &UpstreamError{StatusCode: 502}), true},
		{context.Canceled, false},
		{&TransportError{Err: context.Canceled}, false},
		{context.DeadlineExceeded, true},
		{&TransportError{Err: io.ErrUnexpectedEOF}, true},
		{fmt.Errorf("open: %w", ErrCircuitOpen), true},
		{retryableErr{true}, true},
		{retryableErr{false}, false},
		{errors.New("some bug"), false},
	}
	for i, c := range cases {
		if got := IsRetryable(c.err); got != c.want {
			t.Fatalf("case %d (%v): got %v want %v", i, c.err, got, c.want)
		}
	}
	if StatusCode(fmt.Errorf("w: %w", &UpstreamError{StatusCode: 418})) != 418 || StatusCode(errors.New("x")) != 0 {
		t.Fatal("StatusCode helper")
	}
}

func TestTransportErrorHidesURL(t *testing.T) {
	p := NewOpenAIProvider("openai", "sk-secret", "http://127.0.0.1:1/v1?key=sk-secret")
	_, err := p.ChatCompletions(context.Background(), &models.LLMRequest{Model: "m", Messages: []models.Message{{Role: "user", Content: strp("hi")}}})
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "sk-secret") || strings.Contains(err.Error(), "127.0.0.1:1/v1") {
		t.Fatalf("error leaks URL/key: %v", err)
	}
	if !IsRetryable(err) {
		t.Fatalf("connection refused should be retryable: %v", err)
	}
}

// ------------------------------------------------------------- URL / models

func TestURLNormalization(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"x","choices":[]}`)
	}))
	defer srv.Close()
	req := &models.LLMRequest{Model: "m", Messages: []models.Message{{Role: "user", Content: strp("hi")}}}
	for _, base := range []string{srv.URL, srv.URL + "/", srv.URL + "/v1", srv.URL + "/v1/"} {
		if _, err := NewOpenAIProvider("o", "k", base).ChatCompletions(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		if _, err := NewLocalProvider(base, "m").ChatCompletions(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range paths {
		if p != "/v1/chat/completions" {
			t.Fatalf("bad path %q (all: %v)", p, paths)
		}
	}
	for _, base := range []string{"https://api.anthropic.com", "https://api.anthropic.com/", "https://api.anthropic.com/v1", ""} {
		u, err := AnthropicMessagesURL(base)
		if err != nil || u != "https://api.anthropic.com/v1/messages" {
			t.Fatalf("%q -> %q %v", base, u, err)
		}
	}
	if _, err := NormalizeBaseURL("ftp://x", ""); err == nil {
		t.Fatal("expected scheme error")
	}
	if _, err := NormalizeBaseURL("http://", ""); err == nil {
		t.Fatal("expected host error")
	}
	bad := NewOpenAIProvider("o", "k", "::not a url")
	if _, err := bad.ChatCompletions(context.Background(), req); err == nil || bad.Health().Healthy {
		t.Fatal("misconfigured base URL must fail explicitly")
	}
}

func TestMatchModelAndSupports(t *testing.T) {
	if !MatchModel("gpt-4o*", "GPT-4o-mini") || MatchModel("gpt-4o*", "gpt-4") || !MatchModel("*", "x") || MatchModel("", "x") {
		t.Fatal("MatchModel")
	}
	a := NewAnthropicProvider("", "k", "claude-3-5-sonnet-latest")
	if !a.SupportsModel("claude-3-opus-20240229") || a.SupportsModel("gpt-4o") || !a.SupportsModel("") {
		t.Fatal("anthropic SupportsModel")
	}
	a.SetModels("my-claude-alias")
	if !a.SupportsModel("my-claude-alias") || a.SupportsModel("claude-3-opus") {
		t.Fatal("anthropic allowlist")
	}
	l := NewLocalProvider("http://x", "llama-3-8b")
	if !l.SupportsModel("LLAMA-3-8B") || l.SupportsModel("gpt-4o") || !l.SupportsModel("") {
		t.Fatal("local SupportsModel")
	}
	o := NewOpenAIProvider("o", "k", "")
	if !o.SupportsModel("anything") {
		t.Fatal("openai default accepts all")
	}
	o.SetModels("gpt-*")
	if o.SupportsModel("claude-3") || !o.SupportsModel("gpt-4o") {
		t.Fatal("openai allowlist")
	}
}

// ----------------------------------------------------------------- OpenAI

func TestOpenAIChatCompletionsWireFormat(t *testing.T) {
	var got map[string]interface{}
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"f","arguments":"{\"a\":1}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}`)
	}))
	defer srv.Close()

	var req models.LLMRequest
	in := `{"model":"gpt-4o","rag_enabled":true,"messages":[
		{"role":"system","content":"sys","cache_control":{"type":"ephemeral"}},
		{"role":"user","content":[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"https://img"}}]},
		{"role":"tool","tool_call_id":"call_0","tool_result":"42"}],
		"tools":[{"name":"f","description":"d","parameters":{"type":"object"}}],
		"tool_choice":{"type":"function","function":{"name":"f"}},"seed":7,"user":"u"}`
	if err := json.Unmarshal([]byte(in), &req); err != nil {
		t.Fatal(err)
	}
	p := NewOpenAIProvider("openai", "sk-test", srv.URL)
	resp, err := p.ChatCompletions(context.Background(), &req)
	if err != nil {
		t.Fatal(err)
	}
	if auth != "Bearer sk-test" {
		t.Fatalf("auth header %q", auth)
	}
	if _, ok := got["rag_enabled"]; ok {
		t.Fatal("gateway-only field sent upstream")
	}
	msgs := got["messages"].([]interface{})
	if _, ok := msgs[0].(map[string]interface{})["cache_control"]; ok {
		t.Fatal("message-level cache_control sent to OpenAI")
	}
	if parts, ok := msgs[1].(map[string]interface{})["content"].([]interface{}); !ok || len(parts) != 2 {
		t.Fatalf("content parts not forwarded: %v", msgs[1])
	}
	if msgs[2].(map[string]interface{})["content"] != "42" {
		t.Fatalf("tool result not mapped to content: %v", msgs[2])
	}
	tool := got["tools"].([]interface{})[0].(map[string]interface{})
	if tool["type"] != "function" || tool["function"].(map[string]interface{})["name"] != "f" {
		t.Fatalf("tools not in nested format: %v", tool)
	}
	if tc := got["tool_choice"].(map[string]interface{}); tc["type"] != "function" {
		t.Fatalf("tool_choice: %v", tc)
	}
	if got["seed"].(float64) != 7 || got["user"] != "u" {
		t.Fatalf("seed/user not forwarded: %v", got)
	}
	if resp.Choices[0].Message.ToolCalls[0].Function.Name != "f" || resp.Usage.TotalTokens != 8 || resp.Choices[0].Message.Content != nil {
		t.Fatalf("bad response: %+v", resp)
	}
	if h := p.Health(); !h.Healthy || h.LastChecked == 0 {
		t.Fatalf("health not tracked: %+v", h)
	}
}

func TestOpenAIDropsToolChoiceWithoutTools(t *testing.T) {
	b, _ := json.Marshal(NewOpenAIChatRequest(&models.LLMRequest{Model: "m", ToolChoice: &models.ToolChoice{Mode: "auto"}, ParallelToolCalls: new(bool)}, "", false, false))
	if strings.Contains(string(b), "tool_choice") || strings.Contains(string(b), "parallel_tool_calls") {
		t.Fatalf("tool options without tools: %s", b)
	}
}

func TestOpenAIUpstreamErrors(t *testing.T) {
	status := http.StatusTooManyRequests
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(status)
		io.WriteString(w, `{"error":{"message":"slow down","type":"rate_limit"}}`)
	}))
	defer srv.Close()
	p := NewOpenAIProvider("openai", "sk-verysecret", srv.URL)
	req := &models.LLMRequest{Model: "m", Messages: []models.Message{{Role: "user", Content: strp("x")}}}
	_, err := p.ChatCompletions(context.Background(), req)
	var ue *UpstreamError
	if !errors.As(err, &ue) || ue.StatusCode != 429 || ue.RetryAfter != 3*time.Second || ue.Message != "slow down" {
		t.Fatalf("bad error: %#v", err)
	}
	if strings.Contains(err.Error(), "sk-verysecret") {
		t.Fatal("api key leaked")
	}
	status = http.StatusUnauthorized
	_, err = p.ChatCompletions(context.Background(), req)
	if IsRetryable(err) || StatusCode(err) != 401 {
		t.Fatalf("401 must not be retryable: %v", err)
	}
	// Streams report the error before starting.
	ch, err := p.StreamChatCompletions(context.Background(), req)
	if ch != nil || StatusCode(err) != 401 {
		t.Fatalf("stream start error: %v %v", ch, err)
	}
}

func TestOpenAIMalformedAndOversizedResponses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"choices": [`)
	}))
	defer srv.Close()
	_, err := NewOpenAIProvider("o", "k", srv.URL).ChatCompletions(context.Background(), &models.LLMRequest{Model: "m"})
	if StatusCode(err) != http.StatusBadGateway || !IsRetryable(err) {
		t.Fatalf("malformed JSON should be 502: %v", err)
	}
	if _, err := ReadResponseBody(strings.NewReader(strings.Repeat("a", 11)), 10); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected too large, got %v", err)
	}
}

const openAIStreamBody = "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":5,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n" +
	": keep-alive comment\n\n" +
	"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":5,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel\"},\"finish_reason\":null}]}\n\n" +
	"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":5,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\r\n\r\n" +
	"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":5,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_9\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n" +
	"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":5,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"a\\\":1}\"}}]},\"finish_reason\":null}]}\n\n" +
	"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":5,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
	"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":5,\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":4,\"total_tokens\":13}}\n\n" +
	"data: [DONE]\n\n"

func TestOpenAIStream(t *testing.T) {
	var body map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		sse(w, openAIStreamBody)
	}))
	defer srv.Close()
	p := NewOpenAIProvider("openai", "k", srv.URL+"/v1")
	ch, err := p.StreamChatCompletions(context.Background(), &models.LLMRequest{Model: "gpt-4o", Messages: []models.Message{{Role: "user", Content: strp("hi")}}})
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if body["stream"] != true || body["stream_options"].(map[string]interface{})["include_usage"] != true {
		t.Fatalf("stream flags not sent: %v", body)
	}
	var text, args string
	var finish string
	var usage *models.Usage
	for _, c := range chunks {
		if c.Err != nil {
			t.Fatalf("unexpected err chunk: %v", c.Err)
		}
		if c.ID != "c1" || c.Object != "chat.completion.chunk" {
			t.Fatalf("chunk metadata: %+v", c)
		}
		for _, choice := range c.Choices {
			text += choice.Delta.Content
			for _, tc := range choice.Delta.ToolCalls {
				args += tc.Function.Arguments
			}
			if choice.FinishReason != nil {
				finish = *choice.FinishReason
			}
		}
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if text != "Hello" || args != `{"a":1}` || finish != "tool_calls" || usage == nil || usage.TotalTokens != 13 {
		t.Fatalf("text=%q args=%q finish=%q usage=%v", text, args, finish, usage)
	}
	if len(chunks) != 7 {
		t.Fatalf("expected 7 chunks, got %d", len(chunks))
	}
}

func TestOpenAIStreamPrematureEOF(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sse(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"par\"}}]}\n\n")
	}))
	defer srv.Close()
	ch, err := NewOpenAIProvider("o", "k", srv.URL).StreamChatCompletions(context.Background(), &models.LLMRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	last := chunks[len(chunks)-1]
	if last.Err == nil || StatusCode(last.Err) != http.StatusBadGateway {
		t.Fatalf("expected premature EOF error, got %+v", chunks)
	}
}

func TestOpenAIStreamMidStreamErrorEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sse(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n",
			"data: {\"error\":{\"message\":\"server overloaded\",\"type\":\"server_error\"}}\n\n")
	}))
	defer srv.Close()
	ch, _ := NewOpenAIProvider("o", "k", srv.URL).StreamChatCompletions(context.Background(), &models.LLMRequest{Model: "m"})
	chunks := collect(t, ch)
	if len(chunks) != 2 || chunks[1].Err == nil || !strings.Contains(chunks[1].Err.Error(), "server overloaded") {
		t.Fatalf("bad chunks: %+v", chunks)
	}
}

func TestOpenAIStreamJSONFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"x","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"full"},"finish_reason":"stop"}]}`)
	}))
	defer srv.Close()
	ch, err := NewLocalProvider(srv.URL, "m").StreamChatCompletions(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if len(chunks) != 3 || chunks[1].Choices[0].Delta.Content != "full" {
		t.Fatalf("bad fallback chunks: %+v", chunks)
	}
}

func TestStreamCancellationReleasesGoroutine(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var disconnected atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for i := 0; ; i++ {
			if _, err := fmt.Fprintf(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"%d\"}}]}\n\n", i); err != nil {
				disconnected.Store(true)
				return
			}
			f.Flush()
			select {
			case <-r.Context().Done():
				disconnected.Store(true)
				return
			case <-release:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := NewOpenAIProvider("o", "k", srv.URL).StreamChatCompletions(ctx, &models.LLMRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	<-ch
	cancel() // consumer stops reading and cancels
	deadline := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				// channel closed: goroutine exited
				time.Sleep(50 * time.Millisecond)
				if !disconnected.Load() {
					t.Log("server did not observe disconnect yet (non-fatal)")
				}
				return
			}
		case <-deadline:
			t.Fatal("stream goroutine did not exit after cancel")
		}
	}
}

func TestStreamIdleTimeout(t *testing.T) {
	old := StreamIdleTimeout
	StreamIdleTimeout = 100 * time.Millisecond
	defer func() { StreamIdleTimeout = old }()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sse(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n")
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ch, err := NewOpenAIProvider("o", "k", srv.URL).StreamChatCompletions(context.Background(), &models.LLMRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if last := chunks[len(chunks)-1]; StatusCode(last.Err) != http.StatusGatewayTimeout {
		t.Fatalf("expected idle timeout error, got %+v", chunks)
	}
}

func TestStreamHeaderTimeoutDoesNotCutStream(t *testing.T) {
	// Client.Timeout bounds only the header wait, not the whole stream.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for i := 0; i < 4; i++ {
			fmt.Fprintf(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n")
			f.Flush()
			time.Sleep(60 * time.Millisecond)
		}
		fmt.Fprint(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()
	p := NewOpenAIProvider("o", "k", srv.URL)
	p.SetHTTPClient(&http.Client{Timeout: 100 * time.Millisecond})
	ch, err := p.StreamChatCompletions(context.Background(), &models.LLMRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range collect(t, ch) {
		if c.Err != nil {
			t.Fatalf("stream cut by client timeout: %v", c.Err)
		}
	}
}

// -------------------------------------------------------------- Anthropic

func TestAnthropicRequestMapping(t *testing.T) {
	var got AnthropicRequest
	var hdr http.Header
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr = r.Header.Clone()
		path = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[{"type":"text","text":"Let me check."},{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Paris"}}],"stop_reason":"tool_use","usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":90}}`)
	}))
	defer srv.Close()

	var req models.LLMRequest
	in := `{"model":"claude-3-5-sonnet-20241022","temperature":1.5,"max_tokens":100,"stop":["END"],"user":"u1",
	"tool_choice":"required","parallel_tool_calls":false,
	"messages":[
	 {"role":"system","content":"Be brief.","cache_control":{"type":"ephemeral"}},
	 {"role":"user","content":[{"type":"text","text":"Weather?"},{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBOR"}}]},
	 {"role":"assistant","content":null,"tool_calls":[{"id":"toolu_0","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Rome\"}"}},{"id":"toolu_00","type":"function","function":{"name":"get_weather","arguments":""}}]},
	 {"role":"tool","tool_call_id":"toolu_0","content":"sunny"},
	 {"role":"tool","tool_call_id":"toolu_00","content":"rainy"},
	 {"role":"user","content":"And Paris?"}],
	"tools":[{"type":"function","function":{"name":"get_weather","description":"w","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}]}`
	if err := json.Unmarshal([]byte(in), &req); err != nil {
		t.Fatal(err)
	}
	p := NewAnthropicProvider(srv.URL+"/v1", "sk-ant", "claude-default")
	resp, err := p.ChatCompletions(context.Background(), &req)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/v1/messages" || hdr.Get("x-api-key") != "sk-ant" || hdr.Get("anthropic-version") != AnthropicVersion || hdr.Get("Authorization") != "" {
		t.Fatalf("path=%q headers=%v", path, hdr)
	}
	if got.Model != "claude-3-5-sonnet-20241022" || got.MaxTokens != 100 || *got.Temperature != 1 || got.StopSequences[0] != "END" || got.Metadata.UserID != "u1" {
		t.Fatalf("scalars: %+v", got)
	}
	if len(got.System) != 1 || got.System[0].Text != "Be brief." || got.System[0].CacheControl == nil {
		t.Fatalf("system: %+v", got.System)
	}
	if len(got.Messages) != 3 {
		t.Fatalf("expected user/assistant/user, got %+v", got.Messages)
	}
	u0 := got.Messages[0]
	if u0.Role != "user" || u0.Content[1].Type != "image" || u0.Content[1].Source.Type != "base64" || u0.Content[1].Source.MediaType != "image/png" {
		t.Fatalf("user image: %+v", u0)
	}
	a := got.Messages[1]
	if a.Role != "assistant" || len(a.Content) != 2 || a.Content[0].Type != "tool_use" || a.Content[0].ID != "toolu_0" || string(a.Content[1].Input) != "{}" {
		t.Fatalf("assistant tool_use: %+v", a)
	}
	u2 := got.Messages[2]
	if u2.Role != "user" || len(u2.Content) != 3 || u2.Content[0].Type != "tool_result" || u2.Content[0].ToolUseID != "toolu_0" || u2.Content[1].Content != "rainy" || u2.Content[2].Text != "And Paris?" {
		t.Fatalf("merged tool results: %+v", u2)
	}
	if len(got.Tools) != 1 || got.Tools[0].InputSchema["type"] != "object" {
		t.Fatalf("tools: %+v", got.Tools)
	}
	if got.ToolChoice == nil || got.ToolChoice.Type != "any" || !got.ToolChoice.DisableParallelToolUse {
		t.Fatalf("tool_choice: %+v", got.ToolChoice)
	}
	c := resp.Choices
	if len(c) != 1 || *c[0].Message.Content != "Let me check." || c[0].FinishReason != "tool_calls" ||
		c[0].Message.ToolCalls[0].Function.Arguments != `{"city":"Paris"}` || c[0].Message.ToolCalls[0].ID != "toolu_1" {
		t.Fatalf("response: %+v", resp)
	}
	if resp.Usage.PromptTokens != 100 || resp.Usage.CompletionTokens != 5 || resp.Usage.PromptTokensDetails.CachedTokens != 90 {
		t.Fatalf("usage: %+v", resp.Usage)
	}
}

func TestAnthropicRequestValidation(t *testing.T) {
	cases := []*models.LLMRequest{
		{Model: "c", Messages: []models.Message{{Role: "system", Content: strp("only system")}}},
		{Model: "c", N: func() *int { n := 2; return &n }(), Messages: []models.Message{{Role: "user", Content: strp("x")}}},
		{Model: "c", Messages: []models.Message{{Role: "tool", Content: strp("x")}}},
		{Model: "c", Messages: []models.Message{{Role: "user", ContentParts: []models.ContentPart{{Type: "input_audio", InputAudio: &models.InputAudio{Data: "x", Format: "wav"}}}}}},
		{Model: "", Messages: []models.Message{{Role: "user", Content: strp("x")}}},
	}
	for i, r := range cases {
		_, err := BuildAnthropicRequest("anthropic", r, "", false)
		if StatusCode(err) != 400 || IsRetryable(err) {
			t.Fatalf("case %d: expected 400, got %v", i, err)
		}
	}
	// A request without a model uses the provider default.
	b, err := BuildAnthropicRequest("anthropic", &models.LLMRequest{Messages: []models.Message{{Role: "user", Content: strp("x")}}}, "claude-default", false)
	if err != nil || b.Model != "claude-default" || b.MaxTokens != AnthropicDefaultMaxTokens {
		t.Fatalf("defaults: %+v %v", b, err)
	}
}

const anthropicStreamBody = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_9\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-20241022\",\"content\":[],\"usage\":{\"input_tokens\":25,\"output_tokens\":1}}}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
	"event: ping\ndata: {\"type\":\"ping\"}\n\n" +
	"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi \"}}\n\n" +
	"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"there\"}}\n\n" +
	"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_7\",\"name\":\"get_weather\",\"input\":{}}}\n\n" +
	"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\": \"}}\n\n" +
	"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"Paris\\\"}\"}}\n\n" +
	"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n" +
	"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":42}}\n\n" +
	"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

func TestAnthropicStream(t *testing.T) {
	var got AnthropicRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		sse(w, anthropicStreamBody)
	}))
	defer srv.Close()
	p := NewAnthropicProvider(srv.URL, "k", "claude-3-5-sonnet-20241022")
	ch, err := p.StreamChatCompletions(context.Background(), &models.LLMRequest{Messages: []models.Message{{Role: "user", Content: strp("hi")}}})
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if !got.Stream {
		t.Fatal("stream flag not sent")
	}
	var text, args, finish, role, toolID, toolName string
	var usage *models.Usage
	for _, c := range chunks {
		if c.Err != nil {
			t.Fatal(c.Err)
		}
		if c.ID != "msg_9" || c.Model != "claude-3-5-sonnet-20241022" {
			t.Fatalf("metadata: %+v", c)
		}
		for _, ch := range c.Choices {
			if ch.Delta.Role != "" {
				role = string(ch.Delta.Role)
			}
			text += ch.Delta.Content
			for _, tc := range ch.Delta.ToolCalls {
				if tc.Index != 0 {
					t.Fatalf("tool index should be 0, got %d", tc.Index)
				}
				args += tc.Function.Arguments
				if tc.ID != "" {
					toolID, toolName = tc.ID, tc.Function.Name
				}
			}
			if ch.FinishReason != nil {
				finish = *ch.FinishReason
			}
		}
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if role != "assistant" || text != "Hi there" || args != `{"city": "Paris"}` || toolID != "toolu_7" || toolName != "get_weather" || finish != "tool_calls" {
		t.Fatalf("role=%q text=%q args=%q tool=%q/%q finish=%q", role, text, args, toolID, toolName, finish)
	}
	if usage == nil || usage.PromptTokens != 25 || usage.CompletionTokens != 42 || usage.TotalTokens != 67 {
		t.Fatalf("usage: %+v", usage)
	}
	last := chunks[len(chunks)-1]
	if last.Usage == nil || len(last.Choices) != 0 {
		t.Fatalf("final usage chunk: %+v", last)
	}
	b, _ := json.Marshal(last)
	if !strings.Contains(string(b), `"choices":[]`) {
		t.Fatalf("usage chunk must serialize empty choices: %s", b)
	}
}

func TestAnthropicStreamErrorEventAndEOF(t *testing.T) {
	body := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"c\",\"usage\":{\"input_tokens\":1}}}\n\n" +
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sse(w, body) }))
	defer srv.Close()
	ch, err := NewAnthropicProvider(srv.URL, "k", "c").StreamChatCompletions(context.Background(), &models.LLMRequest{Messages: []models.Message{{Role: "user", Content: strp("x")}}})
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	last := chunks[len(chunks)-1]
	if StatusCode(last.Err) != 529 || !IsRetryable(last.Err) {
		t.Fatalf("expected overloaded error, got %+v", last)
	}

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sse(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"c\",\"usage\":{}}}\n\n")
	}))
	defer srv2.Close()
	ch, _ = NewAnthropicProvider(srv2.URL, "k", "c").StreamChatCompletions(context.Background(), &models.LLMRequest{Messages: []models.Message{{Role: "user", Content: strp("x")}}})
	chunks = collect(t, ch)
	if chunks[len(chunks)-1].Err == nil {
		t.Fatal("expected error for stream without message_stop")
	}
}

func TestAnthropicUpstreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"type":"error","error":{"type":"not_found_error","message":"model: claude-x"}}`)
	}))
	defer srv.Close()
	_, err := NewAnthropicProvider(srv.URL, "k", "claude-x").ChatCompletions(context.Background(), &models.LLMRequest{Messages: []models.Message{{Role: "user", Content: strp("x")}}})
	var ue *UpstreamError
	if !errors.As(err, &ue) || ue.StatusCode != 404 || ue.Type != "not_found_error" || IsRetryable(err) {
		t.Fatalf("got %#v", err)
	}
}

// -------------------------------------------------------------------- SSE

func TestSSEReader(t *testing.T) {
	in := ": comment\nevent: a\ndata: line1\ndata: line2\nid: 7\n\n" +
		"data:nospace\n\n" +
		"{\"raw\":true}\n" +
		"retry: 100\n\n" +
		"event: tail\ndata: end"
	r := NewSSEReader(strings.NewReader(in))
	want := []SSEEvent{{Event: "a", Data: "line1\nline2", ID: "7"}, {Data: "nospace"}, {Data: `{"raw":true}`}, {Event: "tail", Data: "end"}}
	for i, w := range want {
		ev, err := r.Next()
		if err != nil || ev != w {
			t.Fatalf("event %d: got %+v %v want %+v", i, ev, err, w)
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	long := "data: " + strings.Repeat("x", MaxSSELineBytes+10) + "\n\n"
	if _, err := NewSSEReader(strings.NewReader(long)).Next(); !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("expected ErrTooLong, got %v", err)
	}
}

func TestHealthTracker(t *testing.T) {
	var h HealthTracker
	for i := 0; i < 3; i++ {
		h.Observe(time.Millisecond, &UpstreamError{StatusCode: 503})
	}
	if s := h.Snapshot("x", ProviderOpenAI); s.Healthy || s.Failures != 3 {
		t.Fatalf("expected unhealthy: %+v", s)
	}
	h.Observe(time.Millisecond, &UpstreamError{StatusCode: 400}) // alive
	h.Observe(10*time.Millisecond, context.Canceled)             // ignored
	if s := h.Snapshot("x", ProviderOpenAI); !s.Healthy {
		t.Fatalf("expected healthy after 4xx: %+v", s)
	}
	h.Observe(20*time.Millisecond, nil)
	if s := h.Snapshot("x", ProviderOpenAI); s.LatencyMs != 20 {
		t.Fatalf("latency: %+v", s)
	}
}

func TestUpstreamErrorRedactsEchoedKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"error":{"message":"Incorrect API key provided: %s"}}`, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	}))
	defer srv.Close()
	p := NewOpenAIProvider("o", "sk-live-0123456789abcdef", srv.URL)
	_, err := p.ChatCompletions(context.Background(), &models.LLMRequest{Model: "m"})
	if err == nil || strings.Contains(err.Error(), "sk-live-0123456789abcdef") || !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("key not redacted: %v", err)
	}
	_, err = p.StreamChatCompletions(context.Background(), &models.LLMRequest{Model: "m"})
	if err == nil || strings.Contains(err.Error(), "sk-live-0123456789abcdef") {
		t.Fatalf("stream: key not redacted: %v", err)
	}
	a := NewAnthropicProvider(srv.URL, "sk-ant-0123456789", "claude")
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"type":"error","error":{"type":"authentication_error","message":"bad key %s"}}`, r.Header.Get("x-api-key"))
	}))
	defer srv2.Close()
	a.BaseURL = srv2.URL
	_, err = a.ChatCompletions(context.Background(), &models.LLMRequest{Messages: []models.Message{{Role: "user", Content: strp("x")}}})
	if err == nil || strings.Contains(err.Error(), "sk-ant-0123456789") {
		t.Fatalf("anthropic: key not redacted: %v", err)
	}
}
