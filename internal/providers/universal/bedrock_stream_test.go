package universal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

type bedrockSeen struct {
	rawURI, accept, auth, ctype string
	body                        converseRequest
}

// bedrockStreamServer serves the given raw bytes (event-stream frames) as a
// ConverseStream response, flushing after each part.
func bedrockStreamServer(t *testing.T, seen *atomic.Pointer[bedrockSeen], parts ...[]byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := &bedrockSeen{rawURI: r.RequestURI, accept: r.Header.Get("Accept"), auth: r.Header.Get("Authorization"), ctype: r.Header.Get("Content-Type")}
		_ = json.NewDecoder(r.Body).Decode(&s.body)
		if seen != nil {
			seen.Store(s)
		}
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		for _, p := range parts {
			_, _ = w.Write(p)
			f.Flush()
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestBedrock(url string) *BedrockAdapter {
	a := NewBedrockAdapter("AKIDEXAMPLE:secret", url)
	a.SetRegion("us-west-2")
	a.now = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
	return a
}

func TestBedrockConverseStreamTextAndToolUse(t *testing.T) {
	var seen atomic.Pointer[bedrockSeen]
	srv := bedrockStreamServer(t, &seen,
		esEvent("messageStart", `{"role":"assistant","p":"abc"}`),
		esEvent("contentBlockDelta", `{"contentBlockIndex":0,"delta":{"text":"Hel"}}`),
		esEvent("contentBlockDelta", `{"contentBlockIndex":0,"delta":{"text":"lo"}}`),
		esEvent("contentBlockDelta", `{"contentBlockIndex":0,"delta":{"reasoningContent":{"text":"hidden"}}}`),
		esEvent("contentBlockStop", `{"contentBlockIndex":0}`),
		esEvent("contentBlockStart", `{"contentBlockIndex":1,"start":{"toolUse":{"toolUseId":"tu_1","name":"get_weather"}}}`),
		esEvent("contentBlockDelta", `{"contentBlockIndex":1,"delta":{"toolUse":{"input":"{\"city\":"}}}`),
		esEvent("contentBlockDelta", `{"contentBlockIndex":1,"delta":{"toolUse":{"input":"\"Oslo\"}"}}}`),
		esEvent("contentBlockStop", `{"contentBlockIndex":1}`),
		esEvent("contentBlockStart", `{"contentBlockIndex":2,"start":{"toolUse":{"toolUseId":"tu_2","name":"now"}}}`),
		esEvent("contentBlockStop", `{"contentBlockIndex":2}`),
		esEvent("futureEvent", `{"whatever":true}`),
		esEvent("messageStop", `{"stopReason":"tool_use"}`),
		esEvent("metadata", `{"usage":{"inputTokens":12,"outputTokens":30,"totalTokens":42,"cacheReadInputTokens":4},"metrics":{"latencyMs":99}}`),
	)
	a := newTestBedrock(srv.URL)
	var sp providers.StreamingProvider = a
	ch, err := sp.StreamChatCompletions(context.Background(), userReq("anthropic.claude-3-5-sonnet-20240620-v1:0", "weather?"))
	if err != nil {
		t.Fatal(err)
	}
	chunks := collectChunks(t, ch)

	s := seen.Load()
	if s.rawURI != "/model/anthropic.claude-3-5-sonnet-20240620-v1%3A0/converse-stream" {
		t.Fatalf("path: %s", s.rawURI)
	}
	if s.accept != "application/vnd.amazon.eventstream" || s.ctype != "application/json" ||
		!strings.HasPrefix(s.auth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-west-2/bedrock/aws4_request") {
		t.Fatalf("request headers: %+v", s)
	}
	if len(s.body.Messages) != 1 || s.body.Messages[0].Content[0].Text != "weather?" {
		t.Fatalf("converse body: %+v", s.body)
	}

	var role, text, finish string
	args := map[int]string{}
	ids := map[int]string{}
	names := map[int]string{}
	var usage *models.Usage
	for i, c := range chunks {
		if c.Err != nil {
			t.Fatalf("chunk %d: %v", i, c.Err)
		}
		if c.ID == "" || c.ID != chunks[0].ID || c.Model != "anthropic.claude-3-5-sonnet-20240620-v1:0" || c.Object != "chat.completion.chunk" {
			t.Fatalf("chunk metadata: %+v", c)
		}
		for _, ch := range c.Choices {
			if ch.Delta.Role != "" {
				role = string(ch.Delta.Role)
			}
			text += ch.Delta.Content
			for _, tc := range ch.Delta.ToolCalls {
				args[tc.Index] += tc.Function.Arguments
				if tc.ID != "" {
					ids[tc.Index], names[tc.Index] = tc.ID, tc.Function.Name
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
	if role != "assistant" || text != "Hello" || finish != "tool_calls" {
		t.Fatalf("role=%q text=%q finish=%q", role, text, finish)
	}
	if ids[0] != "tu_1" || names[0] != "get_weather" || args[0] != `{"city":"Oslo"}` {
		t.Fatalf("tool 0: id=%q name=%q args=%q", ids[0], names[0], args[0])
	}
	if ids[1] != "tu_2" || names[1] != "now" || args[1] != "{}" {
		t.Fatalf("tool without input must get {} arguments: id=%q args=%q", ids[1], args[1])
	}
	last := chunks[len(chunks)-1]
	if usage == nil || last.Usage != usage || len(last.Choices) != 0 || usage.PromptTokens != 12 || usage.CompletionTokens != 30 ||
		usage.TotalTokens != 42 || usage.PromptTokensDetails == nil || usage.PromptTokensDetails.CachedTokens != 4 {
		t.Fatalf("usage chunk: %+v", last)
	}
	if a.Health()["healthy"] != true {
		t.Fatal("successful stream must keep the adapter healthy")
	}

	// Legacy chunk format rides on the native stream.
	legacy, err := a.Stream(context.Background(), userReq("m", "x"))
	if err != nil {
		t.Fatal(err)
	}
	var ltext string
	finished := false
	for c := range legacy {
		if c.Err != nil {
			t.Fatal(c.Err)
		}
		ltext += c.Delta
		finished = finished || c.Finish
	}
	if ltext != "Hello" || !finished {
		t.Fatalf("legacy stream: %q finished=%v", ltext, finished)
	}
}

func TestBedrockConverseStreamFirstFrameExceptionIsDirect(t *testing.T) {
	srv := bedrockStreamServer(t, nil, esException("throttlingException", `{"message":"Too many requests, please wait"}`))
	ch, err := newTestBedrock(srv.URL).StreamChatCompletions(context.Background(), userReq("amazon.nova-lite-v1:0", "x"))
	if ch != nil || err == nil {
		t.Fatalf("an exception before any content must be returned directly (fallback): ch=%v err=%v", ch, err)
	}
	var ue *providers.UpstreamError
	if !errors.As(err, &ue) || ue.StatusCode != http.StatusTooManyRequests || ue.Type != "throttlingException" ||
		!strings.Contains(ue.Message, "Too many requests") || !providers.IsRetryable(err) {
		t.Fatalf("throttling mapping: %#v", err)
	}
}

func TestBedrockConverseStreamMidStreamErrors(t *testing.T) {
	start := [][]byte{
		esEvent("messageStart", `{"role":"assistant"}`),
		esEvent("contentBlockDelta", `{"contentBlockIndex":0,"delta":{"text":"partial"}}`),
	}
	corrupt := esEvent("contentBlockDelta", `{"contentBlockIndex":0,"delta":{"text":"x"}}`)
	corrupt[len(corrupt)-1] ^= 0xFF
	truncated := esEvent("contentBlockDelta", `{"contentBlockIndex":0,"delta":{"text":"y"}}`)
	truncated = truncated[:len(truncated)-5]
	errMsg := encodeES([]esHeader{
		esString(":message-type", "error"),
		esString(":error-code", "InternalFailure"),
		esString(":error-message", "boom"),
	}, nil)

	cases := []struct {
		name   string
		tail   []byte
		status int
		typ    string
	}{
		{"validation exception", esException("validationException", `{"message":"bad input"}`), 400, "validationException"},
		{"model stream error keeps original status", esException("modelStreamErrorException", `{"message":"model failed","originalStatusCode":503}`), 503, "modelStreamErrorException"},
		{"internal server", esException("internalServerException", `{"Message":"oops"}`), 500, "internalServerException"},
		{"unknown exception", esException("somethingNewException", `{}`), 502, "somethingNewException"},
		{"error message", errMsg, 502, "InternalFailure"},
		{"bad crc", corrupt, 502, ""},
		{"truncated frame", truncated, 502, ""},
		{"eof before messageStop", nil, 502, ""},
		{"invalid json payload", esEvent("contentBlockDelta", `{not json`), 502, ""},
	}
	for _, c := range cases {
		parts := append(append([][]byte{}, start...), c.tail)
		srv := bedrockStreamServer(t, nil, parts...)
		a := newTestBedrock(srv.URL)
		ch, err := a.StreamChatCompletions(context.Background(), userReq("m", "x"))
		if err != nil {
			t.Fatalf("%s: stream should start: %v", c.name, err)
		}
		chunks := collectChunks(t, ch)
		last := chunks[len(chunks)-1]
		var text string
		for _, ck := range chunks[:len(chunks)-1] {
			if ck.Err != nil {
				t.Fatalf("%s: Err only on the final chunk", c.name)
			}
			for _, chc := range ck.Choices {
				text += chc.Delta.Content
			}
		}
		var ue *providers.UpstreamError
		if last.Err == nil || !errors.As(last.Err, &ue) || ue.StatusCode != c.status || (c.typ != "" && ue.Type != c.typ) {
			t.Fatalf("%s: final chunk %+v (err %v)", c.name, last, last.Err)
		}
		if text != "partial" {
			t.Fatalf("%s: content before the failure must be delivered: %q", c.name, text)
		}
	}
}

func TestBedrockConverseStreamStartFailures(t *testing.T) {
	// Corrupt first frame: reported directly as a 502.
	bad := esEvent("messageStart", `{"role":"assistant"}`)
	bad[8] ^= 0xFF
	srv := bedrockStreamServer(t, nil, bad)
	if _, err := newTestBedrock(srv.URL).StreamChatCompletions(context.Background(), userReq("m", "x")); providers.StatusCode(err) != 502 {
		t.Fatalf("corrupt first frame: %v", err)
	}
	// Empty body.
	srv = bedrockStreamServer(t, nil)
	if _, err := newTestBedrock(srv.URL).StreamChatCompletions(context.Background(), userReq("m", "x")); providers.StatusCode(err) != 502 {
		t.Fatalf("empty stream: %v", err)
	}
	// HTTP error before the stream.
	httpErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"not authorized"}`))
	}))
	defer httpErr.Close()
	if _, err := newTestBedrock(httpErr.URL).StreamChatCompletions(context.Background(), userReq("m", "x")); providers.StatusCode(err) != 403 {
		t.Fatalf("http error: %v", err)
	}
	// Invalid requests never reach the network.
	var calls atomic.Int32
	counting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer counting.Close()
	a := newTestBedrock(counting.URL)
	if _, err := a.StreamChatCompletions(context.Background(), userReq("bad model/../x y", "x")); providers.StatusCode(err) != 400 {
		t.Fatalf("bad model id: %v", err)
	}
	if _, err := a.StreamChatCompletions(context.Background(), nil); err == nil {
		t.Fatal("nil request")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid requests must not be sent")
	}
	// Legacy Stream reports a start failure as a single error chunk.
	legacy, err := a.Stream(context.Background(), userReq("bad model", "x"))
	if err != nil {
		t.Fatal(err)
	}
	c := <-legacy
	if c.Err == nil || !c.Finish {
		t.Fatalf("legacy error chunk: %+v", c)
	}
}

func TestBedrockConverseStreamCancellation(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var disconnected atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		f := w.(http.Flusher)
		_, _ = w.Write(esEvent("messageStart", `{"role":"assistant"}`))
		f.Flush()
		for {
			if _, err := w.Write(esEvent("contentBlockDelta", `{"contentBlockIndex":0,"delta":{"text":"tick"}}`)); err != nil {
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
	ch, err := newTestBedrock(srv.URL).StreamChatCompletions(ctx, userReq("m", "x"))
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	<-ch
	cancel()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return
			}
			if c.Err != nil {
				t.Fatalf("cancellation must not produce an error chunk: %v", c.Err)
			}
		case <-deadline:
			t.Fatal("stream goroutine did not exit after cancel")
		}
	}
}
