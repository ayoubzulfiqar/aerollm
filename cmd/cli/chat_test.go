package main

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestReadSSE(t *testing.T) {
	stream := ": keep-alive comment\r\n" +
		"event: message\r\n" +
		"id: 1\r\n" +
		"data: first\r\n" +
		"data:second line\r\n" +
		"\r\n" +
		"data: {\"x\":1}\n" +
		"\n" +
		"\n" + // blank line with no pending data dispatches nothing
		"data: trailing-without-blank-line"
	var got []sseEvent
	err := readSSE(strings.NewReader(stream), func(ev sseEvent) error {
		got = append(got, ev)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []sseEvent{
		{Event: "message", ID: "1", Data: "first\nsecond line"},
		{Data: `{"x":1}`},
		{Data: "trailing-without-blank-line"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d events %+v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestReadSSEStopAndError(t *testing.T) {
	stream := "data: a\n\ndata: b\n\ndata: c\n\n"
	var n int
	if err := readSSE(strings.NewReader(stream), func(ev sseEvent) error {
		n++
		if ev.Data == "b" {
			return errStopSSE
		}
		return nil
	}); err != nil || n != 2 {
		t.Fatalf("stop: err=%v n=%d", err, n)
	}
	boom := errors.New("boom")
	if err := readSSE(strings.NewReader(stream), func(sseEvent) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("expected callback error, got %v", err)
	}
}

func chunk(content string) string {
	return fmt.Sprintf(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":%q},"finish_reason":null}]}`, content)
}

func TestStreamChatCompletion(t *testing.T) {
	stream := "data: " + `{"choices":[{"index":0,"delta":{"role":"assistant"}}]}` + "\n\n" +
		"data: " + chunk("Hel") + "\n\n" +
		": ping\n\n" +
		"data: " + `{"choices":[{"index":1,"delta":{"content":"other choice"}}]}` + "\n\n" +
		"data: " + chunk("lo") + "\n\n" +
		"data: [DONE]\n\n" +
		"data: " + chunk("ignored after done") + "\n\n"
	var b strings.Builder
	var raw int
	err := streamChatCompletion(strings.NewReader(stream),
		func(s string) error { b.WriteString(s); return nil },
		func([]byte) error { raw++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if b.String() != "Hello" {
		t.Fatalf("content = %q", b.String())
	}
	if raw != 4 {
		t.Fatalf("raw chunks = %d, want 4", raw)
	}
}

func TestStreamWithoutDoneButFinished(t *testing.T) {
	stream := "data: " + chunk("hi") + "\n\n" +
		"data: " + `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n"
	if err := streamChatCompletion(strings.NewReader(stream), func(string) error { return nil }, nil); err != nil {
		t.Fatalf("a stream with finish_reason should be complete, got %v", err)
	}
}

func TestStreamChatCompletionErrors(t *testing.T) {
	cases := map[string]string{
		"truncated":   "data: " + chunk("partial") + "\n\n",
		"error chunk": "data: " + chunk("a") + "\n\ndata: {\"error\":{\"message\":\"upstream exploded\"}}\n\n",
		"error event": "event: error\ndata: {\"error\":\"rate limited\"}\n\n",
		"bad json":    "data: {not json\n\n",
	}
	for name, stream := range cases {
		err := streamChatCompletion(strings.NewReader(stream), func(string) error { return nil }, nil)
		if err == nil {
			t.Errorf("%s: expected error", name)
			continue
		}
		switch name {
		case "truncated":
			if !errors.Is(err, errStreamTruncated) {
				t.Errorf("truncated: got %v", err)
			}
		case "error chunk":
			if !strings.Contains(err.Error(), "upstream exploded") {
				t.Errorf("error chunk: got %v", err)
			}
		case "error event":
			if !strings.Contains(err.Error(), "rate limited") {
				t.Errorf("error event: got %v", err)
			}
		}
	}
}

// sseHandler streams events with flushes and small delays, like a real
// upstream, so the CLI must handle incremental delivery.
func sseHandler(events ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		for _, e := range events {
			fmt.Fprintf(w, "data: %s\n\n", e)
			if fl != nil {
				fl.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestChatStreamEndToEnd(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.handle("/v1/chat/completions", sseHandler(chunk("The "), chunk("answer "), chunk("is 42."), "[DONE]"))

	out, _, err := g.run(t, "", "chat", "What is the answer?", "-m", "gpt-4o", "--stream", "--temperature", "0.2", "--max-tokens", "64", "--system", "be brief")
	if err != nil {
		t.Fatal(err)
	}
	if out != "The answer is 42.\n" {
		t.Fatalf("stdout = %q", out)
	}
	req := g.last(t)
	if req.Header.Get("Accept") != "text/event-stream" {
		t.Errorf("Accept = %q", req.Header.Get("Accept"))
	}
	body := decodeBody(t, req.Body)
	if body["stream"] != true || body["model"] != "gpt-4o" || body["temperature"] != 0.2 || body["max_tokens"] != float64(64) {
		t.Fatalf("unexpected request body: %s", req.Body)
	}
	msgs := body["messages"].([]any)
	if len(msgs) != 2 || msgs[0].(map[string]any)["role"] != "system" || msgs[1].(map[string]any)["content"] != "What is the answer?" {
		t.Fatalf("unexpected messages: %v", msgs)
	}
}

func TestChatStreamJSONOutputAndTruncation(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.handle("/v1/chat/completions", sseHandler(chunk("a"), chunk("b"), "[DONE]"))
	out, _, err := g.run(t, "", "chat", "hi", "-m", "m", "--stream", "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSpace(out), "\n"); len(lines) != 2 || !strings.Contains(lines[0], `"content":"a"`) {
		t.Fatalf("expected 2 NDJSON chunks, got %q", out)
	}

	g.handle("/v1/chat/completions", sseHandler(chunk("cut")))
	out, _, err = g.run(t, "", "chat", "hi", "-m", "m", "--stream")
	if !errors.Is(err, errStreamTruncated) {
		t.Fatalf("expected truncation error, got %v", err)
	}
	if !strings.Contains(out, "cut") {
		t.Fatalf("partial output should still be printed, got %q", out)
	}
}

func TestChatStreamServerAnswersWithJSON(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/chat/completions", 200, `{"choices":[{"index":0,"message":{"role":"assistant","content":"not streamed"}}]}`)
	out, _, err := g.run(t, "", "chat", "hi", "-m", "m", "--stream")
	if err != nil || out != "not streamed\n" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestChatNonStreamAndStdin(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/chat/completions", 200, `{"id":"x","choices":[{"index":0,"message":{"role":"assistant","content":[{"type":"text","text":"part one, "},{"type":"text","text":"part two"}]}}],"usage":{"total_tokens":5}}`)

	out, _, err := g.run(t, "prompt from\nstdin\n", "chat", "-", "-m", "gpt-4o")
	if err != nil {
		t.Fatal(err)
	}
	if out != "part one, part two\n" {
		t.Fatalf("stdout = %q", out)
	}
	body := decodeBody(t, g.last(t).Body)
	if _, ok := body["stream"]; ok {
		t.Errorf("stream should be omitted for non-streaming requests: %s", g.last(t).Body)
	}
	if _, ok := body["temperature"]; ok {
		t.Errorf("temperature should be omitted unless set: %s", g.last(t).Body)
	}
	msgs := body["messages"].([]any)
	if msgs[0].(map[string]any)["content"] != "prompt from\nstdin\n" {
		t.Fatalf("stdin prompt not sent: %v", msgs)
	}

	out, _, err = g.run(t, "", "chat", "hi", "-m", "gpt-4o", "-o", "json")
	if err != nil || !strings.Contains(out, `"total_tokens": 5`) {
		t.Fatalf("json output: out=%q err=%v", out, err)
	}
}

func TestChatValidation(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	for _, args := range [][]string{
		{"chat", "hi"}, // no model
		{"chat", "hi", "-m", "m", "--temperature", "3"}, // out of range
		{"chat", "hi", "-m", "m", "--max-tokens", "0"},  // not positive
		{"chat", "hi", "-m", "m", "--top-p", "0"},       // out of range
		{"chat", "   ", "-m", "m"},                      // empty prompt
	} {
		if _, _, err := g.run(t, "", args...); err == nil {
			t.Errorf("%v: expected validation error", args)
		}
	}
	if n := len(g.all()); n != 0 {
		t.Fatalf("invalid input must not reach the server, got %d requests", n)
	}
	t.Setenv("AEROLLM_MODEL", "env-model")
	g.json("/v1/chat/completions", 200, `{"choices":[{"message":{"content":"ok"}}]}`)
	if _, _, err := g.run(t, "", "chat", "hi"); err != nil {
		t.Fatal(err)
	}
	if decodeBody(t, g.last(t).Body)["model"] != "env-model" {
		t.Fatal("AEROLLM_MODEL not used")
	}
}

func TestChatErrorResponse(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/chat/completions", http.StatusTooManyRequests, `{"error":{"message":"rate limit exceeded","type":"rate_limit"}}`)
	for _, stream := range []bool{false, true} {
		args := []string{"chat", "hi", "-m", "m"}
		if stream {
			args = append(args, "--stream")
		}
		_, _, err := g.run(t, "", args...)
		if err == nil || !strings.Contains(err.Error(), "429") || !strings.Contains(err.Error(), "rate limit exceeded") {
			t.Fatalf("stream=%v: got %v", stream, err)
		}
	}
}
