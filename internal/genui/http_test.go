package genui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func completion(content string) string {
	b, _ := json.Marshal(map[string]interface{}{
		"id":    "chatcmpl-1",
		"model": "gpt-x",
		"choices": []interface{}{map[string]interface{}{
			"message":       map[string]interface{}{"role": "assistant", "content": content},
			"finish_reason": "stop",
		}},
		"usage": map[string]int{"total_tokens": 5},
	})
	return string(b)
}

func jsonHandler(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Custom", "kept")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func optIn() *http.Request {
	return httptest.NewRequest(http.MethodPost, "/v1/chat/completions?genui=1", strings.NewReader("{}"))
}

func TestGenUIRewritesSchemaAsSSE(t *testing.T) {
	content := "Here you go:\n```json\n{\"type\": \"aerollm_ui\", \"components\": [{\"kind\": \"card\", \"props\": {\"title\": \"Hi\"}}]}\n```\nEnjoy!"
	rec := httptest.NewRecorder()
	NewGenUIHandler(jsonHandler(http.StatusOK, completion(content)))(rec, optIn())

	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected SSE, got %q: %s", ct, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		"event: text\ndata: \"Here you go:\"\n\n",
		"event: ui_schema\ndata: {\"type\":\"aerollm_ui\",\"components\":[{\"kind\":\"card\"",
		"event: text\ndata: \"Enjoy!\"\n\n",
		"event: done\ndata: {",
		`"id":"chatcmpl-1"`,
		`"usage":{"total_tokens":5}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}

func TestGenUIPassThroughCases(t *testing.T) {
	plain := completion("just text")
	cases := []struct {
		name    string
		handler http.HandlerFunc
		req     *http.Request
		status  int
		body    string
	}{
		{"not opted in", jsonHandler(200, completion(`{"type":"aerollm_ui","components":[]}`)), httptest.NewRequest(http.MethodPost, "/", nil), 200, completion(`{"type":"aerollm_ui","components":[]}`)},
		{"no schema", jsonHandler(200, plain), optIn(), 200, plain},
		{"error status", jsonHandler(502, `{"error":"upstream"}`), optIn(), 502, `{"error":"upstream"}`},
		{"not json", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("hello"))
		}, optIn(), 200, "hello"},
		{"invalid json", jsonHandler(200, `{"choices":`), optIn(), 200, `{"choices":`},
		{"http.Error", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"bad"}`, http.StatusBadRequest)
		}, optIn(), 400, "{\"error\":\"bad\"}\n"},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		NewGenUIHandler(tc.handler)(rec, tc.req)
		if rec.Code != tc.status || rec.Body.String() != tc.body {
			t.Errorf("%s: got %d %q, want %d %q", tc.name, rec.Code, rec.Body.String(), tc.status, tc.body)
		}
		if strings.HasPrefix(tc.name, "no schema") && rec.Header().Get("X-Custom") != "kept" {
			t.Errorf("%s: headers not preserved", tc.name)
		}
	}
}

func TestGenUIStreamingPassesThroughLive(t *testing.T) {
	flushesSeen := 0
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for i := 0; i < 3; i++ {
			_, _ = w.Write([]byte("data: {\"choices\":[]}\n\n"))
			f, ok := w.(http.Flusher)
			if !ok {
				t.Fatal("wrapped writer must implement http.Flusher")
			}
			f.Flush()
			flushesSeen++
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}
	rec := httptest.NewRecorder()
	req := optIn()
	req.Header.Set("Accept", "text/event-stream")
	NewGenUIHandler(handler)(rec, req)
	if !rec.Flushed || flushesSeen != 3 {
		t.Fatal("flushes must reach the underlying writer")
	}
	if !strings.HasSuffix(rec.Body.String(), "data: [DONE]\n\n") || strings.Count(rec.Body.String(), "data: ") != 4 {
		t.Fatalf("SSE stream altered: %q", rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatal("content type altered")
	}
}

func TestGenUIAcceptHeaderAloneIsNotOptIn(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Accept", "text/event-stream")
	if IsGenUIRequest(req) {
		t.Fatal("Accept: text/event-stream is sent by OpenAI SDKs for normal streaming")
	}
	req.Header.Set(OptInHeader, "true")
	if !IsGenUIRequest(req) {
		t.Fatal("header opt-in not honoured")
	}
}

func TestGenUILargeBodyPassesThrough(t *testing.T) {
	big := completion(strings.Repeat("x", DefaultMaxBufferBytes) + `{"type":"aerollm_ui","components":[]}`)
	rec := httptest.NewRecorder()
	NewGenUIHandler(jsonHandler(200, big))(rec, optIn())
	if rec.Body.Len() != len(big) || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatal("oversized responses must be forwarded unchanged")
	}
}

func TestGenUIContextFlag(t *testing.T) {
	var flagged bool
	NewGenUIHandler(func(w http.ResponseWriter, r *http.Request) { flagged = GenUIFromContext(r.Context()) })(httptest.NewRecorder(), optIn())
	if !flagged {
		t.Fatal("downstream handler should see the GenUI context flag")
	}
}

func TestInterceptNestedBracesAndSpacing(t *testing.T) {
	chunks := Intercept(`Use {curly} text first. {"type" : "aerollm_ui", "components":[{"kind":"row","children":[{"kind":"text","props":{"v":"}"}}]}]} done`)
	if len(chunks) != 3 || chunks[1].Event != EventUISchema || chunks[0].Data != "Use {curly} text first." || chunks[2].Data != "done" {
		t.Fatalf("unexpected chunks: %+v", chunks)
	}
	schema := chunks[1].Data.(UISchema)
	if len(schema.Components) != 1 || len(schema.Components[0].Children) != 1 {
		t.Fatalf("nested components lost: %+v", schema)
	}
	if got := Intercept(strings.Repeat("{", 5000) + `"type":"aerollm_ui"`); len(got) != 1 || got[0].Event != EventText {
		t.Fatal("pathological input must fall back to text")
	}
}
