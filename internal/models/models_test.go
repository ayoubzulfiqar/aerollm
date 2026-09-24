package models

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"regexp"
	"strings"
	"testing"
)

func TestMessageUnmarshalStringContent(t *testing.T) {
	var m Message
	if err := json.Unmarshal([]byte(`{"role":"user","content":"hello"}`), &m); err != nil {
		t.Fatal(err)
	}
	if m.Content == nil || *m.Content != "hello" || m.ContentParts != nil {
		t.Fatalf("unexpected message: %+v", m)
	}
	out, _ := json.Marshal(m)
	if string(out) != `{"role":"user","content":"hello"}` {
		t.Fatalf("round trip mismatch: %s", out)
	}
}

func TestMessageUnmarshalNullAndMissingContent(t *testing.T) {
	for _, in := range []string{`{"role":"assistant","content":null}`, `{"role":"assistant"}`} {
		var m Message
		if err := json.Unmarshal([]byte(in), &m); err != nil {
			t.Fatal(err)
		}
		if m.Content != nil || m.ContentParts != nil {
			t.Fatalf("%s: expected nil content, got %+v", in, m)
		}
	}
}

func TestMessageUnmarshalContentParts(t *testing.T) {
	in := `{"role":"user","content":[{"type":"text","text":"what is"},{"type":"image_url","image_url":{"url":"https://x/y.png","detail":"high"}},{"type":"text","text":"this?"},{"type":"future_part","foo":{"bar":1}}]}`
	var m Message
	if err := json.Unmarshal([]byte(in), &m); err != nil {
		t.Fatal(err)
	}
	if len(m.ContentParts) != 4 {
		t.Fatalf("expected 4 parts, got %d", len(m.ContentParts))
	}
	if m.Content == nil || *m.Content != "what is\nthis?" {
		t.Fatalf("expected concatenated text, got %v", m.Content)
	}
	if m.ContentParts[1].ImageURL == nil || m.ContentParts[1].ImageURL.URL != "https://x/y.png" {
		t.Fatalf("image part lost: %+v", m.ContentParts[1])
	}
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]interface{}
	_ = json.Unmarshal(out, &back)
	parts, ok := back["content"].([]interface{})
	if !ok || len(parts) != 4 {
		t.Fatalf("parts not preserved: %s", out)
	}
	if !strings.Contains(string(out), `"future_part","foo":{"bar":1}`) {
		t.Fatalf("unknown part not passed through verbatim: %s", out)
	}
}

func TestMessageImageOnlyParts(t *testing.T) {
	var m Message
	if err := json.Unmarshal([]byte(`{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}`), &m); err != nil {
		t.Fatal(err)
	}
	if m.Content != nil {
		t.Fatalf("expected nil text content, got %q", *m.Content)
	}
	out, _ := json.Marshal(m)
	if !strings.Contains(string(out), `"image_url"`) {
		t.Fatalf("image part missing: %s", out)
	}
}

func TestMessageModifiedContentRebuildsTextParts(t *testing.T) {
	var m Message
	if err := json.Unmarshal([]byte(`{"role":"user","content":[{"type":"text","text":"my ssn is 123"},{"type":"image_url","image_url":{"url":"https://x"}}]}`), &m); err != nil {
		t.Fatal(err)
	}
	redacted := "my ssn is [REDACTED]"
	m.Content = &redacted
	out, _ := json.Marshal(m)
	if strings.Contains(string(out), "123") {
		t.Fatalf("redaction lost on marshal: %s", out)
	}
	if !strings.Contains(string(out), "[REDACTED]") || !strings.Contains(string(out), `"image_url"`) {
		t.Fatalf("unexpected marshal: %s", out)
	}
}

func TestMessageInvalidContent(t *testing.T) {
	for _, in := range []string{`{"role":"user","content":42}`, `{"role":"user","content":{"a":1}}`, `{"role":"user","content":[{"text":"no type"}]}`} {
		var m Message
		if err := json.Unmarshal([]byte(in), &m); err == nil {
			t.Fatalf("%s: expected error", in)
		}
	}
}

func TestMessageEmptyStringContentPreserved(t *testing.T) {
	empty := ""
	out, _ := json.Marshal(Message{Role: RoleAssistant, Content: &empty})
	if string(out) != `{"role":"assistant","content":""}` {
		t.Fatalf("got %s", out)
	}
}

func TestToolDefinitionNestedAndFlat(t *testing.T) {
	nested := `{"type":"function","function":{"name":"get_weather","description":"d","parameters":{"type":"object"},"strict":true}}`
	flat := `{"name":"get_weather","description":"d","parameters":{"type":"object"}}`
	anth := `{"name":"get_weather","description":"d","input_schema":{"type":"object"}}`
	for _, in := range []string{nested, flat, anth} {
		var td ToolDefinition
		if err := json.Unmarshal([]byte(in), &td); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if td.Name != "get_weather" || td.Description != "d" || td.Parameters["type"] != "object" {
			t.Fatalf("%s: bad decode %+v", in, td)
		}
		out, _ := json.Marshal(td)
		if !strings.HasPrefix(string(out), `{"type":"function","function":{"name":"get_weather"`) {
			t.Fatalf("expected nested encoding, got %s", out)
		}
	}
	var td ToolDefinition
	_ = json.Unmarshal([]byte(nested), &td)
	if td.Strict == nil || !*td.Strict {
		t.Fatal("strict lost")
	}
}

func TestToolDefinitionNonFunctionPassthrough(t *testing.T) {
	in := `{"type":"web_search_preview","search_context_size":"low"}`
	var td ToolDefinition
	if err := json.Unmarshal([]byte(in), &td); err != nil {
		t.Fatal(err)
	}
	if td.IsFunction() {
		t.Fatal("expected non-function tool")
	}
	out, _ := json.Marshal(td)
	if string(out) != in {
		t.Fatalf("expected verbatim passthrough, got %s", out)
	}
}

func TestToolChoice(t *testing.T) {
	cases := map[string]ToolChoice{
		`"auto"`:     {Mode: "auto"},
		`"none"`:     {Mode: "none"},
		`"required"`: {Mode: "required"},
		`{"type":"function","function":{"name":"f"}}`: {Function: "f"},
		`{"type":"tool","name":"g"}`:                  {Function: "g"},
		`{"type":"any"}`:                              {Mode: "required"},
	}
	for in, want := range cases {
		var c ToolChoice
		if err := json.Unmarshal([]byte(in), &c); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if c != want {
			t.Fatalf("%s: got %+v want %+v", in, c, want)
		}
	}
	for _, bad := range []string{`"sometimes"`, `{"type":"function"}`, `{"type":"weird"}`, `5`} {
		var c ToolChoice
		if err := json.Unmarshal([]byte(bad), &c); err == nil {
			t.Fatalf("%s: expected error", bad)
		}
	}
	out, _ := json.Marshal(ToolChoice{Function: "f"})
	if string(out) != `{"type":"function","function":{"name":"f"}}` {
		t.Fatalf("got %s", out)
	}
	out, _ = json.Marshal(ToolChoice{Mode: "none"})
	if string(out) != `"none"` {
		t.Fatalf("got %s", out)
	}
}

func TestLLMRequestNewFieldsRoundTrip(t *testing.T) {
	in := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true},"n":2,"seed":42,"user":"u1","tool_choice":"required","parallel_tool_calls":false,"logprobs":true,"top_logprobs":3,"max_completion_tokens":100,"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}]}`
	var req LLMRequest
	if err := json.Unmarshal([]byte(in), &req); err != nil {
		t.Fatal(err)
	}
	if req.StreamOptions == nil || !req.StreamOptions.IncludeUsage || *req.N != 2 || *req.Seed != 42 || req.User != "u1" ||
		req.ToolChoice == nil || req.ToolChoice.Mode != "required" || req.ParallelToolCalls == nil || *req.ParallelToolCalls ||
		!*req.Logprobs || *req.TopLogprobs != 3 || *req.EffectiveMaxTokens() != 100 || len(req.Tools) != 1 || req.Tools[0].Name != "f" {
		t.Fatalf("bad decode: %+v", req)
	}
	out, _ := json.Marshal(req)
	var again LLMRequest
	if err := json.Unmarshal(out, &again); err != nil {
		t.Fatalf("re-decode: %v (%s)", err, out)
	}
	if again.ToolChoice.Mode != "required" || again.Tools[0].Name != "f" || *again.Messages[0].Content != "hi" {
		t.Fatalf("round trip lost data: %s", out)
	}
}

func TestEmbeddingRequestInputForms(t *testing.T) {
	var r EmbeddingRequest
	if err := json.Unmarshal([]byte(`{"model":"m","input":"hello"}`), &r); err != nil {
		t.Fatal(err)
	}
	if r.Input != "hello" || r.Inputs != nil {
		t.Fatalf("bad: %+v", r)
	}
	if err := json.Unmarshal([]byte(`{"model":"m","input":["a","b"],"dimensions":256}`), &r); err != nil {
		t.Fatal(err)
	}
	if r.Input != "a\nb" || len(r.Inputs) != 2 || *r.Dimensions != 256 {
		t.Fatalf("bad: %+v", r)
	}
	out, _ := json.Marshal(r)
	if !strings.Contains(string(out), `"input":["a","b"]`) {
		t.Fatalf("array not preserved: %s", out)
	}
	if err := json.Unmarshal([]byte(`{"model":"m","input":[[1,2],[3]]}`), &r); err != nil {
		t.Fatal(err)
	}
	out, _ = json.Marshal(r)
	if !strings.Contains(string(out), `"input":[[1,2],[3]]`) {
		t.Fatalf("token input not preserved: %s", out)
	}
	if err := json.Unmarshal([]byte(`{"model":"m","input":{"x":1}}`), &r); err == nil {
		t.Fatal("expected error for object input")
	}
	out, _ = json.Marshal(EmbeddingRequest{Model: "m", Input: "q"})
	if string(out) != `{"model":"m","input":"q"}` {
		t.Fatalf("got %s", out)
	}
}

func TestEmbeddingBase64Decode(t *testing.T) {
	vals := []float32{1.5, -2.25, 0}
	b := make([]byte, 12)
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	in := `{"object":"embedding","index":0,"embedding":"` + base64.StdEncoding.EncodeToString(b) + `"}`
	var e Embedding
	if err := json.Unmarshal([]byte(in), &e); err != nil {
		t.Fatal(err)
	}
	if len(e.Embedding) != 3 || e.Embedding[0] != 1.5 || e.Embedding[1] != -2.25 {
		t.Fatalf("bad decode: %+v", e.Embedding)
	}
	if err := json.Unmarshal([]byte(`{"embedding":[0.1,0.2]}`), &e); err != nil || len(e.Embedding) != 2 {
		t.Fatalf("float form failed: %v %+v", err, e)
	}
	if err := json.Unmarshal([]byte(`{"embedding":"AAA"}`), &e); err == nil {
		t.Fatal("expected error for bad base64 length")
	}
}

func TestResponsesRequestInputForms(t *testing.T) {
	var r ResponsesRequest
	if err := json.Unmarshal([]byte(`{"model":"m","input":"hi"}`), &r); err != nil || r.Input != "hi" {
		t.Fatalf("%v %+v", err, r)
	}
	in := `{"model":"m","input":[{"role":"user","content":"a"},{"role":"user","content":[{"type":"input_text","text":"b"}]}]}`
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatal(err)
	}
	if r.Input != "a\nb" || len(r.InputItems) == 0 {
		t.Fatalf("bad: %+v", r)
	}
	out, _ := json.Marshal(r)
	if !strings.Contains(string(out), `"input":[{"role":"user","content":"a"}`) {
		t.Fatalf("items not passed through: %s", out)
	}
}

func TestGenerateTraceID(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{32}$`)
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := GenerateTraceID()
		if !re.MatchString(id) || id == strings.Repeat("0", 32) {
			t.Fatalf("bad trace id %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate trace id %q", id)
		}
		seen[id] = true
	}
}

func TestToolResultContentToString(t *testing.T) {
	cases := []struct {
		in   interface{}
		want string
	}{
		{nil, ""},
		{"s", "s"},
		{[]byte("b"), "b"},
		{map[string]int{"a": 1}, `{"a":1}`},
		{42, "42"},
	}
	for _, c := range cases {
		tr := &ToolResult{Content: c.in}
		if got := tr.ContentToString(); got != c.want {
			t.Fatalf("%v: got %q want %q", c.in, got, c.want)
		}
	}
	var nilTR *ToolResult
	if nilTR.ContentToString() != "" {
		t.Fatal("nil receiver")
	}
}

func TestStreamChunksFromResponse(t *testing.T) {
	text := "hello"
	resp := &LLMResponse{ID: "x", Model: "m", Choices: []Choice{{Message: Message{Role: RoleAssistant, Content: &text,
		ToolCalls: []ToolCall{{ID: "c1", Function: ToolFunction{Name: "f", Arguments: "{}"}}}}, FinishReason: "tool_calls"}},
		Usage: &Usage{TotalTokens: 3}}
	chunks := StreamChunksFromResponse(resp)
	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(chunks))
	}
	if chunks[3].Usage == nil || *chunks[3].Choices[0].FinishReason != "tool_calls" {
		t.Fatalf("bad final chunk: %+v", chunks[3])
	}
	if StreamChunksFromResponse(nil) != nil {
		t.Fatal("nil response")
	}
}

func TestResponsesResponsePassthrough(t *testing.T) {
	in := `{"id":"resp_1","object":"response","created_at":5,"model":"gpt-4.1","output":[{"type":"reasoning","id":"rs_1","summary":[]},{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":"Hello","annotations":[]}]},{"type":"function_call","call_id":"c1","name":"f","arguments":"{}"}],"usage":{"input_tokens":10,"output_tokens":4,"total_tokens":14,"input_tokens_details":{"cached_tokens":2}}}`
	var r ResponsesResponse
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatal(err)
	}
	if len(r.Output) != 1 || *r.Output[0].Content != "Hello" || r.Usage.PromptTokens != 10 || r.Usage.CompletionTokens != 4 || r.Usage.PromptTokensDetails.CachedTokens != 2 {
		t.Fatalf("bad decode: %+v %+v", r, r.Usage)
	}
	out, _ := json.Marshal(r)
	if string(out) != in {
		t.Fatalf("not passed through verbatim:\n%s", out)
	}
	built, _ := json.Marshal(ResponsesResponse{ID: "x"})
	if !strings.Contains(string(built), `"id":"x"`) {
		t.Fatalf("typed marshal: %s", built)
	}
}
