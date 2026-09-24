package universal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// The API layer type-asserts adapters for exactly this method.
type streamer interface {
	StreamChatCompletions(context.Context, *models.LLMRequest) (<-chan models.StreamChunk, error)
}

var (
	_ providers.StreamingProvider = (*OpenAICompatibleAdapter)(nil)
	_ providers.StreamingProvider = (*AnthropicAdapter)(nil)
	_ providers.StreamingProvider = (*LegacyProviderAdapter)(nil)
	_ StreamProvider              = (*OpenAICompatibleAdapter)(nil)
	_ ProviderAdapter             = (*BedrockAdapter)(nil)
	_ ProviderAdapter             = (*AnthropicAdapter)(nil)
)

func collectChunks(t *testing.T, ch <-chan models.StreamChunk) []models.StreamChunk {
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

func userReq(model, text string) *models.LLMRequest {
	return &models.LLMRequest{Model: model, Messages: []models.Message{{Role: models.RoleUser, Content: strPtr(text)}}}
}

// --------------------------------------------------------------- adapters

func TestAdapterEndpointsAndAuth(t *testing.T) {
	type seen struct{ path, auth, apiKey string }
	var got seen
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = seen{r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("api-key")}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"x","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer srv.Close()
	cases := []struct {
		a        *OpenAICompatibleAdapter
		path     string
		bearer   bool
		azureKey bool
	}{
		{NewOpenAICompatibleAdapter("o", "openai", "k", srv.URL), "/v1/chat/completions", true, false},
		{NewOpenAICompatibleAdapter("o", "openai", "k", srv.URL+"/v1/"), "/v1/chat/completions", true, false},
		{NewGeminiAdapter("k", srv.URL), "/v1beta/openai/chat/completions", true, false},
		{NewCohereAdapter("k", srv.URL), "/compatibility/v1/chat/completions", true, false},
		{NewGroqAdapter("k", srv.URL+"/openai/v1"), "/openai/v1/chat/completions", true, false},
		{NewDeepSeekAdapter("k", srv.URL), "/v1/chat/completions", true, false},
		{NewAzureOpenAIAdapter("k", srv.URL, "res"), "/openai/v1/chat/completions", false, true},
	}
	for _, c := range cases {
		resp, err := c.a.ChatCompletions(context.Background(), userReq("m", "hi"))
		if err != nil {
			t.Fatalf("%s: %v", c.a.Name(), err)
		}
		if got.path != c.path || (c.bearer && got.auth != "Bearer k") || (c.azureKey && (got.apiKey != "k" || got.auth != "")) {
			t.Fatalf("%s: got %+v", c.a.Name(), got)
		}
		if *resp.Choices[0].Message.Content != "ok" {
			t.Fatalf("%s: bad response", c.a.Name())
		}
	}
	if NewGeminiAdapter("k", "").BaseURL() != "https://generativelanguage.googleapis.com/v1beta/openai" ||
		NewCohereAdapter("k", "").BaseURL() != "https://api.cohere.com/compatibility/v1" ||
		NewAzureOpenAIAdapter("k", "", "res").BaseURL() != "https://res.openai.azure.com/openai/v1" ||
		NewOpenAICompatibleAdapter("o", "openai", "k", "https://api.openai.com/v1").BaseURL() != "https://api.openai.com/v1" {
		t.Fatal("default base URLs")
	}
	bad := NewOpenAICompatibleAdapter("bad", "openai", "k", "javascript:alert(1)")
	if _, err := bad.ChatCompletions(context.Background(), userReq("m", "x")); err == nil || bad.Health()["healthy"] != false {
		t.Fatal("invalid base URL must fail explicitly")
	}
}

func TestOpenAICompatibleStreamAndLegacyStream(t *testing.T) {
	var body map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n"+
			"data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n"+
			"data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"b\"},\"finish_reason\":\"length\"}]}\n\n"+
			"data: {\"id\":\"c\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer srv.Close()
	a := NewOpenAICompatibleAdapter("o", "openai", "k", srv.URL)
	var s streamer = a
	ch, err := s.StreamChatCompletions(context.Background(), userReq("m", "hi"))
	if err != nil {
		t.Fatal(err)
	}
	chunks := collectChunks(t, ch)
	if len(chunks) != 4 || chunks[3].Usage == nil || chunks[3].Usage.TotalTokens != 3 {
		t.Fatalf("chunks: %+v", chunks)
	}
	if body["stream_options"].(map[string]interface{})["include_usage"] != true {
		t.Fatal("include_usage not requested")
	}
	// Gemini does not get stream_options by default.
	body = nil
	g := NewGeminiAdapter("k", srv.URL)
	ch, _ = g.StreamChatCompletions(context.Background(), userReq("gemini-2.0-flash", "hi"))
	collectChunks(t, ch)
	if _, ok := body["stream_options"]; ok {
		t.Fatal("stream_options sent to gemini")
	}

	legacy, err := a.Stream(context.Background(), userReq("m", "hi"))
	if err != nil {
		t.Fatal(err)
	}
	var text string
	finished := false
	for c := range legacy {
		text += c.Delta
		if c.Finish {
			finished = true
		}
	}
	if text != "ab" || !finished {
		t.Fatalf("legacy stream: %q finished=%v", text, finished)
	}
}

func TestOpenAICompatibleEmbeddingsAudioResponses(t *testing.T) {
	var embBody map[string]interface{}
	var audioModel, audioName string
	var audioBytes []byte
	var respBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/embeddings":
			_ = json.NewDecoder(r.Body).Decode(&embBody)
			io.WriteString(w, `{"object":"list","model":"e","data":[{"object":"embedding","index":0,"embedding":[0.5]},{"object":"embedding","index":1,"embedding":"AAAAPw=="}]}`)
		case "/v1/audio/transcriptions":
			_, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			mr := multipart.NewReader(r.Body, params["boundary"])
			for {
				p, err := mr.NextPart()
				if err != nil {
					break
				}
				b, _ := io.ReadAll(p)
				switch p.FormName() {
				case "model":
					audioModel = string(b)
				case "file":
					audioName, audioBytes = p.FileName(), b
				}
			}
			io.WriteString(w, `{"text":"hello world"}`)
		case "/v1/responses":
			_ = json.NewDecoder(r.Body).Decode(&respBody)
			io.WriteString(w, `{"id":"resp_1","object":"response","model":"m","output":[{"type":"function_call","name":"f","arguments":"{}","call_id":"c"}]}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	a := NewOpenAICompatibleAdapter("o", "openai", "k", srv.URL)

	var er models.EmbeddingRequest
	_ = json.Unmarshal([]byte(`{"model":"e","input":["x","y"]}`), &er)
	emb, err := a.Embeddings(context.Background(), &er)
	if err != nil {
		t.Fatal(err)
	}
	if in, ok := embBody["input"].([]interface{}); !ok || len(in) != 2 {
		t.Fatalf("array input not forwarded: %v", embBody)
	}
	if len(emb.Data) != 2 || emb.Data[1].Embedding[0] != 0.5 {
		t.Fatalf("embeddings: %+v", emb)
	}

	wav := append([]byte("RIFF\x00\x00\x00\x00WAVE"), make([]byte, 16)...)
	audio, err := a.AudioTranscriptions(context.Background(), &models.AudioRequest{Model: "whisper-1", File: "data:audio/wav;base64," + b64(wav)})
	if err != nil {
		t.Fatal(err)
	}
	if audio.Text != "hello world" || audioModel != "whisper-1" || audioName != "audio.wav" || string(audioBytes) != string(wav) {
		t.Fatalf("audio: %q %q %q %d", audio.Text, audioModel, audioName, len(audioBytes))
	}
	if _, err := a.AudioTranscriptions(context.Background(), &models.AudioRequest{Model: "w", File: "not base64!"}); providers.StatusCode(err) != 400 {
		t.Fatalf("expected 400 for bad audio, got %v", err)
	}

	resp, err := a.Responses(context.Background(), &models.ResponsesRequest{Model: "m", Input: "hi", Tools: []models.ToolDefinition{{Name: "f", Parameters: map[string]interface{}{"type": "object"}}}})
	if err != nil {
		t.Fatal(err)
	}
	tool := respBody["tools"].([]interface{})[0].(map[string]interface{})
	if tool["name"] != "f" || tool["type"] != "function" || tool["function"] != nil {
		t.Fatalf("responses tools must be flat: %v", tool)
	}
	out, _ := json.Marshal(resp)
	if !strings.Contains(string(out), `"function_call"`) {
		t.Fatalf("responses output not passed through: %s", out)
	}
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func TestAnthropicAdapterNative(t *testing.T) {
	var path, key, version string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, key, version = r.URL.Path, r.Header.Get("x-api-key"), r.Header.Get("anthropic-version")
		var body providers.AnthropicRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\",\"usage\":{\"input_tokens\":3}}}\n\n"+
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"yo\"}}\n\n"+
				"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n"+
				"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"m1","type":"message","role":"assistant","model":"claude","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":1}}`)
	}))
	defer srv.Close()
	a := NewAnthropicAdapter("sk-ant", srv.URL+"/v1")
	resp, err := a.ChatCompletions(context.Background(), userReq("claude", "hi"))
	if err != nil {
		t.Fatal(err)
	}
	if path != "/v1/messages" || key != "sk-ant" || version != providers.AnthropicVersion {
		t.Fatalf("path=%q key=%q version=%q", path, key, version)
	}
	if len(resp.Choices) != 1 || *resp.Choices[0].Message.Content != "hello" || resp.Choices[0].FinishReason != "stop" {
		t.Fatalf("resp: %+v", resp)
	}
	ch, err := a.StreamChatCompletions(context.Background(), userReq("claude", "hi"))
	if err != nil {
		t.Fatal(err)
	}
	var text string
	var usage *models.Usage
	for _, c := range collectChunks(t, ch) {
		for _, ch := range c.Choices {
			text += ch.Delta.Content
		}
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if text != "yo" || usage == nil || usage.TotalTokens != 4 {
		t.Fatalf("stream text=%q usage=%+v", text, usage)
	}
	if h := a.Health(); h["healthy"] != true {
		t.Fatalf("health: %v", h)
	}
}

// ---------------------------------------------------------------- bedrock

func TestSigV4TestVectors(t *testing.T) {
	creds := awsCredentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)

	req, _ := http.NewRequest(http.MethodGet, "https://example.amazonaws.com/", nil)
	signSigV4(req, nil, creds, "us-east-1", "service", now)
	want := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("get-vanilla:\n got %s\nwant %s", got, want)
	}
	if req.Header.Get("X-Amz-Date") != "20150830T123600Z" {
		t.Fatal("x-amz-date")
	}

	req, _ = http.NewRequest(http.MethodPost, "https://example.amazonaws.com/", nil)
	signSigV4(req, nil, creds, "us-east-1", "service", now)
	want = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("post-vanilla:\n got %s\nwant %s", got, want)
	}

	if canonicalURI("/model/anthropic.claude-v2%3A1/converse") != "/model/anthropic.claude-v2%253A1/converse" {
		t.Fatalf("double encoding: %s", canonicalURI("/model/anthropic.claude-v2%3A1/converse"))
	}
}

func TestBedrockConverseSigned(t *testing.T) {
	var got struct {
		rawURI, auth, date, token, ctype string
		body                             converseRequest
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.rawURI = r.RequestURI
		got.auth = r.Header.Get("Authorization")
		got.date = r.Header.Get("X-Amz-Date")
		got.token = r.Header.Get("X-Amz-Security-Token")
		got.ctype = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&got.body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"output":{"message":{"role":"assistant","content":[{"text":"Checking."},{"toolUse":{"toolUseId":"tu1","name":"get_weather","input":{"city":"Oslo"}}}]}},"stopReason":"tool_use","usage":{"inputTokens":11,"outputTokens":7,"totalTokens":18}}`)
	}))
	defer srv.Close()
	a := NewBedrockAdapter("AKIDEXAMPLE:secret:sessiontok", srv.URL)
	a.SetRegion("eu-west-1")
	a.now = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

	var req models.LLMRequest
	_ = json.Unmarshal([]byte(`{"model":"anthropic.claude-3-5-sonnet-20240620-v1:0","max_tokens":50,"temperature":0.2,"stop":["X"],
	"tool_choice":"required","tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}],
	"messages":[{"role":"system","content":"sys"},{"role":"user","content":"weather?"},
	{"role":"assistant","tool_calls":[{"id":"tu0","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Rome\"}"}}]},
	{"role":"tool","tool_call_id":"tu0","content":"sunny"},{"role":"user","content":"and Oslo?"}]}`), &req)
	resp, err := a.ChatCompletions(context.Background(), &req)
	if err != nil {
		t.Fatal(err)
	}
	if got.rawURI != "/model/anthropic.claude-3-5-sonnet-20240620-v1%3A0/converse" {
		t.Fatalf("path: %s", got.rawURI)
	}
	if !strings.HasPrefix(got.auth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/eu-west-1/bedrock/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-security-token, Signature=") {
		t.Fatalf("auth: %s", got.auth)
	}
	if got.date != "20260102T030405Z" || got.token != "sessiontok" || got.ctype != "application/json" {
		t.Fatalf("headers: %+v", got)
	}
	b := got.body
	if len(b.System) != 1 || b.System[0].Text != "sys" || *b.InferenceConfig.MaxTokens != 50 || b.InferenceConfig.StopSequences[0] != "X" {
		t.Fatalf("system/inference: %+v", b)
	}
	if len(b.Messages) != 3 || b.Messages[1].Content[0].ToolUse == nil || string(b.Messages[1].Content[0].ToolUse.Input) != `{"city":"Rome"}` {
		t.Fatalf("messages: %+v", b.Messages)
	}
	last := b.Messages[2]
	if last.Role != "user" || last.Content[0].ToolResult == nil || last.Content[0].ToolResult.ToolUseID != "tu0" || last.Content[1].Text != "and Oslo?" {
		t.Fatalf("tool result merge: %+v", last)
	}
	if b.ToolConfig == nil || b.ToolConfig.Tools[0].ToolSpec.Name != "get_weather" || b.ToolConfig.ToolChoice["any"] == nil {
		t.Fatalf("tool config: %+v", b.ToolConfig)
	}
	c := resp.Choices[0]
	if *c.Message.Content != "Checking." || c.FinishReason != "tool_calls" || c.Message.ToolCalls[0].Function.Arguments != `{"city":"Oslo"}` || resp.Usage.TotalTokens != 18 {
		t.Fatalf("response: %+v", resp)
	}
}

func TestBedrockAuthModes(t *testing.T) {
	var auth atomic.Value
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		auth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"output":{"message":{"role":"assistant","content":[{"text":"hi"}]}},"stopReason":"end_turn","usage":{"inputTokens":1,"outputTokens":1}}`)
	}))
	defer srv.Close()

	a := NewBedrockAdapter("ABSKbedrockapikey", srv.URL)
	a.SetRegion("us-east-1")
	if _, err := a.ChatCompletions(context.Background(), userReq("amazon.nova-lite-v1:0", "x")); err != nil {
		t.Fatal(err)
	}
	if auth.Load() != "Bearer ABSKbedrockapikey" {
		t.Fatalf("bearer auth: %v", auth.Load())
	}

	t.Setenv("AWS_BEARER_TOKEN_BEDROCK", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	none := NewBedrockAdapter("", srv.URL)
	none.SetRegion("us-east-1")
	before := calls.Load()
	_, err := none.ChatCompletions(context.Background(), userReq("m", "x"))
	if err == nil || !strings.Contains(err.Error(), "no AWS credentials") || calls.Load() != before {
		t.Fatalf("must refuse to send unsigned requests: %v (calls %d->%d)", err, before, calls.Load())
	}
	if none.Health()["healthy"] != false {
		t.Fatal("unconfigured bedrock must report unhealthy")
	}

	t.Setenv("AWS_ACCESS_KEY_ID", "AKENV")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "SKENV")
	if _, err := none.ChatCompletions(context.Background(), userReq("m", "x")); err != nil {
		t.Fatal(err)
	}
	if s, _ := auth.Load().(string); !strings.Contains(s, "Credential=AKENV/") {
		t.Fatalf("env sigv4: %v", s)
	}
	if _, err := NewBedrockAdapter(":broken", srv.URL).ChatCompletions(context.Background(), userReq("m", "x")); err == nil {
		t.Fatal("malformed credentials must fail")
	}
}

func TestBedrockEndpointAndRegion(t *testing.T) {
	a := NewBedrockAdapter("k", "https://bedrock.eu-central-1.amazonaws.com")
	if a.base.Host != "bedrock-runtime.eu-central-1.amazonaws.com" || a.Region() != "eu-central-1" {
		t.Fatalf("host=%s region=%s", a.base.Host, a.Region())
	}
	a = NewBedrockAdapter("k", "https://vpce-123.bedrock-runtime.us-west-2.vpce.amazonaws.com")
	if a.Region() != "us-west-2" {
		t.Fatalf("vpce region: %s", a.Region())
	}
	if _, err := NewBedrockAdapter("k", "https://bedrock-runtime.us-east-1.amazonaws.com").buildConverse(&models.LLMRequest{
		Messages: []models.Message{{Role: "user", ContentParts: []models.ContentPart{{Type: "image_url", ImageURL: &models.ImageURL{URL: "https://remote/img.png"}}}}},
	}); providers.StatusCode(err) != 400 {
		t.Fatalf("remote images must be rejected with 400: %v", err)
	}
	ch, err := NewBedrockAdapter("k", "https://bedrock-runtime.us-east-1.amazonaws.com").Stream(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	c := <-ch
	if c.Err == nil || !c.Finish {
		t.Fatalf("expected error chunk for invalid request: %+v", c)
	}
	var s interface{} = NewBedrockAdapter("k", "")
	if _, ok := s.(streamer); ok {
		t.Fatal("bedrock must not claim streaming support (gateway falls back)")
	}
}

// --------------------------------------------------------------- registry

type fakeAdapter struct {
	name     string
	lastReq  atomic.Pointer[models.LLMRequest]
	closed   atomic.Bool
	streamOK bool
}

func (f *fakeAdapter) Name() string { return f.name }
func (f *fakeAdapter) Type() string { return "fake" }
func (f *fakeAdapter) ChatCompletions(_ context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	f.lastReq.Store(req)
	return &models.LLMResponse{Model: req.Model}, nil
}
func (f *fakeAdapter) Stream(context.Context, *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	return nil, errors.New("no")
}
func (f *fakeAdapter) Health() map[string]interface{} { return map[string]interface{}{"healthy": true} }
func (f *fakeAdapter) Close() error                   { f.closed.Store(true); return nil }

func TestRegistryResolution(t *testing.T) {
	reg := NewProviderRegistry()
	openai := &fakeAdapter{name: "openai"}
	azure := &fakeAdapter{name: "azure"}
	groq := &fakeAdapter{name: "groq"}
	if err := reg.Register(openai, "gpt-4o", "gpt-4*", "fast=gpt-4o-mini"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(azure, "GPT-4O", "gpt-4o-mini*"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(groq, "llama*"); err != nil {
		t.Fatal(err)
	}
	check := func(model, provider, upstream string) {
		t.Helper()
		res, err := reg.Resolve(model)
		if err != nil || res.ProviderName != provider || res.Model != upstream {
			t.Fatalf("%s: got %+v %v, want %s/%s", model, res, err, provider, upstream)
		}
	}
	check("gpt-4o", "openai", "gpt-4o")                                // exact, first registered wins
	check("gpt-4o-mini-2024-07-18", "azure", "gpt-4o-mini-2024-07-18") // longest prefix wins
	check("gpt-4-turbo", "openai", "gpt-4-turbo")                      // wildcard
	check("fast", "openai", "gpt-4o-mini")                             // provider alias with target
	check("llama-3.1-70b", "groq", "llama-3.1-70b")

	all, _ := reg.ResolveAll("gpt-4o")
	if len(all) != 2 || all[1].ProviderName != "azure" {
		t.Fatalf("ResolveAll: %+v", all)
	}

	if _, err := reg.Resolve("claude-3"); !errors.Is(err, ErrModelNotFound) {
		t.Fatalf("unknown model: %v", err)
	}
	if _, err := reg.Resolve("  "); !errors.Is(err, ErrModelNotFound) {
		t.Fatalf("empty model: %v", err)
	}

	// Global aliases with cycle detection.
	if err := reg.SetAlias("smart", "gpt-4o"); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetAlias("default", "smart"); err != nil {
		t.Fatal(err)
	}
	check("default", "openai", "gpt-4o")
	if err := reg.SetAlias("gpt-4o", "default"); !errors.Is(err, ErrAliasCycle) {
		t.Fatalf("expected cycle error, got %v", err)
	}
	if err := reg.SetAlias("x", "x"); !errors.Is(err, ErrAliasCycle) {
		t.Fatalf("expected self-cycle error, got %v", err)
	}

	// Aliased requests are rewritten transparently.
	a, err := reg.ResolveProviderByModel("fast")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.ChatCompletions(context.Background(), userReq("fast", "hi")); err != nil {
		t.Fatal(err)
	}
	if openai.lastReq.Load().Model != "gpt-4o-mini" {
		t.Fatalf("alias not rewritten: %s", openai.lastReq.Load().Model)
	}
	if _, ok := a.(streamer); !ok {
		t.Fatal("rewrite adapter must expose StreamChatCompletions")
	}
	if _, err := a.(streamer).StreamChatCompletions(context.Background(), userReq("fast", "x")); !errors.Is(err, providers.ErrStreamingNotSupported) {
		t.Fatalf("expected ErrStreamingNotSupported, got %v", err)
	}
	direct, _ := reg.ResolveProviderByModel("gpt-4o")
	if direct != ProviderAdapter(openai) {
		t.Fatal("non-alias resolution must return the adapter itself")
	}

	// Invalid specs.
	for _, bad := range []string{"a*b", "=x", "a=", "p*=x", "a=b*"} {
		if err := reg.Register(&fakeAdapter{name: "bad"}, bad); err == nil {
			t.Fatalf("spec %q should be rejected", bad)
		}
	}
	// Re-registering replaces entries.
	if err := reg.Register(&fakeAdapter{name: "groq"}, "mixtral*"); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Resolve("llama-3"); err == nil {
		t.Fatal("stale entries after re-register")
	}
	if !reg.Unregister("groq") || reg.Unregister("groq") {
		t.Fatal("unregister")
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	reg := NewProviderRegistry()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = reg.Register(&fakeAdapter{name: fmt.Sprintf("p%d", i)}, "m*", fmt.Sprintf("x%d", j%3))
				_, _ = reg.Resolve("model")
				_ = reg.All()
				_ = reg.SetAlias(fmt.Sprintf("a%d", j%5), "m1")
			}
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestRegistryReloadFromConfig(t *testing.T) {
	reg := NewProviderRegistry()
	good := []config.ProviderConfig{
		{Name: "oa", Type: "openai", APIKey: "k", Models: []string{"gpt-4o*"}},
		{Name: "an", Type: "anthropic", APIKey: "k", Models: []string{"claude*"}},
		{Name: "br", Type: "bedrock", BaseURL: "https://bedrock-runtime.us-east-1.amazonaws.com", Models: []string{"amazon.*"}},
		{Name: "local", Type: "vllm", BaseURL: "http://127.0.0.1:8000", Models: []string{"llama"}},
	}
	if err := reg.ReloadFromConfig(good); err != nil {
		t.Fatal(err)
	}
	if res, err := reg.Resolve("claude-3-5-sonnet"); err != nil || res.ProviderName != "an" {
		t.Fatalf("resolve after reload: %+v %v", res, err)
	}
	if _, ok := mustGet(t, reg, "an").(*AnthropicAdapter); !ok {
		t.Fatal("anthropic type must use the native adapter")
	}
	if _, ok := mustGet(t, reg, "br").(*BedrockAdapter); !ok {
		t.Fatal("bedrock type must use the native adapter")
	}
	bad := []config.ProviderConfig{{Name: "x", Type: "openai", Models: []string{"a*b"}}}
	if err := reg.ReloadFromConfig(bad); err == nil {
		t.Fatal("expected error")
	}
	if _, err := reg.Resolve("claude-3"); err != nil {
		t.Fatal("failed reload must leave registry unchanged")
	}
	dup := []config.ProviderConfig{{Name: "x", Type: "openai"}, {Name: "x", Type: "groq"}}
	if err := reg.RegisterFromConfig(dup); err == nil {
		t.Fatal("duplicate names must be rejected")
	}
	if err := reg.ReloadFromConfig([]config.ProviderConfig{{Name: "az", Type: "azure", APIKey: "k"}}); err == nil {
		t.Fatal("azure without base_url must be rejected")
	}
	if err := reg.ReloadFromConfig(good[:1]); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Resolve("claude-3"); err == nil {
		t.Fatal("removed provider still resolvable")
	}
}

func mustGet(t *testing.T, reg *ProviderRegistry, name string) ProviderAdapter {
	t.Helper()
	a, ok := reg.Get(name)
	if !ok {
		t.Fatalf("adapter %q missing", name)
	}
	return a
}

// ------------------------------------------------------------ misc pieces

func TestModelRegistryReRegister(t *testing.T) {
	reg := NewModelRegistry()
	ctx := context.Background()
	_ = reg.Register(ctx, ModelCard{ID: "m", Provider: "a", Capabilities: []string{"chat"}})
	_ = reg.Register(ctx, ModelCard{ID: "m", Provider: "a"})
	_ = reg.Register(ctx, ModelCard{ID: "m", Provider: "b"})
	if len(reg.ByProvider("a")) != 0 || len(reg.ByProvider("b")) != 1 {
		t.Fatalf("provider index: a=%v b=%v", reg.ByProvider("a"), reg.ByProvider("b"))
	}
	card, _ := reg.Get("m")
	card.Capabilities = append(card.Capabilities, "mutated")
	if c2, _ := reg.Get("m"); len(c2.Capabilities) != 0 {
		t.Fatal("Get must return a copy")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := reg.Register(cctx, ModelCard{ID: "z"}); err == nil {
		t.Fatal("cancelled ctx")
	}
	if !reg.Unregister("m") || len(reg.List()) != 0 {
		t.Fatal("unregister")
	}
}

func TestNormalizerFinishReasons(t *testing.T) {
	n := NewStreamNormalizer()
	for _, fr := range []string{"stop", "length", "tool_calls", "content_filter"} {
		c, err := n.Normalize("p", []byte(`{"choices":[{"delta":{},"finish_reason":"`+fr+`"}]}`))
		if err != nil || !c.Finish {
			t.Fatalf("%s: finish=%v err=%v", fr, c.Finish, err)
		}
	}
	if c, _ := n.Normalize("p", []byte("[DONE]")); !c.Finish {
		t.Fatal("[DONE]")
	}
}

type streamingLegacy struct{ fakeLegacy }

func (s *streamingLegacy) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	ch := make(chan models.StreamChunk, 1)
	ch <- models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: "z"}}}}
	close(ch)
	return ch, nil
}

type fakeLegacy struct{}

func (fakeLegacy) Name() string                     { return "legacy" }
func (fakeLegacy) Type() providers.ProviderType     { return "local" }
func (fakeLegacy) Health() providers.ProviderHealth { return providers.ProviderHealth{Healthy: true} }
func (fakeLegacy) Close() error                     { return nil }
func (fakeLegacy) ChatCompletions(context.Context, *models.LLMRequest) (*models.LLMResponse, error) {
	return &models.LLMResponse{}, nil
}

func TestLegacyProviderAdapterStreaming(t *testing.T) {
	a := NewLegacyProviderAdapter(fakeLegacy{})
	if _, err := a.StreamChatCompletions(context.Background(), &models.LLMRequest{}); !errors.Is(err, providers.ErrStreamingNotSupported) {
		t.Fatalf("got %v", err)
	}
	if _, err := a.Stream(context.Background(), &models.LLMRequest{}); !errors.Is(err, providers.ErrStreamingNotSupported) {
		t.Fatalf("got %v", err)
	}
	s := NewLegacyProviderAdapter(&streamingLegacy{})
	ch, err := s.Stream(context.Background(), &models.LLMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if c := <-ch; c.Delta != "z" {
		t.Fatalf("delta %q", c.Delta)
	}
	if a.Health()["healthy"] != true {
		t.Fatal("health")
	}
}

func TestEventStreamReader(t *testing.T) {
	r := NewEventStreamReader(strings.NewReader("event: x\ndata: a\n\n: c\n\ndata: b\ndata: c\n\n"))
	if ev, _ := r.ReadEvent(); ev != "a" {
		t.Fatalf("got %q", ev)
	}
	if ev, _ := r.ReadEvent(); ev != "b\nc" {
		t.Fatalf("got %q", ev)
	}
	if _, err := r.ReadEvent(); err != io.EOF {
		t.Fatalf("got %v", err)
	}
}

func TestBedrockRejectsBadModelIDs(t *testing.T) {
	a := NewBedrockAdapter("AK:SK", "https://bedrock-runtime.us-east-1.amazonaws.com")
	for _, m := range []string{"", "..", ".", "a b", "x\ny", strings.Repeat("a", 3000)} {
		if _, err := a.ChatCompletions(context.Background(), userReq(m, "x")); providers.StatusCode(err) != 400 {
			t.Fatalf("%q: expected 400, got %v", m, err)
		}
	}
	u := a.converseURL("arn:aws:bedrock:us-east-1:123:inference-profile/us.anthropic.claude-3-5-sonnet-20241022-v2:0")
	if u.EscapedPath() != "/model/arn%3Aaws%3Abedrock%3Aus-east-1%3A123%3Ainference-profile%2Fus.anthropic.claude-3-5-sonnet-20241022-v2%3A0/converse" {
		t.Fatalf("arn path: %s", u.EscapedPath())
	}
}
