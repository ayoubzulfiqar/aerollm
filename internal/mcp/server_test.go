package mcp

import (
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

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
)

// testSessions caches one session per server for post.
var testSessions sync.Map // *Server -> session ID

// sessionFor returns a live session of s, creating it directly.
func sessionFor(t *testing.T, s *Server) string {
	t.Helper()
	if id, ok := testSessions.Load(s); ok {
		return id.(string)
	}
	info, err := s.sessions.create("", LatestProtocolVersion, "test", "1")
	if err != nil {
		t.Fatal(err)
	}
	testSessions.Store(s, info.ID)
	return info.ID
}

// post sends a JSON-RPC body. Unless the body is an initialize request or
// hdr sets Mcp-Session-Id itself (an empty value sends no header), a session
// of s is attached automatically when sessions are enabled.
func post(t *testing.T, s *Server, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	_, explicit := hdr[HeaderSessionID]
	for k, v := range hdr {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	if s.sessions != nil && !explicit && peekMethod([]byte(body)) != "initialize" {
		req.Header.Set(HeaderSessionID, sessionFor(t, s))
	}
	w := httptest.NewRecorder()
	s.HandleHTTP(w, req)
	return w
}

type rpcResp struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func decode(t *testing.T, w *httptest.ResponseRecorder) rpcResp {
	t.Helper()
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %q (body %s)", ct, w.Body.String())
	}
	var r rpcResp
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
		t.Fatalf("bad JSON-RPC body %q: %v", w.Body.String(), err)
	}
	if r.JSONRPC != "2.0" {
		t.Fatalf("missing jsonrpc 2.0: %s", w.Body.String())
	}
	return r
}

func expectErrCode(t *testing.T, w *httptest.ResponseRecorder, status, code int) rpcResp {
	t.Helper()
	if w.Code != status {
		t.Fatalf("expected HTTP %d, got %d: %s", status, w.Code, w.Body.String())
	}
	r := decode(t, w)
	if r.Error == nil || r.Error.Code != code {
		t.Fatalf("expected error code %d, got %s", code, w.Body.String())
	}
	return r
}

type callResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	IsError bool `json:"isError"`
}

func toolCall(t *testing.T, s *Server, name string, args string) callResult {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"` + name + `","arguments":` + args + `}}`
	w := post(t, s, body, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	r := decode(t, w)
	if r.Error != nil {
		t.Fatalf("unexpected error: %s", w.Body.String())
	}
	var cr callResult
	if err := json.Unmarshal(r.Result, &cr); err != nil {
		t.Fatal(err)
	}
	if len(cr.Content) != 1 || cr.Content[0].Type != "text" {
		t.Fatalf("expected one text content block, got %s", r.Result)
	}
	return cr
}

func echoServer() *Server {
	s := NewServer()
	s.RegisterTool(ToolDefinition{
		Name:        "echo",
		Description: "echoes input",
		InputSchema: map[string]interface{}{"type": "object"},
		Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			return args, nil
		},
	})
	return s
}

func TestMCPInitialize(t *testing.T) {
	s := NewServer()
	w := post(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	r := decode(t, w)
	var res struct {
		ProtocolVersion string                 `json:"protocolVersion"`
		Capabilities    map[string]interface{} `json:"capabilities"`
		ServerInfo      map[string]string      `json:"serverInfo"`
	}
	if err := json.Unmarshal(r.Result, &res); err != nil {
		t.Fatal(err)
	}
	if res.ProtocolVersion != "2024-11-05" {
		t.Fatalf("expected echoed supported version, got %q", res.ProtocolVersion)
	}
	if _, ok := res.Capabilities["tools"]; !ok || res.ServerInfo["name"] != "aerollm-mcp" {
		t.Fatalf("unexpected initialize result: %s", r.Result)
	}
	if string(r.ID) != "1" {
		t.Fatalf("id not echoed: %s", r.ID)
	}
}

func TestMCPInitializeNegotiatesVersion(t *testing.T) {
	s := NewServer()
	for body, want := range map[string]string{
		`{"jsonrpc":"2.0","id":"a","method":"initialize","params":{"protocolVersion":"1999-01-01"}}`: LatestProtocolVersion,
		`{"jsonrpc":"2.0","id":"b","method":"initialize","params":{"protocolVersion":"2025-06-18"}}`: "2025-06-18",
		`{"jsonrpc":"2.0","id":"c","method":"initialize"}`:                                           LatestProtocolVersion,
	} {
		r := decode(t, post(t, s, body, nil))
		if !strings.Contains(string(r.Result), `"protocolVersion":"`+want+`"`) {
			t.Fatalf("body %s: expected %s, got %s", body, want, r.Result)
		}
	}
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":5}}`, nil), http.StatusOK, CodeInvalidParams)
}

func TestMCPToolsListAndCall(t *testing.T) {
	s := echoServer()
	w := post(t, s, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"name":"echo"`) {
		t.Fatalf("expected tool listing, got %d: %s", w.Code, w.Body.String())
	}
	cr := toolCall(t, s, "echo", `{"x":1}`)
	if cr.IsError || cr.Content[0].Text != `{"x":1}` {
		t.Fatalf("expected JSON-encoded echo, got %+v", cr)
	}
}

func TestMCPNotificationsAndPing(t *testing.T) {
	s := NewServer()
	w := post(t, s, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, nil)
	if w.Code != http.StatusAccepted || w.Body.Len() != 0 {
		t.Fatalf("expected 202 with empty body, got %d %q", w.Code, w.Body.String())
	}
	// A client response message is also just accepted.
	w = post(t, s, `{"jsonrpc":"2.0","id":9,"result":{}}`, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for client response, got %d", w.Code)
	}
	r := decode(t, post(t, s, `{"jsonrpc":"2.0","id":"p1","method":"ping"}`, nil))
	if r.Error != nil || string(r.Result) != "{}" || string(r.ID) != `"p1"` {
		t.Fatalf("unexpected ping response: %+v", r)
	}
}

func TestMCPNotificationDoesNotExecuteTools(t *testing.T) {
	s := NewServer()
	called := false
	s.RegisterTool(ToolDefinition{Name: "side", Handler: func(ctx context.Context, a map[string]interface{}) (interface{}, error) {
		called = true
		return nil, nil
	}})
	w := post(t, s, `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"side"}}`, nil)
	if w.Code != http.StatusAccepted || called {
		t.Fatalf("tools/call without id must not execute (code %d, called %v)", w.Code, called)
	}
}

func TestMCPJSONRPCErrors(t *testing.T) {
	s := echoServer()
	cases := []struct {
		name   string
		body   string
		status int
		code   int
	}{
		{"parse error", `{"jsonrpc":`, http.StatusBadRequest, CodeParseError},
		{"empty body", ``, http.StatusBadRequest, CodeParseError},
		{"bad version", `{"jsonrpc":"1.0","id":1,"method":"ping"}`, http.StatusOK, CodeInvalidRequest},
		{"missing version", `{"id":1,"method":"ping"}`, http.StatusOK, CodeInvalidRequest},
		{"non-string method", `{"jsonrpc":"2.0","id":1,"method":5}`, http.StatusOK, CodeInvalidRequest},
		{"missing method", `{"jsonrpc":"2.0","id":1}`, http.StatusOK, CodeInvalidRequest},
		{"null id", `{"jsonrpc":"2.0","id":null,"method":"ping"}`, http.StatusOK, CodeInvalidRequest},
		{"object id", `{"jsonrpc":"2.0","id":{},"method":"ping"}`, http.StatusOK, CodeInvalidRequest},
		{"not an object", `42`, http.StatusOK, CodeInvalidRequest},
		{"unknown method", `{"jsonrpc":"2.0","id":1,"method":"bogus/method"}`, http.StatusOK, CodeMethodNotFound},
		{"resources not configured", `{"jsonrpc":"2.0","id":1,"method":"resources/list"}`, http.StatusOK, CodeMethodNotFound},
		{"call without params", `{"jsonrpc":"2.0","id":1,"method":"tools/call"}`, http.StatusOK, CodeInvalidParams},
		{"array params", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":["echo"]}`, http.StatusOK, CodeInvalidParams},
		{"missing name", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}`, http.StatusOK, CodeInvalidParams},
		{"array arguments", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":[1]}}`, http.StatusOK, CodeInvalidParams},
		{"unknown tool", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"nope"}}`, http.StatusOK, CodeInvalidParams},
		{"list with array params", `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":[]}`, http.StatusOK, CodeInvalidParams},
		{"empty batch", `[]`, http.StatusBadRequest, CodeInvalidRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expectErrCode(t, post(t, s, tc.body, nil), tc.status, tc.code)
		})
	}
}

func TestMCPToolErrorIsInBand(t *testing.T) {
	s := NewServer()
	s.RegisterTool(ToolDefinition{Name: "fail", Handler: func(ctx context.Context, a map[string]interface{}) (interface{}, error) {
		return nil, errors.New("city not found")
	}})
	s.RegisterTool(ToolDefinition{Name: "panic", Handler: func(ctx context.Context, a map[string]interface{}) (interface{}, error) {
		panic("secret internal state")
	}})
	s.RegisterTool(ToolDefinition{Name: "slow", Handler: func(ctx context.Context, a map[string]interface{}) (interface{}, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	s.RegisterTool(ToolDefinition{Name: "big", Handler: func(ctx context.Context, a map[string]interface{}) (interface{}, error) {
		return strings.Repeat("é", 1000), nil
	}})
	s.ToolTimeout = 50 * time.Millisecond
	s.MaxToolOutputBytes = 101

	if cr := toolCall(t, s, "fail", `{}`); !cr.IsError || cr.Content[0].Text != "city not found" {
		t.Fatalf("expected in-band tool error, got %+v", cr)
	}
	cr := toolCall(t, s, "panic", `null`)
	if !cr.IsError || strings.Contains(cr.Content[0].Text, "secret") {
		t.Fatalf("panic must be reported generically, got %+v", cr)
	}
	if cr := toolCall(t, s, "slow", `{}`); !cr.IsError || !strings.Contains(cr.Content[0].Text, "timed out") {
		t.Fatalf("expected timeout, got %+v", cr)
	}
	cr = toolCall(t, s, "big", `{}`)
	if cr.IsError || len(cr.Content[0].Text) > 101 || !strings.HasSuffix(cr.Content[0].Text, "...") {
		t.Fatalf("expected truncated output, got %d bytes", len(cr.Content[0].Text))
	}
	if !json.Valid([]byte(`"` + cr.Content[0].Text + `"`)) {
		t.Fatal("truncation produced invalid UTF-8")
	}
}

func TestMCPBatch(t *testing.T) {
	s := echoServer()
	body := `[
		{"jsonrpc":"2.0","id":1,"method":"ping"},
		{"jsonrpc":"2.0","method":"notifications/initialized"},
		{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo","arguments":{"a":"b"}}},
		{"jsonrpc":"2.0","id":3,"method":"bogus"}
	]`
	w := post(t, s, body, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var rs []rpcResp
	if err := json.Unmarshal(w.Body.Bytes(), &rs); err != nil {
		t.Fatal(err)
	}
	if len(rs) != 3 {
		t.Fatalf("expected 3 responses (notification omitted), got %d: %s", len(rs), w.Body.String())
	}
	if rs[2].Error == nil || rs[2].Error.Code != CodeMethodNotFound {
		t.Fatalf("expected method-not-found for bogus, got %s", w.Body.String())
	}
	w = post(t, s, `[{"jsonrpc":"2.0","method":"notifications/initialized"}]`, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for notification-only batch, got %d", w.Code)
	}
	s.MaxBatchSize = 2
	expectErrCode(t, post(t, s, body, nil), http.StatusBadRequest, CodeInvalidRequest)
}

func TestMCPHTTPGuards(t *testing.T) {
	s := echoServer()

	// No server-initiated SSE stream: GET (and other verbs) yield 405.
	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodPatch} {
		req := httptest.NewRequest(m, "/mcp", nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "POST, DELETE" {
			t.Fatalf("%s: expected 405 with Allow: POST, DELETE, got %d %v", m, w.Code, w.Header())
		}
	}
	stateless := NewServer(WithoutSessions())
	for _, m := range []string{http.MethodGet, http.MethodDelete, http.MethodPut} {
		req := httptest.NewRequest(m, "/mcp", nil)
		w := httptest.NewRecorder()
		stateless.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost {
			t.Fatalf("stateless %s: expected 405 with Allow: POST, got %d %v", m, w.Code, w.Header())
		}
	}

	// Accept must admit application/json when present.
	if w := post(t, s, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, map[string]string{"Accept": "text/event-stream"}); w.Code != http.StatusNotAcceptable {
		t.Fatalf("expected 406 for an Accept header without JSON, got %d", w.Code)
	}
	if w := post(t, s, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, map[string]string{"Accept": "application/json, text/event-stream"}); w.Code != http.StatusOK {
		t.Fatalf("expected 200 for the spec Accept header, got %d", w.Code)
	}

	// text/plain (cross-site form / no-cors fetch) is rejected.
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	req.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	s.HandleHTTP(w, req)
	if w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415, got %d", w.Code)
	}
	// Charset parameter is fine.
	if w := post(t, s, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, map[string]string{"Content-Type": "application/json; charset=utf-8"}); w.Code != http.StatusOK {
		t.Fatalf("expected 200 with charset, got %d", w.Code)
	}

	// Body cap.
	s.MaxBodyBytes = 64
	big := `{"jsonrpc":"2.0","id":1,"method":"ping","params":{"pad":"` + strings.Repeat("x", 200) + `"}}`
	expectErrCode(t, post(t, s, big, nil), http.StatusRequestEntityTooLarge, CodeInvalidRequest)
	s.MaxBodyBytes = 0

	// Protocol version header.
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, map[string]string{"MCP-Protocol-Version": "1.0"}), http.StatusBadRequest, CodeInvalidRequest)
	if w := post(t, s, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, map[string]string{"MCP-Protocol-Version": "2025-06-18"}); w.Code != http.StatusOK {
		t.Fatalf("expected 200 for supported version header, got %d", w.Code)
	}
}

func TestMCPOriginValidation(t *testing.T) {
	s := echoServer()
	s.AllowedOrigins = []string{"https://ide.example.com"}
	ping := `{"jsonrpc":"2.0","id":1,"method":"ping"}`

	expectErrCode(t, post(t, s, ping, map[string]string{"Origin": "https://evil.example"}), http.StatusForbidden, CodeInvalidRequest)
	for _, origin := range []string{"https://ide.example.com", "http://example.com" /* same host as httptest */, ""} {
		hdr := map[string]string{}
		if origin != "" {
			hdr["Origin"] = origin
		}
		if w := post(t, s, ping, hdr); w.Code != http.StatusOK {
			t.Fatalf("origin %q: expected 200, got %d", origin, w.Code)
		}
	}

	t.Setenv(EnvAllowedOrigins, "https://env.example")
	s2 := NewServer()
	if w := post(t, s2, ping, map[string]string{"Origin": "https://env.example"}); w.Code != http.StatusOK {
		t.Fatalf("expected env origin to be allowed, got %d", w.Code)
	}
}

// --- agent registry exposure ---

type regTool struct{ name string }

func (r *regTool) Name() string        { return r.name }
func (r *regTool) Description() string { return "registry tool " + r.name }
func (r *regTool) Parameters() map[string]interface{} {
	return map[string]interface{}{"type": "object", "properties": map[string]interface{}{"q": map[string]interface{}{"type": "string"}}}
}
func (r *regTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	q, _ := args["q"].(string)
	return r.name + ":" + q, nil
}

func TestMCPServerWithRegistry(t *testing.T) {
	reg := agent.NewToolRegistry()
	if err := reg.Register(&regTool{name: "lookup"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(agent.NewApprovalTool(&regTool{name: "delete_everything"})); err != nil {
		t.Fatal(err)
	}
	s := NewServerWithRegistry(reg)
	// Static tool shadows a registry tool of the same name.
	s.RegisterTool(ToolDefinition{Name: "shadowed", Handler: func(ctx context.Context, a map[string]interface{}) (interface{}, error) { return "static", nil }})
	if err := reg.Register(&regTool{name: "shadowed"}); err != nil {
		t.Fatal(err)
	}

	w := post(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, nil)
	body := w.Body.String()
	if !strings.Contains(body, `"name":"lookup"`) || !strings.Contains(body, `"description":"registry tool lookup"`) {
		t.Fatalf("expected registry tool in listing: %s", body)
	}
	if strings.Contains(body, "delete_everything") {
		t.Fatalf("approval-gated tool must not be exposed: %s", body)
	}
	if strings.Count(body, `"name":"shadowed"`) != 1 {
		t.Fatalf("expected shadowed tool once: %s", body)
	}
	if got := s.Tools(); strings.Join(got, ",") != "lookup,shadowed" {
		t.Fatalf("unexpected Tools(): %v", got)
	}

	if cr := toolCall(t, s, "lookup", `{"q":"x"}`); cr.IsError || cr.Content[0].Text != "lookup:x" {
		t.Fatalf("unexpected registry call result: %+v", cr)
	}
	if cr := toolCall(t, s, "shadowed", `{}`); cr.Content[0].Text != "static" {
		t.Fatalf("static tool should win, got %+v", cr)
	}
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"delete_everything"}}`, nil), http.StatusOK, CodeInvalidParams)

	// Tools registered after construction are picked up dynamically.
	_ = reg.Register(&regTool{name: "late"})
	if cr := toolCall(t, s, "late", `{}`); cr.Content[0].Text != "late:" {
		t.Fatalf("expected dynamic registry lookup, got %+v", cr)
	}
}

func TestRegisterToolValidation(t *testing.T) {
	s := NewServer()
	s.RegisterTool(ToolDefinition{Name: "", Handler: func(ctx context.Context, a map[string]interface{}) (interface{}, error) { return nil, nil }})
	s.RegisterTool(ToolDefinition{Name: "nohandler"})
	if len(s.Tools()) != 0 {
		t.Fatalf("invalid definitions must be ignored, got %v", s.Tools())
	}
	if err := s.AddTool(ToolDefinition{Name: "x"}); err == nil {
		t.Fatal("expected AddTool error for missing handler")
	}
	// tools/list with a nil input schema still returns an object schema.
	_ = s.AddTool(ToolDefinition{Name: "noschema", Handler: func(ctx context.Context, a map[string]interface{}) (interface{}, error) { return nil, nil }})
	w := post(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, nil)
	if !strings.Contains(w.Body.String(), `"inputSchema":{"type":"object"}`) {
		t.Fatalf("expected default schema: %s", w.Body.String())
	}
}

func TestTruncateString(t *testing.T) {
	if TruncateString("hello", 10) != "hello" || TruncateString("hello", 0) != "" || TruncateString("hello", 2) != ".." {
		t.Fatal("unexpected truncation")
	}
	if got := TruncateString("hello world", 8); got != "hello..." {
		t.Fatalf("got %q", got)
	}
	got := TruncateString("ééééé", 6) // 10 bytes; must not split a rune
	if !strings.HasSuffix(got, "...") || !json.Valid([]byte(`"`+got+`"`)) || len(got) > 6 {
		t.Fatalf("bad UTF-8 truncation %q", got)
	}
}
