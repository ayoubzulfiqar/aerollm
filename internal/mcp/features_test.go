package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
)

const initBody = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`

// send posts body with exactly the given headers (no automatic session).
func send(t *testing.T, s *Server, method, body string, hdr map[string]string, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "/mcp", rd)
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	return w
}

// initSession runs initialize and returns the issued session ID.
func initSession(t *testing.T, s *Server, ctx context.Context) string {
	t.Helper()
	w := send(t, s, http.MethodPost, initBody, nil, ctx)
	if w.Code != http.StatusOK {
		t.Fatalf("initialize: %d %s", w.Code, w.Body.String())
	}
	sid := w.Header().Get(HeaderSessionID)
	if !validSessionID(sid) || len(sid) < 32 {
		t.Fatalf("initialize must issue a visible-ASCII session id, got %q", sid)
	}
	return sid
}

func withSession(sid string) map[string]string {
	return map[string]string{HeaderSessionID: sid, "MCP-Protocol-Version": "2025-06-18"}
}

func TestSessionLifecycle(t *testing.T) {
	s := echoServer()
	sid := initSession(t, s, nil)
	if other := initSession(t, s, nil); other == sid {
		t.Fatal("session ids must be unique")
	}
	if s.SessionCount() != 2 {
		t.Fatalf("expected 2 sessions, got %d", s.SessionCount())
	}

	list := `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`
	if w := send(t, s, http.MethodPost, list, withSession(sid), nil); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"echo"`) {
		t.Fatalf("request with session failed: %d %s", w.Code, w.Body.String())
	}
	if w := send(t, s, http.MethodPost, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, withSession(sid), nil); w.Code != http.StatusAccepted {
		t.Fatalf("notification with session: %d", w.Code)
	}

	// Missing session: 400; unknown or malformed session: 404.
	expectErrCode(t, send(t, s, http.MethodPost, list, nil, nil), http.StatusBadRequest, CodeSessionRequired)
	expectErrCode(t, send(t, s, http.MethodPost, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, nil, nil), http.StatusBadRequest, CodeSessionRequired)
	expectErrCode(t, send(t, s, http.MethodPost, list, withSession("deadbeef"), nil), http.StatusNotFound, CodeSessionNotFound)
	expectErrCode(t, send(t, s, http.MethodPost, list, map[string]string{HeaderSessionID: "bad id"}, nil), http.StatusNotFound, CodeSessionNotFound)

	// Pings are allowed before a session exists.
	if w := send(t, s, http.MethodPost, `{"jsonrpc":"2.0","id":"p","method":"ping"}`, nil, nil); w.Code != http.StatusOK {
		t.Fatalf("session-less ping: %d", w.Code)
	}
	if w := send(t, s, http.MethodPost, `[{"jsonrpc":"2.0","id":1,"method":"ping"},{"jsonrpc":"2.0","id":2,"method":"ping"}]`, nil, nil); w.Code != http.StatusOK {
		t.Fatalf("session-less ping batch: %d", w.Code)
	}

	// DELETE terminates the session.
	expectErrCode(t, send(t, s, http.MethodDelete, "", nil, nil), http.StatusBadRequest, CodeSessionRequired)
	if w := send(t, s, http.MethodDelete, "", withSession(sid), nil); w.Code != http.StatusNoContent {
		t.Fatalf("DELETE: expected 204, got %d", w.Code)
	}
	expectErrCode(t, send(t, s, http.MethodPost, list, withSession(sid), nil), http.StatusNotFound, CodeSessionNotFound)
	expectErrCode(t, send(t, s, http.MethodDelete, "", withSession(sid), nil), http.StatusNotFound, CodeSessionNotFound)
	if s.SessionCount() != 1 {
		t.Fatalf("expected 1 session left, got %d", s.SessionCount())
	}

	// A new initialize (the client's recovery after 404) works even when a
	// stale session header is still sent.
	w := send(t, s, http.MethodPost, initBody, withSession(sid), nil)
	if w.Code != http.StatusOK || w.Header().Get(HeaderSessionID) == "" || w.Header().Get(HeaderSessionID) == sid {
		t.Fatalf("re-initialize should issue a fresh session: %d %v", w.Code, w.Header())
	}
}

func TestSessionNotIssuedOnFailedInitialize(t *testing.T) {
	s := NewServer()
	w := send(t, s, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":5}}`, nil, nil)
	if w.Header().Get(HeaderSessionID) != "" || s.SessionCount() != 0 {
		t.Fatal("a failed initialize must not create a session")
	}
	// initialize sent as a notification creates nothing either.
	if w := send(t, s, http.MethodPost, `{"jsonrpc":"2.0","method":"initialize"}`, nil, nil); w.Code != http.StatusAccepted || s.SessionCount() != 0 {
		t.Fatalf("initialize notification: %d, sessions %d", w.Code, s.SessionCount())
	}
	// initialize must not be batched.
	sid := initSession(t, s, nil)
	w = send(t, s, http.MethodPost, `[{"jsonrpc":"2.0","id":1,"method":"initialize"},{"jsonrpc":"2.0","id":2,"method":"ping"}]`, withSession(sid), nil)
	var rs []rpcResp
	if err := json.Unmarshal(w.Body.Bytes(), &rs); err != nil || len(rs) != 2 {
		t.Fatalf("unexpected batch response %s", w.Body.String())
	}
	if rs[0].Error == nil || rs[0].Error.Code != CodeInvalidRequest || rs[1].Error != nil {
		t.Fatalf("initialize inside a batch must be rejected: %s", w.Body.String())
	}
	if w.Header().Get(HeaderSessionID) != "" {
		t.Fatal("batched initialize must not issue a session")
	}
}

func TestSessionIdleExpiryAndCapacity(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }

	s := NewServer(WithSessionIdleTimeout(time.Minute), WithMaxSessions(2))
	s.sessions.now = clock
	ping := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`

	a := initSession(t, s, nil)
	advance(50 * time.Second)
	if w := send(t, s, http.MethodPost, ping, withSession(a), nil); w.Code != http.StatusOK {
		t.Fatalf("session should be live: %d", w.Code)
	}
	advance(50 * time.Second) // 50s since last use: still live
	if w := send(t, s, http.MethodPost, ping, withSession(a), nil); w.Code != http.StatusOK {
		t.Fatalf("use must refresh the idle timer: %d", w.Code)
	}
	advance(61 * time.Second)
	expectErrCode(t, send(t, s, http.MethodPost, ping, withSession(a), nil), http.StatusNotFound, CodeSessionNotFound)

	// Capacity: the least recently used session is evicted.
	b := initSession(t, s, nil)
	c := initSession(t, s, nil)
	if w := send(t, s, http.MethodPost, ping, withSession(b), nil); w.Code != http.StatusOK { // b is now most recent
		t.Fatal(w.Code)
	}
	d := initSession(t, s, nil)
	if s.SessionCount() != 2 {
		t.Fatalf("session count must stay bounded, got %d", s.SessionCount())
	}
	expectErrCode(t, send(t, s, http.MethodPost, ping, withSession(c), nil), http.StatusNotFound, CodeSessionNotFound)
	for _, sid := range []string{b, d} {
		if w := send(t, s, http.MethodPost, ping, withSession(sid), nil); w.Code != http.StatusOK {
			t.Fatalf("recent session evicted: %d", w.Code)
		}
	}
}

func TestSessionBoundToPrincipal(t *testing.T) {
	s := echoServer()
	alice := middleware.WithPrincipal(context.Background(), &middleware.Principal{KeyID: "key_alice"})
	mallory := middleware.WithPrincipal(context.Background(), &middleware.Principal{KeyID: "key_mallory"})
	sid := initSession(t, s, alice)
	list := `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`
	if w := send(t, s, http.MethodPost, list, withSession(sid), alice); w.Code != http.StatusOK {
		t.Fatalf("owner request failed: %d", w.Code)
	}
	expectErrCode(t, send(t, s, http.MethodPost, list, withSession(sid), mallory), http.StatusNotFound, CodeSessionNotFound)
	expectErrCode(t, send(t, s, http.MethodPost, list, withSession(sid), nil), http.StatusNotFound, CodeSessionNotFound)
	expectErrCode(t, send(t, s, http.MethodDelete, "", withSession(sid), mallory), http.StatusNotFound, CodeSessionNotFound)
	if w := send(t, s, http.MethodDelete, "", withSession(sid), alice); w.Code != http.StatusNoContent {
		t.Fatalf("owner DELETE: %d", w.Code)
	}

	custom := NewServer(WithSessionOwner(func(r *http.Request) string { return r.Header.Get("X-Tenant") }))
	w := send(t, custom, http.MethodPost, initBody, map[string]string{"X-Tenant": "t1"}, nil)
	sid = w.Header().Get(HeaderSessionID)
	hdr := withSession(sid)
	hdr["X-Tenant"] = "t2"
	expectErrCode(t, send(t, custom, http.MethodPost, list, hdr, nil), http.StatusNotFound, CodeSessionNotFound)
}

func TestStatelessMode(t *testing.T) {
	check := func(s *Server) {
		t.Helper()
		w := send(t, s, http.MethodPost, initBody, nil, nil)
		if w.Code != http.StatusOK || w.Header().Get(HeaderSessionID) != "" {
			t.Fatalf("stateless initialize must not issue a session: %d %v", w.Code, w.Header())
		}
		if w := send(t, s, http.MethodPost, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`, nil, nil); w.Code != http.StatusOK {
			t.Fatalf("stateless request without session: %d", w.Code)
		}
		// A stray session header is ignored.
		if w := send(t, s, http.MethodPost, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`, withSession("whatever"), nil); w.Code != http.StatusOK {
			t.Fatalf("stateless request with unknown session: %d", w.Code)
		}
	}
	check(NewServer(WithoutSessions()))
	t.Setenv(EnvStateless, "true")
	check(NewServer())
	if s := NewServer(WithSessions()); s.sessions == nil {
		t.Fatal("WithSessions must override the environment")
	}
}

func TestSessionIDReachesTools(t *testing.T) {
	s := NewServer()
	got := make(chan string, 1)
	s.RegisterTool(ToolDefinition{Name: "who", Handler: func(ctx context.Context, _ map[string]interface{}) (interface{}, error) {
		got <- SessionIDFromContext(ctx)
		return "ok", nil
	}})
	sid := initSession(t, s, nil)
	w := send(t, s, http.MethodPost, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"who"}}`, withSession(sid), nil)
	if w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	if id := <-got; id != sid {
		t.Fatalf("tool saw session %q, want %q", id, sid)
	}
}

func TestCapabilitiesAdvertisedOnlyWhenConfigured(t *testing.T) {
	caps := func(s *Server) map[string]interface{} {
		t.Helper()
		r := decode(t, send(t, s, http.MethodPost, initBody, nil, nil))
		var res struct {
			Capabilities map[string]interface{} `json:"capabilities"`
			Instructions string                 `json:"instructions"`
		}
		if err := json.Unmarshal(r.Result, &res); err != nil {
			t.Fatal(err)
		}
		if res.Instructions != "" {
			res.Capabilities["_instructions"] = res.Instructions
		}
		return res.Capabilities
	}
	plain := caps(NewServer())
	if _, ok := plain["resources"]; ok {
		t.Fatalf("resources advertised without a provider: %v", plain)
	}
	if _, ok := plain["prompts"]; ok {
		t.Fatalf("prompts advertised without prompts: %v", plain)
	}
	s := NewServer(WithResourceProvider(NewStaticResourceProvider()), WithInstructions("use the tools"))
	s.RegisterPrompt(PromptDefinition{Name: "p", Messages: []PromptMessage{{Role: RoleUser, Text: "hi"}}})
	full := caps(s)
	if _, ok := full["resources"]; !ok {
		t.Fatalf("resources not advertised: %v", full)
	}
	if _, ok := full["prompts"]; !ok {
		t.Fatalf("prompts not advertised: %v", full)
	}
	if full["_instructions"] != "use the tools" {
		t.Fatalf("instructions missing: %v", full)
	}
}

// --- resources ---

type errProvider struct{ panics bool }

func (p errProvider) ListResources(context.Context) ([]Resource, error) {
	if p.panics {
		panic("provider bug")
	}
	return nil, errors.New("backend at 10.0.0.1 unreachable")
}

func (p errProvider) ReadResource(context.Context, string) ([]ResourceContents, error) {
	if p.panics {
		panic("provider bug")
	}
	return nil, errors.New("backend at 10.0.0.1 unreachable")
}

func TestResources(t *testing.T) {
	rp := NewStaticResourceProvider()
	if err := rp.AddText(Resource{URI: "relative/path"}, "x"); err == nil {
		t.Fatal("relative URIs must be rejected")
	}
	for _, uri := range []string{"file:///docs/a.md", "file:///docs/b.md", "file:///docs/c.md"} {
		if err := rp.AddText(Resource{URI: uri, MimeType: "text/markdown", Description: "doc"}, "# "+uri); err != nil {
			t.Fatal(err)
		}
	}
	if err := rp.AddBlob(Resource{URI: "mem://logo.png", Name: "logo", MimeType: "image/png"}, []byte{0x89, 'P', 'N', 'G'}); err != nil {
		t.Fatal(err)
	}
	s := NewServer(WithResourceProvider(rp))
	s.PageSize = 3

	// Pagination.
	r := decode(t, post(t, s, `{"jsonrpc":"2.0","id":1,"method":"resources/list"}`, nil))
	var page struct {
		Resources  []Resource `json:"resources"`
		NextCursor string     `json:"nextCursor"`
	}
	if err := json.Unmarshal(r.Result, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Resources) != 3 || page.NextCursor == "" || page.Resources[0].Name != "file:///docs/a.md" {
		t.Fatalf("unexpected first page: %s", r.Result)
	}
	r = decode(t, post(t, s, `{"jsonrpc":"2.0","id":2,"method":"resources/list","params":{"cursor":"`+page.NextCursor+`"}}`, nil))
	page.NextCursor = ""
	page.Resources = nil
	_ = json.Unmarshal(r.Result, &page)
	if len(page.Resources) != 1 || page.Resources[0].URI != "mem://logo.png" || page.Resources[0].Size != 4 || page.NextCursor != "" {
		t.Fatalf("unexpected last page: %s", r.Result)
	}
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":3,"method":"resources/list","params":{"cursor":"garbage!"}}`, nil), http.StatusOK, CodeInvalidParams)

	// Read text and blob.
	r = decode(t, post(t, s, `{"jsonrpc":"2.0","id":4,"method":"resources/read","params":{"uri":"file:///docs/b.md"}}`, nil))
	if !strings.Contains(string(r.Result), `"text":"# file:///docs/b.md"`) || !strings.Contains(string(r.Result), `"mimeType":"text/markdown"`) {
		t.Fatalf("unexpected text read: %s", r.Result)
	}
	r = decode(t, post(t, s, `{"jsonrpc":"2.0","id":5,"method":"resources/read","params":{"uri":"mem://logo.png"}}`, nil))
	want := base64.StdEncoding.EncodeToString([]byte{0x89, 'P', 'N', 'G'})
	if !strings.Contains(string(r.Result), `"blob":"`+want+`"`) || strings.Contains(string(r.Result), `"text"`) {
		t.Fatalf("unexpected blob read: %s", r.Result)
	}

	// Errors.
	w := post(t, s, `{"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"file:///nope"}}`, nil)
	expectErrCode(t, w, http.StatusOK, CodeResourceNotFound)
	if !strings.Contains(w.Body.String(), `"data":{"uri":"file:///nope"}`) {
		t.Fatalf("not-found error must carry the uri: %s", w.Body.String())
	}
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":7,"method":"resources/read","params":{}}`, nil), http.StatusOK, CodeInvalidParams)
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":7,"method":"resources/read"}`, nil), http.StatusOK, CodeInvalidParams)
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":7,"method":"resources/subscribe","params":{"uri":"file:///docs/a.md"}}`, nil), http.StatusOK, CodeMethodNotFound)

	// Templates: empty list.
	r = decode(t, post(t, s, `{"jsonrpc":"2.0","id":8,"method":"resources/templates/list"}`, nil))
	if string(r.Result) != `{"resourceTemplates":[]}` {
		t.Fatalf("unexpected templates result: %s", r.Result)
	}

	// Size cap.
	s.MaxResourceBytes = 4
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":9,"method":"resources/read","params":{"uri":"file:///docs/a.md"}}`, nil), http.StatusOK, CodeInternalError)

	// Provider failures are generic and never leak internals.
	for _, p := range []errProvider{{}, {panics: true}} {
		bad := NewServer(WithResourceProvider(p))
		for _, body := range []string{
			`{"jsonrpc":"2.0","id":1,"method":"resources/list"}`,
			`{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"file:///x"}}`,
		} {
			w := post(t, bad, body, nil)
			expectErrCode(t, w, http.StatusOK, CodeInternalError)
			if strings.Contains(w.Body.String(), "10.0.0.1") || strings.Contains(w.Body.String(), "provider bug") {
				t.Fatalf("provider error leaked: %s", w.Body.String())
			}
		}
	}
	if !rp.Remove("mem://logo.png") || rp.Remove("mem://logo.png") {
		t.Fatal("Remove should report existence")
	}
}

// --- prompts ---

func TestPromptRegistrationValidation(t *testing.T) {
	s := NewServer()
	msgs := []PromptMessage{{Role: RoleUser, Text: "x"}}
	for name, def := range map[string]PromptDefinition{
		"bad name":      {Name: "has space", Messages: msgs},
		"no body":       {Name: "empty"},
		"bad role":      {Name: "r", Messages: []PromptMessage{{Role: "system", Text: "x"}}},
		"bad arg":       {Name: "a", Messages: msgs, Arguments: []PromptArgument{{Name: ""}}},
		"duplicate arg": {Name: "d", Messages: msgs, Arguments: []PromptArgument{{Name: "x"}, {Name: "x"}}},
	} {
		if err := s.AddPrompt(def); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
	if len(s.Prompts()) != 0 {
		t.Fatalf("invalid prompts registered: %v", s.Prompts())
	}
}

func TestPrompts(t *testing.T) {
	s := NewServer()
	if err := s.AddPrompt(PromptDefinition{
		Name:        "code_review",
		Title:       "Code review",
		Description: "Review a snippet",
		Arguments: []PromptArgument{
			{Name: "code", Description: "the code", Required: true},
			{Name: "focus", Description: "what to focus on"},
		},
		Messages: []PromptMessage{
			{Role: RoleUser, Text: "Review this code focusing on {{ focus }}:\n{{code}}\nKeep {{unknown}} as is."},
			{Role: RoleAssistant, Text: "Sure."},
		},
	}); err != nil {
		t.Fatal(err)
	}
	var handlerArgs map[string]string
	s.RegisterPrompt(PromptDefinition{
		Name:      "dynamic",
		Arguments: []PromptArgument{{Name: "n", Required: true}},
		Handler: func(ctx context.Context, args map[string]string) ([]PromptMessage, error) {
			handlerArgs = args
			if args["n"] == "fail" {
				return nil, errors.New("secret failure detail")
			}
			return []PromptMessage{{Role: RoleUser, Text: "n=" + args["n"]}}, nil
		},
	})
	if got := s.Prompts(); strings.Join(got, ",") != "code_review,dynamic" {
		t.Fatalf("unexpected prompts: %v", got)
	}

	r := decode(t, post(t, s, `{"jsonrpc":"2.0","id":1,"method":"prompts/list"}`, nil))
	body := string(r.Result)
	if !strings.Contains(body, `"name":"code_review"`) || !strings.Contains(body, `"title":"Code review"`) ||
		!strings.Contains(body, `{"name":"code","description":"the code","required":true}`) {
		t.Fatalf("unexpected prompts/list: %s", body)
	}

	get := func(args string) *httptest.ResponseRecorder {
		return post(t, s, `{"jsonrpc":"2.0","id":2,"method":"prompts/get","params":{"name":"code_review","arguments":`+args+`}}`, nil)
	}
	r = decode(t, get(`{"code":"x := {{focus}}","focus":"errors","extra":"ignored"}`))
	var res struct {
		Description string `json:"description"`
		Messages    []struct {
			Role    string `json:"role"`
			Content struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(r.Result, &res); err != nil {
		t.Fatal(err)
	}
	if res.Description != "Review a snippet" || len(res.Messages) != 2 || res.Messages[1].Role != RoleAssistant {
		t.Fatalf("unexpected prompt result: %s", r.Result)
	}
	text := res.Messages[0].Content.Text
	if res.Messages[0].Content.Type != "text" ||
		text != "Review this code focusing on errors:\nx := {{focus}}\nKeep {{unknown}} as is." {
		t.Fatalf("bad substitution (values must not be re-expanded, undeclared kept): %q", text)
	}
	// Optional argument omitted -> empty.
	r = decode(t, get(`{"code":"y"}`))
	if !strings.Contains(string(r.Result), `focusing on :\ny`) {
		t.Fatalf("omitted optional argument should render empty: %s", r.Result)
	}

	expectErrCode(t, get(`{"focus":"x"}`), http.StatusOK, CodeInvalidParams) // missing required
	expectErrCode(t, get(`{"code":5}`), http.StatusOK, CodeInvalidParams)    // non-string
	expectErrCode(t, get(`["code"]`), http.StatusOK, CodeInvalidParams)      // not an object
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":3,"method":"prompts/get","params":{"name":"nope"}}`, nil), http.StatusOK, CodeInvalidParams)
	expectErrCode(t, post(t, s, `{"jsonrpc":"2.0","id":3,"method":"prompts/get","params":{}}`, nil), http.StatusOK, CodeInvalidParams)

	// Handler-backed prompt.
	r = decode(t, post(t, s, `{"jsonrpc":"2.0","id":4,"method":"prompts/get","params":{"name":"dynamic","arguments":{"n":"7","zz":"dropped"}}}`, nil))
	if !strings.Contains(string(r.Result), `"text":"n=7"`) || len(handlerArgs) != 1 {
		t.Fatalf("handler prompt: %s (args %v)", r.Result, handlerArgs)
	}
	w := post(t, s, `{"jsonrpc":"2.0","id":5,"method":"prompts/get","params":{"name":"dynamic","arguments":{"n":"fail"}}}`, nil)
	expectErrCode(t, w, http.StatusOK, CodeInternalError)
	if strings.Contains(w.Body.String(), "secret") {
		t.Fatalf("handler error leaked: %s", w.Body.String())
	}

	// Pagination.
	s.PageSize = 1
	r = decode(t, post(t, s, `{"jsonrpc":"2.0","id":6,"method":"prompts/list"}`, nil))
	if !strings.Contains(string(r.Result), `"nextCursor"`) || strings.Contains(string(r.Result), "dynamic") {
		t.Fatalf("expected a first page with a cursor: %s", r.Result)
	}
	if !s.RemovePrompt("dynamic") || s.RemovePrompt("dynamic") {
		t.Fatal("RemovePrompt should report existence")
	}
}

// --- billing ---

type fakeBiller struct {
	mu       sync.Mutex
	calls    []string
	sessions []string
	err      error
	panics   bool
}

func (b *fakeBiller) BillToolCall(ctx context.Context, tool string) error {
	if b.panics {
		panic("billing bug")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, tool)
	b.sessions = append(b.sessions, SessionIDFromContext(ctx))
	return b.err
}

func TestToolBilling(t *testing.T) {
	var ran atomic.Int32
	newServer := func(b *fakeBiller) *Server {
		s := NewServer(WithToolBilling(b))
		s.RegisterTool(ToolDefinition{Name: "paid", Handler: func(context.Context, map[string]interface{}) (interface{}, error) {
			ran.Add(1)
			return "done", nil
		}})
		return s
	}

	ok := &fakeBiller{}
	s := newServer(ok)
	sid := initSession(t, s, nil)
	w := send(t, s, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"paid"}}`, withSession(sid), nil)
	r := decode(t, w)
	if !strings.Contains(string(r.Result), `"text":"done"`) || ran.Load() != 1 {
		t.Fatalf("billed call should run: %s", r.Result)
	}
	if len(ok.calls) != 1 || ok.calls[0] != "paid" || ok.sessions[0] != sid {
		t.Fatalf("biller not called with the tool and session: %+v", ok)
	}
	// Unknown tools and notifications are never billed.
	post(t, s, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"nope"}}`, nil)
	post(t, s, `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"paid"}}`, nil)
	if len(ok.calls) != 1 {
		t.Fatalf("unexpected billing: %v", ok.calls)
	}

	for _, b := range []*fakeBiller{{err: errors.New("insufficient balance for wallet w-123")}, {panics: true}} {
		ran.Store(0)
		cr := toolCall(t, newServer(b), "paid", `{}`)
		if !cr.IsError || cr.Content[0].Text != "tool call rejected: billing failed" {
			t.Fatalf("billing failure must reject the call cleanly: %+v", cr)
		}
		if ran.Load() != 0 {
			t.Fatal("tool executed although billing failed")
		}
	}
}

// TestEndToEndOverHTTP exercises the session flow through a real HTTP
// server, as an MCP client would.
func TestEndToEndOverHTTP(t *testing.T) {
	s := echoServer()
	ts := httptest.NewServer(s)
	defer ts.Close()
	do := func(method, body, sid string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if sid != "" {
			req.Header.Set("Mcp-Session-Id", sid)
			req.Header.Set("MCP-Protocol-Version", "2025-06-18")
		}
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	resp := do(http.MethodPost, initBody, "")
	sid := resp.Header.Get("Mcp-Session-Id")
	if resp.StatusCode != http.StatusOK || sid == "" {
		t.Fatalf("initialize over HTTP: %d %q", resp.StatusCode, sid)
	}
	if resp := do(http.MethodPost, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, sid); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("initialized notification: %d", resp.StatusCode)
	}
	resp = do(http.MethodPost, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo","arguments":{"a":1}}}`, sid)
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), `{\"a\":1}`) {
		t.Fatalf("tools/call over HTTP: %d %s", resp.StatusCode, b)
	}
	if resp := do(http.MethodGet, "", sid); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET must be 405 without an SSE stream, got %d", resp.StatusCode)
	}
	if resp := do(http.MethodDelete, "", sid); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: %d", resp.StatusCode)
	}
	if resp := do(http.MethodPost, `{"jsonrpc":"2.0","id":3,"method":"tools/list"}`, sid); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("terminated session must yield 404, got %d", resp.StatusCode)
	}
}
