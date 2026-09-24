package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// isolateEnv clears every environment variable the CLI reads so tests do
// not pick up the developer's configuration.
func isolateEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"AEROLLM_URL", "AEROLLM_SERVER_URL", "AEROLLM_API_KEY", "AEROLLM_MODEL",
		"EDGE_LISTEN", "AEROLLM_PLUGIN_PRIVATE_KEY", "AEROLLM_PLUGIN_CREATOR",
		"AEROLLM_MARKETPLACE_URL", "AEROLLM_MARKETPLACE_TOKEN", "AEROLLM_GITOPS_REPO",
	} {
		t.Setenv(k, "")
	}
}

// runCLI executes the CLI in-process and returns stdout, stderr and the
// error returned by the root command.
func runCLI(t *testing.T, stdin string, args ...string) (string, string, error) {
	t.Helper()
	root := newRootCmd()
	var out, errb bytes.Buffer
	root.SetArgs(args)
	root.SetIn(strings.NewReader(stdin))
	root.SetOut(&out)
	root.SetErr(&errb)
	err := root.ExecuteContext(context.Background())
	return out.String(), errb.String(), err
}

// recordedRequest is one request received by the fake gateway.
type recordedRequest struct {
	Method string
	Path   string
	Query  string
	Auth   string
	Header http.Header
	Body   string
}

// fakeGateway is an httptest server that records requests and dispatches
// them to per-path handlers.
type fakeGateway struct {
	*httptest.Server
	mu       sync.Mutex
	requests []recordedRequest
	handlers map[string]http.HandlerFunc
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
	g := &fakeGateway{handlers: map[string]http.HandlerFunc{}}
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		g.mu.Lock()
		g.requests = append(g.requests, recordedRequest{
			Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery,
			Auth: r.Header.Get("Authorization"), Header: r.Header.Clone(), Body: string(body),
		})
		h := g.handlers[r.URL.Path]
		g.mu.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(body))
		if h == nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		h(w, r)
	}))
	t.Cleanup(g.Close)
	return g
}

// handle registers a handler for an exact path.
func (g *fakeGateway) handle(path string, h http.HandlerFunc) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.handlers[path] = h
}

// json registers a handler that replies with status and a JSON body.
func (g *fakeGateway) json(path string, status int, body string) {
	g.handle(path, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
}

func (g *fakeGateway) all() []recordedRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]recordedRequest(nil), g.requests...)
}

// last returns the most recent request, failing the test if there is none.
func (g *fakeGateway) last(t *testing.T) recordedRequest {
	t.Helper()
	reqs := g.all()
	if len(reqs) == 0 {
		t.Fatal("fake gateway received no requests")
	}
	return reqs[len(reqs)-1]
}

// decodeBody unmarshals a recorded JSON body.
func decodeBody(t *testing.T, body string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("request body is not a JSON object: %v: %s", err, body)
	}
	return m
}

// gw runs the CLI against the fake gateway with an API key.
func (g *fakeGateway) run(t *testing.T, stdin string, args ...string) (string, string, error) {
	t.Helper()
	return runCLI(t, stdin, append([]string{"--server", g.URL, "--api-key", "sk-test-master-key"}, args...)...)
}
