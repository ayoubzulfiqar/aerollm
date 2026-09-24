package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

const testAdminKey = "test-admin-key-0123456789abcdef"

type echoProvider struct{}

func (echoProvider) Name() string                 { return "echo" }
func (echoProvider) Type() providers.ProviderType { return providers.ProviderOpenAI }
func (echoProvider) Health() providers.ProviderHealth {
	return providers.ProviderHealth{Name: "echo", Healthy: true}
}
func (echoProvider) Close() error { return nil }
func (echoProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	text := "echo: " + req.Messages[len(req.Messages)-1].TextContent()
	return &models.LLMResponse{
		ID: "chatcmpl-1", Model: req.Model,
		Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: &text}, FinishReason: "stop"}},
		Usage:   &models.Usage{PromptTokens: 5, CompletionTokens: 3, TotalTokens: 8},
	}, nil
}

func newTestApp(t *testing.T, mutate func(*config.Config)) (*app, *bytes.Buffer) {
	t.Helper()
	t.Setenv("AEROLLM_STATE_DIR", t.TempDir())
	t.Setenv("AEROLLM_API_KEY", "")
	t.Setenv("AEROLLM_MASTER_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("AEROLLM_LOCAL_URL", "")
	cfg, err := config.LoadConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Auth.MasterKey = testAdminKey
	cfg.Auth.APIKeys = []string{"client-key-abcdefghijklmnop"}
	if mutate != nil {
		mutate(cfg)
	}
	var notice bytes.Buffer
	a, err := newApp(context.Background(), cfg, newLogger(io.Discard, "error", "json"), appOptions{skipRedis: true, notice: &notice})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.close(5 * time.Second) })
	return a, &notice
}

func do(a *app, method, path, key, body string) *httptest.ResponseRecorder {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	w := httptest.NewRecorder()
	a.handler.ServeHTTP(w, req)
	return w
}

func TestPublicEndpoints(t *testing.T) {
	a, _ := newTestApp(t, nil)
	if w := do(a, "GET", "/health", "", ""); w.Code != 200 {
		t.Fatalf("/health: %d", w.Code)
	}
	if w := do(a, "GET", "/readyz", "", ""); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz without providers should be 503, got %d: %s", w.Code, w.Body.String())
	}
	a.router.RegisterProvider(echoProvider{})
	if w := do(a, "GET", "/readyz", "", ""); w.Code != 200 {
		t.Fatalf("/readyz with a provider: %d %s", w.Code, w.Body.String())
	}
	w := do(a, "GET", "/metrics", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "aerollm_requests_total") {
		t.Fatalf("/metrics: %d", w.Code)
	}
	if w := do(a, "GET", "/swagger/doc.json", "", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"swagger"`) {
		t.Fatalf("/swagger/doc.json must serve the spec, got %d", w.Code)
	}
	if w.Header().Get("X-Request-ID") == "" {
		t.Fatal("responses must carry X-Request-ID")
	}
}

func TestAuthTiers(t *testing.T) {
	a, _ := newTestApp(t, nil)
	a.router.RegisterProvider(echoProvider{})
	chat := `{"model":"gpt-4o","messages":[{"role":"user","content":"ping"}]}`

	if w := do(a, "POST", "/v1/chat/completions", "", chat); w.Code != 401 {
		t.Fatalf("anonymous chat: %d", w.Code)
	}
	if w := do(a, "POST", "/v1/chat/completions", "wrong", chat); w.Code != 401 {
		t.Fatalf("bad key chat: %d", w.Code)
	}
	w := do(a, "POST", "/v1/chat/completions", "client-key-abcdefghijklmnop", chat)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "echo: ping") {
		t.Fatalf("client chat: %d %s", w.Code, w.Body.String())
	}
	if w.Header().Get("X-RateLimit-Limit-Requests") == "" {
		t.Fatal("rate limit headers missing")
	}

	adminOnly := []struct{ method, path string }{
		{"GET", "/v1/secrets"}, {"GET", "/config/yaml"}, {"POST", "/key/generate"},
		{"GET", "/v1/rsi/stats"}, {"POST", "/v1/chaos/fault"}, {"GET", "/global/spend/report"},
		{"GET", "/v1/cache/stats"}, {"GET", "/v1/incidents"}, {"GET", "/v1/meter/usage"},
		{"POST", "/v1/federated/aggregate"}, {"POST", "/v1/agents/approvals/x"},
	}
	for _, e := range adminOnly {
		if w := do(a, e.method, e.path, "", ""); w.Code != 401 {
			t.Errorf("%s %s anonymous: %d", e.method, e.path, w.Code)
		}
		if w := do(a, e.method, e.path, "client-key-abcdefghijklmnop", ""); w.Code != 403 {
			t.Errorf("%s %s with client key: %d", e.method, e.path, w.Code)
		}
	}
	if w := do(a, "GET", "/config/yaml", testAdminKey, ""); w.Code != 200 || strings.Contains(w.Body.String(), testAdminKey) {
		t.Fatalf("admin config: %d (must not leak keys) %s", w.Code, w.Body.String())
	}
	if w := do(a, "POST", "/mcp", "", `{}`); w.Code != 401 {
		t.Fatalf("/mcp must require a key: %d", w.Code)
	}
}

func TestVirtualKeyLifecycle(t *testing.T) {
	a, _ := newTestApp(t, nil)
	a.router.RegisterProvider(echoProvider{})
	w := do(a, "POST", "/key/generate", testAdminKey, `{"models":["gpt-4o"]}`)
	if w.Code != 200 {
		t.Fatalf("generate: %d %s", w.Code, w.Body.String())
	}
	var gen struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &gen); err != nil || gen.Key == "" {
		t.Fatalf("generate response: %s", w.Body.String())
	}
	chat := func(model string) int {
		return do(a, "POST", "/v1/chat/completions", gen.Key, `{"model":"`+model+`","messages":[{"role":"user","content":"hi"}]}`).Code
	}
	if c := chat("gpt-4o"); c != 200 {
		t.Fatalf("virtual key on allowed model: %d", c)
	}
	if c := chat("claude-3"); c != 403 {
		t.Fatalf("virtual key on disallowed model: %d", c)
	}
	if w := do(a, "GET", "/v1/secrets", gen.Key, ""); w.Code != 403 {
		t.Fatalf("virtual key must not reach admin routes: %d", w.Code)
	}
	if w := do(a, "GET", "/key/info", gen.Key, ""); w.Code != 200 {
		t.Fatalf("virtual key must be able to read its own info: %d %s", w.Code, w.Body.String())
	}
	if w := do(a, "POST", "/key/generate", gen.Key, `{}`); w.Code != 403 {
		t.Fatalf("virtual key must not generate keys: %d", w.Code)
	}
	if recs := a.meter.Records(); len(recs) == 0 || recs[len(recs)-1].TokensIn != 5 {
		t.Fatalf("usage not metered: %+v", recs)
	}
}

func TestGeneratedAdminKeyWhenUnconfigured(t *testing.T) {
	a, notice := newTestApp(t, func(c *config.Config) { c.Auth.MasterKey = "" })
	if a.generatedAdminKey == "" || !strings.Contains(notice.String(), a.generatedAdminKey) {
		t.Fatalf("expected a generated admin key to be announced, notice=%q", notice.String())
	}
	if w := do(a, "GET", "/config/yaml", a.generatedAdminKey, ""); w.Code != 200 {
		t.Fatalf("generated key must work: %d", w.Code)
	}
	if w := do(a, "GET", "/config/yaml", "sk-demo", ""); w.Code != 401 {
		t.Fatalf("legacy default key must not be accepted: %d", w.Code)
	}
}

func TestAnthropicEndpointWithXAPIKey(t *testing.T) {
	a, _ := newTestApp(t, nil)
	a.router.RegisterProvider(echoProvider{})
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"claude-3","max_tokens":10,"messages":[{"role":"user","content":"yo"}]}`))
	req.Header.Set("X-API-Key", "client-key-abcdefghijklmnop")
	req.Header.Set("Anthropic-Version", "2023-06-01")
	w := httptest.NewRecorder()
	a.handler.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"type":"message"`) || !strings.Contains(w.Body.String(), "echo: yo") {
		t.Fatalf("anthropic endpoint: %d %s", w.Code, w.Body.String())
	}
}

func TestCloseStopsWorkersPromptly(t *testing.T) {
	a, _ := newTestApp(t, nil)
	start := time.Now()
	a.close(5 * time.Second)
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("shutdown took %s", d)
	}
	a.close(time.Second) // idempotent
}

func TestBatchLifecycleAndOwnerIsolation(t *testing.T) {
	a, _ := newTestApp(t, func(c *config.Config) {
		c.Auth.APIKeys = []string{"client-key-abcdefghijklmnop", "other-key-abcdefghijklmnopq"}
	})
	a.router.RegisterProvider(echoProvider{})
	input := `{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4o","messages":[{"role":"user","content":"one"}]}}` + "\n" +
		`{"custom_id":"b","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4o","messages":[{"role":"user","content":"two"}]}}` + "\n"
	body, _ := json.Marshal(map[string]string{"input": input})
	w := do(a, "POST", "/v1/batches", "client-key-abcdefghijklmnop", string(body))
	if w.Code != 200 {
		t.Fatalf("create batch: %d %s", w.Code, w.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &created)

	deadline := time.Now().Add(5 * time.Second)
	for {
		w = do(a, "GET", "/v1/batches/"+created.ID, "client-key-abcdefghijklmnop", "")
		if strings.Contains(w.Body.String(), `"status":"completed"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("batch did not complete: %s", w.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	w = do(a, "GET", "/v1/batches/"+created.ID+"/results", "client-key-abcdefghijklmnop", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "echo: one") || !strings.Contains(w.Body.String(), "echo: two") {
		t.Fatalf("results: %d %s", w.Code, w.Body.String())
	}
	if w := do(a, "GET", "/v1/batches/"+created.ID, "other-key-abcdefghijklmnopq", ""); w.Code != 404 {
		t.Fatalf("another key must not see the batch: %d", w.Code)
	}
	if w := do(a, "GET", "/v1/batches/"+created.ID, testAdminKey, ""); w.Code != 200 {
		t.Fatalf("admin should see every batch: %d", w.Code)
	}
	if w := do(a, "GET", "/v1/batches", "other-key-abcdefghijklmnopq", ""); !strings.Contains(w.Body.String(), `"data":[]`) {
		t.Fatalf("list must be owner-scoped: %s", w.Body.String())
	}
}
