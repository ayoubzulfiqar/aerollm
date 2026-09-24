package universal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

var (
	_ providers.Prober = (*OpenAICompatibleAdapter)(nil)
	_ providers.Prober = (*AnthropicAdapter)(nil)
	_ providers.Prober = (*LegacyProviderAdapter)(nil)
)

type recordedReq struct {
	method, uri, apiKey, auth string
	body                      map[string]interface{}
}

// recorder answers every request with a minimal OpenAI-style body.
func recorder(t *testing.T) (*httptest.Server, func() []recordedReq) {
	t.Helper()
	var mu sync.Mutex
	var reqs []recordedReq
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rr := recordedReq{method: r.Method, uri: r.RequestURI, apiKey: r.Header.Get("api-key"), auth: r.Header.Get("Authorization")}
		_ = json.NewDecoder(r.Body).Decode(&rr.body)
		mu.Lock()
		reqs = append(reqs, rr)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/embeddings"):
			io.WriteString(w, `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1]}],"model":"m"}`)
		case strings.HasSuffix(r.URL.Path, "/models"):
			io.WriteString(w, `{"data":[]}`)
		default:
			io.WriteString(w, `{"id":"x","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() []recordedReq {
		mu.Lock()
		defer mu.Unlock()
		return append([]recordedReq(nil), reqs...)
	}
}

func TestAzureDeploymentURLs(t *testing.T) {
	srv, reqs := recorder(t)
	ctx := context.Background()

	// Resource root + api_version → deployment URLs named by the model.
	a := NewAzureAdapter("azure-east", "azkey", srv.URL, "2024-10-21")
	if !a.AzureDeploymentMode() || a.Name() != "azure-east" {
		t.Fatal("deployment mode expected")
	}
	if _, err := a.ChatCompletions(ctx, userReq("my-gpt4o", "hi")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Embeddings(ctx, &models.EmbeddingRequest{Model: "embed-small", Input: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	got := reqs()
	want := []string{
		"/openai/deployments/my-gpt4o/chat/completions?api-version=2024-10-21",
		"/openai/deployments/embed-small/embeddings?api-version=2024-10-21",
		"/openai/models?api-version=2024-10-21",
	}
	for i, w := range want {
		if got[i].uri != w {
			t.Fatalf("request %d: got %s want %s", i, got[i].uri, w)
		}
		if got[i].apiKey != "azkey" || got[i].auth != "" {
			t.Fatalf("request %d must use the api-key header: %+v", i, got[i])
		}
	}
	if got[2].method != http.MethodGet {
		t.Fatal("probe must be a GET")
	}

	// Streaming uses the deployment URL too.
	stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/openai/deployments/dep1/chat/completions" || r.URL.Query().Get("api-version") != "2025-01-01-preview" {
			http.Error(w, "wrong path "+r.RequestURI, 404)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer stream.Close()
	sa := NewAzureAdapter("az", "k", stream.URL+"/openai/", "2025-01-01-preview")
	ch, err := sa.StreamChatCompletions(ctx, userReq("dep1", "hi"))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range collectChunks(t, ch) {
		if c.Err != nil {
			t.Fatal(c.Err)
		}
	}

	// Deployment fixed in the base URL; api-version from its query.
	fixed := NewAzureAdapter("az", "k", srv.URL+"/openai/deployments/fixed-dep?api-version=2024-06-01", "")
	if _, err := fixed.ChatCompletions(ctx, userReq("ignored", "hi")); err != nil {
		t.Fatal(err)
	}
	if u := reqs()[len(reqs())-1].uri; u != "/openai/deployments/fixed-dep/chat/completions?api-version=2024-06-01" {
		t.Fatalf("fixed deployment: %s", u)
	}

	// v1 base URLs keep the v1 path; api_version is only added when set.
	v1 := NewAzureAdapter("az", "k", srv.URL+"/openai/v1", "")
	if v1.AzureDeploymentMode() {
		t.Fatal("v1 URLs must not use deployment mode")
	}
	if _, err := v1.ChatCompletions(ctx, userReq("dep", "hi")); err != nil {
		t.Fatal(err)
	}
	if u := reqs()[len(reqs())-1].uri; u != "/openai/v1/chat/completions" {
		t.Fatalf("v1: %s", u)
	}
	v1p := NewAzureAdapter("az", "k", srv.URL+"/openai/v1", "preview")
	if _, err := v1p.ChatCompletions(ctx, userReq("dep", "hi")); err != nil {
		t.Fatal(err)
	}
	if u := reqs()[len(reqs())-1].uri; u != "/openai/v1/chat/completions?api-version=preview" {
		t.Fatalf("v1 preview: %s", u)
	}
	// No api_version and no path: the v1 API, as before.
	legacy := NewAzureAdapter("az", "k", srv.URL, "")
	if legacy.AzureDeploymentMode() || legacy.BaseURL() != srv.URL+"/openai/v1" {
		t.Fatalf("default azure base: %s", legacy.BaseURL())
	}

	// Invalid deployment names are rejected before any request is sent.
	before := len(reqs())
	for _, bad := range []string{"", "../admin", "a/b", "dep?x=1", "dep name"} {
		if _, err := a.ChatCompletions(ctx, userReq(bad, "hi")); providers.StatusCode(err) != http.StatusBadRequest {
			t.Fatalf("deployment %q: %v", bad, err)
		}
	}
	if len(reqs()) != before {
		t.Fatal("invalid deployments must not reach the upstream")
	}
	if b := NewAzureAdapter("az", "k", srv.URL+"/openai/deployments/dep", ""); b.baseErr == nil {
		t.Fatal("deployment base URL without api_version must be a config error")
	}
}

func TestAzureRegistryConfig(t *testing.T) {
	srv, reqs := recorder(t)
	reg := NewProviderRegistry()
	err := reg.RegisterFromConfig([]config.ProviderConfig{{
		Name: "azure-prod", Type: "azure", BaseURL: srv.URL, APIKey: "azkey", APIVersion: "2024-10-21",
		Models: []string{"gpt-4o=prod-gpt4o-deployment"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	ad, err := reg.ResolveAdapter("gpt-4o")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ad.ChatCompletions(context.Background(), userReq("gpt-4o", "hi")); err != nil {
		t.Fatal(err)
	}
	if u := reqs()[0].uri; u != "/openai/deployments/prod-gpt4o-deployment/chat/completions?api-version=2024-10-21" {
		t.Fatalf("alias must select the deployment: %s", u)
	}
	if err := reg.RegisterFromConfig([]config.ProviderConfig{{Name: "az", Type: "azure"}}); err == nil {
		t.Fatal("azure without base_url must fail")
	}
	if err := reg.RegisterFromConfig([]config.ProviderConfig{{Name: "az", Type: "azure", BaseURL: srv.URL + "/openai/deployments/x"}}); err == nil {
		t.Fatal("deployment URL without api_version must fail")
	}
}

func TestStreamOptionsRejectedIsRetriedWithout(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var bodies []map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var b map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		if _, ok := b["stream_options"]; ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"message":"invalid request: unknown field stream_options"}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()
	a := NewCohereAdapter("k", srv.URL)
	a.SetIncludeStreamUsage(true) // operator opts in; upstream refuses
	ch, err := a.StreamChatCompletions(context.Background(), userReq("command-a", "hi"))
	if err != nil {
		t.Fatal(err)
	}
	collectChunks(t, ch)
	if calls.Load() != 2 || a.IncludeStreamUsage() {
		t.Fatalf("expected one retry without stream_options and the option turned off (calls=%d)", calls.Load())
	}
	// Later streams go straight through without stream_options.
	ch, err = a.StreamChatCompletions(context.Background(), userReq("command-a", "hi"))
	if err != nil {
		t.Fatal(err)
	}
	collectChunks(t, ch)
	if calls.Load() != 3 {
		t.Fatalf("calls=%d", calls.Load())
	}
	// Other 400s are not retried.
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"error":{"message":"messages is required"}}`)
	}))
	defer other.Close()
	calls.Store(0)
	g := NewGeminiAdapter("k", other.URL)
	if _, err := g.StreamChatCompletions(context.Background(), userReq("gemini", "hi")); providers.StatusCode(err) != 400 || calls.Load() != 1 || !g.IncludeStreamUsage() {
		t.Fatalf("unrelated 400 must not be retried: %v calls=%d", err, calls.Load())
	}
}

func TestAdapterProbes(t *testing.T) {
	var status atomic.Int32
	status.Store(200)
	var lastPath, lastKey, lastVersion atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPath.Store(r.Method + " " + r.URL.Path)
		lastKey.Store(r.Header.Get("Authorization") + r.Header.Get("x-api-key"))
		lastVersion.Store(r.Header.Get("anthropic-version"))
		w.WriteHeader(int(status.Load()))
		io.WriteString(w, `{"error":{"message":"bad key sk-secret-value-123"}}`)
	}))
	defer srv.Close()
	ctx := context.Background()

	o := NewOpenAICompatibleAdapter("o", "openai", "sk-secret-value-123", srv.URL)
	if o.Health()["healthy"] != true {
		t.Fatal("healthy until proven otherwise")
	}
	if err := o.Probe(ctx); err != nil || lastPath.Load() != "GET /v1/models" || lastKey.Load() != "Bearer sk-secret-value-123" {
		t.Fatalf("openai probe: %v %v %v", err, lastPath.Load(), lastKey.Load())
	}
	if h := o.Health(); h["healthy"] != true || h["last_probe"] == nil {
		t.Fatalf("health after good probe: %v", h)
	}
	for _, code := range []int{401, 403, 500, 503} {
		status.Store(int32(code))
		err := o.Probe(ctx)
		if providers.StatusCode(err) != code {
			t.Fatalf("status %d: %v", code, err)
		}
		h := o.Health()
		if h["healthy"] != false || !strings.Contains(h["probe_error"].(string), "status") {
			t.Fatalf("status %d must mark unhealthy: %v", code, h)
		}
		if strings.Contains(h["probe_error"].(string), "sk-secret-value-123") {
			t.Fatal("probe error must not echo credentials")
		}
	}
	// 404/405/429 prove the upstream is up.
	for _, code := range []int{404, 405, 429} {
		status.Store(int32(code))
		if err := o.Probe(ctx); err != nil || o.Health()["healthy"] != true {
			t.Fatalf("status %d should count as alive: %v", code, err)
		}
	}
	// A failed probe is cleared by a later successful call.
	status.Store(500)
	_ = o.Probe(ctx)
	status.Store(200)
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"x","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer ok.Close()
	o2 := NewOpenAICompatibleAdapter("o2", "openai", "k", ok.URL)
	o2.health.ObserveProbe(0, errors.New("down"))
	if o2.Health()["healthy"] != false {
		t.Fatal("failed probe must mark unhealthy")
	}
	if _, err := o2.ChatCompletions(ctx, userReq("m", "hi")); err != nil {
		t.Fatal(err)
	}
	if o2.Health()["healthy"] != true {
		t.Fatal("a successful call must clear a failed probe")
	}

	// Anthropic: GET /v1/models with x-api-key and anthropic-version.
	status.Store(200)
	an := NewAnthropicAdapterV2("ak-123456789", srv.URL+"/v1")
	if err := an.Probe(ctx); err != nil || lastPath.Load() != "GET /v1/models" || lastKey.Load() != "ak-123456789" || lastVersion.Load() != providers.AnthropicVersion {
		t.Fatalf("anthropic probe: %v %v %v", err, lastPath.Load(), lastKey.Load())
	}
	status.Store(401)
	if err := an.Probe(ctx); providers.StatusCode(err) != 401 || an.Health()["healthy"] != false {
		t.Fatalf("anthropic bad key: %v", err)
	}

	// Transport failure.
	dead := NewOpenAICompatibleAdapter("dead", "openai", "k", "http://127.0.0.1:1")
	var te *providers.TransportError
	if err := dead.Probe(ctx); !errors.As(err, &te) || dead.Health()["healthy"] != false {
		t.Fatalf("unreachable upstream: %v", err)
	}
}

func TestRegistryProbeAll(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"data":[]}`) }))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer bad.Close()
	reg := NewProviderRegistry()
	_ = reg.Register(NewOpenAICompatibleAdapter("good", "openai", "k", good.URL), "a")
	_ = reg.Register(NewOpenAICompatibleAdapter("bad", "openai", "k", bad.URL), "b")
	_ = reg.Register(&fakeAdapter{name: "noprobe"}, "c")
	_ = reg.Register(NewLegacyProviderAdapter(fakeLegacy{}), "d")
	_ = reg.Register(NewLegacyProviderAdapter(providers.NewOpenAIProvider("legacy-openai", "k", good.URL)), "e")
	res := reg.ProbeAll(context.Background())
	if len(res) != 3 {
		t.Fatalf("only probe-capable adapters are reported: %v", res)
	}
	if res["good"] != nil || providers.StatusCode(res["bad"]) != 503 || res["legacy-openai"] != nil {
		t.Fatalf("results: %v", res)
	}
	if a, _ := reg.Get("bad"); a.Health()["healthy"] != false {
		t.Fatal("probe result must be reflected in Health")
	}
	if a, _ := reg.Get("legacy-openai"); a.Health()["last_probe"] == nil {
		t.Fatal("legacy adapter health must expose the probe")
	}
	// Alias-rewriting adapters forward probes.
	_ = reg.Register(NewOpenAICompatibleAdapter("aliased", "openai", "k", good.URL), "fast=real-model")
	ad, err := reg.ResolveAdapter("fast")
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := ad.(providers.Prober); !ok || p.Probe(context.Background()) != nil {
		t.Fatal("rewrite adapter must forward Probe")
	}
}
