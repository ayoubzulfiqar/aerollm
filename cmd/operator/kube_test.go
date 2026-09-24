package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/api/v1alpha1"
	"github.com/ayoubzulfiqar/aerollm/internal/k8s"
	"github.com/ayoubzulfiqar/aerollm/internal/k8s/k8stest"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
)

const (
	testAdminKey = "adm-super-secret-key"
	testKubeTok  = "kube-bearer-token-xyz"
)

var (
	routeGVR    = k8stest.GVR{Group: v1alpha1.GroupName, Version: v1alpha1.Version, Resource: v1alpha1.PluralAeroRoute}
	budgetGVR   = k8stest.GVR{Group: v1alpha1.GroupName, Version: v1alpha1.Version, Resource: v1alpha1.PluralAeroBudget}
	pipelineGVR = k8stest.GVR{Group: v1alpha1.GroupName, Version: v1alpha1.Version, Resource: v1alpha1.PluralAeroAgentPipeline}
)

// withEnv replaces getenv for the duration of a test.
func withEnv(t *testing.T, env map[string]string) {
	t.Helper()
	old := getenv
	getenv = func(k string) string { return env[k] }
	t.Cleanup(func() { getenv = old })
}

// gwRequest is a request recorded by the fake gateway.
type gwRequest struct {
	Method, Path, Query, Auth string
	Body                      map[string]interface{}
}

// fakeGateway records admin calls and can inject failures.
type fakeGateway struct {
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []gwRequest
	fail map[string][]int // method+path -> status codes to return first
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
	g := &fakeGateway{fail: map[string][]int{}}
	g.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var body map[string]interface{}
		_ = json.Unmarshal(b, &body)
		g.mu.Lock()
		g.reqs = append(g.reqs, gwRequest{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Auth: r.Header.Get("Authorization"), Body: body})
		k := r.Method + " " + r.URL.Path
		var code int
		if codes := g.fail[k]; len(codes) > 0 {
			code, g.fail[k] = codes[0], codes[1:]
		}
		g.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer "+testAdminKey {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":{"message":"invalid key"}}`)
			return
		}
		if code != 0 {
			w.WriteHeader(code)
			fmt.Fprintf(w, `{"error":"injected %d"}`, code)
			return
		}
		if r.URL.Path == "/config/update" && strings.Contains(string(b), `"weighted"`) {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":{"message":"router.strategy \"weighted\" unknown"}}`)
			return
		}
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	t.Cleanup(g.srv.Close)
	return g
}

func (g *fakeGateway) failNext(method, path string, codes ...int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fail[method+" "+path] = append(g.fail[method+" "+path], codes...)
}

func (g *fakeGateway) requests() []gwRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]gwRequest(nil), g.reqs...)
}

func (g *fakeGateway) caFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gw-ca.pem")
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: g.srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func (g *fakeGateway) find(pred func(gwRequest) bool) (gwRequest, bool) {
	for _, r := range g.requests() {
		if pred(r) {
			return r, true
		}
	}
	return gwRequest{}, false
}

func writeKubeconfig(t *testing.T, srv *k8stest.Server) string {
	t.Helper()
	kc := fmt.Sprintf(`apiVersion: v1
kind: Config
current-context: test
clusters:
- name: fake
  cluster:
    server: %s
    certificate-authority-data: %s
contexts:
- name: test
  context:
    cluster: fake
    user: op
users:
- name: op
  user:
    token: %s
`, srv.URL, base64.StdEncoding.EncodeToString(srv.CAPEM()), testKubeTok)
	p := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(p, []byte(kc), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func obj(kind, ns, name string, spec map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": v1alpha1.APIVersion, "kind": kind,
		"metadata": map[string]interface{}{"namespace": ns, "name": name},
		"spec":     spec,
	}
}

func statusOf(t *testing.T, srv *k8stest.Server, g k8stest.GVR, ns, name string) (int64, map[string]v1alpha1.Condition) {
	t.Helper()
	o, ok := srv.Object(g, ns, name)
	if !ok {
		return 0, nil
	}
	conds := map[string]v1alpha1.Condition{}
	for _, c := range currentConditions(o) {
		conds[c.Type] = c
	}
	return observedGeneration(o), conds
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readyReason(t *testing.T, srv *k8stest.Server, g k8stest.GVR, ns, name string, gen int64) string {
	t.Helper()
	og, conds := statusOf(t, srv, g, ns, name)
	if og != gen {
		return ""
	}
	return conds[v1alpha1.ConditionReady].Reason
}

func TestKubeModeEndToEnd(t *testing.T) {
	oldRetry := retryBaseDelay
	retryBaseDelay = 20 * time.Millisecond
	t.Cleanup(func() { retryBaseDelay = oldRetry })

	api := k8stest.NewTLSServer()
	defer api.Close()
	api.SetToken(testKubeTok)
	gw := newFakeGateway(t)
	withEnv(t, map[string]string{"AEROLLM_ADMIN_KEY": testAdminKey})

	api.Create(routeGVR, obj("AeroRoute", "team-a", "primary", map[string]interface{}{
		"strategy": "cost", "providers": []interface{}{"openai"},
		"breaker_config": map[string]interface{}{"max_failures": 5, "reset_timeout": "30s", "window": 3},
	}))
	api.Create(routeGVR, obj("AeroRoute", "team-b", "secondary", map[string]interface{}{"strategy": "latency"}))
	api.Create(budgetGVR, obj("AeroBudget", "team-a", "global", map[string]interface{}{"max_usd": 100}))
	api.Create(budgetGVR, obj("AeroBudget", "team-a", "inline", map[string]interface{}{"api_key": "sk-inline-secret", "max_usd": 5, "alert_webhook": "https://hooks.example.com/x"}))
	api.SetSecret("team-a", "llm-keys", map[string][]byte{"api-key": []byte("sk-from-secret\n")})
	api.Create(budgetGVR, obj("AeroBudget", "team-a", "from-secret", map[string]interface{}{"api_key_secret_ref": "llm-keys/api-key", "monthly_cap": 50}))
	api.Create(budgetGVR, obj("AeroBudget", "team-a", "missing-secret", map[string]interface{}{"api_key_secret_ref": "nope/api-key", "max_usd": 1}))
	api.Create(budgetGVR, obj("AeroBudget", "team-a", "broken", map[string]interface{}{"max_usd": -1}))
	api.Create(pipelineGVR, obj("AeroAgentPipeline", "team-a", "flow", map[string]interface{}{"nodes": []interface{}{"a", "b"}, "edges": []interface{}{"a->b"}}))

	// The first config update fails transiently: it must be retried.
	gw.failNext(http.MethodPost, "/config/update", http.StatusServiceUnavailable)

	var logs syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	args := []string{"--kube", "--kubeconfig", writeKubeconfig(t, api),
		"--gateway-url", gw.srv.URL, "--gateway-ca-file", gw.caFile(t), "--resync", "0"}
	go func() { done <- run(ctx, args, &logs, &logs) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("operator did not stop after cancel")
		}
	}()

	waitUntil(t, "route applied", func() bool { return readyReason(t, api, routeGVR, "team-a", "primary", 1) == reasonApplied })
	waitUntil(t, "global budget applied", func() bool { return readyReason(t, api, budgetGVR, "team-a", "global", 1) == reasonApplied })
	waitUntil(t, "inline budget applied", func() bool { return readyReason(t, api, budgetGVR, "team-a", "inline", 1) == reasonApplied })
	waitUntil(t, "secret budget applied", func() bool { return readyReason(t, api, budgetGVR, "team-a", "from-secret", 1) == reasonApplied })
	waitUntil(t, "route conflict", func() bool { return readyReason(t, api, routeGVR, "team-b", "secondary", 1) == reasonConflict })
	waitUntil(t, "missing secret", func() bool {
		return readyReason(t, api, budgetGVR, "team-a", "missing-secret", 1) == reasonSecretUnavailable
	})
	waitUntil(t, "invalid budget", func() bool { return readyReason(t, api, budgetGVR, "team-a", "broken", 1) == reasonInvalidSpec })
	waitUntil(t, "pipeline unsupported", func() bool { return readyReason(t, api, pipelineGVR, "team-a", "flow", 1) == reasonUnsupported })

	// Route: translated into router strategy + circuit breaker.
	routeReq, ok := gw.find(func(r gwRequest) bool {
		router, _ := r.Body["router"].(map[string]interface{})
		return r.Path == "/config/update" && router["strategy"] == "cost"
	})
	if !ok {
		t.Fatalf("no router update sent: %+v", gw.requests())
	}
	cb, _ := routeReq.Body["router"].(map[string]interface{})["circuit_break"].(map[string]interface{})
	if cb["max_failures"] != float64(5) || cb["reset_timeout"] != float64(30*time.Second) {
		t.Fatalf("unexpected circuit_break: %v", cb)
	}
	if routeReq.Auth != "Bearer "+testAdminKey || routeReq.Method != http.MethodPost {
		t.Fatalf("bad gateway request: %+v", routeReq)
	}
	_, conds := statusOf(t, api, routeGVR, "team-a", "primary")
	if c := conds[v1alpha1.ConditionValid]; c.Status != v1alpha1.ConditionTrue || c.LastTransitionTime == "" || c.ObservedGeneration != 1 {
		t.Fatalf("bad Valid condition: %+v", c)
	}
	if msg := conds[v1alpha1.ConditionReady].Message; !strings.Contains(msg, "providers") || !strings.Contains(msg, "breaker_config.window") {
		t.Fatalf("ignored fields not reported: %q", msg)
	}

	// Budgets: global via /config/update, per-key via PUT /v1/budgets with
	// the key's non-secret ID only.
	if _, ok := gw.find(func(r gwRequest) bool {
		fin, _ := r.Body["finops"].(map[string]interface{})
		return r.Path == "/config/update" && fin["default_max_usd"] == float64(100) && fin["enabled"] == true
	}); !ok {
		t.Fatalf("no finops update: %+v", gw.requests())
	}
	inlineID, secretID := middleware.KeyID("sk-inline-secret"), middleware.KeyID("sk-from-secret")
	if r, ok := gw.find(func(r gwRequest) bool { return r.Path == "/v1/budgets" && r.Body["key_id"] == inlineID }); !ok || r.Method != http.MethodPut || r.Body["max_usd"] != float64(5) {
		t.Fatalf("inline budget request: %+v %v", r, ok)
	}
	if r, ok := gw.find(func(r gwRequest) bool { return r.Path == "/v1/budgets" && r.Body["key_id"] == secretID }); !ok || r.Body["max_usd"] != float64(50) || r.Body["period"] != "monthly" {
		t.Fatalf("secret budget request: %+v %v", r, ok)
	}
	for _, r := range gw.requests() {
		raw, _ := json.Marshal(r.Body)
		if strings.Contains(string(raw), "sk-inline-secret") || strings.Contains(string(raw), "sk-from-secret") {
			t.Fatalf("raw API key sent to the gateway: %s", raw)
		}
	}

	_, conds = statusOf(t, api, budgetGVR, "team-a", "broken")
	if c := conds[v1alpha1.ConditionValid]; c.Status != v1alpha1.ConditionFalse || !strings.Contains(c.Message, "max_usd") {
		t.Fatalf("bad Valid condition for invalid budget: %+v", c)
	}
	_, conds = statusOf(t, api, pipelineGVR, "team-a", "flow")
	if conds[v1alpha1.ConditionValid].Status != v1alpha1.ConditionTrue || conds[v1alpha1.ConditionReady].Status != v1alpha1.ConditionFalse {
		t.Fatalf("bad pipeline conditions: %+v", conds)
	}

	// No patch -> watch event -> patch loop once things settle.
	time.Sleep(200 * time.Millisecond)
	countPatches := func(name string) int {
		n := 0
		for _, p := range api.Patches() {
			if p.Name == name {
				n++
			}
		}
		return n
	}
	before := countPatches("primary")
	time.Sleep(300 * time.Millisecond)
	if after := countPatches("primary"); after != before || before > 2 {
		t.Fatalf("status patch loop: %d -> %d patches", before, after)
	}

	// The missing Secret appears: the transient retry picks it up.
	api.SetSecret("team-a", "nope", map[string][]byte{"api-key": []byte("sk-late")})
	waitUntil(t, "late secret applied", func() bool { return readyReason(t, api, budgetGVR, "team-a", "missing-secret", 1) == reasonApplied })

	// Spec change: new generation is applied and observed.
	api.UpdateSpec(routeGVR, "team-a", "primary", map[string]interface{}{"strategy": "fallback"})
	waitUntil(t, "generation 2 applied", func() bool { return readyReason(t, api, routeGVR, "team-a", "primary", 2) == reasonApplied })
	if _, ok := gw.find(func(r gwRequest) bool {
		router, _ := r.Body["router"].(map[string]interface{})
		return router["strategy"] == "fallback"
	}); !ok {
		t.Fatal("updated strategy not sent")
	}

	// Deleting the router owner hands the router to the other AeroRoute.
	api.Delete(routeGVR, "team-a", "primary")
	waitUntil(t, "conflict resolved", func() bool { return readyReason(t, api, routeGVR, "team-b", "secondary", 1) == reasonApplied })

	// Deleting a per-key budget removes it from the gateway.
	api.Delete(budgetGVR, "team-a", "inline")
	waitUntil(t, "budget deleted", func() bool {
		_, ok := gw.find(func(r gwRequest) bool { return r.Method == http.MethodDelete && r.Query == "key_id="+inlineID })
		return ok
	})

	// A spec the gateway rejects is reported, not retried in a loop.
	api.UpdateSpec(routeGVR, "team-b", "secondary", map[string]interface{}{"strategy": "weighted", "providers": []interface{}{"a"}, "weights": map[string]interface{}{"a": 1}})
	waitUntil(t, "gateway rejection", func() bool { return readyReason(t, api, routeGVR, "team-b", "secondary", 2) == reasonGatewayRejected })
	_, conds = statusOf(t, api, routeGVR, "team-b", "secondary")
	if !strings.Contains(conds[v1alpha1.ConditionReady].Message, "weighted") {
		t.Fatalf("gateway message not surfaced: %+v", conds[v1alpha1.ConditionReady])
	}

	// Kubernetes requests carried the bearer token; secrets never logged.
	for _, r := range api.Requests() {
		if r.Auth != "Bearer "+testKubeTok {
			t.Fatalf("request without token: %s %s", r.Method, r.Path)
		}
	}
	out := logs.String()
	for _, secret := range []string{testAdminKey, testKubeTok, "sk-inline-secret", "sk-from-secret", "sk-late"} {
		if strings.Contains(out, secret) {
			t.Fatalf("secret %q leaked into logs", secret)
		}
	}
	if !strings.Contains(out, `"mode":"kube"`) {
		t.Fatalf("missing startup log:\n%s", out)
	}
}

func TestKubeModeInClusterConfig(t *testing.T) {
	api := k8stest.NewTLSServer()
	defer api.Close()
	api.SetToken("sa-token")
	gw := newFakeGateway(t)
	api.Create(routeGVR, obj("AeroRoute", "default", "r", map[string]interface{}{"strategy": "cost"}))

	dir := t.TempDir()
	for name, data := range map[string][]byte{"token": []byte("sa-token"), "ca.crt": api.CAPEM(), "namespace": []byte("default")} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	oldDir := serviceAccountDir
	serviceAccountDir = dir
	t.Cleanup(func() { serviceAccountDir = oldDir })
	host, port, _ := strings.Cut(strings.TrimPrefix(api.URL, "https://"), ":")
	withEnv(t, map[string]string{
		"KUBERNETES_SERVICE_HOST": host, "KUBERNETES_SERVICE_PORT": port,
		"AEROLLM_GATEWAY_URL": gw.srv.URL, "GW_KEY": testAdminKey,
	})

	var logs syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	// No mode flag: inside a "pod" the operator defaults to kube mode.
	args := []string{"--admin-key-env", "GW_KEY", "--gateway-ca-file", gw.caFile(t), "--namespace", "default"}
	go func() { done <- run(ctx, args, &logs, &logs) }()
	waitUntil(t, "applied in-cluster", func() bool { return readyReason(t, api, routeGVR, "default", "r", 1) == reasonApplied })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, r := range api.Requests() {
		if !strings.Contains(r.Path, "/namespaces/default/") {
			t.Fatalf("expected namespaced requests, got %s", r.Path)
		}
	}
}

func TestKubeModeStartupErrors(t *testing.T) {
	api := k8stest.NewTLSServer()
	defer api.Close()
	kc := writeKubeconfig(t, api)
	withEnv(t, map[string]string{})
	var out syncBuffer
	err := run(context.Background(), []string{"--kube", "--kubeconfig", kc, "--gateway-url", "https://gw.example.com"}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "AEROLLM_ADMIN_KEY") {
		t.Fatalf("expected missing admin key error, got %v", err)
	}
	err = run(context.Background(), []string{"--kube", "--kubeconfig", filepath.Join(t.TempDir(), "missing"), "--gateway-url", "https://gw.example.com"}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "kubernetes config") {
		t.Fatalf("expected kubeconfig error, got %v", err)
	}
}

func TestManifestModeAppliesToGateway(t *testing.T) {
	gw := newFakeGateway(t)
	withEnv(t, map[string]string{"AEROLLM_ADMIN_KEY": testAdminKey})
	var out syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	args := []string{"--config", writeConfig(t, manifests), "--gateway-url", gw.srv.URL, "--gateway-ca-file", gw.caFile(t)}
	go func() { done <- run(ctx, args, &out, &out) }()
	waitUntil(t, "manifests reconciled", func() bool { return strings.Count(out.String(), `"msg":"reconciled"`) >= 2 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, ok := gw.find(func(r gwRequest) bool { return r.Path == "/config/update" }); !ok {
		t.Fatalf("no gateway call: %+v", gw.requests())
	}
	if _, ok := gw.find(func(r gwRequest) bool {
		return r.Path == "/v1/budgets" && r.Body["key_id"] == middleware.KeyID("sk-supersecret")
	}); !ok {
		t.Fatalf("no budget call: %+v", gw.requests())
	}
	logs := out.String()
	if strings.Contains(logs, "sk-supersecret") || strings.Contains(logs, testAdminKey) {
		t.Fatalf("secret leaked:\n%s", logs)
	}
	if !strings.Contains(logs, "applied router.strategy=cost") {
		t.Fatalf("expected applied status:\n%s", logs)
	}
}

func TestGatewayKeyIDMatchesMiddleware(t *testing.T) {
	for _, k := range []string{"sk-a", "sk-aero-admin-xyz", ""} {
		if gatewayKeyID(k) != middleware.KeyID(k) {
			t.Fatalf("key id mismatch for %q", k)
		}
	}
}

func TestPlanRoute(t *testing.T) {
	p, err := planRoute(v1alpha1.AeroRouteSpec{Strategy: "cost", Models: []string{"m"}, Fallback: []string{"x"}})
	if err != nil || p.target != "router" || p.body["router"].(map[string]interface{})["strategy"] != "cost" {
		t.Fatalf("%+v %v", p, err)
	}
	if !strings.Contains(p.note(), "models") || !strings.Contains(p.note(), "fallback") {
		t.Fatalf("note: %s", p.note())
	}
	p, err = planRoute(v1alpha1.AeroRouteSpec{BreakerConfig: map[string]interface{}{"reset_timeout": float64(2), "half_open_max_calls": float64(3)}})
	if err != nil {
		t.Fatal(err)
	}
	cb := p.body["router"].(map[string]interface{})["circuit_break"].(map[string]interface{})
	if cb["reset_timeout"] != int64(2*time.Second) || cb["half_open_max_calls"] != int64(3) {
		t.Fatalf("cb: %v", cb)
	}
	if _, err := planRoute(v1alpha1.AeroRouteSpec{BreakerConfig: map[string]interface{}{"max_failures": 1.5}}); err == nil {
		t.Fatal("expected non-integer error")
	}
	if _, err := planRoute(v1alpha1.AeroRouteSpec{BreakerConfig: map[string]interface{}{"reset_timeout": "soon"}}); err == nil {
		t.Fatal("expected bad duration error")
	}
	p, err = planRoute(v1alpha1.AeroRouteSpec{Providers: []string{"openai"}})
	if err != nil || p.unsupported == "" {
		t.Fatalf("expected nothing-to-apply: %+v %v", p, err)
	}
}

func TestPlanBudget(t *testing.T) {
	p := planBudget(v1alpha1.AeroBudgetSpec{MaxUSD: 10, MonthlyCap: 20}, "")
	if p.target != "finops" || !strings.Contains(p.note(), "monthly_cap") {
		t.Fatalf("%+v", p)
	}
	p = planBudget(v1alpha1.AeroBudgetSpec{MonthlyCap: 20}, "sk-x")
	if p.method != http.MethodPut || p.body["period"] != "monthly" || p.body["max_usd"] != float64(20) || p.keyID != gatewayKeyID("sk-x") {
		t.Fatalf("%+v", p)
	}
	if p := planBudget(v1alpha1.AeroBudgetSpec{}, "sk-x"); p.unsupported == "" {
		t.Fatal("expected nothing to apply")
	}
	if _, err := planManifest(k8s.KindAeroBudget, map[string]interface{}{"api_key_secret_ref": "a/b", "max_usd": 1.0}); err == nil {
		t.Fatal("secret refs need kube mode")
	}
}

func TestGatewayClientErrors(t *testing.T) {
	var redirected bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/unavailable":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/bad":
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "{\"error\":{\"message\":\"nope\\u0000\\n\"}}")
		case "/auth":
			w.WriteHeader(http.StatusForbidden)
		case "/redirect":
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
		case "/elsewhere":
			redirected = true
		}
	}))
	defer srv.Close()
	if _, err := newGatewayClient(srv.URL, "k", false, "", time.Second); err == nil {
		t.Fatal("plain http must require opt-in")
	}
	if _, err := newGatewayClient("https://u:p@gw", "k", false, "", time.Second); err == nil {
		t.Fatal("credentials in URL must be refused")
	}
	if _, err := newGatewayClient("https://gw", "", false, "", time.Second); err == nil {
		t.Fatal("empty admin key must be refused")
	}
	g, err := newGatewayClient(srv.URL, "k", true, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	check := func(path string, transient bool, want string) {
		t.Helper()
		err := g.send(ctx, http.MethodPost, path, nil, map[string]int{"a": 1})
		ge, ok := err.(*gatewayError)
		if !ok || ge.Transient != transient || !strings.Contains(ge.Error(), want) {
			t.Fatalf("%s: %v", path, err)
		}
	}
	check("/unavailable", true, "503")
	check("/bad", false, "nope")
	check("/auth", false, "admin key rejected")
	check("/redirect", false, "302")
	if redirected {
		t.Fatal("redirect was followed")
	}
	g.base = "http://127.0.0.1:1"
	check("/x", true, "unreachable")
}

func TestConditionsKeepTransitionTime(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	first := buildConditions(nil, 1, t0, outcome{valid: true, ready: true, reason: reasonApplied, msg: "applied"})
	obj := map[string]interface{}{}
	b, _ := json.Marshal(map[string]interface{}{"status": map[string]interface{}{"observedGeneration": 1, "conditions": first}})
	_ = json.Unmarshal(b, &obj)
	if needsStatusPatch(obj, 1, first) {
		t.Fatal("identical status should not need a patch")
	}
	second := buildConditions(currentConditions(obj), 2, t0.Add(time.Hour), outcome{valid: true, reason: reasonGatewayUnavailable, msg: "down"})
	if second[0].LastTransitionTime != v1alpha1.FormatTime(t0) {
		t.Fatalf("Valid transition time changed: %+v", second[0])
	}
	if second[1].LastTransitionTime != v1alpha1.FormatTime(t0.Add(time.Hour)) {
		t.Fatalf("Ready transition time not updated: %+v", second[1])
	}
	if !needsStatusPatch(obj, 2, second) {
		t.Fatal("changed status should need a patch")
	}
}

func TestParseFlagsOperatorConfig(t *testing.T) {
	withEnv(t, map[string]string{})
	dir := t.TempDir()
	cfg := filepath.Join(dir, "operator.yaml")
	body := `kube: true
kubeconfig: /tmp/kc
context: prod
namespace: aerollm
kinds: [AeroRoute, AeroBudget]
resync_interval: 2m
watch_timeout: 90s
request_timeout: 7s
gateway_url: http://aerollm.aerollm.svc:8080
gateway_allow_http: true
admin_key_env: MY_ADMIN_KEY
`
	if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	var out syncBuffer
	o, err := parseFlags([]string{"--operator-config", cfg, "--namespace", "override", "--resync", "0"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !o.kube || o.kubeconfig != "/tmp/kc" || o.kubeContext != "prod" || o.namespace != "override" ||
		len(o.kinds) != 2 || o.resync != 0 || o.watchTimeout != 90*time.Second || o.requestTimeout != 7*time.Second ||
		o.gatewayURL != "http://aerollm.aerollm.svc:8080" || !o.gatewayAllowHTTP || o.adminKeyEnv != "MY_ADMIN_KEY" {
		t.Fatalf("unexpected options: %+v", o)
	}

	bad := map[string]string{
		"unknown key":  "kube: true\ngateway: x\n",
		"bad duration": "resync_interval: 5\n",
		"bad kind":     "kinds: [Pod]\n",
		"not yaml":     "kube: [\n",
	}
	for name, b := range bad {
		p := filepath.Join(dir, strings.ReplaceAll(name, " ", "_")+".yaml")
		if err := os.WriteFile(p, []byte(b), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := parseFlags([]string{"--operator-config", p, "--gateway-url", "https://gw"}, &out); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := parseFlags([]string{"--operator-config", filepath.Join(dir, "missing.yaml")}, &out); err == nil {
		t.Error("expected missing file error")
	}
}

func TestParseFlagsModes(t *testing.T) {
	var out syncBuffer
	withEnv(t, map[string]string{"KUBERNETES_SERVICE_HOST": "10.0.0.1"})
	if _, err := parseFlags(nil, &out); err == nil || !strings.Contains(err.Error(), "gateway-url") {
		t.Fatalf("in-cluster default should select kube mode and need a gateway: %v", err)
	}
	withEnv(t, map[string]string{"KUBERNETES_SERVICE_HOST": "10.0.0.1", "AEROLLM_GATEWAY_URL": "https://gw.local"})
	o, err := parseFlags(nil, &out)
	if err != nil || !o.kube || o.gatewayURL != "https://gw.local" {
		t.Fatalf("expected kube mode: %+v %v", o, err)
	}
	o, err = parseFlags([]string{"--config", "m.json"}, &out)
	if err != nil || o.kube {
		t.Fatalf("explicit --config must stay in manifest mode: %+v %v", o, err)
	}
	cases := [][]string{
		{"--kube", "--config", "x.json", "--gateway-url", "https://gw"},
		{"--kube", "--once", "--gateway-url", "https://gw"},
		{"--kube", "--gateway-url", "http://gw"},
		{"--kube", "--gateway-url", "https://gw", "--kinds", "Nope"},
		{"--kube", "--gateway-url", "https://gw", "--watch-timeout", "0s"},
		{"--kube", "--gateway-url", "https://gw", "--admin-key-env", ""},
	}
	for _, args := range cases {
		if _, err := parseFlags(args, &out); err == nil {
			t.Errorf("parseFlags(%v) expected error", args)
		}
	}
}
