package k8s

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/k8s/k8stest"
)

var routeGVR = k8stest.GVR{Group: AeroRouteResource.Group, Version: AeroRouteResource.Version, Resource: AeroRouteResource.Resource}

func newFake(t *testing.T) *k8stest.Server {
	t.Helper()
	srv := k8stest.NewTLSServer()
	t.Cleanup(srv.Close)
	return srv
}

func testClient(t *testing.T, srv *k8stest.Server, token string) *Client {
	t.Helper()
	srv.SetToken(token)
	c, err := NewClient(&RESTConfig{Host: srv.URL, CAData: srv.CAPEM(), BearerToken: token, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func route(ns, name, strategy string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "aerollm.io/v1alpha1", "kind": "AeroRoute",
		"metadata": map[string]interface{}{"namespace": ns, "name": name},
		"spec":     map[string]interface{}{"strategy": strategy},
	}
}

func TestGVRPaths(t *testing.T) {
	if got := AeroRouteResource.CollectionPath(""); got != "/apis/aerollm.io/v1alpha1/aeroroutes" {
		t.Fatal(got)
	}
	if got := AeroBudgetResource.ObjectPath("ns1", "b1", "status"); got != "/apis/aerollm.io/v1alpha1/namespaces/ns1/aerobudgets/b1/status" {
		t.Fatal(got)
	}
	if got := secretsResource.ObjectPath("ns", "s", ""); got != "/api/v1/namespaces/ns/secrets/s" {
		t.Fatal(got)
	}
	for _, k := range []ResourceKind{KindAeroRoute, KindAeroBudget, KindAeroAgentPipeline} {
		if _, ok := ResourceForKind(k); !ok {
			t.Fatalf("no resource for %s", k)
		}
	}
}

func TestClientTLSVerificationRequiresCA(t *testing.T) {
	srv := newFake(t)
	c, err := NewClient(&RESTConfig{Host: srv.URL, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), AeroRouteResource, "", ListOptions{}); err == nil {
		t.Fatal("expected TLS verification failure without CA data")
	}
	if _, err := NewClient(&RESTConfig{Host: srv.URL, CAData: []byte("garbage")}); err == nil {
		t.Fatal("expected invalid CA error")
	}
}

func TestClientRejectsTokenOverPlainHTTP(t *testing.T) {
	if _, err := NewClient(&RESTConfig{Host: "http://10.0.0.1:8080", BearerToken: "x"}); err == nil {
		t.Fatal("expected refusal to send token over http")
	}
	if _, err := NewClient(&RESTConfig{Host: "http://127.0.0.1:8080", BearerToken: "x"}); err != nil {
		t.Fatalf("loopback http should be allowed: %v", err)
	}
	if _, err := NewClient(&RESTConfig{Host: "https://user:pw@h:1"}); err == nil {
		t.Fatal("expected refusal of embedded credentials")
	}
}

func TestListAllPaginatesAndSendsToken(t *testing.T) {
	srv := newFake(t)
	c := testClient(t, srv, "secret-token")
	for i := 0; i < 7; i++ {
		srv.Create(routeGVR, route("ns1", fmt.Sprintf("r%d", i), "cost"))
	}
	srv.Create(routeGVR, route("ns2", "other", "cost"))
	items, rv, err := c.ListAll(context.Background(), AeroRouteResource, "ns1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 7 || rv == "" {
		t.Fatalf("got %d items rv=%q", len(items), rv)
	}
	all, _, err := c.ListAll(context.Background(), AeroRouteResource, "", 0)
	if err != nil || len(all) != 8 {
		t.Fatalf("all namespaces: %d %v", len(all), err)
	}
	pages := 0
	for _, r := range srv.Requests() {
		if r.Auth != "Bearer secret-token" {
			t.Fatalf("missing bearer token on %s", r.Path)
		}
		if strings.Contains(r.Path, "/namespaces/ns1/") && strings.Contains(r.Query, "limit=3") {
			pages++
		}
	}
	if pages != 3 {
		t.Fatalf("expected 3 pages, got %d", pages)
	}
}

func TestListAllRestartsOnExpiredContinue(t *testing.T) {
	var calls int
	var mu sync.Mutex
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		switch {
		case n == 1:
			fmt.Fprint(w, `{"metadata":{"resourceVersion":"5","continue":"tok"},"items":[{"metadata":{"name":"a"}}]}`)
		case n == 2:
			w.WriteHeader(http.StatusGone)
			fmt.Fprint(w, `{"kind":"Status","code":410,"reason":"Expired","message":"continue expired"}`)
		default:
			fmt.Fprint(w, `{"metadata":{"resourceVersion":"9"},"items":[{"metadata":{"name":"a"}},{"metadata":{"name":"b"}}]}`)
		}
	}))
	defer srv.Close()
	c, err := NewClient(&RESTConfig{Host: srv.URL, CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})})
	if err != nil {
		t.Fatal(err)
	}
	items, rv, err := c.ListAll(context.Background(), AeroRouteResource, "", 1)
	if err != nil || len(items) != 2 || rv != "9" {
		t.Fatalf("items=%d rv=%s err=%v", len(items), rv, err)
	}
}

func TestStatusErrors(t *testing.T) {
	srv := newFake(t)
	c := testClient(t, srv, "")
	_, err := c.Get(context.Background(), AeroRouteResource, "ns", "missing")
	if !IsNotFound(err) || IsGone(err) {
		t.Fatalf("expected not found, got %v", err)
	}
	ev := WatchEvent{Type: EventError, Object: map[string]interface{}{"kind": "Status", "code": float64(410), "reason": "Expired", "message": "too old"}}
	if !IsGone(ev.StatusError()) {
		t.Fatal("expected gone")
	}
	if !IsConflict(&StatusError{Code: 409}) {
		t.Fatal("expected conflict")
	}
}

func TestPatchStatusAndSecret(t *testing.T) {
	srv := newFake(t)
	c := testClient(t, srv, "tok")
	srv.Create(routeGVR, route("ns", "r1", "cost"))
	patch := []byte(`{"status":{"observedGeneration":1,"conditions":[{"type":"Ready","status":"True"}]}}`)
	obj, err := c.PatchStatus(context.Background(), AeroRouteResource, "ns", "r1", patch)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := obj["status"].(map[string]interface{})
	if st["observedGeneration"] != float64(1) {
		t.Fatalf("status not patched: %v", obj)
	}
	ps := srv.Patches()
	if len(ps) != 1 || string(ps[0].Body) != string(patch) || ps[0].Name != "r1" {
		t.Fatalf("patches: %+v", ps)
	}
	if _, err := c.PatchStatus(context.Background(), AeroRouteResource, "ns", "gone", patch); !IsNotFound(err) {
		t.Fatalf("expected not found: %v", err)
	}

	srv.SetSecret("ns", "keys", map[string][]byte{"api-key": []byte("sk-live-123")})
	v, err := c.SecretValue(context.Background(), "ns", "keys", "api-key")
	if err != nil || string(v) != "sk-live-123" {
		t.Fatalf("secret: %q %v", v, err)
	}
	if _, err := c.SecretValue(context.Background(), "ns", "keys", "nope"); err == nil {
		t.Fatal("expected missing key error")
	}
}

// eventLog collects informer events.
type eventLog struct {
	mu  sync.Mutex
	evs []string
}

func (l *eventLog) handle(ev WatchEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	spec, _ := ev.Object["spec"].(map[string]interface{})
	l.evs = append(l.evs, fmt.Sprintf("%s %s %v", ev.Type, ObjectKey(ev.Object), spec["strategy"]))
}

func (l *eventLog) has(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.evs {
		if e == s {
			return true
		}
	}
	return false
}

func (l *eventLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.evs, "\n")
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func startInformer(t *testing.T, c *Client, log *eventLog) (*Informer, func()) {
	t.Helper()
	inf := &Informer{Client: c, Resource: AeroRouteResource, Handle: log.handle, Backoff: Backoff{Initial: time.Millisecond, Max: 20 * time.Millisecond}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- inf.Run(ctx) }()
	return inf, func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("informer returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("informer did not stop after cancel")
		}
	}
}

func watchQueries(srv *k8stest.Server) []url.Values {
	var out []url.Values
	for _, r := range srv.Requests() {
		q, _ := url.ParseQuery(r.Query)
		if q.Get("watch") == "true" {
			out = append(out, q)
		}
	}
	return out
}

func TestInformerListWatchResumeAndRelist(t *testing.T) {
	srv := newFake(t)
	c := testClient(t, srv, "tok")
	srv.Create(routeGVR, route("a", "r1", "cost"))
	srv.Create(routeGVR, route("b", "r2", "latency"))
	log := &eventLog{}
	inf, stop := startInformer(t, c, log)
	defer stop()

	waitFor(t, "initial adds", func() bool { return log.has("ADDED a/r1 cost") && log.has("ADDED b/r2 latency") })
	waitFor(t, "watch open", func() bool { return srv.WatchCount() == 1 })
	if !inf.HasSynced() {
		t.Fatal("expected synced")
	}
	q := watchQueries(srv)
	if len(q) != 1 || q[0].Get("allowWatchBookmarks") != "true" || q[0].Get("resourceVersion") == "" || q[0].Get("timeoutSeconds") == "" {
		t.Fatalf("unexpected watch query: %v", q)
	}

	// Live events.
	srv.UpdateSpec(routeGVR, "a", "r1", map[string]interface{}{"strategy": "fallback"})
	srv.Create(routeGVR, route("a", "r3", "cost"))
	srv.Delete(routeGVR, "b", "r2")
	waitFor(t, "live events", func() bool {
		return log.has("MODIFIED a/r1 fallback") && log.has("ADDED a/r3 cost") && log.has("DELETED b/r2 latency")
	})

	// Bookmark then server-side close: the informer must resume from the
	// bookmark's resourceVersion without relisting.
	srv.Bookmark(routeGVR)
	bookmarkRV := srv.ResourceVersion()
	waitFor(t, "bookmark processed", func() bool { return inf.LastResourceVersion() == bookmarkRV })
	srv.CloseWatches()
	waitFor(t, "resumed watch", func() bool {
		qs := watchQueries(srv)
		return len(qs) >= 2 && qs[len(qs)-1].Get("resourceVersion") == bookmarkRV
	})
	if inf.Lists() != 1 {
		t.Fatalf("unexpected relist: %d lists", inf.Lists())
	}

	// A change missed while disconnected plus a compacted history: the
	// resume gets 410 and the informer relists, synthesizing DELETED.
	waitFor(t, "watch open", func() bool { return srv.WatchCount() == 1 })
	srv.DeleteSilently(routeGVR, "a", "r3")
	srv.Compact()
	srv.CloseWatches()
	waitFor(t, "relist after 410", func() bool { return inf.Lists() == 2 && log.has("DELETED a/r3 cost") })

	// HTTP-level 410 on the watch request also triggers a relist; a 500
	// is retried with backoff from the same resourceVersion.
	waitFor(t, "watch open", func() bool { return srv.WatchCount() == 1 })
	srv.FailNextWatches(http.StatusInternalServerError, http.StatusGone)
	srv.CloseWatches()
	waitFor(t, "relist after HTTP 410", func() bool { return inf.Lists() == 3 })
	waitFor(t, "watch reopened", func() bool { return srv.WatchCount() == 1 })
	srv.Create(routeGVR, route("c", "r4", "cost"))
	waitFor(t, "event after recovery", func() bool { return log.has("ADDED c/r4 cost") })
}

func TestInformerStopsOnCancelDuringWatch(t *testing.T) {
	srv := newFake(t)
	c := testClient(t, srv, "")
	log := &eventLog{}
	_, stop := startInformer(t, c, log)
	waitFor(t, "watch open", func() bool { return srv.WatchCount() == 1 })
	stop()
}

func TestInformerRetriesListFailures(t *testing.T) {
	var mu sync.Mutex
	fails := 2
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Query().Get("watch") == "true" {
			<-r.Context().Done()
			return
		}
		if fails > 0 {
			fails--
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"metadata":{"resourceVersion":"3"},"items":[{"metadata":{"namespace":"x","name":"ok"},"spec":{"strategy":"cost"}}]}`)
	}))
	defer srv.Close()
	c, err := NewClient(&RESTConfig{Host: srv.URL, CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})})
	if err != nil {
		t.Fatal(err)
	}
	log := &eventLog{}
	_, stop := startInformer(t, c, log)
	defer stop()
	waitFor(t, "list after retries", func() bool { return log.has("ADDED x/ok cost") })
}

func TestBackoff(t *testing.T) {
	b := Backoff{Initial: 100 * time.Millisecond, Max: time.Second}
	prevMax := time.Duration(0)
	for i := 0; i < 10; i++ {
		d := b.Next()
		if d <= 0 || d > time.Second {
			t.Fatalf("delay %d out of range: %s", i, d)
		}
		if d > prevMax {
			prevMax = d
		}
	}
	if prevMax < 500*time.Millisecond {
		t.Fatalf("backoff never grew: %s", prevMax)
	}
	b.Reset()
	if d := b.Next(); d > 100*time.Millisecond || d < 50*time.Millisecond {
		t.Fatalf("reset delay %s", d)
	}
}

func TestBoundedReader(t *testing.T) {
	br := &boundedReader{r: strings.NewReader(strings.Repeat("x", 100)), max: 10}
	b, err := io.ReadAll(br)
	if !errors.Is(err, errEventTooLarge) || len(b) != 10 {
		t.Fatalf("got %d bytes, %v", len(b), err)
	}
	br.reset()
	if n, err := br.Read(make([]byte, 5)); n != 5 || err != nil {
		t.Fatalf("after reset: %d %v", n, err)
	}
}

func TestWatchRejectsOversizedEvent(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"type":"ADDED","object":{"metadata":{"name":"`)
		chunk := strings.Repeat("a", 1<<20)
		for i := 0; i < (MaxWatchEventBytes>>20)+2; i++ {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	c, err := NewClient(&RESTConfig{Host: srv.URL, CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})})
	if err != nil {
		t.Fatal(err)
	}
	w, err := c.Watch(context.Background(), AeroRouteResource, "", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Next(); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected size error, got %v", err)
	}
}

// ---- in-cluster and kubeconfig -------------------------------------------

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func envFrom(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestInClusterConfigAndTokenRotation(t *testing.T) {
	srv := newFake(t)
	srv.SetToken("token-1")
	u, _ := url.Parse(srv.URL)
	dir := t.TempDir()
	writeFile(t, dir, "token", []byte("token-1\n"))
	writeFile(t, dir, "ca.crt", srv.CAPEM())
	writeFile(t, dir, "namespace", []byte("aerollm-system"))
	env := envFrom(map[string]string{"KUBERNETES_SERVICE_HOST": u.Hostname(), "KUBERNETES_SERVICE_PORT": u.Port()})

	cfg, err := InClusterConfigFrom(InClusterOptions{Dir: dir, Getenv: env})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Namespace != "aerollm-system" || cfg.BearerTokenFile == "" || cfg.Host != srv.URL {
		t.Fatalf("unexpected config: %s", cfg)
	}
	if strings.Contains(cfg.String(), "token-1") {
		t.Fatal("String leaked the token")
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c.now = func() time.Time { return now }
	if _, err := c.List(context.Background(), AeroRouteResource, "", ListOptions{}); err != nil {
		t.Fatal(err)
	}

	// Rotate: within the refresh interval the cached token is used...
	writeFile(t, dir, "token", []byte("token-2"))
	srv.SetToken("token-2")
	now = now.Add(10 * time.Second)
	if _, err := c.List(context.Background(), AeroRouteResource, "", ListOptions{}); err == nil {
		t.Fatal("expected 401 with the stale cached token")
	}
	// ...but a 401 forces a re-read.
	if _, err := c.List(context.Background(), AeroRouteResource, "", ListOptions{}); err != nil {
		t.Fatalf("expected success after 401-triggered re-read: %v", err)
	}
	// Time-based refresh.
	writeFile(t, dir, "token", []byte("token-3"))
	srv.SetToken("token-3")
	now = now.Add(2 * tokenRefreshInterval)
	if _, err := c.List(context.Background(), AeroRouteResource, "", ListOptions{}); err != nil {
		t.Fatalf("expected refreshed token: %v", err)
	}
	// A vanished token file keeps the last good token.
	if err := os.Remove(filepath.Join(dir, "token")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * tokenRefreshInterval)
	if _, err := c.List(context.Background(), AeroRouteResource, "", ListOptions{}); err != nil {
		t.Fatalf("expected last good token to be kept: %v", err)
	}
}

func TestInClusterConfigErrors(t *testing.T) {
	if _, err := InClusterConfigFrom(InClusterOptions{Dir: t.TempDir(), Getenv: envFrom(nil)}); !errors.Is(err, ErrNotInCluster) {
		t.Fatalf("expected ErrNotInCluster, got %v", err)
	}
	env := envFrom(map[string]string{"KUBERNETES_SERVICE_HOST": "10.0.0.1", "KUBERNETES_SERVICE_PORT": "443"})
	if _, err := InClusterConfigFrom(InClusterOptions{Dir: t.TempDir(), Getenv: env}); err == nil {
		t.Fatal("expected missing token error")
	}
	dir := t.TempDir()
	writeFile(t, dir, "token", []byte("t"))
	if _, err := InClusterConfigFrom(InClusterOptions{Dir: dir, Getenv: env}); err == nil {
		t.Fatal("expected missing CA error")
	}
	writeFile(t, dir, "ca.crt", []byte("x"))
	cfg, err := InClusterConfigFrom(InClusterOptions{Dir: dir, Getenv: envFrom(map[string]string{"KUBERNETES_SERVICE_HOST": "fd00::1", "KUBERNETES_SERVICE_PORT": "443"})})
	if err != nil || cfg.Host != "https://[fd00::1]:443" {
		t.Fatalf("ipv6 host: %v %v", cfg, err)
	}
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func kubeconfigYAML(server, caLine, userBlock string) string {
	return `apiVersion: v1
kind: Config
current-context: dev
clusters:
- name: c1
  cluster:
    server: ` + server + `
    ` + caLine + `
contexts:
- name: dev
  context:
    cluster: c1
    user: u1
    namespace: team-a
- name: other
  context:
    cluster: missing
    user: u1
users:
- name: u1
  user:
` + userBlock
}

func TestKubeconfigToken(t *testing.T) {
	srv := newFake(t)
	srv.SetToken("kc-token")
	dir := t.TempDir()
	path := writeFile(t, dir, "config", []byte(kubeconfigYAML(srv.URL, "certificate-authority-data: "+b64(srv.CAPEM()), "    token: kc-token\n")))
	cfg, err := LoadKubeconfig(path, KubeconfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Namespace != "team-a" || cfg.BearerToken != "kc-token" {
		t.Fatalf("unexpected config %s", cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), AeroRouteResource, "", ListOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKubeconfig(path, KubeconfigOptions{Context: "other"}); err == nil {
		t.Fatal("expected missing cluster error")
	}
	if _, err := LoadKubeconfig(path, KubeconfigOptions{Context: "nope"}); err == nil {
		t.Fatal("expected missing context error")
	}
}

func TestKubeconfigRelativeFilesAndTokenFile(t *testing.T) {
	srv := newFake(t)
	srv.SetToken("file-token")
	dir := t.TempDir()
	writeFile(t, dir, "ca.pem", srv.CAPEM())
	writeFile(t, dir, "tok", []byte("file-token\n"))
	path := writeFile(t, dir, "config", []byte(kubeconfigYAML(srv.URL, "certificate-authority: ca.pem", "    tokenFile: tok\n")))
	cfg, err := LoadKubeconfig(path, KubeconfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), AeroRouteResource, "", ListOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestKubeconfigRejections(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"insecure":      kubeconfigYAML("https://127.0.0.1:1", "insecure-skip-tls-verify: true", "    token: t\n"),
		"exec":          kubeconfigYAML("https://127.0.0.1:1", "", "    exec:\n      command: aws\n"),
		"auth-provider": kubeconfigYAML("https://127.0.0.1:1", "", "    auth-provider:\n      name: gcp\n"),
		"basic":         kubeconfigYAML("https://127.0.0.1:1", "", "    username: admin\n    password: pw\n"),
		"bad-ca":        kubeconfigYAML("https://127.0.0.1:1", "certificate-authority-data: '!!!'", "    token: t\n"),
		"no-server":     kubeconfigYAML("''", "", "    token: t\n"),
		"cert-no-key":   kubeconfigYAML("https://127.0.0.1:1", "", "    client-certificate-data: "+b64([]byte("x"))+"\n"),
	}
	for name, body := range cases {
		path := writeFile(t, dir, name, []byte(body))
		if _, err := LoadKubeconfig(path, KubeconfigOptions{}); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	path := filepath.Join(dir, "insecure")
	cfg, err := LoadKubeconfig(path, KubeconfigOptions{AllowInsecure: true})
	if err != nil || !cfg.Insecure {
		t.Fatalf("insecure opt-in: %v", err)
	}
	if _, err := LoadKubeconfig(filepath.Join(dir, "missing"), KubeconfigOptions{}); err == nil {
		t.Fatal("expected missing file error")
	}
}

// genCert creates a certificate signed by parent (self-signed when nil).
func genCert(t *testing.T, cn string, isCA bool, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, usage x509.ExtKeyUsage) (*x509.Certificate, *ecdsa.PrivateKey, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  isCA,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{usage},
	}
	if !isCA {
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	}
	signer, signerKey := tmpl, key
	if parent != nil {
		signer, signerKey = parent, parentKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func TestKubeconfigClientCertificate(t *testing.T) {
	ca, caKey, _, _ := genCert(t, "test-ca", true, nil, nil, x509.ExtKeyUsageClientAuth)
	_, _, certPEM, keyPEM := genCert(t, "operator", false, ca, caKey, x509.ExtKeyUsageClientAuth)
	pool := x509.NewCertPool()
	pool.AddCert(ca)

	var gotCN string
	var mu sync.Mutex
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			gotCN = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"metadata": map[string]string{"resourceVersion": "1"}, "items": []interface{}{}})
	}))
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	defer srv.Close()
	serverCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})

	dir := t.TempDir()
	user := "    client-certificate-data: " + b64(certPEM) + "\n    client-key-data: " + b64(keyPEM) + "\n"
	path := writeFile(t, dir, "config", []byte(kubeconfigYAML(srv.URL, "certificate-authority-data: "+b64(serverCA), user)))
	cfg, err := LoadKubeconfig(path, KubeconfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), AeroRouteResource, "", ListOptions{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotCN != "operator" {
		t.Fatalf("server saw client cert CN %q", gotCN)
	}
}

func TestDefaultKubeconfigPath(t *testing.T) {
	p := filepath.Join(t.TempDir(), "kc")
	t.Setenv("KUBECONFIG", p+string(os.PathListSeparator)+"other")
	if got := DefaultKubeconfigPath(); got != p {
		t.Fatalf("got %q", got)
	}
}

func TestValidateObject(t *testing.T) {
	kind, name, spec, err := ValidateObject(route("ns", "r1", "cost"))
	if err != nil || kind != KindAeroRoute || name != "r1" || spec["strategy"] != "cost" {
		t.Fatalf("%v %v %v %v", kind, name, spec, err)
	}
	if _, _, _, err := ValidateObject(route("ns", "r1", "bogus")); err == nil {
		t.Fatal("expected invalid strategy")
	}
	bad := route("ns", "r1", "cost")
	bad["spec"].(map[string]interface{})["typo"] = 1
	if _, _, _, err := ValidateObject(bad); err == nil {
		t.Fatal("expected unknown field error")
	}
	if _, _, _, err := ValidateObject(nil); err == nil {
		t.Fatal("expected nil error")
	}
}
