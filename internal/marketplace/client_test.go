package marketplace

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
)

type fakeRegistry struct {
	mu        sync.Mutex
	manifests map[string][]byte
	wasm      map[string][]byte
	paths     []string
}

func (f *fakeRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.paths = append(f.paths, r.URL.EscapedPath())
	f.mu.Unlock()
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/plugins/"), "/")
	switch {
	case len(parts) == 2 && parts[1] == "manifest.json":
		if b, ok := f.manifests[parts[0]]; ok {
			_, _ = w.Write(b)
			return
		}
	case len(parts) == 3 && parts[2] == "plugin.wasm":
		if b, ok := f.wasm[parts[0]+"@"+parts[1]]; ok {
			_, _ = w.Write(b)
			return
		}
	}
	http.NotFound(w, r)
}

func newTLSRegistry(t *testing.T, f *fakeRegistry) (*httptest.Server, *Client) {
	t.Helper()
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	return srv, NewClient(srv.URL, WithHTTPClient(srv.Client()))
}

func manifestJSON(t *testing.T, req PublishRequest) []byte {
	t.Helper()
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestClientRefusesInsecureAndMalformedURLs(t *testing.T) {
	for _, raw := range []string{"", "http://registry.example", "ftp://x", "registry.example", "https://user:pw@x", "https://x/?q=1"} {
		c := NewClient(raw)
		if c.Err() == nil {
			t.Errorf("expected config error for %q", raw)
		}
		if _, err := c.FetchManifest(context.Background(), "p1"); err == nil {
			t.Errorf("expected fetch error for %q", raw)
		}
	}
	if err := NewClient("https://registry.aerollm.io").Err(); err != nil {
		t.Fatalf("https URL rejected: %v", err)
	}
	if err := NewClient("http://127.0.0.1:1", WithInsecureHTTP()).Err(); err != nil {
		t.Fatalf("explicit insecure URL rejected: %v", err)
	}
}

func TestClientFetchManifestVerifiesSignature(t *testing.T) {
	_, priv := testKey(t)
	good := signedRequest(t, priv, "p1", "1.0.0", "creator")
	tampered := good
	tampered.Version = "2.0.0"
	other := signedRequest(t, priv, "other", "1.0.0", "creator")

	f := &fakeRegistry{manifests: map[string][]byte{
		"p1":       manifestJSON(t, good),
		"tampered": manifestJSON(t, PublishRequest{ID: "tampered", Name: tampered.Name, Version: tampered.Version, CreatorID: tampered.CreatorID, WASMHash: tampered.WASMHash, PublicKey: tampered.PublicKey, Signature: tampered.Signature}),
		"swap":     manifestJSON(t, other),
		"unsigned": []byte(`{"id":"unsigned","name":"x","version":"1.0.0","creator_id":"c","wasm_hash":"` + HashWASM(nil) + `"}`),
		"huge":     []byte(`{"name":"` + strings.Repeat("a", MaxManifestBytes+10) + `"}`),
	}}
	_, c := newTLSRegistry(t, f)

	m, err := c.FetchManifest(context.Background(), "p1")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if m.ID != "p1" || m.Version != "1.0.0" {
		t.Fatalf("unexpected manifest %+v", m)
	}
	for _, id := range []string{"tampered", "swap", "unsigned", "huge"} {
		if _, err := c.FetchManifest(context.Background(), id); err == nil {
			t.Errorf("expected %s manifest to be rejected", id)
		}
	}
	if _, err := c.FetchManifest(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestClientRejectsPathInjection(t *testing.T) {
	f := &fakeRegistry{}
	_, c := newTLSRegistry(t, f)
	for _, id := range []string{"../admin", "a/b", "a?x=1", "a#b", "", ".hidden", "%2e%2e"} {
		if _, err := c.FetchManifest(context.Background(), id); err == nil {
			t.Errorf("expected invalid id %q to be rejected", id)
		}
	}
	if _, err := c.DownloadWASM(context.Background(), "p1", "../../x"); err == nil {
		t.Error("expected invalid version to be rejected")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.paths) != 0 {
		t.Fatalf("no request should reach the registry, got %v", f.paths)
	}
}

func TestClientTrustStorePinning(t *testing.T) {
	pub, priv := testKey(t)
	_, otherPriv := testKey(t)
	f := &fakeRegistry{manifests: map[string][]byte{
		"good": manifestJSON(t, signedRequest(t, priv, "good", "1.0.0", "alice")),
		"evil": manifestJSON(t, signedRequest(t, otherPriv, "evil", "1.0.0", "alice")),
	}}
	srv := httptest.NewTLSServer(f)
	defer srv.Close()
	trust := NewTrustStore()
	if err := trust.Pin("alice", pub); err != nil {
		t.Fatal(err)
	}
	c := NewClient(srv.URL, WithHTTPClient(srv.Client()), WithTrustStore(trust))
	if _, err := c.FetchManifest(context.Background(), "good"); err != nil {
		t.Fatalf("pinned key rejected: %v", err)
	}
	if _, err := c.FetchManifest(context.Background(), "evil"); !errors.Is(err, ErrUntrustedKey) {
		t.Fatalf("expected ErrUntrustedKey, got %v", err)
	}
}

func TestClientDownloadVerifiedWASM(t *testing.T) {
	_, priv := testKey(t)
	wasm := []byte("\x00asm\x01\x00\x00\x00module")
	req, err := SignManifest(PublishRequest{ID: "p1", Name: "P", Version: "1.2.3", CreatorID: "c", WASMHash: HashWASM(wasm)}, priv)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRegistry{
		manifests: map[string][]byte{"p1": manifestJSON(t, req)},
		wasm:      map[string][]byte{"p1@1.2.3": wasm},
	}
	srv, c := newTLSRegistry(t, f)
	m, err := c.FetchManifest(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.DownloadVerifiedWASM(context.Background(), m)
	if err != nil || string(got) != string(wasm) {
		t.Fatalf("download: %v", err)
	}
	f.wasm["p1@1.2.3"] = []byte("tampered")
	if _, err := c.DownloadVerifiedWASM(context.Background(), m); err == nil {
		t.Fatal("expected hash mismatch")
	}
	small := NewClient(srv.URL, WithHTTPClient(srv.Client()), WithMaxWASMBytes(4))
	if _, err := small.DownloadWASM(context.Background(), "p1", "1.2.3"); err == nil {
		t.Fatal("expected size cap error")
	}
}

func TestClientRefusesHTTPSDowngradeRedirect(t *testing.T) {
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("downgraded request must not be sent")
	}))
	defer plain.Close()
	tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+r.URL.Path, http.StatusFound)
	}))
	defer tlsSrv.Close()
	c := NewClient(tlsSrv.URL, WithHTTPClient(tlsSrv.Client()))
	if _, err := c.FetchManifest(context.Background(), "p1"); err == nil {
		t.Fatal("expected redirect refusal")
	}
}

func TestSignedRegistryVerifiesAndPins(t *testing.T) {
	_, priv := testKey(t)
	_, otherPriv := testKey(t)
	f := &fakeRegistry{manifests: map[string][]byte{
		"p1": manifestJSON(t, signedRequest(t, priv, "p1", "1.0.0", "alice")),
		"p2": manifestJSON(t, signedRequest(t, otherPriv, "p2", "1.0.0", "alice")),
		"p3": []byte(`{"id":"p3","name":"x","version":"1.0.0","creator_id":"bob","wasm_hash":"` + HashWASM(nil) + `","public_key":"key","signature":"sig"}`),
	}}
	_, c := newTLSRegistry(t, f)
	reg := NewSignedRegistry(plugins.NewInMemoryRegistry(), c)

	// Register (no ctx) must verify too — no unverified bypass.
	if err := reg.Register(plugins.Metadata{ID: "p1"}); err != nil {
		t.Fatalf("register verified: %v", err)
	}
	if m, ok := reg.Get("p1"); !ok || m.Version != "1.0.0" {
		t.Fatalf("expected registered p1 with manifest version, got %+v %v", m, ok)
	}
	if err := reg.RegisterVerified(context.Background(), plugins.Metadata{ID: "p2"}); !errors.Is(err, ErrUntrustedKey) {
		t.Fatalf("expected key change to be rejected, got %v", err)
	}
	if err := reg.Register(plugins.Metadata{ID: "p3"}); err == nil {
		t.Fatal("expected fake signature to be rejected")
	}
	if err := reg.Register(plugins.Metadata{ID: "p1", Version: "9.9.9"}); err == nil {
		t.Fatal("expected version mismatch to be rejected")
	}
	if _, ok := reg.CreatorPublicKey("alice"); !ok {
		t.Fatal("expected pinned creator key")
	}
	if len(ed25519.PublicKey(reg.SnapshotCreatorKeys()["alice"])) != ed25519.PublicKeySize {
		t.Fatal("unexpected snapshot key")
	}

	noClient := NewSignedRegistry(nil, nil)
	if err := noClient.Register(plugins.Metadata{ID: "p1"}); err == nil {
		t.Fatal("expected error without client")
	}
}

func TestClientRedirectPolicy(t *testing.T) {
	_, priv := testKey(t)
	manifest := manifestJSON(t, signedRequest(t, priv, "p1", "1.0.0", "alice"))

	var otherHits int
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherHits++
		_, _ = w.Write(manifest)
	}))
	defer other.Close()

	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/moved/"):
			_, _ = w.Write(manifest)
		case r.URL.Query().Get("to") == "other":
			http.Redirect(w, r, other.URL+r.URL.Path, http.StatusFound)
		case r.URL.Query().Get("to") == "localhost":
			// Same port, different host name: still another origin.
			http.Redirect(w, r, strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)+"/moved"+r.URL.Path, http.StatusFound)
		default:
			http.Redirect(w, r, "/moved"+r.URL.Path, http.StatusMovedPermanently)
		}
	}))
	defer srv.Close()
	// The TLS test client trusts both servers' certificates, so only the
	// redirect policy can stop the cross-host fetch.
	hc := srv.Client()

	c := NewClient(srv.URL, WithHTTPClient(hc))
	if m, err := c.FetchManifest(context.Background(), "p1"); err != nil || m.ID != "p1" {
		t.Fatalf("same-host redirect must be followed: %+v %v", m, err)
	}

	for _, to := range []string{"other", "localhost"} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/plugins/p1/manifest.json?to="+to, nil)
		c := NewClient(srv.URL, WithHTTPClient(hc))
		// "other" differs only by port (both listen on 127.0.0.1), so it is
		// refused as another origin too.
		if _, err := c.httpClient.Do(req); err == nil || !strings.Contains(err.Error(), "refusing redirect to another") {
			t.Fatalf("redirect to %s: expected cross-origin refusal, got %v", to, err)
		}
	}
	if otherHits != 0 {
		t.Fatalf("cross-host redirect target was contacted %d times", otherHits)
	}
}

func TestClientCheckRedirectRules(t *testing.T) {
	mk := func(raw string) *http.Request {
		r, err := http.NewRequest(http.MethodGet, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	secure := NewClient("https://registry.example.com/api")
	insecure := NewClient("http://registry.example.com/api", WithInsecureHTTP())
	cases := []struct {
		name string
		c    *Client
		to   string
		via  []string
		ok   bool
	}{
		{"same host path", secure, "https://registry.example.com/other/x", nil, true},
		{"same host explicit default port", secure, "https://REGISTRY.example.com:443/x", nil, true},
		{"other host", secure, "https://evil.example.com/x", nil, false},
		{"sub domain", secure, "https://cdn.registry.example.com/x", nil, false},
		{"other port", secure, "https://registry.example.com:8443/x", nil, false},
		{"downgrade", secure, "http://registry.example.com/x", nil, false},
		{"internal ip", secure, "https://169.254.169.254/latest", nil, false},
		{"credentials", secure, "https://user:pw@registry.example.com/x", nil, false},
		{"odd scheme", secure, "ftp://registry.example.com/x", nil, false},
		{"insecure same host http", insecure, "http://registry.example.com/x", nil, true},
		{"insecure upgrade", insecure, "https://registry.example.com/x", nil, true},
		{"insecure other host", insecure, "http://evil.example.com/x", nil, false},
		{"insecure re-downgrade", insecure, "http://registry.example.com/y", []string{"http://registry.example.com/api", "https://registry.example.com/x"}, false},
		{"insecure upgrade other port", insecure, "https://registry.example.com:8443/x", nil, false},
	}
	for _, tc := range cases {
		var via []*http.Request
		for _, v := range tc.via {
			via = append(via, mk(v))
		}
		if len(via) == 0 {
			via = []*http.Request{mk(tc.c.BaseURL() + "/plugins/p1/manifest.json")}
		}
		err := tc.c.checkRedirect(mk(tc.to), via)
		if (err == nil) != tc.ok {
			t.Errorf("%s: redirect to %s: ok=%v, err=%v", tc.name, tc.to, tc.ok, err)
		}
	}
	many := make([]*http.Request, 5)
	for i := range many {
		many[i] = mk("https://registry.example.com/x")
	}
	if err := secure.checkRedirect(mk("https://registry.example.com/y"), many); err == nil {
		t.Fatal("expected too many redirects")
	}
}
