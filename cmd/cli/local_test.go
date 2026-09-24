package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/federated"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
)

func TestInitCmd(t *testing.T) {
	isolateEnv(t)
	dir := filepath.Join(t.TempDir(), "project")
	out, _, err := runCLI(t, "", "init", "--dir", dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "created config.yaml, docker-compose.yml, and plugin.go") {
		t.Fatalf("unexpected output: %s", out)
	}
	for name, perm := range map[string]os.FileMode{"config.yaml": 0o600, "docker-compose.yml": 0o644, "plugin.go": 0o644} {
		fi, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
		if fi.Mode().Perm() != perm {
			t.Errorf("%s mode = %v, want %v", name, fi.Mode().Perm(), perm)
		}
	}
	// Existing files are not clobbered.
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("custom: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCLI(t, "", "init", "--dir", dir); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected overwrite refusal, got %v", err)
	}
	if b, _ := os.ReadFile(cfgPath); string(b) != "custom: true\n" {
		t.Fatal("init overwrote an existing config without --force")
	}
	if _, _, err := runCLI(t, "", "init", "--dir", dir, "--force"); err != nil {
		t.Fatal(err)
	}
}

func testKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func writeWASM(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "weather.wasm")
	if err := os.WriteFile(p, []byte("\x00asm\x01\x00\x00\x00"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPluginPublishSignsManifestWithoutLeakingKey(t *testing.T) {
	isolateEnv(t)
	dir := t.TempDir()
	wasm := writeWASM(t, dir)
	_, priv := testKeyPair(t)
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(dir, "plugin.pem")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}

	g := newFakeGateway(t)
	g.json("/v1/marketplace/plugins", 201, `{"id":"weather"}`)
	out, stderr, err := runCLI(t, "", "plugin", "publish", wasm, "--private-key", keyFile, "--creator", "acme",
		"--version", "1.2.0", "--registry-url", g.URL, "--registry-token", "reg-token")
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{hex.EncodeToString(priv), base64.StdEncoding.EncodeToString(priv), hex.EncodeToString(priv.Seed())} {
		if strings.Contains(out+stderr, secret) || strings.Contains(g.last(t).Body, secret) {
			t.Fatal("private key material leaked")
		}
	}
	var sent marketplace.PublishRequest
	if err := json.Unmarshal([]byte(g.last(t).Body), &sent); err != nil {
		t.Fatal(err)
	}
	if sent.ID != "weather" || sent.Version != "1.2.0" || sent.CreatorID != "acme" {
		t.Fatalf("manifest = %+v", sent)
	}
	if _, err := marketplace.VerifyPublishRequest(sent); err != nil {
		t.Fatalf("registry would reject the signature: %v", err)
	}
	if g.last(t).Auth != "Bearer reg-token" {
		t.Fatalf("registry auth = %q", g.last(t).Auth)
	}
	if !strings.Contains(out, "prepared manifest") || !strings.Contains(out, "published to registry") {
		t.Fatalf("stdout = %q", out)
	}
}

func TestPluginPublishValidation(t *testing.T) {
	isolateEnv(t)
	dir := t.TempDir()
	wasm := writeWASM(t, dir)
	_, priv := testKeyPair(t)
	seedHex := hex.EncodeToString(priv.Seed())

	if _, _, err := runCLI(t, "", "plugin", "publish", wasm); err == nil || !strings.Contains(err.Error(), "missing private key") {
		t.Fatalf("expected missing key error, got %v", err)
	}
	t.Setenv("AEROLLM_PLUGIN_PRIVATE_KEY", "fake-key")
	if _, _, err := runCLI(t, "", "plugin", "publish", wasm, "--creator", "acme"); err == nil {
		t.Fatal("expected invalid key error")
	}
	t.Setenv("AEROLLM_PLUGIN_PRIVATE_KEY", seedHex)
	if _, _, err := runCLI(t, "", "plugin", "publish", wasm); err == nil || !strings.Contains(err.Error(), "--creator") {
		t.Fatalf("expected creator error, got %v", err)
	}
	notWasm := filepath.Join(dir, "x.wasm")
	_ = os.WriteFile(notWasm, []byte("MZ\x90\x00"), 0o644)
	if _, _, err := runCLI(t, "", "plugin", "publish", notWasm, "--creator", "acme"); err == nil {
		t.Fatal("expected non-wasm error")
	}
	out, _, err := runCLI(t, "", "plugin", "publish", wasm, "--creator", "acme")
	if err != nil || !strings.Contains(out, "prepared manifest") {
		t.Fatalf("seed from env: %q %v", out, err)
	}
	if strings.Contains(out, seedHex) {
		t.Fatal("seed leaked")
	}
}

func TestLoadEd25519PrivateKey(t *testing.T) {
	_, priv := testKeyPair(t)
	for name, spec := range map[string]string{
		"seed hex":    hex.EncodeToString(priv.Seed()),
		"full base64": base64.StdEncoding.EncodeToString(priv),
	} {
		got, err := loadEd25519PrivateKey(spec)
		if err != nil || !got.Equal(priv) {
			t.Errorf("%s: %v", name, err)
		}
	}
	bad := append([]byte(nil), priv...)
	bad[40] ^= 0xff
	if _, err := loadEd25519PrivateKey(hex.EncodeToString(bad)); err == nil {
		t.Error("expected inconsistent key error")
	}
}

func TestPluginBuildRejectsFlagInjection(t *testing.T) {
	isolateEnv(t)
	if _, _, err := runCLI(t, "", "plugin", "build", "-toolexec=/bin/sh"); err == nil {
		t.Fatal("expected rejection")
	}
	if _, _, err := runCLI(t, "", "plugin", "build", "--", "-toolexec=/bin/sh"); err == nil || !strings.Contains(err.Error(), "must not start") {
		t.Fatalf("expected rejection of dash-prefixed source, got %v", err)
	}
	if _, _, err := runCLI(t, "", "plugin", "build", "missing.go", "-o", "out.wasm"); err == nil || !strings.Contains(err.Error(), "build failed") {
		t.Fatalf("expected build failure error, got %v", err)
	}
}

func TestPqcKeys(t *testing.T) {
	isolateEnv(t)
	out, _, err := runCLI(t, "", "pqc", "keys")
	if err != nil || !strings.Contains(out, "hybrid-ed25519+mldsa-65") {
		t.Fatalf("list: %q %v", out, err)
	}
	if _, _, err := runCLI(t, "", "pqc", "keys", "-a", "hybrid-ed25519+mldsa-65"); err == nil {
		t.Fatal("expected --private-key-out requirement")
	}
	dir := t.TempDir()
	privPath := filepath.Join(dir, "node.key")
	out, _, err = runCLI(t, "", "pqc", "keys", "-a", "hybrid-ed25519+mldsa-65", "--private-key-out", privPath)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(privPath)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("private key file: %v %v", fi, err)
	}
	priv, _ := os.ReadFile(privPath)
	if strings.Contains(out, strings.TrimSpace(string(priv))) || strings.Contains(out, "private=") {
		t.Fatal("private key printed to stdout")
	}
	if !strings.Contains(out, "public=") {
		t.Fatalf("stdout = %q", out)
	}
	if _, _, err := runCLI(t, "", "pqc", "keys", "-a", "hybrid-ed25519+mldsa-65", "--private-key-out", privPath); err == nil {
		t.Fatal("expected refusal to overwrite existing key")
	}
}

func TestFederatedVerify(t *testing.T) {
	isolateEnv(t)
	pub, priv := testKeyPair(t)
	m := &federated.LoRAMatrix{Rows: 1, Cols: 2, Data: []float64{1, 2}, Owner: "e1"}
	sig, err := federated.SignUpdate(priv, m)
	if err != nil {
		t.Fatal(err)
	}
	matrix, _ := json.Marshal(m)
	out, _, err := runCLI(t, "", "federated", "verify", "-m", string(matrix),
		"-s", base64.StdEncoding.EncodeToString(sig), "-k", hex.EncodeToString(pub))
	if err != nil || strings.TrimSpace(out) != "ok" {
		t.Fatalf("valid signature: %q %v", out, err)
	}
	otherPub, _ := testKeyPair(t)
	if _, _, err := runCLI(t, "", "federated", "verify", "-m", string(matrix),
		"-s", base64.StdEncoding.EncodeToString(sig), "-k", hex.EncodeToString(otherPub)); err == nil {
		t.Fatal("expected verification failure with the wrong key")
	}
	if _, _, err := runCLI(t, "", "federated", "verify", "-m", string(matrix), "-s", "abc"); err == nil {
		t.Fatal("expected missing public key error")
	}
}

func TestFederatedAggregateAndList(t *testing.T) {
	isolateEnv(t)
	out, _, err := runCLI(t, "", "federated", "list")
	if err != nil || !strings.Contains(out, "fedavg") {
		t.Fatalf("list: %q %v", out, err)
	}
	updates := `[{"Rows":1,"Cols":2,"Data":[1,2],"Owner":"e1"},{"Rows":1,"Cols":2,"Data":[3,4],"Owner":"e2"}]`
	out, _, err = runCLI(t, "", "federated", "aggregate", "-i", updates)
	if err != nil || !strings.Contains(out, "aggregated rows=1 cols=2") {
		t.Fatalf("aggregate: %q %v", out, err)
	}
	if _, _, err := runCLI(t, "", "federated", "aggregate", "-i", "not json"); err == nil {
		t.Fatal("expected JSON error")
	}
}

func TestBillingGenerate(t *testing.T) {
	isolateEnv(t)
	out, stderr, err := runCLI(t, "", "billing", "generate")
	if err != nil || !strings.Contains(out, "generated invoice") || !strings.Contains(stderr, "sample") {
		t.Fatalf("sample: out=%q stderr=%q err=%v", out, stderr, err)
	}
	dir := t.TempDir()
	input := filepath.Join(dir, "usage.json")
	_ = os.WriteFile(input, []byte(`[{"customer_id":"=HYPERLINK(\"x\")","event_name":"token","value":10}]`), 0o600)
	csvPath := filepath.Join(dir, "invoice.csv")
	if _, _, err := runCLI(t, "", "billing", "generate", "--input", input, "--output", csvPath); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(csvPath)
	if !strings.Contains(string(data), `'=HYPERLINK`) {
		t.Fatalf("formula not neutralised:\n%s", data)
	}
	if fi, _ := os.Stat(csvPath); fi.Mode().Perm() != 0o600 {
		t.Fatalf("invoice mode = %v", fi.Mode().Perm())
	}
	if _, _, err := runCLI(t, "", "billing", "generate", "--input", input, "--output", csvPath); err == nil {
		t.Fatal("expected overwrite refusal")
	}
	if _, _, err := runCLI(t, "", "billing", "generate", "--output", filepath.Join(dir, "x.pdf")); err == nil {
		t.Fatal("expected unsupported format error")
	}
	_ = os.WriteFile(input, []byte(`[{"customer_id":"c","event_name":"e","value":-1}]`), 0o600)
	if _, _, err := runCLI(t, "", "billing", "generate", "--input", input); err == nil {
		t.Fatal("expected negative value error")
	}
}

func TestGitOpsSyncValidation(t *testing.T) {
	isolateEnv(t)
	if _, _, err := runCLI(t, "", "sync"); err == nil || !strings.Contains(err.Error(), "--repo") {
		t.Fatalf("expected --repo requirement, got %v", err)
	}
	for _, args := range [][]string{
		{"sync", "--repo", "--upload-pack=touch /tmp/pwned"},
		{"sync", "--repo", "ext::sh -c touch% /tmp/pwned"},
		{"sync", "--repo", "https://example.com/r.git", "--branch", "--foo"},
		{"sync", "--repo", "https://example.com/r.git", "--branch", "a..b"},
	} {
		if _, _, err := runCLI(t, "", args...); err == nil {
			t.Errorf("%v: expected rejection", args)
		}
	}
}

func TestOpenStandardCapability(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/marketplace/openstandard/capability", 202, `{"version":"1.0"}`)
	if _, _, err := g.run(t, "", "openstandard", "capability", "--gpu", "--gpu-name", "RTX", "--memory-gb", "64"); err != nil {
		t.Fatal(err)
	}
	b := decodeBody(t, g.last(t).Body)
	hw := b["hardware"].(map[string]any)
	if hw["has_local_gpu"] != true || hw["memory_gb"] != 64.0 || hw["gpu_name"] != "RTX" {
		t.Fatalf("manifest = %v", b)
	}
	if _, _, err := g.run(t, "", "openstandard", "capability", "--gpu-name", "RTX"); err == nil {
		t.Fatal("expected validation error (gpu_name without --gpu)")
	}
	g.json("/v1/marketplace/openstandard/receipt", 200, `{}`)
	if _, _, err := g.run(t, "", "openstandard", "receipt", "--customer", `bad"id`); err == nil {
		t.Fatal("expected receipt validation error")
	}
}

func TestSpatialParse(t *testing.T) {
	isolateEnv(t)
	out, _, err := runCLI(t, "", "spatial", "parse", "-t", `{"type":"spatial_anchor","x":1.2,"y":0.5,"z":0.1}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"x"`) || !strings.Contains(out, "---") {
		t.Fatalf("unexpected output: %s", out)
	}
	if _, _, err := runCLI(t, "", "spatial", "parse"); err == nil {
		t.Fatal("expected --text requirement")
	}
}

func TestInitConfigLoadsWithGatewayLoader(t *testing.T) {
	isolateEnv(t)
	dir := t.TempDir()
	if _, _, err := runCLI(t, "", "init", "--dir", dir); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENAI_API_KEY", "sk-x")
	cfg, err := config.LoadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatalf("gateway rejected the init config: %v", err)
	}
	if cfg.Server.ReadTimeout.Seconds() != 15 || len(cfg.Providers) != 2 || cfg.Providers[0].APIKey != "sk-x" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestGitOpsSyncLocalRepo(t *testing.T) {
	isolateEnv(t)
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	base := t.TempDir()
	src := filepath.Join(base, "src")
	git := func(dir string, args ...string) {
		t.Helper()
		c := exec.Command("git", args...)
		c.Dir = dir
		c.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t", "GIT_CONFIG_NOSYSTEM=1", "HOME="+base)
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	if err := os.MkdirAll(filepath.Join(src, "prompts"), 0o755); err != nil {
		t.Fatal(err)
	}
	git(src, "init", "-q", "-b", "main")
	_ = os.WriteFile(filepath.Join(src, "prompts", "v1.json"), []byte(`{"system":"hi"}`), 0o644)
	git(src, "add", ".")
	git(src, "commit", "-q", "-m", "init")

	dest := filepath.Join(base, "checkout")
	t.Setenv("HOME", base)
	out, _, err := runCLI(t, "", "sync", "--repo", src, "--dir", dest, "--branch", "main")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "gitops sync complete") || !strings.Contains(out, "- v1") {
		t.Fatalf("first sync: %q", out)
	}
	// A second run fast-forwards the existing checkout.
	_ = os.WriteFile(filepath.Join(src, "prompts", "v2.json"), []byte(`{"system":"hello"}`), 0o644)
	git(src, "add", ".")
	git(src, "commit", "-q", "-m", "v2")
	out, _, err = runCLI(t, "", "sync", "--repo", src, "--dir", dest, "--branch", "main")
	if err != nil || !strings.Contains(out, "- v2") {
		t.Fatalf("second sync: %q %v", out, err)
	}
}
