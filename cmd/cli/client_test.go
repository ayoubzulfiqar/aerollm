package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseBaseURL(t *testing.T) {
	ok := map[string]string{
		"http://localhost:8080":        "http://localhost:8080",
		"https://gw.example.com/":      "https://gw.example.com",
		"gw.example.com:9000":          "http://gw.example.com:9000",
		"https://gw.example.com/api//": "https://gw.example.com/api",
	}
	for in, want := range ok {
		u, err := parseBaseURL(in)
		if err != nil {
			t.Errorf("parseBaseURL(%q): %v", in, err)
			continue
		}
		if u.String() != want {
			t.Errorf("parseBaseURL(%q) = %q, want %q", in, u.String(), want)
		}
	}
	for _, bad := range []string{"", "ftp://x", "http://", "http://h?x=1", "http://h/#frag"} {
		if _, err := parseBaseURL(bad); err == nil {
			t.Errorf("parseBaseURL(%q) expected error", bad)
		}
	}
}

func TestExtractErrorMessage(t *testing.T) {
	cases := map[string]string{
		`{"error":"unauthorized"}`:                                 "unauthorized",
		`{"error":{"message":"model not found","type":"invalid"}}`: "model not found (invalid)",
		`{"message":"boom"}`:                                       "boom",
		"plain text failure\n":                                     "plain text failure",
		"":                                                         "",
		`{"other":1}`:                                              `{"other":1}`,
	}
	for in, want := range cases {
		if got := extractErrorMessage([]byte(in)); got != want {
			t.Errorf("extractErrorMessage(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestServerFlagsEnvAndAuthHeader(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/resilience/status", 200, `{"state":"ok"}`)

	// Flags.
	if _, _, err := g.run(t, "", "resilience"); err != nil {
		t.Fatal(err)
	}
	if got := g.last(t).Auth; got != "Bearer sk-test-master-key" {
		t.Fatalf("Authorization = %q", got)
	}

	// Environment.
	t.Setenv("AEROLLM_URL", g.URL)
	t.Setenv("AEROLLM_API_KEY", "sk-from-env")
	out, _, err := runCLI(t, "", "resilience", "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"state": "ok"`) {
		t.Fatalf("unexpected output %q", out)
	}
	if got := g.last(t).Auth; got != "Bearer sk-from-env" {
		t.Fatalf("Authorization from env = %q", got)
	}

	// --server beats the environment.
	t.Setenv("AEROLLM_URL", "http://127.0.0.1:1")
	if _, _, err := runCLI(t, "", "--server", g.URL, "resilience"); err != nil {
		t.Fatalf("--server should override AEROLLM_URL: %v", err)
	}
}

func TestDeprecatedAddrFlag(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/rsi/headroom", 200, `[{"dimension":"latency","headroom":0.2}]`)
	out, _, err := runCLI(t, "", "rsi", "headroom", "--addr", g.URL)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "latency") || g.last(t).Path != "/v1/rsi/headroom" {
		t.Fatalf("--addr not honoured: %q", out)
	}
}

func TestNon2xxReturnsServerErrorAndExitCode(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/models", http.StatusUnauthorized, `{"error":{"message":"invalid api key sk-test-master-key","type":"auth_error"}}`)

	_, _, err := g.run(t, "", "models", "list")
	var apiErr *apiError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusUnauthorized {
		t.Fatalf("expected apiError 401, got %v", err)
	}
	if !strings.Contains(err.Error(), "invalid api key") || !strings.Contains(err.Error(), "auth_error") {
		t.Fatalf("server message missing: %v", err)
	}
	if strings.Contains(err.Error(), "sk-test-master-key") {
		t.Fatalf("API key leaked in error: %v", err)
	}

	// run() prints the error to stderr and exits 1.
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--server", g.URL, "--api-key", "sk-test-master-key", "models", "list"}, strings.NewReader(""), &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "401") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunExitCodes(t *testing.T) {
	isolateEnv(t)
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"federated", "list"}, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, stderr.String())
	}
	if code := run(context.Background(), []string{"no-such-command"}, strings.NewReader(""), &stdout, &stderr); code != 1 {
		t.Fatalf("unknown command exit code = %d", code)
	}
	// Connection failures are errors, not silent successes.
	if code := run(context.Background(), []string{"--server", "http://127.0.0.1:1", "health"}, strings.NewReader(""), &stdout, &stderr); code != 1 {
		t.Fatalf("unreachable server exit code = %d", code)
	}
}

func TestInvalidOutputFormat(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/resilience/status", 200, `{"state":"ok"}`)
	if _, _, err := g.run(t, "", "resilience", "-o", "yaml"); err == nil || !strings.Contains(err.Error(), "--output") {
		t.Fatalf("expected --output validation error, got %v", err)
	}
}

func TestRenderResultTableSanitizesControlChars(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/policy", 200, `[{"id":"a\u001b[31m","name":"n","expression":"allow","severity":"low"}]`)
	out, _, err := g.run(t, "", "policy")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "\x1b") {
		t.Fatalf("terminal escape passed through: %q", out)
	}
	if !strings.Contains(out, "EXPRESSION") || !strings.Contains(out, "allow") {
		t.Fatalf("unexpected table: %q", out)
	}
}

func TestHelpListsEveryCommandWithDescription(t *testing.T) {
	isolateEnv(t)
	out, _, err := runCLI(t, "", "--help")
	if err != nil {
		t.Fatal(err)
	}
	root := newRootCmd()
	for _, c := range root.Commands() {
		if c.Hidden {
			continue
		}
		if strings.TrimSpace(c.Short) == "" {
			t.Errorf("command %q has no short description", c.Name())
		}
		if !strings.Contains(out, c.Name()) {
			t.Errorf("--help does not list %q", c.Name())
		}
	}
	for _, want := range []string{"chat", "models", "keys", "metrics", "health", "migrate", "rsi"} {
		if !strings.Contains(out, want) {
			t.Errorf("--help missing %q", want)
		}
	}
}

func TestWriteFileSafely(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.yaml")
	if err := writeFileSafely(p, []byte("one"), 0o600, false); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, want 0600", fi.Mode().Perm())
	}
	if err := writeFileSafely(p, []byte("two"), 0o600, false); err == nil {
		t.Fatal("expected refusal to overwrite without force")
	}
	if b, _ := os.ReadFile(p); string(b) != "one" {
		t.Fatalf("file was modified: %q", b)
	}
	if err := writeFileSafely(p, []byte("two"), 0o600, true); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(p); string(b) != "two" {
		t.Fatalf("force did not overwrite: %q", b)
	}

	// Never write through a symlink.
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skip("symlinks unsupported")
	}
	if err := writeFileSafely(link, []byte("evil"), 0o600, true); err == nil {
		t.Fatal("expected refusal to overwrite a symlink")
	}
	if b, _ := os.ReadFile(target); string(b) != "keep" {
		t.Fatalf("symlink target modified: %q", b)
	}
	// No temp files left behind.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
}

func TestMaskSecret(t *testing.T) {
	if got := maskSecret("sk-abcdefghijklmnopqrstuvwxyz"); got != "sk-abc...wxyz" {
		t.Fatalf("maskSecret = %q", got)
	}
	if got := maskSecret("short"); got != "****" {
		t.Fatalf("maskSecret(short) = %q", got)
	}
}
