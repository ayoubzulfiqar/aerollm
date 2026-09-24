package gitops

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func writePrompt(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, PromptsDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, PromptsDir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestGitPromptStoreGetLatest(t *testing.T) {
	dir := t.TempDir()
	writePrompt(t, dir, "v1.json", `{"prompt":"hello"}`)
	store := NewGitPromptStore("", dir, "main", 5*time.Minute)
	if err := store.sync(); err != nil {
		t.Fatalf("local-mode sync: %v", err)
	}
	tmpl, err := store.Get("latest")
	if err != nil {
		t.Fatalf("Get latest: %v", err)
	}
	if tmpl.Version != "v1" || tmpl.Payload["prompt"] != "hello" {
		t.Fatalf("unexpected template: %+v", tmpl)
	}
}

func TestValidateBranch(t *testing.T) {
	ok := []string{"main", "release/1.2", "feature-x", "v1.0", "a_b"}
	bad := []string{
		"", "-x", "--upload-pack=touch /tmp/pwned", "a..b", "a b", "a~1", "a^", "a:b", "a?", "a*", "a[",
		`a\b`, "a@{1}", "@", "/a", "a/", "a.", "a.lock", "a/.hidden", "a//b", "a\x00b", "a\nb", strings.Repeat("a", 256),
	}
	for _, b := range ok {
		if err := ValidateBranch(b); err != nil {
			t.Errorf("ValidateBranch(%q) unexpected error: %v", b, err)
		}
	}
	for _, b := range bad {
		if err := ValidateBranch(b); !errors.Is(err, ErrInvalidBranch) {
			t.Errorf("ValidateBranch(%q) = %v, want ErrInvalidBranch", b, err)
		}
	}
}

func TestValidateRepoURL(t *testing.T) {
	ok := []string{
		"https://github.com/org/prompts.git",
		"ssh://git@github.com/org/prompts.git",
		"git://example.com/prompts.git",
		"file:///srv/git/prompts.git",
		"git@github.com:org/prompts.git",
		"https://[::1]/repo.git",
	}
	bad := []string{
		"",
		"-uhttps://x",
		"--upload-pack=touch /tmp/pwned",
		"ext::sh -c touch% /tmp/pwned",
		"fd::17",
		"ftp://example.com/repo",
		"http://example.com/repo",
		"ssh://-oProxyCommand=touch/tmp/pwned/repo",
		"ssh://-oProxyCommand@host/repo",
		"-oProxyCommand=x@host:repo",
		"file://remote-host/repo",
		"/srv/git/repo",
		"https://example.com/a b",
		"https://example.com/\nrepo",
		"https:///nohost",
	}
	for _, u := range ok {
		if err := ValidateRepoURL(u); err != nil {
			t.Errorf("ValidateRepoURL(%q) unexpected error: %v", u, err)
		}
	}
	for _, u := range bad {
		if err := ValidateRepoURL(u); !errors.Is(err, ErrInvalidRepoURL) {
			t.Errorf("ValidateRepoURL(%q) = %v, want ErrInvalidRepoURL", u, err)
		}
	}
}

func TestInvalidConfigRefusesToSync(t *testing.T) {
	store := NewGitPromptStore("ext::sh -c id", t.TempDir(), "--upload-pack=id", time.Minute)
	if store.Validate() == nil {
		t.Fatal("expected validation error")
	}
	err := store.Sync(context.Background())
	if !errors.Is(err, ErrInvalidRepoURL) || !errors.Is(err, ErrInvalidBranch) {
		t.Fatalf("expected both validation errors from Sync, got %v", err)
	}
	if store.LastSyncError() == nil {
		t.Fatal("LastSyncError should record the failure")
	}
	if _, err := NewGitPromptStoreFromConfig(GitConfig{RepoURL: "https://x/y"}); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("expected ErrInvalidPath for empty local path, got %v", err)
	}
}

func TestGetRejectsTraversalAndInvalidVersions(t *testing.T) {
	dir := t.TempDir()
	writePrompt(t, dir, "v1.json", `{"p":1}`)
	secret := filepath.Join(filepath.Dir(dir), "secret.json")
	_ = os.WriteFile(secret, []byte(`{"secret":true}`), 0o600)
	t.Cleanup(func() { os.Remove(secret) })

	store := NewGitPromptStore("", dir, "main", time.Minute)
	for _, v := range []string{"../secret", "../../etc/passwd", "a/b", "..", ".", "v1.json\x00", strings.Repeat("a", 129), "v 1"} {
		if _, err := store.Get(v); !errors.Is(err, ErrInvalidVersion) {
			t.Errorf("Get(%q) = %v, want ErrInvalidVersion", v, err)
		}
	}
	if _, err := store.Get("missing"); !errors.Is(err, ErrPromptNotFound) {
		t.Fatalf("expected ErrPromptNotFound, got %v", err)
	}
}

func TestSymlinksAndNonRegularFilesSkipped(t *testing.T) {
	dir := t.TempDir()
	writePrompt(t, dir, "v1.json", `{"p":1}`)
	outside := filepath.Join(t.TempDir(), "host-secret.json")
	if err := os.WriteFile(outside, []byte(`{"secret":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, PromptsDir, "v9.json")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, PromptsDir, "v8.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	writePrompt(t, dir, "notes.yaml", "a: b")
	writePrompt(t, dir, "broken.json", `not json`)

	store := NewGitPromptStore("", dir, "main", time.Minute)
	versions, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(versions, []string{"v1"}) {
		t.Fatalf("expected only v1, got %v", versions)
	}
	if _, err := store.Get("v9"); err == nil {
		t.Fatal("symlinked prompt must not be served")
	}
	if _, err := store.Get("broken"); err == nil || !strings.Contains(err.Error(), "not a JSON object") {
		t.Fatalf("expected parse error for broken prompt, got %v", err)
	}
	if tmpl, err := store.Get("latest"); err != nil || tmpl.Version != "v1" {
		t.Fatalf("latest should skip invalid entries: %v %v", tmpl, err)
	}
}

func TestSymlinkedPromptsDirRefused(t *testing.T) {
	dir := t.TempDir()
	target := t.TempDir()
	writePrompt(t, target, "v1.json", `{"p":1}`)
	if err := os.Symlink(filepath.Join(target, PromptsDir), filepath.Join(dir, PromptsDir)); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	store := NewGitPromptStore("", dir, "main", time.Minute)
	if _, err := store.List(); err == nil {
		t.Fatal("a symlinked prompts directory must be refused")
	}
}

func TestOversizedPromptRejected(t *testing.T) {
	dir := t.TempDir()
	big := `{"p":"` + strings.Repeat("x", MaxPromptBytes) + `"}`
	writePrompt(t, dir, "big.json", big)
	writePrompt(t, dir, "v1.json", `{"p":1}`)
	store := NewGitPromptStore("", dir, "main", time.Minute)
	if _, err := store.Get("big"); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected size error, got %v", err)
	}
}

func TestLatestResolution(t *testing.T) {
	dir := t.TempDir()
	for _, v := range []string{"v1", "v2", "v10", "v9"} {
		writePrompt(t, dir, v+".json", `{"v":"`+v+`"}`)
	}
	store := NewGitPromptStore("", dir, "main", time.Minute)
	versions, _ := store.List()
	if !reflect.DeepEqual(versions, []string{"v1", "v2", "v9", "v10"}) {
		t.Fatalf("expected natural order, got %v", versions)
	}
	tmpl, err := store.Get("")
	if err != nil || tmpl.Version != "v10" {
		t.Fatalf("expected v10 as latest, got %v %v", tmpl, err)
	}

	writePrompt(t, dir, "latest.json", `{"v":"pinned"}`)
	if err := store.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	tmpl, err = store.Get("latest")
	if err != nil || tmpl.Payload["v"] != "pinned" {
		t.Fatalf("expected latest.json to win, got %v %v", tmpl, err)
	}
}

func TestGetReturnsPrivateCopy(t *testing.T) {
	dir := t.TempDir()
	writePrompt(t, dir, "v1.json", `{"p":"orig"}`)
	store := NewGitPromptStore("", dir, "main", time.Minute)
	a, _ := store.Get("v1")
	a.Payload["p"] = "mutated"
	a.Raw[0] = 'X'
	b, _ := store.Get("v1")
	if b.Payload["p"] != "orig" || b.Raw[0] != '{' {
		t.Fatalf("cached template was mutated through a returned copy: %+v", b)
	}
	if b.Checksum() == "" {
		t.Fatal("expected checksum")
	}
}

func TestNaturalLess(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"v2", "v10", true},
		{"v10", "v2", false},
		{"1.2.9", "1.2.10", true},
		{"a", "b", true},
		{"v1", "v1", false},
		{"v01", "v1", false},
		{"v1", "v01", true},
		{"v1", "v1.1", true},
	}
	for _, tc := range cases {
		if got := naturalLess(tc.a, tc.b); got != tc.want {
			t.Errorf("naturalLess(%q,%q)=%v want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestRedactsCredentials(t *testing.T) {
	g := NewGitPromptStore("https://user:s3cr3t@example.com/repo.git", t.TempDir(), "main", time.Minute)
	out := g.redact("fatal: unable to access 'https://user:s3cr3t@example.com/repo.git/': 403")
	if strings.Contains(out, "s3cr3t") || strings.Contains(out, "user:") {
		t.Fatalf("credentials leaked: %s", out)
	}
}

// --- integration with a real git binary ---

// fileURL returns a file:// URL for a local directory that is valid on
// every platform ("file:///C:/..." on Windows, "file:///tmp/..." elsewhere).
func fileURL(dir string) string {
	p := filepath.ToSlash(dir)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return (&url.URL{Scheme: "file", Path: p}).String()
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	full := append([]string{"-c", "user.name=t", "-c", "user.email=t@t", "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false"}, args...)
	cmd := exec.Command("git", full...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "HOME="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestGitIntegrationCloneAndUpdate(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	src := t.TempDir()
	runGit(t, src, "init", "-q", "-b", "main")
	writePrompt(t, src, "v1.json", `{"prompt":"one"}`)
	runGit(t, src, "add", ".")
	runGit(t, src, "commit", "-q", "-m", "v1")
	first := runGit(t, src, "rev-parse", "HEAD")

	mirror := filepath.Join(t.TempDir(), "mirror")
	store, err := NewGitPromptStoreFromConfig(GitConfig{RepoURL: fileURL(src), LocalPath: mirror, Branch: "main", Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(context.Background()); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	if store.Commit() != first {
		t.Fatalf("commit = %q, want %q", store.Commit(), first)
	}
	tmpl, err := store.Get("latest")
	if err != nil || tmpl.Version != "v1" || tmpl.Commit != first {
		t.Fatalf("unexpected latest: %+v %v", tmpl, err)
	}

	writePrompt(t, src, "v2.json", `{"prompt":"two"}`)
	runGit(t, src, "add", ".")
	runGit(t, src, "commit", "-q", "-m", "v2")
	second := runGit(t, src, "rev-parse", "HEAD")

	// Local modifications in the mirror must be discarded by the sync.
	writePrompt(t, mirror, "injected.json", `{"evil":true}`)
	if err := store.Sync(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if store.Commit() != second {
		t.Fatalf("commit = %q, want %q", store.Commit(), second)
	}
	versions, _ := store.List()
	if !reflect.DeepEqual(versions, []string{"v1", "v2"}) {
		t.Fatalf("versions = %v", versions)
	}
	tmpl, err = store.Get("latest")
	if err != nil || tmpl.Version != "v2" || tmpl.Payload["prompt"] != "two" {
		t.Fatalf("unexpected latest after update: %+v %v", tmpl, err)
	}
	if store.LastSyncError() != nil || store.LastSync().IsZero() {
		t.Fatalf("sync status not recorded: err=%v at=%v", store.LastSyncError(), store.LastSync())
	}
}

func TestGitIntegrationBadBranchKeepsSnapshot(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	src := t.TempDir()
	runGit(t, src, "init", "-q", "-b", "main")
	writePrompt(t, src, "v1.json", `{"prompt":"one"}`)
	runGit(t, src, "add", ".")
	runGit(t, src, "commit", "-q", "-m", "v1")

	store := NewGitPromptStore(fileURL(src), filepath.Join(t.TempDir(), "m"), "does-not-exist", time.Minute)
	err := store.Sync(context.Background())
	if err == nil || !strings.Contains(err.Error(), "git clone failed") {
		t.Fatalf("expected clone failure, got %v", err)
	}
}

func TestStartSyncsImmediatelyAndStops(t *testing.T) {
	dir := t.TempDir()
	writePrompt(t, dir, "v1.json", `{"p":1}`)
	store := NewGitPromptStore("", dir, "main", time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	store.Start(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for store.LastSync().IsZero() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if store.LastSync().IsZero() {
		t.Fatal("Start should sync immediately, not after the first interval")
	}
}

func TestGitTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the fake git binary is a shell script")
	}
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("sleep not available")
	}
	// Use a fake "git" that hangs to prove the timeout is enforced.
	bin := filepath.Join(t.TempDir(), "fakegit")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := NewGitPromptStoreFromConfig(GitConfig{
		RepoURL: "https://example.com/r.git", LocalPath: filepath.Join(t.TempDir(), "m"),
		Timeout: 200 * time.Millisecond, GitBinary: bin,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = store.Sync(context.Background())
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("timeout not enforced promptly: %s", time.Since(start))
	}
}
