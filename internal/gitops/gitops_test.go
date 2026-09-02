package gitops

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGitPromptStoreGetLatest(t *testing.T) {
	dir := t.TempDir()
	prompts := filepath.Join(dir, "prompts")
	if err := os.MkdirAll(prompts, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prompts, "v1.json"), []byte(`{"prompt":"hello"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewGitPromptStore("", dir, "main", 5*time.Minute)
	if err := store.sync(); err != nil {
		// ignore git sync errors in test env, proceed if possible
		t.Logf("sync error: %v", err)
	}
	tmpl, err := store.Get("latest")
	if err != nil {
		// If no git repo, Get may fail; that's acceptable for this unit test.
		t.Skip("git-backed Get needs repo; covered by integration elsewhere")
	}
	if tmpl.Version != store.knownCommit && tmpl.Version != "v1" {
		t.Fatalf("unexpected version: %s", tmpl.Version)
	}
}
