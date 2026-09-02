package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func captureOutput(t *testing.T, args []string) string {
	t.Helper()
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newRootCmd()
	cmd.SetArgs(args)
	_ = cmd.Execute()

	w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	return strings.TrimSpace(buf.String())
}

func TestInitCmd(t *testing.T) {
	dir := t.TempDir()
	absDir, _ := filepath.Abs(dir)
	_ = os.Chdir(absDir)
	output := captureOutput(t, []string{"init", "--dir", absDir})
	if !strings.Contains(output, "created config.yaml, docker-compose.yml, and plugin.go") {
		t.Fatalf("unexpected output: %s", output)
	}
	if _, err := os.Stat(filepath.Join(absDir, "config.yaml")); err != nil {
		t.Fatalf("config.yaml missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(absDir, "docker-compose.yml")); err != nil {
		t.Fatalf("docker-compose.yml missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(absDir, "plugin.go")); err != nil {
		t.Fatalf("plugin.go missing: %v", err)
	}
}

func TestPluginBuildMissingFile(t *testing.T) {
	output := captureOutput(t, []string{"plugin", "build", "missing.go", "-o", "out.wasm"})
	if !strings.Contains(output, "build failed") {
		t.Fatalf("expected build failure output, got: %s", output)
	}
}

func TestPluginPublishMissingKey(t *testing.T) {
	output := captureOutput(t, []string{"plugin", "publish", "plugin.wasm"})
	if !strings.Contains(output, "missing private key") {
		t.Fatalf("expected missing key output, got: %s", output)
	}
}

func TestPluginPublishPreparesManifest(t *testing.T) {
	_ = os.Setenv("AEROLLM_PLUGIN_PRIVATE_KEY", "fake-key")
	defer os.Unsetenv("AEROLLM_PLUGIN_PRIVATE_KEY")
	output := captureOutput(t, []string{"plugin", "publish", "plugin.wasm"})
	if !strings.Contains(output, "prepared manifest") {
		t.Fatalf("expected prepared manifest output, got: %s", output)
	}
}

func TestGitOpsSync(t *testing.T) {
	output := captureOutput(t, []string{"sync"})
	if !strings.Contains(output, "gitops sync triggered") {
		t.Fatalf("unexpected output: %s", output)
	}
}
