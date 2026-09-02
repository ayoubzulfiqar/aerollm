package main

import (
	"bytes"
	"os"
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
	_ = os.Chdir(dir)
	output := captureOutput(t, []string{"init"})
	if !strings.Contains(output, "created config.yaml and plugin.go") {
		t.Fatalf("unexpected output: %s", output)
	}
	if _, err := os.Stat("config.yaml"); err != nil {
		t.Fatalf("config.yaml missing: %v", err)
	}
	if _, err := os.Stat("plugin.go"); err != nil {
		t.Fatalf("plugin.go missing: %v", err)
	}
}

func TestPluginBuildMissingFile(t *testing.T) {
	output := captureOutput(t, []string{"plugin", "build", "missing.go", "-o", "out.wasm"})
	if !strings.Contains(output, "build failed") {
		t.Fatalf("expected build failure output, got: %s", output)
	}
}

func TestGitOpsSync(t *testing.T) {
	output := captureOutput(t, []string{"sync"})
	if !strings.Contains(output, "gitops sync triggered") {
		t.Fatalf("unexpected output: %s", output)
	}
}
