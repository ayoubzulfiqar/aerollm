package main

import (
	"bytes"
	"context"
	"encoding/json"
	"go/format"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
)

func TestPluginInitScaffold(t *testing.T) {
	isolateEnv(t)
	base := t.TempDir()
	for _, kind := range []string{"hook", "tool"} {
		dir := filepath.Join(base, kind)
		out, _, err := runCLI(t, "", "plugin", "init", "--kind", kind, "--name", "my_"+kind, "--dir", dir)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out, "created "+kind+" my_"+kind+" plugin") || !strings.Contains(out, "aerollm plugin build") {
			t.Fatalf("%s: stdout = %q", kind, out)
		}
		gomod, _ := os.ReadFile(filepath.Join(dir, "go.mod"))
		if !strings.HasPrefix(string(gomod), "module my_"+kind+"\n") {
			t.Fatalf("%s: go.mod = %q", kind, gomod)
		}
		src, err := os.ReadFile(filepath.Join(dir, "main.go"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(src), "aerollm/internal") || strings.Contains(string(src), "__PLUGIN_NAME__") {
			t.Fatalf("%s: template not standalone/rendered:\n%s", kind, src)
		}
		if formatted, err := format.Source(src); err != nil || !bytes.Equal(formatted, src) {
			t.Fatalf("%s: generated code is not gofmt-clean (%v)", kind, err)
		}
		assertPerm(t, filepath.Join(dir, "main.go"), 0o644)
	}
	// Default directory is ./<name>; existing files are not clobbered.
	t.Chdir(base)
	if _, _, err := runCLI(t, "", "plugin", "init"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "example-hook", "main.go")); err != nil {
		t.Fatalf("default dir: %v", err)
	}
	if _, _, err := runCLI(t, "", "plugin", "init"); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected overwrite refusal, got %v", err)
	}
	if _, _, err := runCLI(t, "", "plugin", "init", "--force", "--kind", "tool", "--dir", "example-hook"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]string{
		{"plugin", "init", "--kind", "daemon"},
		{"plugin", "init", "--name", "../escape"},
		{"plugin", "init", "--name", "1abc"},
		{"plugin", "init", "--name", "has space"},
	} {
		if _, _, err := runCLI(t, "", bad...); err == nil {
			t.Errorf("%v: expected error", bad)
		}
	}
}

// requireWasip1 skips unless a Go toolchain that targets wasip1/wasm exists.
func requireWasip1(t *testing.T) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain not found")
	}
	out, err := exec.Command(goBin, "tool", "dist", "list").Output()
	if err != nil || !bytes.Contains(out, []byte("wasip1/wasm")) {
		t.Skip("toolchain cannot target wasip1/wasm")
	}
	return goBin
}

// wasip1Env is the environment for building a standalone wasip1 module.
func wasip1Env() []string {
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		switch strings.ToUpper(k) {
		case "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS", "GOWORK", "GOTOOLCHAIN", "GO111MODULE", "GOEXPERIMENT":
			continue
		}
		env = append(env, kv)
	}
	return append(env, "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0", "GOWORK=off", "GOTOOLCHAIN=local", "GO111MODULE=on")
}

// buildTemplate scaffolds a plugin of kind, vets it for wasip1/wasm, builds
// it with "aerollm plugin build" and returns the module bytes.
func buildTemplate(t *testing.T, goBin, kind, name string) []byte {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	if _, _, err := runCLI(t, "", "plugin", "init", "--kind", kind, "--name", name, "--dir", dir); err != nil {
		t.Fatal(err)
	}
	vet := exec.Command(goBin, "vet", ".")
	vet.Dir = dir
	vet.Env = wasip1Env()
	if out, err := vet.CombinedOutput(); err != nil {
		t.Fatalf("%s template fails go vet for wasip1/wasm: %v\n%s", kind, err, out)
	}
	for _, k := range []string{"GOFLAGS", "GOEXPERIMENT"} {
		t.Setenv(k, "")
	}
	t.Setenv("GOTOOLCHAIN", "local")
	wasmPath := filepath.Join(t.TempDir(), name+".wasm")
	if _, stderr, err := runCLI(t, "", "plugin", "build", dir, "-o", wasmPath); err != nil {
		t.Fatalf("plugin build: %v\n%s", err, stderr)
	}
	wasm, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := plugins.ValidateWASM(wasm); err != nil {
		t.Fatal(err)
	}
	return wasm
}

// oneJSONObject decodes stdout, requiring exactly one JSON object.
func oneJSONObject(t *testing.T, stdout []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(stdout))
	var m map[string]any
	if err := dec.Decode(&m); err != nil || m == nil {
		t.Fatalf("stdout is not a JSON object: %v: %q", err, stdout)
	}
	if _, err := dec.Token(); err != io.EOF {
		t.Fatalf("stdout has data after the JSON object: %q", stdout)
	}
	return m
}

// TestPluginTemplatesRunInGatewaySandbox builds both templates for
// wasip1/wasm and runs them through the gateway's own WASM host and tool
// adapters, checking the hook and tool protocols end to end.
func TestPluginTemplatesRunInGatewaySandbox(t *testing.T) {
	isolateEnv(t)
	goBin := requireWasip1(t)
	ctx := context.Background()
	// The interpreter compiles a Go-built module much faster than the
	// compiler does under -race, which dominates this test's run time.
	rt, err := wasmrt.New(wasmrt.Config{Timeout: 2 * time.Minute, Interpreter: true})
	if err != nil {
		t.Skipf("wasm runtime unavailable: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	t.Run("hook", func(t *testing.T) {
		wasm := buildTemplate(t, goBin, "hook", "example-hook")
		if testing.Short() {
			t.Skip("skipping sandbox execution in -short mode")
		}
		host, err := plugins.NewWasmHostWithOptions(nil, plugins.WasmHostOptions{Runtime: rt, Timeout: 2 * time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Close(ctx) })
		if err := host.LoadPlugin(ctx, "example-hook", wasm); err != nil {
			t.Fatal(err)
		}
		in := map[string]any{"model": "gpt-4o", "metadata": map[string]any{"team": "search"}}
		out, err := host.RunHook(ctx, "example-hook", plugins.HookOnRequest, in)
		if err != nil {
			t.Fatal(err)
		}
		md, _ := out["metadata"].(map[string]any)
		if out["model"] != "gpt-4o" || md["team"] != "search" || md["example-hook"] != "seen" {
			t.Fatalf("OnRequest payload = %v", out)
		}
		out, err = host.RunHook(ctx, "example-hook", plugins.HookOnResponse, in)
		if err != nil || !reflect.DeepEqual(out, in) {
			t.Fatalf("OnResponse must leave the payload unchanged: %v %v", out, err)
		}

		// Raw protocol: exactly one JSON object per run, errors as {"error":...}.
		mod, err := rt.Compile(ctx, wasm)
		if err != nil {
			t.Fatal(err)
		}
		for input, want := range map[string]string{
			`{"version":1,"plugin_id":"p","hook":"OnToolCall","payload":{"a":1}}`: "",
			`{"version":2,"plugin_id":"p","hook":"OnRequest","payload":{}}`:       "unsupported hook protocol version 2",
			`not json`: "invalid hook input",
		} {
			res, err := rt.RunModule(ctx, mod, []byte(input), wasmrt.RunOptions{})
			if err != nil {
				t.Fatalf("%s: %v", input, err)
			}
			m := oneJSONObject(t, res.Stdout)
			if want == "" {
				if len(m) != 0 {
					t.Fatalf("%s: want {} (unchanged), got %s", input, res.Stdout)
				}
				continue
			}
			if msg, _ := m["error"].(string); !strings.Contains(msg, want) || len(m) != 1 {
				t.Fatalf("%s: want error %q, got %s", input, want, res.Stdout)
			}
		}
	})

	t.Run("tool", func(t *testing.T) {
		wasm := buildTemplate(t, goBin, "tool", "word_count")
		if testing.Short() {
			t.Skip("skipping sandbox execution in -short mode")
		}
		tool, err := plugins.NewWasmTool("word_count", "Count words", nil, wasm, rt)
		if err != nil {
			t.Fatal(err)
		}
		got, err := tool.Execute(ctx, map[string]any{"text": "hello big wörld"})
		if err != nil {
			t.Fatal(err)
		}
		if m, _ := got.(map[string]any); m["words"] != 3.0 || m["characters"] != 15.0 {
			t.Fatalf("result = %#v", got)
		}
		if _, err := tool.Execute(ctx, map[string]any{}); err == nil || !strings.Contains(err.Error(), "(string) is required") {
			t.Fatalf("expected missing-argument failure, got %v", err)
		}
	})
}
