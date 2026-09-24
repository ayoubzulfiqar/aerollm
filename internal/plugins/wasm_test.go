package plugins

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt/wasmrttest"
)

func newTestHost(t *testing.T, opts WasmHostOptions) *WasmHost {
	t.Helper()
	h, err := NewWasmHostWithOptions(nil, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	return h
}

func TestWasmHostRejectsInvalidModules(t *testing.T) {
	h := newTestHost(t, WasmHostOptions{})
	ctx := context.Background()
	for name, b := range map[string][]byte{
		"junk after magic": append([]byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}, 0x01, 0x02),
		"host import":      wasmrttest.Import("env", "exec"),
		"no _start":        wasmrttest.NoStart(),
		"not wasm":         []byte("#!/bin/sh\nrm -rf /"),
	} {
		if err := h.LoadPlugin(ctx, "p", b); err == nil {
			t.Errorf("%s: expected LoadPlugin to fail", name)
		}
	}
	if err := h.LoadPlugin(ctx, "../evil", wasmrttest.Nop()); !errors.Is(err, ErrInvalidPluginID) {
		t.Fatalf("expected invalid id, got %v", err)
	}
	if got := h.Loaded(); len(got) != 0 {
		t.Fatalf("nothing should be loaded, got %v", got)
	}
}

func TestWasmHostHookProtocol(t *testing.T) {
	h := newTestHost(t, WasmHostOptions{Timeout: 200 * time.Millisecond})
	ctx := context.Background()
	load := func(id string, wasm []byte) {
		t.Helper()
		if err := h.LoadPlugin(ctx, id, wasm); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}
	load("replace", wasmrttest.Hello(`{"payload":{"x":1,"nested":{"ok":true}}}`))
	load("unchanged", wasmrttest.Hello(`{}`))
	load("nullpayload", wasmrttest.Hello(`{"payload":null}`))
	load("fails", wasmrttest.Hello("{\"error\":\"denied\\nby policy\\u001b[31m\"}"))
	load("array", wasmrttest.Hello(`[1,2,3]`))
	load("badpayload", wasmrttest.Hello(`{"payload":[1]}`))
	load("trailing", wasmrttest.Hello(`{} {}`))
	load("exit", wasmrttest.Exit(7))
	load("loop", wasmrttest.Loop())

	in := map[string]interface{}{"orig": "v"}
	out, err := h.RunHook(ctx, "replace", HookOnRequest, in)
	if err != nil || out["x"] != float64(1) || out["orig"] != nil {
		t.Fatalf("replace: %v %v", out, err)
	}
	for _, id := range []string{"unchanged", "nullpayload"} {
		out, err := h.RunHook(ctx, id, HookOnRequest, in)
		if err != nil || out["orig"] != "v" || len(out) != 1 {
			t.Fatalf("%s: expected unchanged payload, got %v %v", id, out, err)
		}
	}

	var he *HookError
	_, err = h.RunHook(ctx, "fails", HookOnToolCall, in)
	if !errors.Is(err, ErrPluginFailed) || !errors.As(err, &he) || he.PluginID != "fails" || he.Hook != HookOnToolCall {
		t.Fatalf("expected ErrPluginFailed HookError, got %v", err)
	}
	if strings.ContainsAny(err.Error(), "\n\x1b") || !strings.Contains(err.Error(), "denied by policy") {
		t.Fatalf("plugin error text must be sanitised: %q", err.Error())
	}
	for _, id := range []string{"array", "badpayload", "trailing"} {
		if _, err := h.RunHook(ctx, id, HookOnRequest, in); !errors.Is(err, ErrInvalidPluginOutput) {
			t.Errorf("%s: expected ErrInvalidPluginOutput, got %v", id, err)
		}
	}
	_, err = h.RunHook(ctx, "exit", HookOnResponse, in)
	var ee *wasmrt.ExitError
	if !errors.As(err, &ee) || ee.Code != 7 || !errors.As(err, &he) {
		t.Fatalf("expected ExitError(7) in HookError, got %v", err)
	}
	start := time.Now()
	_, err = h.RunHook(ctx, "loop", HookOnRequest, in)
	if !errors.Is(err, ErrHookTimeout) || !errors.Is(err, wasmrt.ErrTimeout) {
		t.Fatalf("expected ErrHookTimeout, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("hook timeout not enforced promptly")
	}
	if _, err := h.RunHook(ctx, "replace", HookOnRequest, map[string]interface{}{"ch": make(chan int)}); err == nil {
		t.Fatal("expected non-JSON payload to be rejected")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := h.RunHook(cctx, "replace", HookOnRequest, in); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestWasmHostSharedRuntimeIsNotClosed(t *testing.T) {
	rt, err := wasmrt.New(wasmrt.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	h := newTestHost(t, WasmHostOptions{Runtime: rt})
	if err := h.LoadPlugin(context.Background(), "p", wasmrttest.Nop()); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Run(context.Background(), wasmrttest.Nop(), nil); err != nil {
		t.Fatalf("shared runtime must survive host Close: %v", err)
	}
	if _, err := NewWasmHostWithOptions(nil, WasmHostOptions{RuntimeConfig: wasmrt.Config{MaxMemoryPages: 1 << 20}}); !errors.Is(err, ErrWASMRuntimeUnavailable) {
		t.Fatalf("expected ErrWASMRuntimeUnavailable for a bad runtime config, got %v", err)
	}
}

func TestWasmHostGoGuestHooks(t *testing.T) {
	guest := wasmrttest.Guest(t)
	h := newTestHost(t, WasmHostOptions{Timeout: 20 * time.Second, RuntimeConfig: wasmrt.Config{MaxMemoryPages: 512}})
	ctx := context.Background()
	if err := h.LoadPlugin(ctx, "upper", guest); err != nil {
		t.Fatal(err)
	}
	in := map[string]interface{}{"mode": "hook", "text": "hello"}
	out, err := h.RunHook(ctx, "upper", HookOnRequest, in)
	if err != nil {
		t.Fatal(err)
	}
	if out["text"] != "HELLO" || out["seen_by"] != "upper" || out["hook"] != "OnRequest" {
		t.Fatalf("unexpected output %v", out)
	}
	if in["text"] != "hello" {
		t.Fatal("caller payload mutated")
	}
	out, err = h.RunHook(ctx, "upper", HookOnResponse, map[string]interface{}{"mode": "hook-nochange", "k": "v"})
	if err != nil || out["k"] != "v" {
		t.Fatalf("no-change: %v %v", out, err)
	}
	if _, err := h.RunHook(ctx, "upper", HookOnRequest, map[string]interface{}{"mode": "hook-error"}); !errors.Is(err, ErrPluginFailed) {
		t.Fatalf("expected ErrPluginFailed, got %v", err)
	}
	var ee *wasmrt.ExitError
	if _, err := h.RunHook(ctx, "upper", HookOnRequest, map[string]interface{}{"mode": "panic"}); !errors.As(err, &ee) || ee.Code != 2 {
		t.Fatalf("expected guest panic as exit code 2, got %v", err)
	}
	if _, err := h.RunHook(ctx, "upper", HookOnRequest, map[string]interface{}{"mode": "alloc"}); !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("expected ErrMemoryLimit, got %v", err)
	}
	if _, err := h.RunHook(ctx, "upper", HookOnRequest, map[string]interface{}{"mode": "fs"}); err != nil {
		t.Fatal(err)
	}

	short := newTestHost(t, WasmHostOptions{Timeout: 300 * time.Millisecond})
	if err := short.LoadPlugin(ctx, "spin", guest); err != nil {
		t.Fatal(err)
	}
	if _, err := short.RunHook(ctx, "spin", HookOnRequest, map[string]interface{}{"mode": "loop"}); !errors.Is(err, ErrHookTimeout) {
		t.Fatalf("expected ErrHookTimeout, got %v", err)
	}
}

func TestWasmHostRunAllChainsInOrder(t *testing.T) {
	guest := wasmrttest.Guest(t)
	h := newTestHost(t, WasmHostOptions{Timeout: 20 * time.Second})
	ctx := context.Background()
	for _, id := range []string{"b", "a", "c"} {
		if err := h.LoadPlugin(ctx, id, guest); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.SetEnabled("c", false); err != nil {
		t.Fatal(err)
	}
	in := map[string]interface{}{"mode": "hook-append", "steps": ""}
	for name, run := range map[string]func() (map[string]interface{}, error){
		"RunAll": func() (map[string]interface{}, error) { return h.RunAll(ctx, HookOnRequest, in) },
		"AsHost": func() (map[string]interface{}, error) { return h.AsHost().RunHook(ctx, HookOnRequest, in) },
	} {
		out, err := run()
		if err != nil || out["steps"] != "ab" {
			t.Fatalf("%s: expected ordered chain 'ab', got %v %v", name, out, err)
		}
	}
	if in["steps"] != "" {
		t.Fatal("caller payload mutated")
	}
	if _, err := h.RunAll(ctx, Hook("nope"), in); !errors.Is(err, ErrInvalidHook) {
		t.Fatalf("expected invalid hook, got %v", err)
	}
	if err := h.LoadPlugin(ctx, "d", wasmrttest.Exit(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunAll(ctx, HookOnRequest, in); err == nil {
		t.Fatal("expected the chain to abort on a failing plugin")
	}
}

func TestWasmToolFixtures(t *testing.T) {
	rt, err := wasmrt.New(wasmrt.Config{Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	params := map[string]interface{}{"type": "object", "properties": map[string]interface{}{"q": map[string]interface{}{"type": "string"}}}
	tool, err := NewWasmTool("lookup", "Looks things up", params, wasmrttest.Hello(`{"answer":42}`), rt)
	if err != nil {
		t.Fatal(err)
	}
	if tool.Name() != "lookup" || tool.Description() != "Looks things up" || len(tool.ModuleHash()) != 64 {
		t.Fatalf("bad metadata")
	}
	p := tool.Parameters()
	p["properties"].(map[string]interface{})["q"] = "mutated"
	params["type"] = "mutated"
	if tool.Parameters()["type"] != "object" || tool.Parameters()["properties"].(map[string]interface{})["q"] == "mutated" {
		t.Fatal("parameters must be deep-copied in and out")
	}
	out, err := tool.Execute(context.Background(), map[string]interface{}{"q": "x"})
	if err != nil || out.(map[string]interface{})["answer"] != float64(42) {
		t.Fatalf("execute: %v %v", out, err)
	}

	cases := map[string]struct {
		wasm []byte
		want error
	}{
		"not json": {wasmrttest.Hello("hello"), ErrInvalidToolOutput},
		"trailing": {wasmrttest.Hello(`1 2`), ErrInvalidToolOutput},
		"timeout":  {wasmrttest.Loop(), wasmrt.ErrTimeout},
	}
	for name, c := range cases {
		tl, err := NewWasmTool("t_"+strings.ReplaceAll(name, " ", "_"), "", nil, c.wasm, rt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tl.Execute(context.Background(), nil); !errors.Is(err, c.want) {
			t.Errorf("%s: expected %v, got %v", name, c.want, err)
		}
	}
	nop, err := NewWasmTool("nop", "", nil, wasmrttest.Nop(), rt)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := nop.Execute(context.Background(), nil); err != nil || out != nil {
		t.Fatalf("empty output must decode to nil: %v %v", out, err)
	}
	if nop.Parameters()["type"] != "object" {
		t.Fatal("default parameters must be an object schema")
	}
	var ee *wasmrt.ExitError
	ex, _ := NewWasmTool("exit", "", nil, wasmrttest.Exit(9), rt)
	if _, err := ex.Execute(context.Background(), nil); !errors.As(err, &ee) || ee.Code != 9 {
		t.Fatalf("expected ExitError(9), got %v", err)
	}
	for _, bad := range []string{"", "has space", "a/b", strings.Repeat("x", 65)} {
		if _, err := NewWasmTool(bad, "", nil, wasmrttest.Nop(), rt); !errors.Is(err, ErrInvalidToolName) {
			t.Errorf("%q: expected ErrInvalidToolName, got %v", bad, err)
		}
	}
	if _, err := NewWasmTool("x", "", nil, wasmrttest.Import("env", "f"), rt); !errors.Is(err, wasmrt.ErrInvalidModule) {
		t.Fatalf("expected ErrInvalidModule, got %v", err)
	}
	if _, err := NewWasmTool("x", "", nil, wasmrttest.Nop(), nil); !errors.Is(err, ErrWASMRuntimeUnavailable) {
		t.Fatalf("expected ErrWASMRuntimeUnavailable, got %v", err)
	}
}

func TestWasmToolGoGuestAndHostTool(t *testing.T) {
	guest := wasmrttest.Guest(t)
	h := newTestHost(t, WasmHostOptions{Timeout: 20 * time.Second})
	if err := h.LoadPlugin(context.Background(), "upper", guest); err != nil {
		t.Fatal(err)
	}
	tool, err := h.Tool("upper", "upper_case", "Upper-cases text", nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := tool.Execute(context.Background(), map[string]interface{}{"mode": "upper", "text": "abc"})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := out.(map[string]interface{})
	if m["text"] != "ABC" || m["len"] != float64(3) {
		t.Fatalf("unexpected tool output %v", out)
	}
	if _, err := h.Tool("missing", "x", "", nil); !errors.Is(err, ErrPluginNotFound) {
		t.Fatalf("expected ErrPluginNotFound, got %v", err)
	}
	if _, err := h.Tool("upper", "bad name", "", nil); !errors.Is(err, ErrInvalidToolName) {
		t.Fatalf("expected ErrInvalidToolName, got %v", err)
	}
	_ = h.Close(context.Background())
	if _, err := tool.Execute(context.Background(), nil); !errors.Is(err, wasmrt.ErrClosed) {
		t.Fatalf("tool must fail once its host's runtime is closed, got %v", err)
	}
}
