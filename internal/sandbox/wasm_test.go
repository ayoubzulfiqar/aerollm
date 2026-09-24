package sandbox

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt/wasmrttest"
)

func newWasmExec(t *testing.T, l Limits) *WasmExecutor {
	t.Helper()
	e, err := NewWasmExecutorWithLimits(l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestWasmExecutorFixtures(t *testing.T) {
	e := newWasmExec(t, Limits{Timeout: 200 * time.Millisecond, MaxArgBytes: 64, MaxOutputBytes: 32, MaxMemoryBytes: 4 * wasmrt.PageSize})
	ctx := context.Background()
	must := func(name string, wasm []byte) {
		t.Helper()
		if err := e.Register(name, wasm); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}
	must("answer", wasmrttest.Hello(`{"answer":42}`))
	must("nop", wasmrttest.Nop())
	must("text", wasmrttest.Hello("plain text"))
	must("big", wasmrttest.Hello(strings.Repeat("x", 33)))
	must("spin", wasmrttest.Loop())
	must("fail", wasmrttest.Exit(5))
	must("grow", wasmrttest.Grow(8))

	res, err := ExecuteAgentTool(ctx, e, models.ToolDefinition{Name: "answer"}, map[string]interface{}{"q": "?"})
	if err != nil || res.Content.(map[string]interface{})["answer"] != float64(42) || res.Duration < 0 {
		t.Fatalf("answer: %v %+v", err, res)
	}
	if out, err := e.Execute(ctx, "nop", nil); err != nil || out != nil {
		t.Fatalf("nop: %v %v", out, err)
	}
	if _, err := e.Execute(ctx, "text", nil); !errors.Is(err, ErrInvalidOutput) {
		t.Fatalf("expected ErrInvalidOutput, got %v", err)
	}
	if _, err := e.Execute(ctx, "big", nil); !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("expected ErrOutputTooLarge, got %v", err)
	}
	if _, err := e.Execute(ctx, "answer", map[string]interface{}{"q": strings.Repeat("a", 100)}); !errors.Is(err, ErrArgumentsTooLarge) {
		t.Fatalf("expected ErrArgumentsTooLarge, got %v", err)
	}
	start := time.Now()
	if _, err := e.Execute(ctx, "spin", nil); !errors.Is(err, ErrTimeout) || time.Since(start) > 5*time.Second {
		t.Fatalf("expected prompt ErrTimeout, got %v after %s", err, time.Since(start))
	}
	var ee *wasmrt.ExitError
	if _, err := e.Execute(ctx, "fail", nil); !errors.As(err, &ee) || ee.Code != 5 {
		t.Fatalf("expected ExitError(5), got %v", err)
	}
	if _, err := e.Execute(ctx, "grow", nil); !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("expected ErrMemoryLimit with a 4-page cap, got %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := e.Execute(cctx, "answer", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := e.Tools(); len(got) != 7 || got[0] != "answer" {
		t.Fatalf("unexpected tools %v", got)
	}
	if !e.Unregister("answer") || e.Unregister("answer") {
		t.Fatal("Unregister must report existence")
	}
	if _, err := e.Execute(ctx, "answer", nil); !errors.Is(err, ErrUnknownTool) {
		t.Fatalf("expected ErrUnknownTool after unregister, got %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(ctx, "nop", nil); !errors.Is(err, ErrExecutorClosed) {
		t.Fatalf("expected ErrExecutorClosed, got %v", err)
	}
	if err := e.Register("nop2", wasmrttest.Nop()); !errors.Is(err, ErrExecutorClosed) {
		t.Fatalf("expected ErrExecutorClosed, got %v", err)
	}
}

func TestWasmExecutorRunPayload(t *testing.T) {
	e := newWasmExec(t, Limits{Timeout: 5 * time.Second})
	ctx := context.Background()
	res, err := e.RunPayload(ctx, WasmToolPayload{Module: wasmrttest.Hello("hi"), Input: "ignored"})
	if err != nil || string(res.Stdout) != "hi" {
		t.Fatalf("payload: %v %q", err, res.Stdout)
	}
	if _, err := e.RunPayload(ctx, WasmToolPayload{Module: wasmrttest.Loop(), Timeout: 50 * time.Millisecond}); !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if _, err := e.RunPayload(ctx, WasmToolPayload{Module: wasmrttest.Grow(8), MaxMemory: 2 * wasmrt.PageSize}); !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("expected ErrMemoryLimit, got %v", err)
	}
	if _, err := e.RunPayload(ctx, WasmToolPayload{Module: wasmrttest.Grow(8), MaxMemory: 16 * wasmrt.PageSize}); err != nil {
		t.Fatalf("growth within the requested memory must succeed: %v", err)
	}
	if _, err := e.RunPayload(ctx, WasmToolPayload{Module: []byte("#!/bin/sh")}); err == nil {
		t.Fatal("expected invalid payload to be rejected")
	}
}

func TestWasmExecutorSharedRuntimeAndConcurrency(t *testing.T) {
	rt, err := wasmrt.New(wasmrt.Config{MaxConcurrent: 1, QueueTimeout: 20 * time.Millisecond, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	e := NewWasmExecutorWithRuntime(rt, Limits{Timeout: 2 * time.Second})
	if err := e.Register("spin", wasmrttest.Loop()); err != nil {
		t.Fatal(err)
	}
	if err := e.Register("nop", wasmrttest.Nop()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = e.Execute(ctx, "spin", nil)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for rt.Stats().Active == 0 {
		if time.Now().After(deadline) {
			t.Fatal("spin never started")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := e.Execute(context.Background(), "nop", nil); !errors.Is(err, ErrBusy) {
		t.Fatalf("expected ErrBusy, got %v", err)
	}
	cancel()
	wg.Wait()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Run(context.Background(), wasmrttest.Nop(), nil); err != nil {
		t.Fatalf("shared runtime must survive executor Close: %v", err)
	}
	if _, err := NewWasmExecutorWithRuntime(nil, Limits{}).Execute(context.Background(), "x", nil); !errors.Is(err, ErrWasmRuntimeUnavailable) {
		t.Fatalf("expected ErrWasmRuntimeUnavailable, got %v", err)
	}
}

func TestWasmExecutorGoGuest(t *testing.T) {
	guest := wasmrttest.Guest(t)
	// Go wasip1 guests need ~35 MiB of initial memory; "alloc" still hits
	// the cap because it allocates without bound.
	e := newWasmExec(t, Limits{Timeout: 20 * time.Second, MaxOutputBytes: 128 << 10, MaxMemoryBytes: 128 << 20})
	if err := e.Register("guest", guest); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	args := map[string]interface{}{"mode": "upper", "text": "sandbox"}
	out, err := e.Execute(ctx, "guest", args)
	if err != nil {
		t.Fatal(err)
	}
	if m := out.(map[string]interface{}); m["text"] != "SANDBOX" {
		t.Fatalf("unexpected output %v", out)
	}
	cases := []struct {
		mode string
		want error
	}{
		{"alloc", wasmrt.ErrMemoryLimit},
		{"bigout", ErrOutputTooLarge},
		{"not-json", ErrInvalidOutput},
	}
	for _, c := range cases {
		if _, err := e.Execute(ctx, "guest", map[string]interface{}{"mode": c.mode}); !errors.Is(err, c.want) {
			t.Errorf("%s: expected %v, got %v", c.mode, c.want, err)
		}
	}
	var ee *wasmrt.ExitError
	if _, err := e.Execute(ctx, "guest", map[string]interface{}{"mode": "exit"}); !errors.As(err, &ee) || ee.Code != 3 {
		t.Fatalf("expected ExitError(3), got %v", err)
	}
	if _, err := e.Execute(ctx, "guest", map[string]interface{}{"mode": "panic"}); !errors.As(err, &ee) || ee.Code != 2 {
		t.Fatalf("expected guest panic as ExitError(2), got %v", err)
	}
	out, err = e.Execute(ctx, "guest", map[string]interface{}{"mode": "fs"})
	if err != nil {
		t.Fatal(err)
	}
	if esc, _ := out.(map[string]interface{})["escapes"].([]interface{}); len(esc) != 0 {
		t.Fatalf("guest reached the filesystem: %v", esc)
	}
	short := newWasmExec(t, Limits{Timeout: 300 * time.Millisecond})
	if err := short.Register("guest", guest); err != nil {
		t.Fatal(err)
	}
	if _, err := short.Execute(ctx, "guest", map[string]interface{}{"mode": "loop"}); !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}
