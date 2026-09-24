package sandbox

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt/wasmrttest"
)

func TestWasmExecutorRejectsBadCalls(t *testing.T) {
	e := NewWasmExecutor()
	defer e.Close()
	out, err := e.Execute(context.Background(), "echo", map[string]interface{}{"text": "hi"})
	if !errors.Is(err, ErrUnknownTool) || out != nil {
		t.Fatalf("expected ErrUnknownTool for an unregistered tool, got %v %v", out, err)
	}
	if _, err := e.Execute(context.Background(), "", nil); err == nil {
		t.Fatal("empty tool name must fail")
	}
	if _, err := e.Execute(context.Background(), "../../bin/sh", nil); !errors.Is(err, ErrInvalidToolName) {
		t.Fatalf("path-like tool name must be rejected, got %v", err)
	}
	if err := e.Register("bad name", wasmrttest.Nop()); !errors.Is(err, ErrInvalidToolName) {
		t.Fatalf("expected ErrInvalidToolName, got %v", err)
	}
	if err := e.Register("evil", wasmrttest.Import("env", "system")); !errors.Is(err, wasmrt.ErrInvalidModule) {
		t.Fatalf("expected ErrInvalidModule, got %v", err)
	}
	if err := e.Register("junk", []byte("\x00asm\x01\x00\x00\x00garbage")); !errors.Is(err, wasmrt.ErrInvalidModule) {
		t.Fatalf("expected ErrInvalidModule, got %v", err)
	}
	if got := e.Tools(); len(got) != 0 {
		t.Fatalf("no tools expected, got %v", got)
	}
}

func TestWasmPayloadValidate(t *testing.T) {
	good := WasmToolPayload{Module: []byte("\x00asm\x01\x00\x00\x00")}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []WasmToolPayload{
		{Module: []byte("#!/bin/sh")},
		{Module: []byte("\x00asm\x0d\x00\x01\x00")}, // component-model binary
		{Module: good.Module, MaxMemory: MaxWasmMemory + 1},
		{Module: good.Module, Timeout: -1},
	} {
		if bad.Validate() == nil {
			t.Errorf("expected validation error for %+v", bad)
		}
	}
}

func TestExecuteAgentToolNilExecutor(t *testing.T) {
	_, err := ExecuteAgentTool(context.Background(), nil, models.ToolDefinition{Name: "echo"}, nil)
	if err == nil {
		t.Fatal("expected error for nil executor")
	}
}

func TestFuncExecutor(t *testing.T) {
	e := NewFuncExecutor(Limits{Timeout: 50 * time.Millisecond, MaxConcurrent: 2, MaxArgBytes: 64, MaxOutputBytes: 64, QueueTimeout: 20 * time.Millisecond})
	_ = e.Register("echo", func(ctx context.Context, args map[string]interface{}) (interface{}, error) { return args["text"], nil })
	_ = e.Register("sleep", func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	_ = e.Register("panic", func(ctx context.Context, args map[string]interface{}) (interface{}, error) { panic("kaboom") })
	_ = e.Register("big", func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
		return strings.Repeat("x", 100), nil
	})
	_ = e.Register("mutate", func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
		args["text"] = "changed"
		return nil, nil
	})
	if err := e.Register("bad name", nil); !errors.Is(err, ErrInvalidToolName) {
		t.Fatalf("expected invalid name, got %v", err)
	}

	ctx := context.Background()
	res, err := ExecuteAgentTool(ctx, e, models.ToolDefinition{Name: "echo"}, map[string]interface{}{"text": "hi"})
	if err != nil || res.Content != "hi" || res.Duration <= 0 {
		t.Fatalf("echo: %v %+v", err, res)
	}
	if _, err := e.Execute(ctx, "missing", nil); !errors.Is(err, ErrUnknownTool) {
		t.Fatalf("unknown: %v", err)
	}
	if _, err := e.Execute(ctx, "sleep", nil); !errors.Is(err, ErrTimeout) {
		t.Fatalf("timeout: %v", err)
	}
	if _, err := e.Execute(ctx, "panic", nil); !errors.Is(err, ErrToolPanicked) {
		t.Fatalf("panic isolation: %v", err)
	}
	if _, err := e.Execute(ctx, "big", nil); !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("output limit: %v", err)
	}
	if _, err := e.Execute(ctx, "echo", map[string]interface{}{"text": strings.Repeat("a", 100)}); !errors.Is(err, ErrArgumentsTooLarge) {
		t.Fatalf("arg limit: %v", err)
	}
	args := map[string]interface{}{"text": "orig"}
	_, _ = e.Execute(ctx, "mutate", args)
	if args["text"] != "orig" {
		t.Fatal("tools must receive a copy of the arguments")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := e.Execute(cctx, "sleep", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: %v", err)
	}
}

func TestFuncExecutorConcurrencyLimit(t *testing.T) {
	e := NewFuncExecutor(Limits{Timeout: time.Second, MaxConcurrent: 2, QueueTimeout: 10 * time.Millisecond})
	var running, peak int32
	release := make(chan struct{})
	_ = e.Register("block", func(ctx context.Context, _ map[string]interface{}) (interface{}, error) {
		n := atomic.AddInt32(&running, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		<-release
		atomic.AddInt32(&running, -1)
		return "ok", nil
	})
	var wg sync.WaitGroup
	var busy int32
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.Execute(context.Background(), "block", nil); errors.Is(err, ErrBusy) {
				atomic.AddInt32(&busy, 1)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if peak > 2 {
		t.Fatalf("concurrency limit exceeded: peak %d", peak)
	}
	if busy == 0 {
		t.Fatal("expected some calls to be rejected as busy")
	}
}
