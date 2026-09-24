package wasmrt_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt/wasmrttest"
)

func newRT(t *testing.T, cfg wasmrt.Config) *wasmrt.Runtime {
	t.Helper()
	rt, err := wasmrt.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

func TestConfigValidation(t *testing.T) {
	if _, err := wasmrt.New(wasmrt.Config{MaxMemoryPages: 65537}); err == nil {
		t.Fatal("expected MaxMemoryPages > 65536 to be rejected")
	}
	if _, err := wasmrt.New(wasmrt.Config{Timeout: -1}); err == nil {
		t.Fatal("expected negative timeout to be rejected")
	}
	d := wasmrt.DefaultConfig()
	if d.MaxMemoryPages != wasmrt.DefaultMaxMemoryPages || d.Timeout != wasmrt.DefaultTimeout || d.MaxConcurrent < 2 {
		t.Fatalf("unexpected defaults %+v", d)
	}
}

func TestRejectsInvalidModules(t *testing.T) {
	rt := newRT(t, wasmrt.Config{MaxModuleBytes: 1 << 10})
	ctx := context.Background()
	cases := map[string][]byte{
		"empty":        nil,
		"pe":           []byte("MZ\x90\x00\x03\x00\x00\x00"),
		"text format":  []byte("(module (func (export \"_start\")))"),
		"component":    []byte("\x00asm\x0d\x00\x01\x00"),
		"truncated":    []byte("\x00asm\x01\x00"),
		"garbage":      append([]byte("\x00asm\x01\x00\x00\x00"), 0xff, 0xff, 0xff),
		"no _start":    wasmrttest.NoStart(),
		"env import":   wasmrttest.Import("env", "evil"),
		"bad wasi fn":  wasmrttest.Import("wasi_snapshot_preview1", "definitely_not_wasi"),
		"bad wasi sig": wasmrttest.Import("wasi_snapshot_preview1", "fd_write"), // ()->() instead of (i32 x4)->i32
	}
	for name, b := range cases {
		if _, err := rt.Compile(ctx, b); !errors.Is(err, wasmrt.ErrInvalidModule) {
			t.Errorf("%s: expected ErrInvalidModule, got %v", name, err)
		}
		if _, err := rt.Run(ctx, b, nil); !errors.Is(err, wasmrt.ErrInvalidModule) {
			t.Errorf("%s: Run: expected ErrInvalidModule, got %v", name, err)
		}
	}
	big := append(wasmrttest.Nop(), make([]byte, 2<<10)...)
	if _, err := rt.Compile(ctx, big); !errors.Is(err, wasmrt.ErrModuleTooLarge) {
		t.Fatalf("expected ErrModuleTooLarge, got %v", err)
	}
	if err := wasmrt.CheckHeader(wasmrttest.Nop()); err != nil {
		t.Fatal(err)
	}
	if st := rt.Stats(); st.CachedModules != 0 {
		t.Fatalf("rejected modules must not be cached: %+v", st)
	}
}

func TestRunHelloAndExitCodes(t *testing.T) {
	rt := newRT(t, wasmrt.Config{})
	ctx := context.Background()
	res, err := rt.Run(ctx, wasmrttest.Hello(`{"ok":true}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Stdout) != `{"ok":true}` || res.ExitCode != 0 || res.PeakMemoryBytes == 0 {
		t.Fatalf("unexpected result %+v", res)
	}
	if res, err := rt.Run(ctx, wasmrttest.Nop(), []byte("ignored")); err != nil || len(res.Stdout) != 0 {
		t.Fatalf("nop: %v %+v", err, res)
	}
	if _, err := rt.Run(ctx, wasmrttest.Exit(0), nil); err != nil {
		t.Fatalf("exit(0) must succeed: %v", err)
	}
	res, err = rt.Run(ctx, wasmrttest.Exit(3), nil)
	var ee *wasmrt.ExitError
	if !errors.As(err, &ee) || ee.Code != 3 || res.ExitCode != 3 {
		t.Fatalf("expected ExitError code 3, got %v (%+v)", err, res)
	}
}

func TestTimeoutInterruptsTightLoop(t *testing.T) {
	rt := newRT(t, wasmrt.Config{Timeout: 100 * time.Millisecond})
	start := time.Now()
	_, err := rt.Run(context.Background(), wasmrttest.Loop(), nil)
	if !errors.Is(err, wasmrt.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("timeout not enforced promptly: %s", d)
	}
	// Per-run override.
	m, err := rt.Compile(context.Background(), wasmrttest.Loop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.RunModule(context.Background(), m, nil, wasmrt.RunOptions{Timeout: 20 * time.Millisecond}); !errors.Is(err, wasmrt.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestCallerCancellationAndDeadline(t *testing.T) {
	rt := newRT(t, wasmrt.Config{Timeout: 10 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	if _, err := rt.Run(ctx, wasmrttest.Loop(), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	dctx, dcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer dcancel()
	if _, err := rt.Run(dctx, wasmrttest.Loop(), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if _, err := rt.Run(ctx, wasmrttest.Nop(), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("already-cancelled context must fail fast, got %v", err)
	}
}

func TestMemoryCap(t *testing.T) {
	rt := newRT(t, wasmrt.Config{MaxMemoryPages: 16})
	ctx := context.Background()
	if _, err := rt.Run(ctx, wasmrttest.Grow(10), nil); err != nil {
		t.Fatalf("growth within the cap must succeed: %v", err)
	}
	for _, pages := range []uint32{16, 31, 100, 65535} { // 1 + pages > 16
		_, err := rt.Run(ctx, wasmrttest.Grow(pages), nil)
		if !errors.Is(err, wasmrt.ErrMemoryLimit) && !errors.Is(err, wasmrt.ErrTrap) {
			t.Fatalf("grow(%d): expected memory limit / trap, got %v", pages, err)
		}
		if pages < 32 && !errors.Is(err, wasmrt.ErrMemoryLimit) {
			t.Fatalf("grow(%d): expected ErrMemoryLimit, got %v", pages, err)
		}
	}
	if _, err := rt.Compile(ctx, wasmrttest.MemoryMin(17)); !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("initial memory above the cap must be rejected, got %v", err)
	}
	// A per-run cap can only lower the limit.
	m, err := rt.Compile(ctx, wasmrttest.Grow(4))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.RunModule(ctx, m, nil, wasmrt.RunOptions{MaxMemoryPages: 2}); !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("expected per-run ErrMemoryLimit, got %v", err)
	}
	if _, err := rt.RunModule(ctx, m, nil, wasmrt.RunOptions{MaxMemoryPages: 1000}); err != nil {
		t.Fatalf("per-run cap above the runtime cap is clamped, run must succeed: %v", err)
	}
	mm, err := rt.Compile(ctx, wasmrttest.MemoryMin(8))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.RunModule(ctx, mm, nil, wasmrt.RunOptions{MaxMemoryPages: 4}); !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("initial memory above the per-run cap must fail, got %v", err)
	}
}

func TestInputAndOutputCaps(t *testing.T) {
	rt := newRT(t, wasmrt.Config{MaxInputBytes: 8, MaxOutputBytes: 16})
	ctx := context.Background()
	if _, err := rt.Run(ctx, wasmrttest.Nop(), make([]byte, 9)); !errors.Is(err, wasmrt.ErrInputTooLarge) {
		t.Fatalf("expected ErrInputTooLarge, got %v", err)
	}
	if _, err := rt.Run(ctx, wasmrttest.Hello(strings.Repeat("x", 17)), nil); !errors.Is(err, wasmrt.ErrOutputTooLarge) {
		t.Fatalf("expected ErrOutputTooLarge, got %v", err)
	}
	if res, err := rt.Run(ctx, wasmrttest.Hello(strings.Repeat("x", 16)), nil); err != nil || len(res.Stdout) != 16 {
		t.Fatalf("output at the cap must succeed: %v", err)
	}
}

func TestRunOptionsValidation(t *testing.T) {
	rt := newRT(t, wasmrt.Config{})
	m, err := rt.Compile(context.Background(), wasmrttest.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []wasmrt.RunOptions{
		{Env: map[string]string{"A=B": "x"}},
		{Env: map[string]string{"": "x"}},
		{Env: map[string]string{"A": "x\x00y"}},
		{Args: []string{""}},
		{Timeout: -1},
	} {
		if _, err := rt.RunModule(context.Background(), m, nil, o); !errors.Is(err, wasmrt.ErrInvalidOptions) {
			t.Errorf("%+v: expected ErrInvalidOptions, got %v", o, err)
		}
	}
	if _, err := rt.RunModule(context.Background(), nil, nil, wasmrt.RunOptions{}); !errors.Is(err, wasmrt.ErrInvalidModule) {
		t.Fatalf("nil module: %v", err)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	rt := newRT(t, wasmrt.Config{MaxConcurrent: 1, QueueTimeout: 20 * time.Millisecond, Timeout: 2 * time.Second})
	loop, err := rt.Compile(context.Background(), wasmrttest.Loop())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rt.RunModule(ctx, loop, nil, wasmrt.RunOptions{})
		done <- err
	}()
	waitActive(t, rt, 1)
	if _, err := rt.Run(context.Background(), wasmrttest.Nop(), nil); !errors.Is(err, wasmrt.ErrBusy) {
		t.Fatalf("expected ErrBusy, got %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled loop, got %v", err)
	}
	if _, err := rt.Run(context.Background(), wasmrttest.Nop(), nil); err != nil {
		t.Fatalf("slot must be released: %v", err)
	}
}

// waitActive waits until n operations hold a concurrency slot.
func waitActive(t *testing.T, rt *wasmrt.Runtime, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for rt.Stats().Active < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d active runs: %+v", n, rt.Stats())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRuntimesSharingCompiledCodeKeepTheirOwnLimits(t *testing.T) {
	big := newRT(t, wasmrt.Config{MaxMemoryPages: 64})
	small := newRT(t, wasmrt.Config{MaxMemoryPages: 16}) // 1+20 pages: over the cap, under the 2x ceiling
	grow := wasmrttest.Grow(20)
	if _, err := big.Run(context.Background(), grow, nil); err != nil {
		t.Fatalf("big runtime: %v", err)
	}
	if _, err := small.Run(context.Background(), grow, nil); !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("small runtime must enforce its own cap, got %v", err)
	}
	_ = big.Close()
	// The engine is reference-counted: closing one runtime must not break
	// another that still uses the same compiled code.
	if _, err := small.Run(context.Background(), wasmrttest.Grow(2), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := small.Run(context.Background(), grow, nil); !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("got %v", err)
	}
}

func TestCompiledCacheIsBoundedAndKeyedByContent(t *testing.T) {
	rt := newRT(t, wasmrt.Config{CacheSize: 2})
	ctx := context.Background()
	a, err := rt.Compile(ctx, wasmrttest.Hello("a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Compile(ctx, wasmrttest.Hello("a")); err != nil {
		t.Fatal(err)
	}
	if st := rt.Stats(); st.CacheMisses != 1 || st.CacheHits != 1 || st.CachedModules != 1 {
		t.Fatalf("expected one compilation for identical bytes, got %+v", st)
	}
	for _, s := range []string{"b", "c"} {
		if _, err := rt.Run(ctx, wasmrttest.Hello(s), nil); err != nil {
			t.Fatal(err)
		}
	}
	if st := rt.Stats(); st.CachedModules != 2 || st.CacheMisses != 3 {
		t.Fatalf("cache must stay bounded: %+v", st)
	}
	// "a" was evicted; its handle transparently recompiles.
	res, err := rt.RunModule(ctx, a, nil, wasmrt.RunOptions{})
	if err != nil || string(res.Stdout) != "a" {
		t.Fatalf("evicted module: %v %q", err, res.Stdout)
	}
	if len(a.Hash()) != 64 || a.Size() == 0 {
		t.Fatalf("bad module metadata %q %d", a.Hash(), a.Size())
	}
}

func TestCallerBufferIsNotRetained(t *testing.T) {
	rt := newRT(t, wasmrt.Config{})
	ctx := context.Background()
	buf := wasmrttest.Hello("original")
	if res, err := rt.Run(ctx, buf, nil); err != nil || string(res.Stdout) != "original" {
		t.Fatalf("%v %q", err, res.Stdout)
	}
	orig := bytes.Clone(buf)
	copy(buf[bytes.Index(buf, []byte("original")):], "CLOBBER!")
	if res, err := rt.Run(ctx, orig, nil); err != nil || string(res.Stdout) != "original" {
		t.Fatalf("cached module must not alias the caller's buffer: %v %q", err, res.Stdout)
	}
	m, err := rt.Compile(ctx, orig)
	if err != nil {
		t.Fatal(err)
	}
	for i := range orig {
		orig[i] = 0
	}
	if res, err := rt.RunModule(ctx, m, nil, wasmrt.RunOptions{}); err != nil || string(res.Stdout) != "original" {
		t.Fatalf("module handle must own its bytes: %v %q", err, res.Stdout)
	}
}

func TestCloseInterruptsAndRejects(t *testing.T) {
	rt, err := wasmrt.New(wasmrt.Config{Timeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := rt.Run(context.Background(), wasmrttest.Loop(), nil)
		done <- err
	}()
	waitActive(t, rt, 1)
	start := time.Now()
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, wasmrt.ErrClosed) {
		t.Fatalf("expected in-flight run to fail with ErrClosed, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("Close did not interrupt the run promptly")
	}
	if _, err := rt.Run(context.Background(), wasmrttest.Nop(), nil); !errors.Is(err, wasmrt.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if _, err := rt.Compile(context.Background(), wasmrttest.Nop()); !errors.Is(err, wasmrt.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("Close must be idempotent: %v", err)
	}
}

func TestExitErrorSanitizesStderr(t *testing.T) {
	e := &wasmrt.ExitError{Code: 2, Stderr: "line1\nline2\x1b[31m\x00" + strings.Repeat("z", 2000)}
	msg := e.Error()
	if strings.ContainsAny(msg, "\n\x1b\x00") {
		t.Fatalf("control characters leaked into error: %q", msg)
	}
	if len(msg) > 700 || !strings.Contains(msg, "code 2: line1 line2[31m") {
		t.Fatalf("unexpected message %q", msg)
	}
}

// ---- tests with the real Go (wasip1) guest ----

type guestFixture struct {
	rt  *wasmrt.Runtime
	mod *wasmrt.Module
}

func guest(t *testing.T, cfg wasmrt.Config) guestFixture {
	t.Helper()
	wasm := wasmrttest.Guest(t)
	rt := newRT(t, cfg)
	m, err := rt.Compile(context.Background(), wasm)
	if err != nil {
		t.Fatal(err)
	}
	return guestFixture{rt: rt, mod: m}
}

func (g guestFixture) run(t *testing.T, req map[string]interface{}, opts wasmrt.RunOptions) (wasmrt.Result, error) {
	t.Helper()
	in, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return g.rt.RunModule(context.Background(), g.mod, in, opts)
}

func decode(t *testing.T, b []byte) map[string]interface{} {
	t.Helper()
	var out map[string]interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("stdout is not JSON: %v: %q", err, b)
	}
	return out
}

func TestGuestJSONRoundTrip(t *testing.T) {
	g := guest(t, wasmrt.Config{Timeout: 30 * time.Second})
	res, err := g.run(t, map[string]interface{}{"mode": "upper", "text": "héllo"}, wasmrt.RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	out := decode(t, res.Stdout)
	if out["text"] != "HÉLLO" || out["len"] != float64(len("héllo")) {
		t.Fatalf("unexpected output %v", out)
	}
	res, err = g.run(t, map[string]interface{}{"mode": "echo", "k": []int{1, 2}}, wasmrt.RunOptions{})
	if err != nil || decode(t, res.Stdout)["k"] == nil {
		t.Fatalf("echo: %v %q", err, res.Stdout)
	}
}

func TestGuestConcurrentRuns(t *testing.T) {
	g := guest(t, wasmrt.Config{Timeout: 60 * time.Second, MaxConcurrent: 4, QueueTimeout: time.Minute})
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			text := fmt.Sprintf("run-%d", i)
			in, _ := json.Marshal(map[string]interface{}{"mode": "upper", "text": text})
			res, err := g.rt.RunModule(context.Background(), g.mod, in, wasmrt.RunOptions{})
			if err != nil {
				errs <- err
				return
			}
			var out map[string]interface{}
			if err := json.Unmarshal(res.Stdout, &out); err != nil || out["text"] != strings.ToUpper(text) {
				errs <- fmt.Errorf("run %d: %v %q", i, err, res.Stdout)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestGuestInfiniteLoopTimesOut(t *testing.T) {
	g := guest(t, wasmrt.Config{Timeout: 30 * time.Second})
	start := time.Now()
	_, err := g.run(t, map[string]interface{}{"mode": "loop"}, wasmrt.RunOptions{Timeout: 300 * time.Millisecond})
	if !errors.Is(err, wasmrt.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("loop was not interrupted promptly")
	}
}

func TestGuestHugeAllocationHitsMemoryCap(t *testing.T) {
	g := guest(t, wasmrt.Config{Timeout: 30 * time.Second, MaxMemoryPages: 1024})
	res, err := g.run(t, map[string]interface{}{"mode": "alloc"}, wasmrt.RunOptions{})
	if !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("expected ErrMemoryLimit, got %v", err)
	}
	if res.PeakMemoryBytes > 1024*wasmrt.PageSize {
		t.Fatalf("memory exceeded the cap: %d", res.PeakMemoryBytes)
	}
}

func TestGuestHugeStdoutHitsOutputCap(t *testing.T) {
	g := guest(t, wasmrt.Config{Timeout: 30 * time.Second, MaxOutputBytes: 256 << 10})
	res, err := g.run(t, map[string]interface{}{"mode": "bigout"}, wasmrt.RunOptions{})
	if !errors.Is(err, wasmrt.ErrOutputTooLarge) {
		t.Fatalf("expected ErrOutputTooLarge, got %v", err)
	}
	if len(res.Stdout) > 256<<10 {
		t.Fatalf("stdout exceeded the cap: %d", len(res.Stdout))
	}
}

func TestGuestStderrIsCapped(t *testing.T) {
	g := guest(t, wasmrt.Config{Timeout: 30 * time.Second, MaxStderrBytes: 4 << 10})
	res, err := g.run(t, map[string]interface{}{"mode": "bigerr", "n": 64}, wasmrt.RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stderr) != 4<<10 || !res.StderrTruncated {
		t.Fatalf("stderr must be truncated to the cap: %d %v", len(res.Stderr), res.StderrTruncated)
	}
	if decode(t, res.Stdout)["ok"] != true {
		t.Fatalf("unexpected stdout %q", res.Stdout)
	}
}

func TestGuestNonZeroExitAndPanic(t *testing.T) {
	g := guest(t, wasmrt.Config{Timeout: 30 * time.Second})
	_, err := g.run(t, map[string]interface{}{"mode": "exit"}, wasmrt.RunOptions{})
	var ee *wasmrt.ExitError
	if !errors.As(err, &ee) || ee.Code != 3 || !strings.Contains(ee.Stderr, "refusing") {
		t.Fatalf("expected exit code 3 with stderr, got %v", err)
	}
	_, err = g.run(t, map[string]interface{}{"mode": "panic"}, wasmrt.RunOptions{})
	if !errors.As(err, &ee) || ee.Code != 2 || !strings.Contains(ee.Stderr, "guest panic") {
		t.Fatalf("expected Go panic to surface as exit code 2, got %v", err)
	}
	if strings.Contains(err.Error(), "\n") {
		t.Fatalf("error message must be single-line: %q", err.Error())
	}
	// The runtime stays usable after guest failures.
	if _, err := g.run(t, map[string]interface{}{"mode": "upper", "text": "ok"}, wasmrt.RunOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestGuestHasNoFilesystem(t *testing.T) {
	g := guest(t, wasmrt.Config{Timeout: 30 * time.Second})
	res, err := g.run(t, map[string]interface{}{"mode": "fs"}, wasmrt.RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	out := decode(t, res.Stdout)
	if esc, _ := out["escapes"].([]interface{}); len(esc) != 0 {
		t.Fatalf("guest reached the filesystem: %v", esc)
	}
}

func TestGuestEnvAndArgsAreExplicit(t *testing.T) {
	t.Setenv("WASMRT_SECRET", "hunter2")
	g := guest(t, wasmrt.Config{Timeout: 30 * time.Second})
	res, err := g.run(t, map[string]interface{}{"mode": "env"}, wasmrt.RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(res.Stdout), "hunter2") {
		t.Fatal("host environment leaked into the guest")
	}
	out := decode(t, res.Stdout)
	if env, _ := out["env"].([]interface{}); len(env) != 0 {
		t.Fatalf("expected empty env, got %v", env)
	}
	if args, _ := out["args"].([]interface{}); len(args) != 1 || args[0] != "module" {
		t.Fatalf("expected argv [module], got %v", out["args"])
	}
	res, err = g.run(t, map[string]interface{}{"mode": "env"}, wasmrt.RunOptions{
		Args: []string{"tool", "--flag"}, Env: map[string]string{"MODE": "test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	out = decode(t, res.Stdout)
	if fmt.Sprint(out["args"]) != "[tool --flag]" || fmt.Sprint(out["env"]) != "[MODE=test]" {
		t.Fatalf("explicit args/env not passed: %v", out)
	}
}

func TestGuestClockAndRandomAreDeterministicByDefault(t *testing.T) {
	g := guest(t, wasmrt.Config{Timeout: 30 * time.Second})
	run := func(g guestFixture, mode string) string {
		res, err := g.run(t, map[string]interface{}{"mode": mode}, wasmrt.RunOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return string(res.Stdout)
	}
	if a, b := run(g, "random"), run(g, "random"); a != b {
		t.Fatalf("default random source must be deterministic: %s vs %s", a, b)
	}
	var clock struct{ Unix int64 }
	_ = json.Unmarshal([]byte(run(g, "clock")), &clock)
	if now := time.Now().Unix(); clock.Unix > now-3600 && clock.Unix < now+3600 {
		t.Fatalf("default clock must not expose host time, got %d", clock.Unix)
	}

	real := guest(t, wasmrt.Config{Timeout: 30 * time.Second, AllowClock: true, AllowRandom: true})
	if a, b := run(real, "random"), run(real, "random"); a == b {
		t.Fatal("AllowRandom must use a real entropy source")
	}
	_ = json.Unmarshal([]byte(run(real, "clock")), &clock)
	if now := time.Now().Unix(); clock.Unix < now-3600 || clock.Unix > now+3600 {
		t.Fatalf("AllowClock must expose host time, got %d", clock.Unix)
	}
	// Real sleeps are still bounded by the run deadline.
	start := time.Now()
	_, err := real.run(t, map[string]interface{}{"mode": "sleep", "n": 60000}, wasmrt.RunOptions{Timeout: 300 * time.Millisecond})
	if !errors.Is(err, wasmrt.ErrTimeout) || time.Since(start) > 10*time.Second {
		t.Fatalf("sleep must be interrupted by the deadline: %v after %s", err, time.Since(start))
	}
}

func TestInterpreterEngine(t *testing.T) {
	rt := newRT(t, wasmrt.Config{Interpreter: true, Timeout: 100 * time.Millisecond, MaxMemoryPages: 16})
	ctx := context.Background()
	if err := rt.Validate(ctx, wasmrttest.Hello("x")); err != nil {
		t.Fatal(err)
	}
	if err := rt.Validate(ctx, wasmrttest.Import("env", "f")); !errors.Is(err, wasmrt.ErrInvalidModule) {
		t.Fatalf("expected ErrInvalidModule, got %v", err)
	}
	if res, err := rt.Run(ctx, wasmrttest.Hello("interp"), nil); err != nil || string(res.Stdout) != "interp" {
		t.Fatalf("%v %q", err, res.Stdout)
	}
	if _, err := rt.Run(ctx, wasmrttest.Loop(), nil); !errors.Is(err, wasmrt.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if _, err := rt.Run(ctx, wasmrttest.Grow(20), nil); !errors.Is(err, wasmrt.ErrMemoryLimit) {
		t.Fatalf("expected ErrMemoryLimit, got %v", err)
	}
}
