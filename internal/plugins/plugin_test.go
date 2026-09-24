package plugins

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestInMemoryRegistryLifecycle(t *testing.T) {
	reg := NewInMemoryRegistry()
	if err := reg.Register(Metadata{ID: "p1", Name: "Plugin One"}); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if err := reg.Register(Metadata{ID: "p1", Name: "Plugin One Again"}); err == nil {
		t.Fatalf("duplicate register should fail")
	}
	m, ok := reg.Get("p1")
	if !ok || m.Name != "Plugin One" {
		t.Fatalf("get failed: %v %v", ok, m)
	}
	if m.CreatedAt == 0 || m.UpdatedAt == 0 {
		t.Fatalf("expected timestamps to be set: %+v", m)
	}
	list := reg.List()
	if len(list) != 1 {
		t.Fatalf("list length mismatch: %d", len(list))
	}
	if err := reg.SetEnabled("p1", false); err != nil {
		t.Fatalf("set enabled failed: %v", err)
	}
	if m, _ := reg.Get("p1"); m.UpdatedAt == 0 {
		t.Fatalf("SetEnabled must not zero UpdatedAt")
	}
	if err := reg.Unregister("p1"); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}
	if _, ok := reg.Get("p1"); ok {
		t.Fatalf("unregistered plugin should be gone")
	}
	if err := reg.SetEnabled("p1", true); !errors.Is(err, ErrPluginNotFound) {
		t.Fatalf("expected ErrPluginNotFound, got %v", err)
	}
}

func TestInMemoryRegistryValidatesIDsAndSortsList(t *testing.T) {
	reg := NewInMemoryRegistry()
	for _, id := range []string{"", "../etc/passwd", "a/b", ".hidden", "a b"} {
		if err := reg.Register(Metadata{ID: id}); err == nil {
			t.Errorf("expected invalid id %q to be rejected", id)
		}
	}
	for _, id := range []string{"c", "a", "b"} {
		if err := reg.Register(Metadata{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	list := reg.List()
	if list[0].ID != "a" || list[1].ID != "b" || list[2].ID != "c" {
		t.Fatalf("expected sorted list, got %+v", list)
	}
}

func TestInMemoryRegistryConcurrentAccess(t *testing.T) {
	reg := NewInMemoryRegistry()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := fmt.Sprintf("p-%d-%d", w, i)
				_ = reg.Register(Metadata{ID: id})
				_ = reg.SetEnabled(id, i%2 == 0)
				_ = reg.List()
				_, _ = reg.Get(id)
				if i%3 == 0 {
					_ = reg.Unregister(id)
				}
			}
		}(w)
	}
	wg.Wait()
}

var validWASM = append([]byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}, 0x01, 0x02)

func TestWasmHostIsHonestAboutMissingRuntime(t *testing.T) {
	host := NewWasmHost(nil)
	if err := host.LoadPlugin(context.Background(), "p1", []byte{1, 2, 3}); err == nil {
		t.Fatal("expected non-WASM bytes to be rejected")
	}
	if err := host.LoadPlugin(context.Background(), "p1", validWASM); err != nil {
		t.Fatalf("load plugin failed: %v", err)
	}
	if host.RuntimeAvailable() {
		t.Fatal("runtime must not claim to be available")
	}
	out, err := host.RunHook(context.Background(), "p1", HookOnRequest, map[string]interface{}{"hello": "world"})
	if !errors.Is(err, ErrWASMRuntimeUnavailable) || out != nil {
		t.Fatalf("expected ErrWASMRuntimeUnavailable and no output, got %v %v", out, err)
	}
	if _, err := host.RunHook(context.Background(), "missing", HookOnRequest, nil); !errors.Is(err, ErrPluginNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if err := host.SetEnabled("p1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunHook(context.Background(), "p1", HookOnRequest, nil); !errors.Is(err, ErrPluginDisabled) {
		t.Fatalf("expected disabled, got %v", err)
	}
	if _, err := host.RunHook(context.Background(), "p1", Hook("Bogus"), nil); !errors.Is(err, ErrInvalidHook) {
		t.Fatalf("expected invalid hook, got %v", err)
	}
	if err := host.Close(context.Background()); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := host.LoadPlugin(context.Background(), "p2", validWASM); !errors.Is(err, ErrHostClosed) {
		t.Fatalf("expected ErrHostClosed after close, got %v", err)
	}
	if _, err := host.RunHook(context.Background(), "p1", HookOnRequest, nil); !errors.Is(err, ErrHostClosed) {
		t.Fatalf("expected ErrHostClosed, got %v", err)
	}
}

func TestWasmHostCopiesModuleBytes(t *testing.T) {
	host := NewWasmHost(nil)
	buf := append([]byte(nil), validWASM...)
	if err := host.LoadPlugin(context.Background(), "p1", buf); err != nil {
		t.Fatal(err)
	}
	buf[0] = 0xFF
	host.mu.RLock()
	defer host.mu.RUnlock()
	if host.modules["p1"][0] != 0x00 {
		t.Fatal("host must keep its own copy of the module")
	}
}

func TestWasmHostLoadPluginFileConfinedToDir(t *testing.T) {
	base := t.TempDir()
	pluginDir := filepath.Join(base, "plugins")
	if err := os.Mkdir(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "ok.wasm"), validWASM, 0o644); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(base, "secret.wasm")
	if err := os.WriteFile(secret, validWASM, 0o644); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Symlink(secret, filepath.Join(pluginDir, "link.wasm")); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(pluginDir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}

	host := NewWasmHost(nil)
	if err := host.LoadPluginFile(context.Background(), "ok", pluginDir, "ok.wasm"); err != nil {
		t.Fatalf("load ok: %v", err)
	}
	bad := []string{"../secret.wasm", secret, "subdir", "missing.wasm", ""}
	if runtime.GOOS != "windows" {
		bad = append(bad, "link.wasm")
	}
	for _, name := range bad {
		if err := host.LoadPluginFile(context.Background(), "x", pluginDir, name); err == nil {
			t.Errorf("expected %q to be rejected", name)
		}
	}
	if got := host.Loaded(); len(got) != 1 || got[0] != "ok" {
		t.Fatalf("unexpected loaded set %v", got)
	}
}

type testPlugin struct {
	id      string
	enabled bool
	fn      func(ctx context.Context, hook Hook, payload map[string]interface{}) (map[string]interface{}, error)
}

func (p *testPlugin) ID() string    { return p.id }
func (p *testPlugin) Name() string  { return p.id }
func (p *testPlugin) Enabled() bool { return p.enabled }
func (p *testPlugin) Invoke(ctx context.Context, hook Hook, payload map[string]interface{}) (map[string]interface{}, error) {
	return p.fn(ctx, hook, payload)
}

func TestNativeHostChainsPayloadInOrder(t *testing.T) {
	h := NewNativeHost(time.Second)
	appendStep := func(id string) *testPlugin {
		return &testPlugin{id: id, enabled: true, fn: func(_ context.Context, _ Hook, p map[string]interface{}) (map[string]interface{}, error) {
			p["steps"] = fmt.Sprint(p["steps"], id)
			return p, nil
		}}
	}
	for _, p := range []Plugin{appendStep("b"), appendStep("a"), &testPlugin{id: "c", enabled: false}} {
		if err := h.Add(p); err != nil {
			t.Fatal(err)
		}
	}
	in := map[string]interface{}{"steps": ""}
	out, err := h.RunHook(context.Background(), HookOnRequest, in)
	if err != nil {
		t.Fatal(err)
	}
	if out["steps"] != "ab" {
		t.Fatalf("expected ordered chain 'ab', got %v", out["steps"])
	}
	if in["steps"] != "" {
		t.Fatal("caller payload must not be mutated")
	}
	if err := h.Add(appendStep("a")); !errors.Is(err, ErrPluginExists) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestNativeHostIsolatesPanicsAndTimeouts(t *testing.T) {
	h := NewNativeHost(50 * time.Millisecond)
	_ = h.Add(&testPlugin{id: "panicky", enabled: true, fn: func(context.Context, Hook, map[string]interface{}) (map[string]interface{}, error) {
		panic("boom")
	}})
	_, err := h.Invoke(context.Background(), "panicky", HookOnRequest, nil)
	var he *HookError
	if !errors.Is(err, ErrPluginPanic) || !errors.As(err, &he) || he.PluginID != "panicky" {
		t.Fatalf("expected isolated panic, got %v", err)
	}

	release := make(chan struct{})
	defer close(release)
	_ = h.Add(&testPlugin{id: "stuck", enabled: true, fn: func(ctx context.Context, _ Hook, p map[string]interface{}) (map[string]interface{}, error) {
		<-release // ignores ctx on purpose
		p["late"] = true
		return p, nil
	}})
	start := time.Now()
	_, err = h.Invoke(context.Background(), "stuck", HookOnRequest, map[string]interface{}{})
	if !errors.Is(err, ErrHookTimeout) {
		t.Fatalf("expected timeout, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("timeout was not enforced promptly")
	}

	_ = h.Remove("panicky")
	_ = h.Remove("stuck")
	_ = h.Add(&testPlugin{id: "failing", enabled: true, fn: func(context.Context, Hook, map[string]interface{}) (map[string]interface{}, error) {
		return nil, errors.New("nope")
	}})
	if _, err := h.RunHook(context.Background(), HookOnResponse, nil); err == nil {
		t.Fatal("expected chain to abort on plugin error")
	}
	if _, err := h.RunHook(context.Background(), Hook("x"), nil); !errors.Is(err, ErrInvalidHook) {
		t.Fatalf("expected invalid hook, got %v", err)
	}
}

func TestNativeHostEnabledPanicIsolated(t *testing.T) {
	h := NewNativeHost(time.Second)
	if err := h.Add(panicEnabled{}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunHook(context.Background(), HookOnRequest, nil); !errors.Is(err, ErrPluginPanic) {
		t.Fatalf("expected panic from Enabled to be isolated, got %v", err)
	}
}

type panicEnabled struct{}

func (panicEnabled) ID() string    { return "pe" }
func (panicEnabled) Name() string  { return "pe" }
func (panicEnabled) Enabled() bool { panic("enabled boom") }
func (panicEnabled) Invoke(context.Context, Hook, map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

func TestCopyPayloadDeep(t *testing.T) {
	in := map[string]interface{}{"m": map[string]interface{}{"k": "v"}, "s": []interface{}{"a", map[string]interface{}{"x": 1}}, "b": []byte("hi")}
	out := CopyPayload(in)
	out["m"].(map[string]interface{})["k"] = "changed"
	out["s"].([]interface{})[1].(map[string]interface{})["x"] = 2
	out["b"].([]byte)[0] = 'X'
	if in["m"].(map[string]interface{})["k"] != "v" || in["s"].([]interface{})[1].(map[string]interface{})["x"] != 1 || string(in["b"].([]byte)) != "hi" {
		t.Fatalf("copy is not deep: %v", in)
	}
	if CopyPayload(nil) != nil {
		t.Fatal("nil in, nil out")
	}
}
