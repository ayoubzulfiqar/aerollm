package plugins

import (
	"context"
	"testing"
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
	list := reg.List()
	if len(list) != 1 {
		t.Fatalf("list length mismatch: %d", len(list))
	}
	if err := reg.SetEnabled("p1", false); err != nil {
		t.Fatalf("set enabled failed: %v", err)
	}
	if err := reg.Unregister("p1"); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}
	if _, ok := reg.Get("p1"); ok {
		t.Fatalf("unregistered plugin should be gone")
	}
}

func TestWasmHostStubbedLifecycle(t *testing.T) {
	host := NewWasmHost(nil)
	if err := host.LoadPlugin(context.Background(), "p1", []byte{1, 2, 3}); err != nil {
		t.Fatalf("load plugin failed: %v", err)
	}
	out, err := host.RunHook(context.Background(), "p1", HookOnRequest, map[string]interface{}{"hello": "world"})
	if err != nil {
		t.Fatalf("run hook failed: %v", err)
	}
	if out["plugin_id"] != "p1" {
		t.Fatalf("unexpected hook output: %v", out)
	}
	if err := host.Close(context.Background()); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}
