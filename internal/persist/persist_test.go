package persist

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

type doc struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func stores(t *testing.T) map[string]Store {
	t.Helper()
	b, err := OpenBolt(filepath.Join(t.TempDir(), "sub", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return map[string]Store{"bolt": b, "memory": NewMemory()}
}

func TestPutGetDeleteForEach(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			if ok, err := s.Get("b", "missing", &doc{}); ok || err != nil {
				t.Fatalf("missing key: ok=%v err=%v", ok, err)
			}
			for i, k := range []string{"c", "a", "b"} {
				if err := s.Put("b", k, doc{Name: k, Count: i}); err != nil {
					t.Fatal(err)
				}
			}
			var d doc
			if ok, err := s.Get("b", "a", &d); !ok || err != nil || d.Name != "a" || d.Count != 1 {
				t.Fatalf("get: %+v ok=%v err=%v", d, ok, err)
			}
			var order []string
			if err := s.ForEach("b", func(k string, raw json.RawMessage) error { order = append(order, k); return nil }); err != nil {
				t.Fatal(err)
			}
			if len(order) != 3 || order[0] != "a" || order[2] != "c" {
				t.Fatalf("iteration order: %v", order)
			}
			if err := s.Delete("b", "a"); err != nil {
				t.Fatal(err)
			}
			if err := s.Delete("b", "a"); err != nil {
				t.Fatal("deleting a missing key must not fail")
			}
			all, err := LoadAll[doc](s, "b")
			if err != nil || len(all) != 2 || all["c"].Name != "c" {
				t.Fatalf("load all: %v %v", all, err)
			}
			stop := errors.New("stop")
			if err := s.ForEach("b", func(string, json.RawMessage) error { return stop }); !errors.Is(err, stop) {
				t.Fatalf("callback error must propagate: %v", err)
			}
			if err := s.Put("", "k", 1); err == nil {
				t.Fatal("empty bucket must be rejected")
			}
			if err := s.Put("b", "", 1); err == nil {
				t.Fatal("empty key must be rejected")
			}
		})
	}
}

func TestBoltSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	b, err := OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("keys", "k1", doc{Name: "persisted"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	var d doc
	if ok, err := b2.Get("keys", "k1", &d); !ok || err != nil || d.Name != "persisted" {
		t.Fatalf("after reopen: %+v %v %v", d, ok, err)
	}
}

func TestBoltLockedFileFailsFast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	b, err := OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := OpenBolt(path); err == nil {
		t.Fatal("a second open of a locked file must fail instead of hanging")
	}
}

func TestLoadAllReportsUndecodable(t *testing.T) {
	m := NewMemory()
	_ = m.Put("b", "good", doc{Name: "ok"})
	_ = m.Put("b", "bad", "not an object")
	all, err := LoadAll[doc](m, "b")
	if err == nil || len(all) != 1 {
		t.Fatalf("expected one doc and an error, got %v %v", all, err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					k := string(rune('a' + i))
					_ = s.Put("c", k, doc{Count: i})
					var d doc
					_, _ = s.Get("c", k, &d)
					_ = s.ForEach("c", func(string, json.RawMessage) error { return nil })
				}(i)
			}
			wg.Wait()
			all, _ := LoadAll[doc](s, "c")
			if len(all) != 16 {
				t.Fatalf("expected 16 docs, got %d", len(all))
			}
		})
	}
}

func TestMemoryClosed(t *testing.T) {
	m := NewMemory()
	_ = m.Close()
	if err := m.Put("b", "k", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("put after close: %v", err)
	}
}
