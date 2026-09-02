package state

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestStateStoreLifecycle(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenBboltStateStore(dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.SaveAgentState(ctx, "s1", []byte("hello")); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	got, err := store.GetAgentState(ctx, "s1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("expected hello, got %s", string(got))
	}
	if err := store.DeleteAgentState(ctx, "s1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	got, _ = store.GetAgentState(ctx, "s1")
	if got != nil {
		t.Fatalf("expected nil after delete, got %s", string(got))
	}
}

func TestShortTermMemorySearch(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenBboltStateStore(dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.StoreShortTermMemory(ctx, "s1", []Vector{
		{ID: "v1", Data: []float64{1, 0, 0}, Meta: map[string]string{"key": "a"}},
		{ID: "v2", Data: []float64{0, 1, 0}, Meta: map[string]string{"key": "b"}},
	}); err != nil {
		t.Fatalf("store failed: %v", err)
	}
	results, err := store.SearchShortTermMemory(ctx, "s1", []float64{1, 0, 0}, 1)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) != 1 || results[0].Vector.ID != "v1" {
		t.Fatalf("expected top-1 v1, got %+v", results)
	}
}

func TestOpenStateStoreCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new", "path")
	if _, err := OpenBboltStateStore(dir); err != nil {
		t.Fatalf("open with new dir failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "aerollm-state.db")); err != nil {
		t.Fatalf("expected db file, got err: %v", err)
	}
}
