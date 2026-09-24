package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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

func TestShortTermMemoryPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store, err := OpenBboltStateStore(dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if err := store.StoreShortTermMemory(ctx, "s1", []Vector{{ID: "a", Data: []float64{1, 0}}}); err != nil {
		t.Fatalf("store a: %v", err)
	}
	// A second batch must not overwrite the first in persisted state.
	if err := store.StoreShortTermMemory(ctx, "s1", []Vector{{ID: "b", Data: []float64{0, 1}}}); err != nil {
		t.Fatalf("store b: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenBboltStateStore(dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()
	res, err := reopened.SearchShortTermMemory(ctx, "s1", []float64{1, 1}, 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected both vectors after reopen, got %+v", res)
	}
}

func TestShortTermMemoryValidation(t *testing.T) {
	store, err := OpenBboltStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.StoreShortTermMemory(ctx, "", []Vector{{ID: "a", Data: []float64{1}}}); !errors.Is(err, ErrEmptySessionID) {
		t.Fatalf("expected ErrEmptySessionID, got %v", err)
	}
	if err := store.StoreShortTermMemory(ctx, "s", []Vector{{Data: []float64{1}}}); !errors.Is(err, ErrEmptyVectorID) {
		t.Fatalf("expected ErrEmptyVectorID, got %v", err)
	}
	if err := store.StoreShortTermMemory(ctx, "s", []Vector{{ID: "n", Data: []float64{math.NaN()}}}); !errors.Is(err, ErrInvalidVector) {
		t.Fatalf("expected ErrInvalidVector for NaN, got %v", err)
	}
	if err := store.StoreShortTermMemory(ctx, "s", []Vector{{ID: "i", Data: []float64{math.Inf(1)}}}); !errors.Is(err, ErrInvalidVector) {
		t.Fatalf("expected ErrInvalidVector for Inf, got %v", err)
	}
	if err := store.SaveAgentState(ctx, "", []byte("x")); !errors.Is(err, ErrEmptySessionID) {
		t.Fatalf("expected ErrEmptySessionID for agent state, got %v", err)
	}
}

func TestSearchEdgeCases(t *testing.T) {
	store, err := OpenBboltStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.StoreShortTermMemory(ctx, "s", []Vector{
		{ID: "b", Data: []float64{1, 0}},
		{ID: "a", Data: []float64{1, 0}},
		{ID: "zero", Data: []float64{0, 0}},
		{ID: "3d", Data: []float64{1, 0, 0}},
	}); err != nil {
		t.Fatalf("store: %v", err)
	}
	// Zero and empty queries return nothing.
	if res, _ := store.SearchShortTermMemory(ctx, "s", []float64{0, 0}, 5); len(res) != 0 {
		t.Fatalf("expected no results for zero query, got %+v", res)
	}
	if res, _ := store.SearchShortTermMemory(ctx, "s", nil, 5); len(res) != 0 {
		t.Fatalf("expected no results for nil query, got %+v", res)
	}
	if res, _ := store.SearchShortTermMemory(ctx, "s", []float64{math.NaN(), 1}, 5); len(res) != 0 {
		t.Fatalf("expected no results for NaN query, got %+v", res)
	}
	res, err := store.SearchShortTermMemory(ctx, "s", []float64{1, 0}, 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	// 3-dim vector skipped; zero vector scores 0; ties broken by ID.
	if len(res) != 3 || res[0].Vector.ID != "a" || res[1].Vector.ID != "b" || res[2].Vector.ID != "zero" {
		t.Fatalf("unexpected ordering: %+v", res)
	}
	for _, r := range res {
		if math.IsNaN(r.Score) {
			t.Fatalf("NaN score: %+v", r)
		}
	}
}

func TestSearchReturnsDeepCopies(t *testing.T) {
	store, err := OpenBboltStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	data := []float64{1, 0}
	meta := map[string]string{"k": "v"}
	if err := store.StoreShortTermMemory(ctx, "s", []Vector{{ID: "a", Data: data, Meta: meta}}); err != nil {
		t.Fatalf("store: %v", err)
	}
	data[0] = 0 // mutate caller copy after insert
	meta["k"] = "changed"
	res, _ := store.SearchShortTermMemory(ctx, "s", []float64{1, 0}, 1)
	if len(res) != 1 || res[0].Vector.Data[0] != 1 || res[0].Vector.Meta["k"] != "v" {
		t.Fatalf("index aliased caller data: %+v", res)
	}
	res[0].Vector.Data[0] = 42
	res2, _ := store.SearchShortTermMemory(ctx, "s", []float64{1, 0}, 1)
	if res2[0].Vector.Data[0] != 1 {
		t.Fatalf("index aliased returned data: %+v", res2)
	}
}

func TestFIFOEviction(t *testing.T) {
	idx := newFlatIndex(2)
	idx.load("s", idx.merged("s", []Vector{{ID: "1", Data: []float64{1}}, {ID: "2", Data: []float64{1}}, {ID: "3", Data: []float64{1}}}))
	res := idx.search("s", []float64{1}, 0)
	if len(res) != 2 {
		t.Fatalf("expected cap of 2, got %d", len(res))
	}
	for _, r := range res {
		if r.Vector.ID == "1" {
			t.Fatalf("oldest vector should have been evicted: %+v", res)
		}
	}
}

func TestCloseIdempotentAndOpsFailAfterClose(t *testing.T) {
	store, err := OpenBboltStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	ctx := context.Background()
	if err := store.SaveAgentState(ctx, "s", []byte("x")); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("expected ErrStoreClosed, got %v", err)
	}
	if _, err := store.SearchShortTermMemory(ctx, "s", []float64{1}, 1); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("expected ErrStoreClosed, got %v", err)
	}
	var nilStore *BboltStateStore
	if _, err := nilStore.GetAgentState(ctx, "s"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("expected ErrStoreClosed on nil store, got %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	store, err := OpenBboltStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				id := fmt.Sprintf("v-%d-%d", n, j)
				_ = store.StoreShortTermMemory(ctx, "shared", []Vector{{ID: id, Data: []float64{float64(n + 1), float64(j)}}})
				_, _ = store.SearchShortTermMemory(ctx, "shared", []float64{1, 1}, 3)
				_ = store.SaveAgentState(ctx, id, []byte("x"))
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = store.Close()
	}()
	wg.Wait()
}

func TestOpenRejectsEmptyPath(t *testing.T) {
	if _, err := OpenBboltStateStore(""); err == nil {
		t.Fatal("expected error for empty base path")
	}
}

func TestOpenFailsFastWhenLocked(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenBboltStateStore(dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer first.Close()

	start := time.Now()
	second, err := OpenBboltStateStoreWithTimeout(dir, 100*time.Millisecond)
	if err == nil {
		_ = second.Close()
		t.Fatal("expected the second open of a locked database to fail")
	}
	if !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("expected ErrStoreLocked, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("open did not honour its timeout: %s", elapsed)
	}

	// Once the first handle is released the store opens normally.
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	third, err := OpenBboltStateStoreWithTimeout(dir, 0)
	if err != nil {
		t.Fatalf("reopen after release: %v", err)
	}
	_ = third.Close()
}

func TestConcurrentMemoryWritesAllPersisted(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenBboltStateStore(dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	ctx := context.Background()
	const writers, perWriter = 8, 10
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				v := Vector{ID: fmt.Sprintf("v-%d-%d", n, j), Data: []float64{1, float64(n)}}
				if err := store.StoreShortTermMemory(ctx, "shared", []Vector{v}); err != nil {
					t.Errorf("store: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenBboltStateStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	res, err := reopened.SearchShortTermMemory(ctx, "shared", []float64{1, 1}, 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(res) != writers*perWriter {
		t.Fatalf("expected %d persisted vectors after concurrent writes, got %d", writers*perWriter, len(res))
	}
}
