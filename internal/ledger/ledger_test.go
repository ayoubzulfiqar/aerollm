package ledger

import (
	"context"
	"testing"
)

func TestComputeChainHashGenesis(t *testing.T) {
	got := ComputeChainHash("", "req", "resp")
	if got == "" {
		t.Fatal("expected non-empty chain hash")
	}
	if len(got) != 64 {
		t.Fatalf("expected sha256 hex length 64, got %d: %s", len(got), got)
	}
}

func TestComputeChainHashDeterministic(t *testing.T) {
	first := ComputeChainHash("abc", "req", "resp")
	second := ComputeChainHash("abc", "req", "resp")
	if first != second {
		t.Fatalf("expected deterministic hash, got %q vs %q", first, second)
	}
}

func TestComputeChainHashDiffersForDifferentPayloads(t *testing.T) {
	a := ComputeChainHash("x", "req1", "resp1")
	b := ComputeChainHash("x", "req2", "resp2")
	if a == b {
		t.Fatal("expected different payloads to produce different chain hashes")
	}
}

func TestInMemoryLedgerStoreAppendAndLatest(t *testing.T) {
	store := NewInMemoryLedgerStore()
	rec := LedgerRecord{RequestPayload: "req", ResponsePayload: "resp"}
	if err := store.Append(context.Background(), rec); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	latest, err := store.Latest(context.Background())
	if err != nil {
		t.Fatalf("latest failed: %v", err)
	}
	if latest.RequestPayload != "req" || latest.ResponsePayload != "resp" {
		t.Fatalf("unexpected latest record: %+v", latest)
	}
}

func TestInMemoryLedgerStoreLatestEmpty(t *testing.T) {
	store := NewInMemoryLedgerStore()
	if _, err := store.Latest(context.Background()); err == nil {
		t.Fatal("expected error when no ledger entries exist")
	}
}

func TestRecordRequestResponseAppendsAndReturnsHash(t *testing.T) {
	store := NewInMemoryLedgerStore()
	hash, err := RecordRequestResponse(store, "", "req", "resp", nil)
	if err != nil {
		t.Fatalf("RecordRequestResponse failed: %v", err)
	}
	if hash == "" || len(hash) != 64 {
		t.Fatalf("expected non-empty sha256 hex hash, got %q", hash)
	}
	latest, err := store.Latest(context.Background())
	if err != nil {
		t.Fatalf("latest failed: %v", err)
	}
	if latest.ChainHash != hash {
		t.Fatalf("expected latest chain hash %q, got %q", hash, latest.ChainHash)
	}
}

func TestRecordRequestResponseChainsPrevHash(t *testing.T) {
	store := NewInMemoryLedgerStore()
	firstHash, err := RecordRequestResponse(store, "", "req1", "resp1", nil)
	if err != nil {
		t.Fatalf("first record failed: %v", err)
	}
	secondHash, err := RecordRequestResponse(store, firstHash, "req2", "resp2", nil)
	if err != nil {
		t.Fatalf("second record failed: %v", err)
	}
	if secondHash == firstHash {
		t.Fatal("expected second chain hash to differ from first")
	}
	if len(store.records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(store.records))
	}
	if store.records[1].PrevHash != firstHash {
		t.Fatalf("expected second record prev hash %q, got %q", firstHash, store.records[1].PrevHash)
	}
}
