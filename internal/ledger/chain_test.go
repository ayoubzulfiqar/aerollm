package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
)

func TestChainHashIsUnambiguous(t *testing.T) {
	if ComputeChainHash("p", "ab", "c") == ComputeChainHash("p", "a", "bc") {
		t.Fatal("moving bytes between request and response must change the hash")
	}
	if ComputeChainHash("pa", "b", "c") == ComputeChainHash("p", "ab", "c") {
		t.Fatal("moving bytes between prev and request must change the hash")
	}
}

func TestAppendChainedConcurrentFormsSingleChain(t *testing.T) {
	s := NewInMemoryLedgerStoreWithOptions(Options{})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.AppendChained(ctx, fmt.Sprintf("req-%d", i), "resp"); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if s.Len() != 200 {
		t.Fatalf("expected 200 records, got %d", s.Len())
	}
	if err := s.Verify(ctx); err != nil {
		t.Fatalf("chain forked under concurrency: %v", err)
	}
}

func TestLegacyAppendPatternIsRaceFreeWithAutoChain(t *testing.T) {
	// The gateway does Latest -> ComputeChainHash -> Append. With AutoChain
	// (the default) the store fixes up the link atomically.
	s := NewInMemoryLedgerStore()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			latest, _ := s.Latest(ctx)
			prev := ""
			if latest != nil {
				prev = latest.ChainHash
			}
			req := fmt.Sprintf("r%d", i)
			_ = s.Append(ctx, LedgerRecord{PrevHash: prev, RequestPayload: req, ResponsePayload: "x", ChainHash: ComputeChainHash(prev, req, "x")})
		}(i)
	}
	wg.Wait()
	if err := s.Verify(ctx); err != nil {
		t.Fatalf("legacy pattern forked the chain: %v", err)
	}
}

func TestVerifyDetectsTampering(t *testing.T) {
	ctx := context.Background()
	build := func() *InMemoryLedgerStore {
		s := NewInMemoryLedgerStoreWithOptions(Options{AutoChain: true})
		for i := 0; i < 5; i++ {
			_, _ = s.AppendChained(ctx, fmt.Sprintf("req%d", i), fmt.Sprintf("resp%d", i))
		}
		return s
	}
	tamper := map[string]func(s *InMemoryLedgerStore){
		"payload": func(s *InMemoryLedgerStore) { s.records[2].ResponsePayload = "evil" },
		"reorder": func(s *InMemoryLedgerStore) { s.records[1], s.records[2] = s.records[2], s.records[1] },
		"delete":  func(s *InMemoryLedgerStore) { s.records = append(s.records[:2], s.records[3:]...) },
		"rehashed": func(s *InMemoryLedgerStore) {
			r := &s.records[2]
			r.RequestPayload = "evil"
			r.RequestHash = hashHex("evil")
			r.ChainHash = ComputeChainHash(r.PrevHash, r.RequestPayload, r.ResponsePayload)
		},
		"insert": func(s *InMemoryLedgerStore) {
			fake := LedgerRecord{PrevHash: s.records[1].ChainHash, RequestPayload: "x", ResponsePayload: "y"}
			fake.ChainHash = ComputeChainHash(fake.PrevHash, "x", "y")
			s.records = append(s.records[:2], append([]LedgerRecord{fake}, s.records[2:]...)...)
		},
	}
	for name, fn := range tamper {
		s := build()
		if err := s.Verify(ctx); err != nil {
			t.Fatalf("%s: untampered chain failed: %v", name, err)
		}
		fn(s)
		if err := s.Verify(ctx); !errors.Is(err, ErrChainBroken) {
			t.Errorf("%s: tampering not detected (%v)", name, err)
		}
	}
}

func TestPruningKeepsVerification(t *testing.T) {
	s := NewInMemoryLedgerStoreWithOptions(Options{MaxRecords: 10, AutoChain: true})
	ctx := context.Background()
	var tenth LedgerRecord
	for i := 0; i < 25; i++ {
		rec, _ := s.AppendChained(ctx, fmt.Sprintf("req%d", i), "resp")
		if i == 14 {
			tenth = rec
		}
	}
	if s.Len() != 10 {
		t.Fatalf("expected 10 retained, got %d", s.Len())
	}
	count, anchor := s.Pruned()
	if count != 15 || anchor != tenth.ChainHash {
		t.Fatalf("unexpected prune state %d %q", count, anchor)
	}
	if err := s.Verify(ctx); err != nil {
		t.Fatalf("pruned chain must verify: %v", err)
	}
	all, _ := s.All(ctx)
	if all[0].PrevHash != anchor {
		t.Fatal("first retained record must link to the anchor")
	}
	s.SetMaxRecords(3)
	if s.Len() != 3 || s.Verify(ctx) != nil {
		t.Fatal("SetMaxRecords must prune and keep chain valid")
	}
}

func TestReturnedRecordsAreCopies(t *testing.T) {
	s := NewInMemoryLedgerStore()
	ctx := context.Background()
	md := map[string]interface{}{"k": "v"}
	_, _ = s.AppendChainedWithMetadata(ctx, "a", "b", md)
	md["k"] = "mutated"
	l, _ := s.Latest(ctx)
	l.Metadata["k"] = "mutated2"
	all, _ := s.All(ctx)
	if all[0].Metadata["k"] != "v" {
		t.Fatal("store shares metadata maps with callers")
	}
}

func TestAppendChainedHonoursContext(t *testing.T) {
	s := NewInMemoryLedgerStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.AppendChained(ctx, "a", "b"); err == nil {
		t.Fatal("cancelled context must abort")
	}
}

func TestPqcLedgerStoreSignsAndVerifies(t *testing.T) {
	ctx := context.Background()
	km := pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridMLKEM768X25519Ed25519)
	inner := NewInMemoryLedgerStore()
	s, err := NewPqcLedgerStore(inner, km)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Append(ctx, LedgerRecord{RequestPayload: fmt.Sprintf("r%d", i), ResponsePayload: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.AppendChained(ctx, "r3", "x"); err != nil {
		t.Fatal(err)
	}
	att, err := s.VerifyLatest(ctx)
	if err != nil {
		t.Fatalf("VerifyLatest: %v", err)
	}
	if err := pqc.VerifyAttestation(ctx, km, att.Attestation, 0); err != nil {
		t.Fatalf("attestation: %v", err)
	}
	if err := s.Verify(ctx); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	// Forged signature is detected.
	inner.records[1].Metadata[MetadataSignature] = inner.records[0].Metadata[MetadataSignature]
	if err := s.Verify(ctx); err == nil {
		t.Fatal("forged signature not detected")
	}
	// Unsigned records are rejected.
	inner.records[1].Metadata = nil
	if err := s.VerifyRecord(ctx, inner.records[1]); !errors.Is(err, ErrUnsigned) {
		t.Fatalf("expected ErrUnsigned, got %v", err)
	}
}

func TestPqcLedgerStoreRejectsNonSigningSuites(t *testing.T) {
	for _, alg := range []string{pqc.AlgorithmPQCMLKEM768, pqc.AlgorithmPQCMLDSA65} {
		if _, err := NewPqcLedgerStore(NewInMemoryLedgerStore(), pqc.NewQuantumSafeKeyManager(alg)); err == nil {
			t.Errorf("%s: expected error", alg)
		}
	}
	if _, err := NewPqcLedgerStore(nil, pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridEd25519MLDSA65)); err == nil {
		t.Error("nil store must be rejected")
	}
}
