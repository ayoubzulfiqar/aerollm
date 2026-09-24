package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
)

func TestV2CoversTimestampAndMetadata(t *testing.T) {
	ctx := context.Background()
	s := NewInMemoryLedgerStoreWithOptions(Options{AutoChain: true})
	for i := 0; i < 4; i++ {
		rec, err := s.AppendChainedWithMetadata(ctx, fmt.Sprintf("req%d", i), "resp", map[string]interface{}{"tenant": "t1", "tokens": i})
		if err != nil {
			t.Fatal(err)
		}
		if rec.Version != RecordVersion2 {
			t.Fatalf("new records must be v2, got %d", rec.Version)
		}
	}
	if err := s.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	tamper := map[string]func(r *LedgerRecord){
		"timestamp":        func(r *LedgerRecord) { r.Timestamp = r.Timestamp.Add(time.Nanosecond) },
		"metadata value":   func(r *LedgerRecord) { r.Metadata["tenant"] = "t2" },
		"metadata added":   func(r *LedgerRecord) { r.Metadata["extra"] = true },
		"metadata removed": func(r *LedgerRecord) { delete(r.Metadata, "tokens") },
		"downgrade":        func(r *LedgerRecord) { r.Version = RecordVersion1 },
		"unknown version":  func(r *LedgerRecord) { r.Version = 99 },
	}
	for name, fn := range tamper {
		recs, _ := s.All(ctx)
		// Tamper with the last record: no successor link protects it, so only
		// the v2 hash can catch the change.
		fn(&recs[len(recs)-1])
		if err := VerifyRecords("", recs); !errors.Is(err, ErrChainBroken) {
			t.Errorf("%s: tampering not detected (%v)", name, err)
		}
	}
}

func TestV1RecordsStillVerifyAndMixWithV2(t *testing.T) {
	ctx := context.Background()
	// Build a legacy v1 chain exactly as the pre-v2 code did (Version 0).
	legacy := NewInMemoryLedgerStoreWithOptions(Options{})
	prev := ""
	for i := 0; i < 3; i++ {
		req := fmt.Sprintf("old%d", i)
		rec := LedgerRecord{
			Timestamp: time.Now(), PrevHash: prev, RequestPayload: req, ResponsePayload: "r",
			RequestHash: hashHex(req), ResponseHash: hashHex("r"),
			ChainHash: ComputeChainHash(prev, req, "r"), Metadata: map[string]interface{}{"sig": "later annotations are fine for v1"},
		}
		if err := legacy.Append(ctx, rec); err != nil {
			t.Fatal(err)
		}
		prev = rec.ChainHash
	}
	if err := legacy.Verify(ctx); err != nil {
		t.Fatalf("v1 chain must verify: %v", err)
	}
	// Continue the chain with v2 records (an import store switched to
	// auto-chaining).
	records, _ := legacy.All(ctx)
	mixed := NewInMemoryLedgerStoreWithOptions(Options{})
	for _, r := range records {
		_ = mixed.Append(ctx, r)
	}
	mixed.autoChain = true
	if _, err := mixed.AppendChained(ctx, "new", "r"); err != nil {
		t.Fatal(err)
	}
	all, _ := mixed.All(ctx)
	if all[2].Version != 0 || all[3].Version != RecordVersion2 || all[3].PrevHash != all[2].ChainHash {
		t.Fatalf("unexpected mixed chain: %+v", all[2:])
	}
	if err := mixed.Verify(ctx); err != nil {
		t.Fatalf("mixed v1/v2 chain must verify: %v", err)
	}
	// v1 metadata/timestamps are not covered (documented legacy behaviour).
	all[1].Metadata = map[string]interface{}{"changed": true}
	all[1].Timestamp = time.Time{}
	if err := VerifyRecords("", all); err != nil {
		t.Fatalf("v1 rule must ignore metadata/timestamp: %v", err)
	}
}

func TestV2HashStableAcrossJSONRoundTrip(t *testing.T) {
	type custom struct {
		B int    `json:"b"`
		A string `json:"a"`
	}
	md := map[string]interface{}{
		"int": 7, "int64": int64(1 << 40), "float": 0.1, "struct": custom{B: 1, A: "x"},
		"slice": []string{"a", "b"}, "html": "<tag>&", "nested": map[string]interface{}{"z": 1, "a": nil},
	}
	ts := time.Date(2026, 9, 24, 10, 11, 12, 123456789, time.FixedZone("X", 3600))
	h1, err := ComputeChainHashV2("p", "q", "r", ts, md)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(struct {
		TS time.Time              `json:"ts"`
		MD map[string]interface{} `json:"md"`
	}{ts, md})
	var back struct {
		TS time.Time              `json:"ts"`
		MD map[string]interface{} `json:"md"`
	}
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	h2, err := ComputeChainHashV2("p", "q", "r", back.TS, back.MD)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Fatal("v2 hash must survive a JSON round trip of timestamp and metadata")
	}
	// nil and empty metadata hash the same; annotation keys are ignored.
	a, _ := ComputeChainHashV2("p", "q", "r", ts, nil)
	b, _ := ComputeChainHashV2("p", "q", "r", ts, map[string]interface{}{MetadataSignature: "sig", MetadataSignatureAlgorithm: "alg"})
	if a != b {
		t.Fatal("annotation keys must not affect the v2 hash")
	}
	if _, err := ComputeChainHashV2("p", "q", "r", ts, map[string]interface{}{"bad": math.NaN()}); err == nil {
		t.Fatal("unencodable metadata must fail")
	}
	s := NewInMemoryLedgerStore()
	if _, err := s.AppendChainedWithMetadata(context.Background(), "q", "r", map[string]interface{}{"bad": math.Inf(1)}); err == nil || s.Len() != 0 {
		t.Fatal("append with unencodable metadata must fail without storing")
	}
}

func TestPqcSignedV2RecordsVerify(t *testing.T) {
	ctx := context.Background()
	km := pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridMLKEM768X25519Ed25519)
	inner := NewInMemoryLedgerStore()
	signed, err := NewPqcLedgerStore(inner, km)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := signed.AppendChainedWithMetadata(ctx, "req", "resp", map[string]interface{}{"tenant": "a"})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Version != RecordVersion2 || rec.Metadata[MetadataSignature] == nil {
		t.Fatalf("signed record: %+v", rec)
	}
	if err := signed.Verify(ctx); err != nil {
		t.Fatalf("signed v2 chain must verify: %v", err)
	}
	// Non-sealed stores (Latest+Append path) also produce v2 records.
	plain := &plainStore{}
	signed2, _ := NewPqcLedgerStore(plain, km)
	for i := 0; i < 3; i++ {
		if _, err := signed2.AppendChained(ctx, fmt.Sprintf("r%d", i), "x"); err != nil {
			t.Fatal(err)
		}
	}
	if err := VerifyRecords("", plain.recs); err != nil {
		t.Fatal(err)
	}
	for _, r := range plain.recs {
		if r.Version != RecordVersion2 {
			t.Fatal("expected v2 records")
		}
		if err := signed2.VerifyRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	tampered := plain.recs[1]
	tampered.Metadata = map[string]interface{}{MetadataSignature: tampered.Metadata[MetadataSignature], "x": 1}
	if err := signed2.VerifyRecord(ctx, tampered); err == nil {
		t.Fatal("metadata tampering on a signed record must be detected")
	}
}

// plainStore is a LedgerStore without atomic chaining.
type plainStore struct {
	mu   sync.Mutex
	recs []LedgerRecord
}

func (p *plainStore) Append(_ context.Context, r LedgerRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recs = append(p.recs, r)
	return nil
}

func (p *plainStore) Latest(context.Context) (*LedgerRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.recs) == 0 {
		return nil, errors.New("empty")
	}
	r := p.recs[len(p.recs)-1]
	return &r, nil
}

func (p *plainStore) All(context.Context) ([]LedgerRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]LedgerRecord(nil), p.recs...), nil
}

func TestRecordRequestResponseFallbackUsesV2(t *testing.T) {
	p := &plainStore{}
	h1, err := RecordRequestResponse(p, "", "a", "b", map[string]interface{}{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RecordRequestResponse(p, h1, "c", "d", nil); err != nil {
		t.Fatal(err)
	}
	if p.recs[0].Version != RecordVersion2 || VerifyRecords("", p.recs) != nil {
		t.Fatalf("fallback path: %+v", p.recs)
	}
}

func TestPersistentLedgerSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewPersistentLedgerStore(ps, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Persistent() || NewInMemoryLedgerStore().Persistent() {
		t.Fatal("Persistent() mismatch")
	}
	for i := 0; i < 5; i++ {
		if _, err := s.AppendChainedWithMetadata(ctx, fmt.Sprintf("req%d", i), "resp", map[string]interface{}{"n": i, "tags": []string{"a"}}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := s.All(ctx)
	_ = ps.Close()

	ps, err = persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	s, err = NewPersistentLedgerStore(ps, 0)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := s.All(ctx)
	if len(after) != 5 {
		t.Fatalf("records after restart: %d", len(after))
	}
	for i := range after {
		if after[i].ChainHash != before[i].ChainHash || !after[i].Timestamp.Equal(before[i].Timestamp) || after[i].Version != RecordVersion2 {
			t.Fatalf("record %d changed across restart", i)
		}
	}
	if err := s.Verify(ctx); err != nil {
		t.Fatalf("restored chain must verify: %v", err)
	}
	// Appends continue the persisted chain.
	rec, err := s.AppendChained(ctx, "next", "resp")
	if err != nil {
		t.Fatal(err)
	}
	if rec.PrevHash != before[4].ChainHash {
		t.Fatal("new record must link to the persisted head")
	}
	if err := s.Verify(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPersistentLedgerPruningAnchorAcrossRestart(t *testing.T) {
	ctx := context.Background()
	ps := persist.NewMemory()
	s, err := NewPersistentLedgerStore(ps, 10)
	if err != nil {
		t.Fatal(err)
	}
	var all []LedgerRecord
	for i := 0; i < 200; i++ {
		rec, err := s.AppendChained(ctx, fmt.Sprintf("req%d", i), "resp")
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, rec)
	}
	count, anchor := s.Pruned()
	if s.Len() != 10 || count != 190 || anchor != all[189].ChainHash {
		t.Fatalf("len=%d pruned=%d", s.Len(), count)
	}
	// Pruned records are removed from disk (up to the compaction lag).
	onDisk := 0
	_ = ps.ForEach(BucketRecords, func(string, json.RawMessage) error { onDisk++; return nil })
	if onDisk > 10+int(s.disk.batch)+maxDeletesPerAppend {
		t.Fatalf("pruned records not deleted from disk: %d on disk", onDisk)
	}

	for _, max := range []int{10, 5, 0} {
		re, err := NewPersistentLedgerStore(ps, max)
		if err != nil {
			t.Fatal(err)
		}
		want := max
		if max == 0 {
			want = 10 // everything still on disk that was not pruned
		}
		got, _ := re.All(ctx)
		if len(got) < want || got[len(got)-1].ChainHash != all[199].ChainHash {
			t.Fatalf("max=%d: restored %d records", max, len(got))
		}
		if max > 0 && len(got) != max {
			t.Fatalf("max=%d: restored %d records", max, len(got))
		}
		c, a := re.Pruned()
		if c != uint64(200-len(got)) || a != got[0].PrevHash {
			t.Fatalf("max=%d: pruned=%d anchor mismatch", max, c)
		}
		if err := re.Verify(ctx); err != nil {
			t.Fatalf("max=%d: %v", max, err)
		}
	}

	// Truncating the retained chain on disk is detected via the anchor.
	re, _ := NewPersistentLedgerStore(ps, 0)
	recs, _ := re.All(ctx)
	first := re.disk.seqs[0]
	_ = ps.Delete(BucketRecords, seqKey(first))
	re, err = NewPersistentLedgerStore(ps, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := re.Verify(ctx); !errors.Is(err, ErrChainBroken) {
		t.Fatalf("front truncation not detected: %v (had %d records)", err, len(recs))
	}
}

func TestPersistentLedgerIgnoresRecordsBelowPrunedSequence(t *testing.T) {
	ctx := context.Background()
	ps := persist.NewMemory()
	s, _ := NewPersistentLedgerStore(ps, 0)
	for i := 0; i < 5; i++ {
		_, _ = s.AppendChained(ctx, fmt.Sprintf("r%d", i), "x")
	}
	all, _ := s.All(ctx)
	// Simulate a crash after the pruning state was advanced but before the
	// pruned records were deleted.
	_ = ps.Put(BucketMeta, metaStateKey, ledgerState{FirstSeq: 3, Anchor: all[2].ChainHash, Pruned: 3})
	re, err := NewPersistentLedgerStore(ps, 0)
	if err != nil {
		t.Fatal(err)
	}
	if re.Len() != 2 {
		t.Fatalf("expected 2 retained records, got %d", re.Len())
	}
	if err := re.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	// Leftovers are deleted by subsequent appends.
	for i := 0; i < 3; i++ {
		if _, err := re.AppendChained(ctx, "more", "x"); err != nil {
			t.Fatal(err)
		}
	}
	if ok, _ := ps.Get(BucketRecords, seqKey(0), &storedRecord{}); ok {
		t.Fatal("leftover pruned record not deleted")
	}
}

type flakyPersist struct {
	persist.Store
	failPut, failDelete atomic.Bool
}

func (f *flakyPersist) Put(b, k string, v any) error {
	if f.failPut.Load() {
		return errors.New("disk full")
	}
	return f.Store.Put(b, k, v)
}

func (f *flakyPersist) Delete(b, k string) error {
	if f.failDelete.Load() {
		return errors.New("io error")
	}
	return f.Store.Delete(b, k)
}

func TestPersistentLedgerSurfacesErrors(t *testing.T) {
	ctx := context.Background()
	fp := &flakyPersist{Store: persist.NewMemory()}
	s, err := NewPersistentLedgerStore(fp, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendChained(ctx, "a", "x"); err != nil {
		t.Fatal(err)
	}
	fp.failPut.Store(true)
	if _, err := s.AppendChained(ctx, "b", "x"); err == nil {
		t.Fatal("append must fail when the record cannot be persisted")
	}
	if err := s.Append(ctx, LedgerRecord{RequestPayload: "c"}); err == nil {
		t.Fatal("Append must fail when the record cannot be persisted")
	}
	if s.Len() != 1 {
		t.Fatalf("failed appends must not be visible: %d", s.Len())
	}
	fp.failPut.Store(false)
	fp.failDelete.Store(true)
	var pruneErr error
	for i := 0; i < 5 && pruneErr == nil; i++ {
		_, pruneErr = s.AppendChained(ctx, fmt.Sprintf("d%d", i), "x")
	}
	if !errors.Is(pruneErr, ErrPersistPrune) {
		t.Fatalf("delete failure must be reported as ErrPersistPrune, got %v", pruneErr)
	}
	fp.failDelete.Store(false)
	if _, err := s.AppendChained(ctx, "e", "x"); err != nil {
		t.Fatal(err)
	}
	if err := s.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	re, err := NewPersistentLedgerStore(fp, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := re.Verify(ctx); err != nil || re.Len() != 2 {
		t.Fatalf("reload after errors: len=%d err=%v", re.Len(), err)
	}
	if _, err := NewPersistentLedgerStore(nil, 1); err == nil {
		t.Fatal("nil store must be rejected")
	}
	bad := persist.NewMemory()
	_ = bad.Put(BucketRecords, seqKey(0), "garbage")
	if _, err := NewPersistentLedgerStore(bad, 0); err == nil {
		t.Fatal("undecodable record must be rejected")
	}
}

func TestPersistentLedgerConcurrentAppends(t *testing.T) {
	ctx := context.Background()
	ps := persist.NewMemory()
	s, _ := NewPersistentLedgerStore(ps, 50)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.AppendChained(ctx, fmt.Sprintf("r%d", i), "x"); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if err := s.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	re, _ := NewPersistentLedgerStore(ps, 50)
	if err := re.Verify(ctx); err != nil || re.Len() != 50 {
		t.Fatalf("reload: len=%d err=%v", re.Len(), err)
	}
	a, _ := s.Latest(ctx)
	b, _ := re.Latest(ctx)
	if a.ChainHash != b.ChainHash {
		t.Fatal("head mismatch after reload")
	}
}

func TestPqcLedgerStableKeyAcrossRestart(t *testing.T) {
	ctx := context.Background()
	km := pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridMLKEM768X25519Ed25519)
	_, priv, err := km.GenerateKeyPair(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ps := persist.NewMemory()
	open := func(priv pqc.PrivateKey) *PqcLedgerStore {
		t.Helper()
		inner, err := NewPersistentLedgerStore(ps, 0)
		if err != nil {
			t.Fatal(err)
		}
		s, err := NewPqcLedgerStoreWithKey(inner, km, priv)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open(priv)
	rec, err := s.AppendChained(ctx, "req", "resp")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Metadata[MetadataSignatureKeyID] != s.KeyID() || s.KeyID() == "" {
		t.Fatalf("key id not recorded: %+v", rec.Metadata)
	}
	// Restart with the same key: persisted signatures still verify.
	s = open(priv)
	if err := s.Verify(ctx); err != nil {
		t.Fatalf("signatures must survive a restart: %v", err)
	}
	// Rotate the signing key: old records need the old key to be trusted.
	oldPub := s.PublicKey()
	_, priv2, _ := km.GenerateKeyPair(ctx)
	s = open(priv2)
	if _, err := s.AppendChained(ctx, "req2", "resp"); err != nil {
		t.Fatal(err)
	}
	if err := s.Verify(ctx); err == nil {
		t.Fatal("records of an untrusted key must not verify")
	}
	s.TrustPublicKey(oldPub)
	if err := s.Verify(ctx); err != nil {
		t.Fatalf("after trusting the previous key: %v", err)
	}
	// A throwaway-key store cannot verify persisted records at all.
	inner, _ := NewPersistentLedgerStore(ps, 0)
	fresh, _ := NewPqcLedgerStore(inner, km)
	if err := fresh.Verify(ctx); err == nil {
		t.Fatal("expected verification failure with an unrelated key")
	}
	if _, err := NewPqcLedgerStoreWithKey(inner, km, pqc.PrivateKey("short")); err == nil {
		t.Fatal("invalid private key must be rejected")
	}
}
