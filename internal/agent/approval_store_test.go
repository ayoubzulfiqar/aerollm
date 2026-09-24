package agent

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// fakeClock is a manually advanced clock for LocalApprovalStore tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func TestMemoryApprovalStoreRoundTripAndClaim(t *testing.T) {
	store := NewMemoryApprovalStore(0)
	ctx := context.Background()
	if _, err := store.Get(ctx, "missing"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("expected ErrApprovalNotFound, got %v", err)
	}
	if _, err := store.Get(ctx, "../etc"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("malformed ids are not found, got %v", err)
	}
	rec := ApprovalRecord{ApprovalID: "id1", Status: ApprovalStatusRejected, Error: errors.New("nope"), Request: userReq("x")}
	if err := store.Save(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "id1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Error == nil || got.Error.Error() != "nope" || got.Request == nil || len(got.Request.Messages) != 1 {
		t.Fatalf("round trip lost data: %+v", got)
	}
	// Returned records are copies: mutating one does not change the store.
	got.Request.Messages = nil
	got.Status = ApprovalStatusApproved
	again, _ := store.Get(ctx, "id1")
	if again.Status != ApprovalStatusRejected || len(again.Request.Messages) != 1 {
		t.Fatalf("store aliased a returned record: %+v", again)
	}

	if ok, err := store.Claim(ctx, "id1"); !ok || err != nil {
		t.Fatalf("first claim should succeed: %v %v", ok, err)
	}
	if ok, _ := store.Claim(ctx, "id1"); ok {
		t.Fatal("second claim must fail")
	}
	if _, err := store.Claim(ctx, "unknown"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("claiming an unknown approval should fail with ErrApprovalNotFound, got %v", err)
	}
	if err := store.Save(ctx, ApprovalRecord{ApprovalID: "bad id*"}); err == nil {
		t.Fatal("invalid approval ids must be rejected")
	}
	if store.Len() != 1 {
		t.Fatalf("expected 1 record, got %d", store.Len())
	}
}

func TestMemoryApprovalStoreExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	store := NewMemoryApprovalStore(time.Hour)
	store.now = clock.Now
	ctx := context.Background()
	if err := store.Save(ctx, ApprovalRecord{ApprovalID: "a", Status: ApprovalStatusPending}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(59 * time.Minute)
	if _, err := store.Get(ctx, "a"); err != nil {
		t.Fatalf("record should still be live: %v", err)
	}
	// Update resets the storage expiry, like a Redis SET with TTL.
	if err := store.Update(ctx, ApprovalRecord{ApprovalID: "a", Status: ApprovalStatusPending}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(59 * time.Minute)
	if _, err := store.Get(ctx, "a"); err != nil {
		t.Fatalf("update should have extended the expiry: %v", err)
	}
	clock.Advance(2 * time.Minute)
	if _, err := store.Get(ctx, "a"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("expired record must not be returned, got %v", err)
	}
	if _, err := store.Claim(ctx, "a"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("expired record must not be claimable, got %v", err)
	}
	if store.Len() != 0 {
		t.Fatalf("expired record still counted: %d", store.Len())
	}
}

func TestMemoryApprovalStoreCapacity(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	store := NewMemoryApprovalStore(time.Hour)
	store.now = clock.Now
	store.SetMaxRecords(2)
	ctx := context.Background()
	for _, id := range []string{"p1", "p2"} {
		if err := store.Save(ctx, ApprovalRecord{ApprovalID: id, Status: ApprovalStatusPending}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Save(ctx, ApprovalRecord{ApprovalID: "p3", Status: ApprovalStatusPending}); !errors.Is(err, ErrApprovalStoreFull) {
		t.Fatalf("expected ErrApprovalStoreFull when full of pending approvals, got %v", err)
	}
	// Updating an existing record is always allowed.
	if err := store.Update(ctx, ApprovalRecord{ApprovalID: "p1", Status: ApprovalStatusApproved}); err != nil {
		t.Fatalf("update of existing record: %v", err)
	}
	// A resolved record is evicted to make room; pending ones are kept.
	if err := store.Save(ctx, ApprovalRecord{ApprovalID: "p3", Status: ApprovalStatusPending}); err != nil {
		t.Fatalf("resolved record should have been evicted: %v", err)
	}
	if _, err := store.Get(ctx, "p1"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("resolved record should be gone, got %v", err)
	}
	if _, err := store.Get(ctx, "p2"); err != nil {
		t.Fatalf("pending record must never be evicted: %v", err)
	}
	// Expired records free their slots too.
	clock.Advance(2 * time.Hour)
	if err := store.Save(ctx, ApprovalRecord{ApprovalID: "p4", Status: ApprovalStatusPending}); err != nil {
		t.Fatalf("expired records should free capacity: %v", err)
	}
}

func TestPersistentApprovalStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "approvals.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPersistentApprovalStore(ps, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, id := range []string{"pending1", "claimed1"} {
		if err := store.Save(ctx, ApprovalRecord{ApprovalID: id, Status: ApprovalStatusPending, Request: userReq("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := store.Claim(ctx, "claimed1"); !ok || err != nil {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	ps2, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()
	store2, err := NewPersistentApprovalStore(ps2, time.Hour)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, err := store2.Get(ctx, "pending1")
	if err != nil || got.Status != ApprovalStatusPending || got.Request == nil {
		t.Fatalf("pending approval lost across restart: %+v %v", got, err)
	}
	if ok, _ := store2.Claim(ctx, "claimed1"); ok {
		t.Fatal("a claim must survive a restart (approvals resolve at most once)")
	}
	if ok, _ := store2.Claim(ctx, "pending1"); !ok {
		t.Fatal("unclaimed approval should be claimable after restart")
	}
	if _, err := NewPersistentApprovalStore(nil, 0); err == nil {
		t.Fatal("nil persist store must be rejected")
	}
}

func TestPersistentApprovalStorePurgesExpiredOnLoad(t *testing.T) {
	ps := persist.NewMemory()
	past := time.Now().UTC().Add(-time.Minute)
	if err := ps.Put(approvalBucket, "old", storedApproval{Record: ApprovalRecord{ApprovalID: "old", Status: ApprovalStatusPending}, Expires: past}); err != nil {
		t.Fatal(err)
	}
	if err := ps.Put(approvalClaimBucket, "old", storedClaim{Expires: past}); err != nil {
		t.Fatal(err)
	}
	store, err := NewPersistentApprovalStore(ps, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), "old"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("expired approval must not load, got %v", err)
	}
	var doc storedApproval
	if found, _ := ps.Get(approvalBucket, "old", &doc); found {
		t.Fatal("expired approval document should have been purged")
	}
	var claim storedClaim
	if found, _ := ps.Get(approvalClaimBucket, "old", &claim); found {
		t.Fatal("expired claim document should have been purged")
	}

	// Undecodable documents are reported, not silently dropped.
	bad := persist.NewMemory()
	_ = bad.Put(approvalBucket, "x", "not an object")
	if _, err := NewPersistentApprovalStore(bad, time.Hour); err == nil {
		t.Fatal("expected an error for an undecodable approval document")
	}
}

type failingPersist struct {
	persist.Store
	fail atomic.Bool
}

func (f *failingPersist) Put(bucket, key string, v any) error {
	if f.fail.Load() {
		return errors.New("disk full")
	}
	return f.Store.Put(bucket, key, v)
}

func TestLocalApprovalStoreSurfacesPersistErrors(t *testing.T) {
	fp := &failingPersist{Store: persist.NewMemory()}
	store, err := NewPersistentApprovalStore(fp, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Save(ctx, ApprovalRecord{ApprovalID: "a", Status: ApprovalStatusPending}); err != nil {
		t.Fatal(err)
	}
	fp.fail.Store(true)
	if err := store.Save(ctx, ApprovalRecord{ApprovalID: "b", Status: ApprovalStatusPending}); err == nil {
		t.Fatal("persist failure must be surfaced by Save")
	}
	if _, err := store.Get(ctx, "b"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("a record that failed to persist must not be visible, got %v", err)
	}
	if ok, err := store.Claim(ctx, "a"); ok || err == nil {
		t.Fatalf("persist failure must be surfaced by Claim: %v %v", ok, err)
	}
	fp.fail.Store(false)
	if ok, err := store.Claim(ctx, "a"); !ok || err != nil {
		t.Fatalf("a failed claim must not consume the approval: %v %v", ok, err)
	}
}

func TestHITLWithMemoryStoreEndToEnd(t *testing.T) {
	p := &scriptedProvider{responses: []*models.LLMResponse{callResp("r1", nil, tc("c1", "wire_money", `{}`))}}
	reg := NewToolRegistry()
	inner := &countingTool{name: "wire_money"}
	_ = reg.Register(NewApprovalTool(inner))
	e := NewAdvancedAgentEngine(p, reg, NewMemoryApprovalStore(time.Hour))
	_, id, err := e.RunToolExecutionLoopWithHITL(context.Background(), userReq("wire_money"))
	if err != nil || id == "" {
		t.Fatalf("pause failed: %v", err)
	}
	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if resp, err := e.ResumeApproval(context.Background(), id, true, nil); err == nil && resp != nil {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || inner.calls.Load() != 1 {
		t.Fatalf("approval resolved %d times, tool ran %d times; want 1/1", ok.Load(), inner.calls.Load())
	}
	if _, err := e.ResumeApproval(context.Background(), id, true, nil); !errors.Is(err, ErrApprovalProcessed) {
		t.Fatalf("expected ErrApprovalProcessed, got %v", err)
	}
}

func TestHITLPersistentStoreResumesAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "approvals.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPersistentApprovalStore(ps, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	reg := NewToolRegistry()
	inner := &countingTool{name: "wire_money"}
	_ = reg.Register(NewApprovalTool(inner))
	p := &scriptedProvider{responses: []*models.LLMResponse{callResp("r1", nil, tc("c1", "wire_money", `{}`))}}
	owner := WithPrincipal(context.Background(), "sk-alice")
	_, id, err := NewAdvancedAgentEngine(p, reg, store).RunToolExecutionLoopWithHITL(owner, userReq("wire_money"))
	if err != nil || id == "" {
		t.Fatalf("pause failed: %v", err)
	}
	_ = ps.Close()

	// "Restart": a new engine over a reopened store continues the conversation.
	ps2, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()
	store2, err := NewPersistentApprovalStore(ps2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	e2 := NewAdvancedAgentEngine(&scriptedProvider{}, reg, store2)
	if _, err := e2.ResumeApproval(WithPrincipal(context.Background(), "sk-mallory"), id, true, nil); !errors.Is(err, ErrApprovalForbidden) {
		t.Fatalf("owner binding must survive a restart, got %v", err)
	}
	resp, err := e2.ResumeApproval(owner, id, true, nil)
	if err != nil || resp == nil || inner.calls.Load() != 1 {
		t.Fatalf("resume after restart failed: resp=%v err=%v calls=%d", resp, err, inner.calls.Load())
	}
	if _, err := e2.ResumeApproval(owner, id, true, nil); !errors.Is(err, ErrApprovalProcessed) {
		t.Fatalf("expected ErrApprovalProcessed, got %v", err)
	}
}
