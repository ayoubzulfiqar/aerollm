package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

type flakyStore struct {
	persist.Store
	failPut atomic.Bool
}

func (f *flakyStore) Put(b, k string, v any) error {
	if f.failPut.Load() {
		return errors.New("disk full")
	}
	return f.Store.Put(b, k, v)
}

func TestPersistentTenantStoreSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tenant.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewPersistentStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateOrganization(ctx, &Organization{ID: "org1", Name: "Org", Settings: map[string]string{"tier": "gold"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateTeam(ctx, &Team{ID: "team1", OrgID: "org1", Name: "T"}); err != nil {
		t.Fatal(err)
	}
	parent := TenantID("team1")
	if _, err := s.CreateTeam(ctx, &Team{ID: "team2", OrgID: "org1", ParentTeamID: &parent}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateUser(ctx, &User{ID: "u1", TeamID: "team1", Email: "a@b.c", Active: true}); err != nil {
		t.Fatal(err)
	}
	teamID := TenantID("team1")
	raw, key, err := s.IssueAPIKey(ctx, &APIKey{ID: "k1", TenantID: "org1", TeamID: &teamID, Scopes: []string{"chat"}, Active: true})
	if err != nil {
		t.Fatal(err)
	}
	raw2, _, _ := s.IssueAPIKey(ctx, &APIKey{ID: "k2", TenantID: "org1", Active: true})
	if err := s.SetAPIKeyActive(ctx, "k2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveByAPIKey(ctx, raw); err != nil {
		t.Fatal(err)
	}
	_ = ps.Close()

	ps, err = persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	s, err = NewPersistentStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.ResolveByAPIKey(ctx, "Bearer "+raw)
	if err != nil {
		t.Fatalf("key lost across restart: %v", err)
	}
	if got.HashedKey != key.HashedKey || !got.HasScope("chat") || got.TeamID == nil || *got.TeamID != "team1" || got.LastUsedAt == 0 {
		t.Fatalf("restored key: %+v", got)
	}
	if _, err := s.ResolveByAPIKey(ctx, raw2); !errors.Is(err, ErrAPIKeyNotFound) {
		t.Fatalf("deactivation lost across restart: %v", err)
	}
	if o, err := s.GetOrganization(ctx, "org1"); err != nil || o.Settings["tier"] != "gold" {
		t.Fatalf("org: %+v %v", o, err)
	}
	if tm, err := s.GetTeam(ctx, "team2"); err != nil || tm.ParentTeamID == nil || *tm.ParentTeamID != "team1" {
		t.Fatalf("team: %+v %v", tm, err)
	}
	if u, err := s.GetUser(ctx, "u1"); err != nil || u.Email != "a@b.c" {
		t.Fatalf("user: %+v %v", u, err)
	}
	// Duplicate detection still works after reload.
	if _, err := s.CreateAPIKey(ctx, &APIKey{ID: "k1"}); err == nil {
		t.Fatal("duplicate id accepted after reload")
	}
	// Only hashes are stored.
	_ = ps.ForEach(BucketAPIKeys, func(_ string, doc json.RawMessage) error {
		if strings.Contains(string(doc), raw) || strings.Contains(string(doc), raw2) {
			t.Fatal("plaintext tenant key persisted")
		}
		return nil
	})
}

func TestPersistentTenantStoreErrors(t *testing.T) {
	ctx := context.Background()
	fs := &flakyStore{Store: persist.NewMemory()}
	s, err := NewPersistentStore(fs)
	if err != nil {
		t.Fatal(err)
	}
	raw, _, err := s.IssueAPIKey(ctx, &APIKey{ID: "k1", Active: true})
	if err != nil {
		t.Fatal(err)
	}
	fs.failPut.Store(true)
	if _, _, err := s.IssueAPIKey(ctx, &APIKey{ID: "k2", Active: true}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("issue: %v", err)
	}
	if _, err := s.GetAPIKey(ctx, "k2"); err == nil {
		t.Fatal("failed issue visible")
	}
	if err := s.SetAPIKeyActive(ctx, "k1", false); !errors.Is(err, ErrPersistence) {
		t.Fatalf("deactivate: %v", err)
	}
	if _, err := s.CreateOrganization(ctx, &Organization{ID: "o"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("org: %v", err)
	}
	if _, err := s.GetOrganization(ctx, "o"); err == nil {
		t.Fatal("failed org visible")
	}
	// LastUsedAt persistence is best effort: resolution still succeeds and
	// the failure is reported.
	var mu sync.Mutex
	var reported []error
	s.SetErrorHandler(func(err error) {
		mu.Lock()
		reported = append(reported, err)
		mu.Unlock()
	})
	if _, err := s.ResolveByAPIKey(ctx, raw); err != nil {
		t.Fatalf("resolution must not fail on best-effort writes: %v", err)
	}
	n, last := s.PersistErrors()
	mu.Lock()
	defer mu.Unlock()
	if n != 1 || !errors.Is(last, ErrPersistence) || len(reported) != 1 {
		t.Fatalf("best-effort failure not reported: n=%d last=%v reported=%d", n, last, len(reported))
	}

	bad := persist.NewMemory()
	_ = bad.Put(BucketAPIKeys, "a", &APIKey{ID: "a", HashedKey: "h"})
	_ = bad.Put(BucketAPIKeys, "b", &APIKey{ID: "b", HashedKey: "H"})
	if _, err := NewPersistentStore(bad); err == nil {
		t.Fatal("duplicate hashes must be rejected")
	}
	if _, err := NewPersistentStore(nil); err == nil {
		t.Fatal("nil store must be rejected")
	}
}

func TestPersistentQuotaStore(t *testing.T) {
	ctx := context.Background()
	ps := persist.NewMemory()
	s, err := NewPersistentQuotaStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	q := &Quota{ID: "q1", Scope: ScopeTeam, TargetID: "t", Limit: 10, Window: time.Hour}
	if _, err := s.Enforce(ctx, q, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enforce(ctx, q, 20); err == nil {
		t.Fatal("over quota")
	}
	if _, err := s.Release(ctx, "q1", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(ctx, &Quota{ID: "q2", Scope: ScopeUser, TargetID: "u", Limit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Reset(ctx, "q2"); err != nil {
		t.Fatal(err)
	}

	re, err := NewPersistentQuotaStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	re.now = func() time.Time { return now.Add(30 * time.Minute) }
	got, err := re.Get(ctx, "q1")
	if err != nil || got.Used != 3 || !got.LastRefill.Equal(now) || got.Window != time.Hour {
		t.Fatalf("restored quota: %+v %v", got, err)
	}
	if _, err := re.Enforce(ctx, &Quota{ID: "q1"}, 8); err == nil {
		t.Fatal("restored usage must count against the quota")
	}
	// The window still resets on schedule after a restart.
	re.now = func() time.Time { return now.Add(61 * time.Minute) }
	if got, err := re.Enforce(ctx, &Quota{ID: "q1"}, 8); err != nil || got.Used != 8 {
		t.Fatalf("after window: %+v %v", got, err)
	}
	if got, _ := re.ForScope(ctx, ScopeUser, "u"); got == nil || got.Limit != 1 {
		t.Fatal("q2 lost")
	}
}

func TestPersistentQuotaStoreErrorsAndConcurrency(t *testing.T) {
	ctx := context.Background()
	fs := &flakyStore{Store: persist.NewMemory()}
	s, _ := NewPersistentQuotaStore(fs)
	q := &Quota{ID: "q", Scope: ScopeTenant, TargetID: "t", Limit: 1000}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Enforce(ctx, q, 3); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	re, _ := NewPersistentQuotaStore(fs)
	if got, _ := re.Get(ctx, "q"); got.Used != 300 {
		t.Fatalf("persisted usage %d, want 300", got.Used)
	}

	fs.failPut.Store(true)
	if _, err := s.Enforce(ctx, q, 1); !errors.Is(err, ErrPersistence) {
		t.Fatalf("enforce: %v", err)
	}
	if err := s.Upsert(ctx, &Quota{ID: "new", Limit: 1}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("upsert: %v", err)
	}
	if _, err := s.Release(ctx, "q", 10); !errors.Is(err, ErrPersistence) {
		t.Fatalf("release: %v", err)
	}
	if err := s.Reset(ctx, "q"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("reset: %v", err)
	}
	if got, _ := s.Get(ctx, "q"); got.Used != 300 {
		t.Fatalf("failed writes changed usage: %d", got.Used)
	}
	if _, err := s.Get(ctx, "new"); err == nil {
		t.Fatal("failed upsert visible")
	}
	// Checks (amount 0) of existing quotas do not write.
	if _, err := s.Enforce(ctx, q, 0); err != nil {
		t.Fatalf("check-only enforce must not need a write: %v", err)
	}

	bad := persist.NewMemory()
	_ = bad.Put(BucketQuotas, "x", Quota{ID: "x", Limit: -1})
	if _, err := NewPersistentQuotaStore(bad); err == nil {
		t.Fatal("invalid stored quota must be rejected")
	}
}
