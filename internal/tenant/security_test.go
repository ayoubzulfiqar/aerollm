package tenant

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestResolverHashesKeysAndRejectsInactive(t *testing.T) {
	r := NewInMemoryTenantResolver()
	r.Add(&APIKey{HashedKey: "token", TenantID: "t1", Active: true})
	r.AddKey("inactive-token", &APIKey{TenantID: "t2", Active: false})

	got, err := r.ResolveByAPIKey(context.Background(), "Bearer token")
	if err != nil || got.TenantID != "t1" {
		t.Fatalf("resolve: %v %+v", err, got)
	}
	if got.HashedKey == "token" || got.HashedKey != HashAPIKey("token") {
		t.Fatalf("plaintext key must not be stored, got %q", got.HashedKey)
	}
	// Presenting the stored hash itself must not authenticate (no pass-the-hash).
	if _, err := r.ResolveByAPIKey(context.Background(), HashAPIKey("token")); err == nil {
		t.Fatal("hash must not be usable as a credential")
	}
	if _, err := r.ResolveByAPIKey(context.Background(), "inactive-token"); err == nil {
		t.Fatal("inactive keys must not resolve")
	}
	// Mutating a resolved copy does not affect the resolver.
	got.TenantID = "evil"
	again, _ := r.ResolveByAPIKey(context.Background(), "token")
	if again.TenantID != "t1" {
		t.Fatal("resolver returned shared state")
	}
}

func TestMiddlewareRejectsTenantHeaderSpoofing(t *testing.T) {
	r := NewInMemoryTenantResolver()
	r.AddKey("k1", &APIKey{ID: "k1", TenantID: "t1", Active: true})
	var gotTenant TenantID
	h := Middleware(r, func(w http.ResponseWriter, req *http.Request) {
		gotTenant, _ = TenantIDFromContext(req.Context())
		w.WriteHeader(http.StatusOK)
	})
	run := func(auth, tenantHeader string) int {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		if tenantHeader != "" {
			req.Header.Set("X-Tenant-ID", tenantHeader)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec.Code
	}
	if c := run("Bearer k1", ""); c != http.StatusOK || gotTenant != "t1" {
		t.Fatalf("valid key: %d %q", c, gotTenant)
	}
	if c := run("Bearer k1", "t1"); c != http.StatusOK {
		t.Fatalf("matching header: %d", c)
	}
	if c := run("Bearer k1", "t2"); c != http.StatusForbidden {
		t.Fatalf("spoofed tenant header: expected 403, got %d", c)
	}
	if c := run("", ""); c != http.StatusUnauthorized {
		t.Fatalf("missing key: %d", c)
	}
	if c := run("Bearer nope", ""); c != http.StatusUnauthorized {
		t.Fatalf("unknown key: %d", c)
	}
}

func TestStoreIssueAndResolve(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()
	raw, key, err := s.IssueAPIKey(ctx, &APIKey{ID: "k1", TenantID: "t1", Active: true})
	if err != nil {
		t.Fatal(err)
	}
	if key.HashedKey != HashAPIKey(raw) {
		t.Fatal("stored hash mismatch")
	}
	got, err := s.ResolveByAPIKey(ctx, raw)
	if err != nil || got.ID != "k1" || got.LastUsedAt == 0 {
		t.Fatalf("resolve: %v %+v", err, got)
	}
	if _, err := s.CreateAPIKey(ctx, &APIKey{ID: "k1"}); err == nil {
		t.Fatal("duplicate id must fail")
	}
	_ = s.SetAPIKeyActive(ctx, "k1", false)
	if _, err := s.ResolveByAPIKey(ctx, raw); err == nil {
		t.Fatal("revoked key must not resolve")
	}
	// CreateAPIKey with a pre-computed hash.
	if _, err := s.CreateAPIKey(ctx, &APIKey{ID: "k2", HashedKey: HashAPIKey("known"), Active: true}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ResolveByAPIKey(ctx, "known"); err != nil || got.ID != "k2" {
		t.Fatalf("pre-hashed key: %v", err)
	}
}

func TestStoreHierarchy(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()
	if _, err := s.CreateTeam(ctx, &Team{ID: "team", OrgID: "missing"}); err == nil {
		t.Fatal("team without org must fail")
	}
	_, _ = s.CreateOrganization(ctx, &Organization{ID: "o1"})
	_, _ = s.CreateOrganization(ctx, &Organization{ID: "o2"})
	if _, err := s.CreateTeam(ctx, &Team{ID: "a", OrgID: "o1"}); err != nil {
		t.Fatal(err)
	}
	parent := TenantID("a")
	if _, err := s.CreateTeam(ctx, &Team{ID: "b", OrgID: "o2", ParentTeamID: &parent}); err == nil {
		t.Fatal("cross-org parent must fail")
	}
	if _, err := s.CreateUser(ctx, &User{ID: "u", TeamID: "a"}); err != nil {
		t.Fatal(err)
	}
	if u, err := s.GetUser(ctx, "u"); err != nil || u.TeamID != "a" {
		t.Fatalf("get user: %v", err)
	}
}

func TestQuotaConcurrentEnforceIsAtomic(t *testing.T) {
	s := NewInMemoryQuotaStore()
	q := &Quota{ID: "q", Scope: ScopeTenant, TargetID: "t", Limit: 100}
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Enforce(context.Background(), q, 1); err == nil {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if granted != 100 {
		t.Fatalf("expected exactly 100 grants, got %d", granted)
	}
	if q.Used != 0 {
		t.Fatal("caller's quota must not be mutated")
	}
}

func TestQuotaOverflowNegativeAndWindows(t *testing.T) {
	s := NewInMemoryQuotaStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	ctx := context.Background()
	q := &Quota{ID: "q", Limit: 10, Window: time.Minute}
	if _, err := s.Enforce(ctx, q, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enforce(ctx, q, math.MaxInt64); err == nil {
		t.Fatal("overflowing amount must be rejected")
	}
	if _, err := s.Enforce(ctx, q, -5); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("negative amount: %v", err)
	}
	_, err := s.Enforce(ctx, q, 3)
	var qe *QuotaEnforcedError
	if !errors.As(err, &qe) || qe.Remaining != 2 || qe.ResetAt.IsZero() {
		t.Fatalf("expected quota error with remaining 2, got %v", err)
	}
	now = now.Add(61 * time.Second)
	got, err := s.Enforce(ctx, q, 3)
	if err != nil || got.Used != 3 {
		t.Fatalf("window should reset: %v %+v", err, got)
	}
	if got, _ := s.Release(ctx, "q", 10); got.Used != 0 {
		t.Fatalf("release should floor at zero, got %d", got.Used)
	}
	if err := s.Upsert(ctx, &Quota{ID: "bad", Limit: -1}); err == nil {
		t.Fatal("negative limit must be rejected")
	}
	// Burst allows extra headroom.
	b := &Quota{ID: "b", Limit: 5, Burst: 2}
	if _, err := s.Enforce(ctx, b, 7); err != nil {
		t.Fatalf("burst: %v", err)
	}
}
