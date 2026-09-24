package keymanager

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// flakyStore wraps a persist.Store and fails Put/Delete while fail is set.
type flakyStore struct {
	persist.Store
	fail atomic.Bool
}

func (f *flakyStore) Put(bucket, key string, v any) error {
	if f.fail.Load() {
		return errors.New("disk full")
	}
	return f.Store.Put(bucket, key, v)
}

func (f *flakyStore) Delete(bucket, key string) error {
	if f.fail.Load() {
		return errors.New("disk full")
	}
	return f.Store.Delete(bucket, key)
}

type persistentStores struct {
	keys  *PersistentKeyStore
	users *PersistentUserStore
	teams *PersistentTeamStore
}

func openPersistent(t *testing.T, ps persist.Store) persistentStores {
	t.Helper()
	keys, err := NewPersistentKeyStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	users, err := NewPersistentUserStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	teams, err := NewPersistentTeamStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	return persistentStores{keys, users, teams}
}

func TestPersistentStoresSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "gateway.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st := openPersistent(t, ps)
	mgr := NewManager(st.keys, testMaster)
	mgr.SetTeamStore(st.teams)
	if err := st.teams.Create(ctx, &Team{ID: "team_x", Name: "X", Budget: 10, BudgetDuration: "30d"}); err != nil {
		t.Fatal(err)
	}
	if err := st.users.Create(ctx, &User{ID: "u1", Email: "a@b.c", Metadata: map[string]interface{}{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	live, err := mgr.Generate(ctx, &GenerateRequest{TeamID: "team_x", MaxBudget: 5, RateLimitTPM: 1000, Models: []string{"gpt-4o"}})
	if err != nil {
		t.Fatal(err)
	}
	revoked, _ := mgr.Generate(ctx, &GenerateRequest{})
	if err := mgr.Delete(ctx, revoked.KeyHash); err != nil {
		t.Fatal(err)
	}
	if err := mgr.RecordSpend(ctx, live.Key, 1.5); err != nil {
		t.Fatal(err)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	// "Restart": reopen the file and rebuild everything from it.
	ps, err = persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	st = openPersistent(t, ps)
	mgr = NewManager(st.keys, testMaster)
	mgr.SetTeamStore(st.teams)
	vk, err := mgr.Validate(ctx, live.Key)
	if err != nil {
		t.Fatalf("key lost across restart: %v", err)
	}
	if vk.Spend != 1.5 || vk.MaxBudget != 5 || !vk.AllowsModel("gpt-4o") || vk.HashedKey != live.KeyHash {
		t.Fatalf("restored key: %+v", vk)
	}
	if tpm, ok := vk.Metadata[MetadataRateLimitTPM].(int); !ok || tpm != 1000 {
		t.Fatalf("rate limit metadata type/value after restart: %#v", vk.Metadata[MetadataRateLimitTPM])
	}
	if _, err := mgr.Validate(ctx, revoked.Key); !errors.Is(err, ErrKeyRevoked) {
		t.Fatalf("revocation lost across restart: %v", err)
	}
	team, err := mgr.TeamInfo(ctx, "team_x")
	if err != nil || team.Spend != 1.5 || team.BudgetResetAt.IsZero() {
		t.Fatalf("team after restart: %+v %v", team, err)
	}
	if u, err := st.users.Get(ctx, "u1"); err != nil || u.Email != "a@b.c" {
		t.Fatalf("user after restart: %+v %v", u, err)
	}
	if _, err := st.users.Get(ctx, "nope"); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("missing user: %v", err)
	}
	if byPrefix, err := st.keys.GetByPrefix(ctx, live.Prefix); err != nil || byPrefix.KeyHash != live.KeyHash {
		t.Fatalf("GetByPrefix after restart: %v", err)
	}

	// Only hashes are stored: the plaintext key never reaches the file.
	var raw []string
	_ = ps.ForEach(BucketKeys, func(key string, doc json.RawMessage) error {
		raw = append(raw, key, string(doc))
		return nil
	})
	for _, s := range raw {
		if strings.Contains(s, live.Key) || strings.Contains(s, revoked.Key) {
			t.Fatal("plaintext key persisted")
		}
	}
}

func TestPersistentStoreSurfacesWriteErrors(t *testing.T) {
	fs := &flakyStore{Store: persist.NewMemory()}
	st := openPersistent(t, fs)
	mgr := NewManager(st.keys, "")
	mgr.SetTeamStore(st.teams)
	ctx := context.Background()
	_ = st.teams.Create(ctx, &Team{ID: "t", Name: "t"})
	ok, err := mgr.Generate(ctx, &GenerateRequest{TeamID: "t"})
	if err != nil {
		t.Fatal(err)
	}

	fs.fail.Store(true)
	if _, err := mgr.Generate(ctx, &GenerateRequest{}); err == nil {
		t.Fatal("Generate must fail when the key cannot be persisted")
	}
	if keys, _ := st.keys.List(ctx); len(keys) != 1 {
		t.Fatalf("failed create must not be visible: %d keys", len(keys))
	}
	if err := mgr.RecordSpend(ctx, ok.Key, 1); err == nil {
		t.Fatal("RecordSpend must report persistence failures")
	}
	if err := mgr.Delete(ctx, ok.KeyHash); err == nil {
		t.Fatal("Delete must report persistence failures")
	}
	if err := st.users.Create(ctx, &User{ID: "u"}); err == nil {
		t.Fatal("user create must report persistence failures")
	}
	// Memory is unchanged by failed writes.
	vk, err := mgr.Validate(ctx, ok.Key)
	if err != nil || vk.Spend != 0 {
		t.Fatalf("failed writes leaked into memory: %+v %v", vk, err)
	}
	if team, _ := st.teams.Get(ctx, "t"); team.Spend != 0 {
		t.Fatalf("team spend changed by failed write: %v", team.Spend)
	}
	if err := st.keys.Delete(ctx, ok.KeyHash); err == nil {
		t.Fatal("hard delete must report persistence failures")
	}
	fs.fail.Store(false)
	if err := mgr.RecordSpend(ctx, ok.Key, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.keys.Delete(ctx, ok.KeyHash); err != nil {
		t.Fatal(err)
	}
	if err := st.keys.Delete(ctx, ok.KeyHash); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}

func TestPersistentStoreRejectsCorruptDocuments(t *testing.T) {
	ps := persist.NewMemory()
	if err := ps.Put(BucketKeys, strings.Repeat("a", 64), map[string]any{"key_hash": strings.Repeat("b", 64)}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPersistentKeyStore(ps); err == nil {
		t.Fatal("mismatched key document must be rejected")
	}
	ps2 := persist.NewMemory()
	_ = ps2.Put(BucketTeams, "t", "not an object")
	if _, err := NewPersistentTeamStore(ps2); err == nil {
		t.Fatal("undecodable team must be rejected")
	}
	if _, err := NewPersistentUserStore(nil); err == nil {
		t.Fatal("nil persist store must be rejected")
	}
}

func TestPersistentStoreConcurrentSpend(t *testing.T) {
	st := openPersistent(t, persist.NewMemory())
	mgr := NewManager(st.keys, "")
	mgr.SetTeamStore(st.teams)
	ctx := context.Background()
	_ = st.teams.Create(ctx, &Team{ID: "t", Name: "t"})
	resp, _ := mgr.Generate(ctx, &GenerateRequest{TeamID: "t"})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = mgr.RecordSpend(ctx, resp.Key, 0.01)
		}()
		go func() {
			defer wg.Done()
			_, _ = mgr.Validate(ctx, resp.Key)
		}()
	}
	wg.Wait()
	info, _ := mgr.Info(ctx, resp.KeyHash)
	team, _ := mgr.TeamInfo(ctx, "t")
	if math.Abs(info.Spend-1) > 1e-9 || math.Abs(team.Spend-1) > 1e-9 {
		t.Fatalf("key spend %v, team spend %v, want 1", info.Spend, team.Spend)
	}
}
