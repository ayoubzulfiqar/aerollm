package keymanager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

// redisTestClient connects to AEROLLM_TEST_REDIS_ADDR and returns a unique
// key prefix that is cleaned up after the test.
func redisTestClient(t *testing.T) (*redis.Client, string) {
	t.Helper()
	addr := os.Getenv("AEROLLM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("AEROLLM_TEST_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("redis at %s: %v", addr, err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	prefix := "aerollm-test:keymanager:" + hex.EncodeToString(b) + ":"
	t.Cleanup(func() {
		iter := client.Scan(ctx, 0, prefix+"*", 200).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
		_ = client.Close()
	})
	return client, prefix
}

func TestRedisKeyStoreIntegration(t *testing.T) {
	client, prefix := redisTestClient(t)
	ctx := context.Background()
	store := NewRedisKeyStore(client, prefix)
	mgr := NewManager(store, testMaster)

	resp, err := mgr.Generate(ctx, &GenerateRequest{Models: []string{"gpt-4o"}, MaxBudget: 2, RateLimitRPS: 5, Metadata: map[string]interface{}{"app": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	vk, err := mgr.Validate(ctx, resp.Key)
	if err != nil {
		t.Fatal(err)
	}
	if vk.HashedKey != resp.KeyHash || vk.Metadata["app"] != "x" || vk.Metadata[MetadataRateLimitRPS] != 5.0 {
		t.Fatalf("round trip: %+v", vk)
	}
	if err := store.Create(ctx, vk); !errors.Is(err, ErrKeyExists) {
		t.Fatalf("duplicate create: %v", err)
	}
	if got, err := store.GetByPrefix(ctx, resp.Prefix); err != nil || got.KeyHash != resp.KeyHash {
		t.Fatalf("GetByPrefix: %v", err)
	}
	if _, err := store.Get(ctx, strings.Repeat("0", 64)); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("missing key: %v", err)
	}

	// A second "instance" sharing Redis sees the same keys and state.
	mgr2 := NewManager(NewRedisKeyStore(client, prefix), testMaster)
	if err := mgr2.RecordSpend(ctx, resp.Key, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Validate(ctx, resp.Key); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("spend from another instance not visible: %v", err)
	}
	rotated, err := mgr2.Regenerate(ctx, resp.KeyHash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Validate(ctx, resp.Key); !errors.Is(err, ErrKeyRevoked) {
		t.Fatalf("revocation from another instance not visible: %v", err)
	}
	infos, err := mgr.List(ctx, KeyFilter{IncludeRevoked: true})
	if err != nil || len(infos) != 2 {
		t.Fatalf("List: %d %v", len(infos), err)
	}
	if err := store.Delete(ctx, resp.KeyHash); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, resp.KeyHash); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("second delete: %v", err)
	}
	if _, err := store.GetByPrefix(ctx, resp.Prefix); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("prefix index not cleaned: %v", err)
	}
	if infos, _ := mgr.List(ctx, KeyFilter{IncludeRevoked: true}); len(infos) != 1 || infos[0].KeyHash != rotated.KeyHash {
		t.Fatalf("List after delete: %+v", infos)
	}

	// Hash-only storage: no plaintext key anywhere in Redis.
	iter := client.Scan(ctx, 0, prefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		if strings.Contains(k, resp.Key) || strings.Contains(k, rotated.Key) {
			t.Fatal("plaintext key in a Redis key name")
		}
		if typ, _ := client.Type(ctx, k).Result(); typ == "string" {
			v, _ := client.Get(ctx, k).Result()
			if strings.Contains(v, resp.Key) || strings.Contains(v, rotated.Key) {
				t.Fatal("plaintext key stored in Redis")
			}
		}
	}
}

func TestRedisStoresConcurrentSpendAcrossInstances(t *testing.T) {
	client, prefix := redisTestClient(t)
	ctx := context.Background()
	newInstance := func() *Manager {
		m := NewManager(NewRedisKeyStore(client, prefix), "")
		m.SetTeamStore(NewRedisTeamStore(client, prefix))
		return m
	}
	a, b := newInstance(), newInstance()
	if err := NewRedisTeamStore(client, prefix).Create(ctx, &Team{ID: "team_r", Name: "R", Budget: 100}); err != nil {
		t.Fatal(err)
	}
	k1, err := a.Generate(ctx, &GenerateRequest{TeamID: "team_r"})
	if err != nil {
		t.Fatal(err)
	}
	k2, err := b.Generate(ctx, &GenerateRequest{TeamID: "team_r"})
	if err != nil {
		t.Fatal(err)
	}
	const perInstance = 40
	var wg sync.WaitGroup
	for i := 0; i < perInstance; i++ {
		for _, m := range []*Manager{a, b} {
			wg.Add(2)
			go func(m *Manager) {
				defer wg.Done()
				if err := m.RecordSpend(ctx, k1.Key, 0.25); err != nil {
					t.Error(err)
				}
			}(m)
			go func(m *Manager) {
				defer wg.Done()
				if err := m.RecordSpend(ctx, k2.KeyHash, 0.5); err != nil {
					t.Error(err)
				}
			}(m)
		}
	}
	wg.Wait()
	i1, _ := a.Info(ctx, k1.KeyHash)
	i2, _ := b.Info(ctx, k2.KeyHash)
	team, err := a.TeamInfo(ctx, "team_r")
	if err != nil {
		t.Fatal(err)
	}
	if i1.Spend != 2*perInstance*0.25 || i2.Spend != 2*perInstance*0.5 {
		t.Fatalf("key spend lost: %v %v", i1.Spend, i2.Spend)
	}
	if want := 2 * perInstance * 0.75; math.Abs(team.Spend-want) > 1e-9 {
		t.Fatalf("team spend = %v, want %v", team.Spend, want)
	}
	if _, err := a.Validate(ctx, k1.Key); err != nil {
		t.Fatalf("under team budget: %v", err)
	}
	if err := b.RecordSpend(ctx, k2.Key, 40); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Validate(ctx, k1.Key); !errors.Is(err, ErrTeamBudgetExceeded) {
		t.Fatalf("team budget not enforced across instances: %v", err)
	}
}

func TestRedisUserAndTeamStores(t *testing.T) {
	client, prefix := redisTestClient(t)
	ctx := context.Background()
	users := NewRedisUserStore(client, prefix)
	if err := users.Update(ctx, &User{ID: "u1"}); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("update missing user: %v", err)
	}
	if err := users.Create(ctx, &User{ID: "u1", Email: "a@b.c"}); err != nil {
		t.Fatal(err)
	}
	if err := users.Create(ctx, &User{ID: "u1"}); !errors.Is(err, ErrUserExists) {
		t.Fatalf("duplicate user: %v", err)
	}
	if err := users.Update(ctx, &User{ID: "u1", Email: "new@b.c"}); err != nil {
		t.Fatal(err)
	}
	if u, err := users.Get(ctx, "u1"); err != nil || u.Email != "new@b.c" {
		t.Fatalf("user: %+v %v", u, err)
	}
	if _, err := users.Get(ctx, "u2"); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("missing user: %v", err)
	}

	teams := NewRedisTeamStore(client, prefix)
	if _, err := teams.Get(ctx, "t"); !errors.Is(err, ErrTeamNotFound) {
		t.Fatalf("missing team: %v", err)
	}
	orig := &Team{ID: "t", Name: "T", Budget: 3}
	if err := teams.Create(ctx, orig); err != nil {
		t.Fatal(err)
	}
	if err := teams.Create(ctx, orig); !errors.Is(err, ErrTeamExists) {
		t.Fatalf("duplicate team: %v", err)
	}
	stored, _ := teams.Get(ctx, "t")
	if err := teams.Update(ctx, &Team{ID: "t", Name: "T2"}); err != nil {
		t.Fatal(err)
	}
	got, _ := teams.Get(ctx, "t")
	if got.Name != "T2" || !got.CreatedAt.Equal(stored.CreatedAt) {
		t.Fatalf("update: %+v", got)
	}
	if _, err := teams.UpdateFunc(ctx, "missing", func(*Team) error { return nil }); !errors.Is(err, ErrTeamNotFound) {
		t.Fatalf("UpdateFunc missing: %v", err)
	}
	boom := errors.New("boom")
	if _, err := teams.UpdateFunc(ctx, "t", func(tm *Team) error { tm.Name = "x"; return boom }); !errors.Is(err, boom) {
		t.Fatalf("fn error: %v", err)
	}
	if got, _ := teams.Get(ctx, "t"); got.Name != "T2" {
		t.Fatal("failed UpdateFunc must not write")
	}

	// A handler wired to Redis stores works end to end.
	h := NewKeyHandler(NewManager(NewRedisKeyStore(client, prefix), testMaster), users, teams, nil)
	if h.Manager.Teams() != teams {
		t.Fatal("team store not attached")
	}
}

func TestRedisStoreWithoutClient(t *testing.T) {
	s := NewRedisKeyStore(nil, "x:")
	if _, err := s.Get(context.Background(), strings.Repeat("a", 64)); !errors.Is(err, errNoRedisClient) {
		t.Fatalf("nil client: %v", err)
	}
	if _, err := s.UpdateFunc(context.Background(), strings.Repeat("a", 64), func(*VirtualKey) error { return nil }); !errors.Is(err, errNoRedisClient) {
		t.Fatalf("nil client update: %v", err)
	}
	if _, err := NewRedisTeamStore(nil, "").Get(context.Background(), "t"); !errors.Is(err, errNoRedisClient) {
		t.Fatalf("nil client team: %v", err)
	}
}
