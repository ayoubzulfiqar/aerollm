package marketplace

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testRedisClient returns a client for the integration Redis named by
// AEROLLM_TEST_REDIS_ADDR, skipping the test when it is unset.
func testRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("AEROLLM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("AEROLLM_TEST_REDIS_ADDR not set; skipping Redis integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("AEROLLM_TEST_REDIS_ADDR=%s unreachable: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// testRedisPrefix returns a unique key prefix whose keys are deleted when the
// test ends.
func testRedisPrefix(t *testing.T, client *redis.Client) string {
	t.Helper()
	var b [6]byte
	_, _ = rand.Read(b[:])
	prefix := "aerollm:test:" + hex.EncodeToString(b[:])
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		iter := client.Scan(ctx, 0, prefix+":*", 500).Iterator()
		for iter.Next(ctx) {
			client.Del(ctx, iter.Val())
		}
	})
	return prefix
}

// signedWithHash signs a manifest whose wasm hash is derived from salt, so two
// manifests for the same id and version can differ.
func signedWithHash(t *testing.T, priv ed25519.PrivateKey, id, version, creator, salt string) PublishRequest {
	t.Helper()
	req, err := SignManifest(PublishRequest{
		ID: id, Name: "Plugin " + id, Version: version, CreatorID: creator,
		WASMHash: HashWASM([]byte(salt + id + version)),
	}, priv)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

// fakeLockStore is an in-memory store with a controllable (unfenced) lock.
type fakeLockStore struct {
	*InMemoryStore
	lockErr  error
	locked   atomic.Int32
	unlocked atomic.Int32
}

type fakeLock struct{ s *fakeLockStore }

func (l fakeLock) Unlock(context.Context) error { l.s.unlocked.Add(1); return nil }

func (s *fakeLockStore) LockPublish(_ context.Context, _ string) (PublishLock, error) {
	if s.lockErr != nil {
		return nil, s.lockErr
	}
	s.locked.Add(1)
	return fakeLock{s}, nil
}

func TestPublishUsesStoreLockAndMapsTimeout(t *testing.T) {
	_, priv := testKey(t)
	store := &fakeLockStore{InMemoryStore: NewInMemoryStore()}
	svc := NewRegistryService(nil, store)
	if _, created, err := svc.Publish(context.Background(), signedRequest(t, priv, "p1", "1.0.0", "alice")); err != nil || !created {
		t.Fatalf("publish: created=%v err=%v", created, err)
	}
	if store.locked.Load() != 1 || store.unlocked.Load() != 1 {
		t.Fatalf("expected one lock/unlock, got %d/%d", store.locked.Load(), store.unlocked.Load())
	}
	// Rejected publishes release the lock too.
	if _, _, err := svc.Publish(context.Background(), signedRequest(t, priv, "p1", "0.9.0", "alice")); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("expected version conflict, got %v", err)
	}
	if store.locked.Load() != 2 || store.unlocked.Load() != 2 {
		t.Fatalf("lock leaked on error path: %d/%d", store.locked.Load(), store.unlocked.Load())
	}

	store.lockErr = ErrLockTimeout
	srv := httptest.NewServer(svc.PluginsHandler())
	defer srv.Close()
	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(string(manifestJSON(t, signedRequest(t, priv, "p1", "2.0.0", "alice")))))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("expected 503 with Retry-After on lock timeout, got %d %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if _, meta, _ := store.Get(context.Background(), "p1"); meta.Version != "1.0.0" {
		t.Fatalf("publish without the lock must not write, stored %q", meta.Version)
	}
}

func TestRedisPublishLockExclusion(t *testing.T) {
	client := testRedisClient(t)
	store := NewRedisStore(RedisOptions{Client: client, Prefix: testRedisPrefix(t, client), LockWait: 150 * time.Millisecond})
	ctx := context.Background()

	lock, err := store.LockPublish(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := store.LockPublish(ctx, "p1"); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("expected ErrLockTimeout while held, got %v", err)
	}
	if waited := time.Since(start); waited < 100*time.Millisecond || waited > 2*time.Second {
		t.Fatalf("bounded wait took %v, want about LockWait", waited)
	}
	// Other plugins are independent.
	other, err := store.LockPublish(ctx, "p2")
	if err != nil {
		t.Fatalf("lock for another plugin: %v", err)
	}
	_ = other.Unlock(ctx)

	// A waiter gets the lock as soon as it is released.
	got := make(chan error, 1)
	go func() {
		l, err := NewRedisStore(RedisOptions{Client: client, Prefix: store.prefix, LockWait: 3 * time.Second}).LockPublish(ctx, "p1")
		if err == nil {
			err = l.Unlock(ctx)
		}
		got <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := lock.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("second unlock must be a no-op: %v", err)
	}
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("waiter: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiter did not acquire the released lock")
	}

	if _, err := store.LockPublish(ctx, "bad:id"); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected invalid id rejection, got %v", err)
	}
}

func TestRedisPublishLockTokenCheckedRelease(t *testing.T) {
	client := testRedisClient(t)
	store := NewRedisStore(RedisOptions{Client: client, Prefix: testRedisPrefix(t, client), LockTTL: 100 * time.Millisecond, LockWait: 2 * time.Second})
	ctx := context.Background()

	stale, err := store.LockPublish(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	// The stale holder stalls past the TTL; a second holder takes over.
	current, err := store.LockPublish(ctx, "p1")
	if err != nil {
		t.Fatalf("lock after TTL expiry: %v", err)
	}
	// Releasing the stale lock must not free the current holder's lock.
	if err := stale.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	token, err := client.Get(ctx, store.lockKey("p1")).Result()
	if err != nil || token != current.(*redisPublishLock).token {
		t.Fatalf("current holder's lock was released by a stale holder: %q %v", token, err)
	}
	// A stale holder's fenced write is rejected and writes nothing.
	_, priv := testKey(t)
	m, err := VerifyPublishRequest(signedRequest(t, priv, "p1", "1.0.0", "alice"))
	if err != nil {
		t.Fatal(err)
	}
	if err := stale.(fencedPublishLock).putFenced(ctx, *m, Metadata{}, false); !errors.Is(err, ErrLockLost) {
		t.Fatalf("expected ErrLockLost for a stale holder, got %v", err)
	}
	if _, _, err := store.Lookup(ctx, "p1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale fenced write must not be stored, got %v", err)
	}
	if err := current.(fencedPublishLock).putFenced(ctx, *m, Metadata{Version: "1.0.0"}, false); err != nil {
		t.Fatalf("current holder's fenced write: %v", err)
	}
	if _, meta, err := store.Lookup(ctx, "p1"); err != nil || meta.Version != "1.0.0" {
		t.Fatalf("fenced write not stored: %+v %v", meta, err)
	}
	if err := current.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := client.Exists(ctx, store.lockKey("p1")).Result(); n != 0 {
		t.Fatal("lock key still present after release")
	}
}

func TestRedisPublishLockContextCancel(t *testing.T) {
	client := testRedisClient(t)
	store := NewRedisStore(RedisOptions{Client: client, Prefix: testRedisPrefix(t, client), LockWait: 10 * time.Second})
	held, err := store.LockPublish(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Unlock(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := store.LockPublish(ctx, "p1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected ctx deadline, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("lock wait ignored ctx")
	}

	// Unlock works even with an already-canceled context.
	canceled, cancelNow := context.WithCancel(context.Background())
	l2, err := store.LockPublish(context.Background(), "p2")
	if err != nil {
		t.Fatal(err)
	}
	cancelNow()
	if err := l2.Unlock(canceled); err != nil {
		t.Fatalf("unlock with canceled ctx: %v", err)
	}
	if n, _ := client.Exists(context.Background(), store.lockKey("p2")).Result(); n != 0 {
		t.Fatal("lock not released with a canceled context")
	}
}

// slowLookupStore widens the read-check-write race window of Publish.
type slowLookupStore struct {
	*RedisStore
	delay time.Duration
}

func (s *slowLookupStore) Lookup(ctx context.Context, id string) (VerifiedManifest, Metadata, error) {
	m, meta, err := s.RedisStore.Lookup(ctx, id)
	time.Sleep(s.delay)
	return m, meta, err
}

// TestRedisRegistryServicesSerializePublishes runs two registry instances on
// the same Redis. Each round they race to publish different manifests for the
// same plugin version; exactly one may win and the other must see the
// winner's record (a version conflict), never a silent overwrite.
func TestRedisRegistryServicesSerializePublishes(t *testing.T) {
	client := testRedisClient(t)
	prefix := testRedisPrefix(t, client)
	newSvc := func() *RegistryService {
		return NewRegistryService(nil, &slowLookupStore{RedisStore: NewRedisStore(RedisOptions{Client: client, Prefix: prefix}), delay: 30 * time.Millisecond})
	}
	a, b := newSvc(), newSvc()
	_, priv := testKey(t)
	ctx := context.Background()

	for round := 0; round < 5; round++ {
		id := "race-" + string(rune('a'+round))
		reqs := []PublishRequest{
			signedWithHash(t, priv, id, "1.0.0", "alice", "A"),
			signedWithHash(t, priv, id, "1.0.0", "alice", "B"),
		}
		type result struct {
			meta    Metadata
			created bool
			err     error
		}
		results := make([]result, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, svc := range []*RegistryService{a, b} {
			wg.Add(1)
			go func(i int, svc *RegistryService) {
				defer wg.Done()
				<-start
				meta, created, err := svc.Publish(ctx, reqs[i])
				results[i] = result{meta, created, err}
			}(i, svc)
		}
		close(start)
		wg.Wait()

		created, conflicts := 0, 0
		winner := -1
		for i, r := range results {
			switch {
			case r.err == nil && r.created:
				created++
				winner = i
			case errors.Is(r.err, ErrVersionConflict):
				conflicts++
			default:
				t.Fatalf("round %d: unexpected result %+v", round, r)
			}
		}
		if created != 1 || conflicts != 1 {
			t.Fatalf("round %d: expected one winner and one conflict, got %d created, %d conflicts", round, created, conflicts)
		}
		stored, _, err := a.store.(*slowLookupStore).Lookup(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if stored.WASMHash != reqs[winner].WASMHash {
			t.Fatalf("round %d: stored manifest is not the winner's (lost update)", round)
		}
	}
	if n, _ := client.Keys(ctx, prefix+":lock:*").Result(); len(n) != 0 {
		t.Fatalf("locks left behind: %v", n)
	}
}

// TestRedisRegistrySharedTOFU checks that trust-on-first-use pins are shared:
// an instance whose local pins were seeded before another instance pinned a
// creator must still reject a different key for that creator.
func TestRedisRegistrySharedTOFU(t *testing.T) {
	client := testRedisClient(t)
	prefix := testRedisPrefix(t, client)
	a := NewRegistryService(nil, NewRedisStore(RedisOptions{Client: client, Prefix: prefix}))
	b := NewRegistryService(nil, NewRedisStore(RedisOptions{Client: client, Prefix: prefix}))
	ctx := context.Background()
	_, alice := testKey(t)
	_, mallory := testKey(t)
	_, bob := testKey(t)

	// b seeds its local pins while the store is still empty of alice.
	if _, _, err := b.Publish(ctx, signedRequest(t, bob, "bob-plugin", "1.0.0", "bob")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.Publish(ctx, signedRequest(t, alice, "alice-1", "1.0.0", "alice")); err != nil {
		t.Fatal(err)
	}
	// Impersonation through the stale instance is rejected.
	if _, _, err := b.Publish(ctx, signedRequest(t, mallory, "alice-2", "1.0.0", "alice")); !errors.Is(err, ErrUntrustedKey) {
		t.Fatalf("expected ErrUntrustedKey for a second key via another instance, got %v", err)
	}
	if _, _, err := b.store.(*RedisStore).Lookup(ctx, "alice-2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected publish must not be stored, got %v", err)
	}
	// The rejection did not poison b's local pins: alice's real key works there.
	if _, created, err := b.Publish(ctx, signedRequest(t, alice, "alice-3", "1.0.0", "alice")); err != nil || !created {
		t.Fatalf("legit publish via b: created=%v err=%v", created, err)
	}

	// A registry written before the shared pins existed is back-filled from
	// its manifests on first use.
	legacyPrefix := testRedisPrefix(t, client)
	legacy := NewRedisStore(RedisOptions{Client: client, Prefix: legacyPrefix})
	m, err := VerifyPublishRequest(signedRequest(t, alice, "old", "1.0.0", "alice"))
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Put(ctx, *m, Metadata{}); err != nil {
		t.Fatal(err)
	}
	c := NewRegistryService(nil, legacy)
	if _, _, err := c.Publish(ctx, signedRequest(t, bob, "bob-2", "1.0.0", "bob")); err != nil {
		t.Fatal(err)
	}
	if n, _ := client.SCard(ctx, legacy.creatorKeysKey("alice")).Result(); n != 1 {
		t.Fatalf("expected alice's stored key to be back-filled, got %d keys", n)
	}
}
