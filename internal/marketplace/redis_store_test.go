package marketplace

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreLifecycle(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	store := NewRedisStore(RedisOptions{Client: client, Prefix: testRedisPrefix(t, client)})
	manifest := VerifiedManifest{ID: "p1", Name: "Redis Plugin", Version: "1.0.0", CreatorID: "creator", PublicKey: []byte("pk"), Signature: []byte("sig"), Payload: []byte("payload")}
	meta := Metadata{ID: "p1", Name: "Redis Plugin", Version: "1.0.0", CreatorID: "creator"}
	if err := store.Put(ctx, manifest, meta); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	gotManifest, gotMeta, ok := store.Get(ctx, "p1")
	if !ok || gotMeta.ID != "p1" || gotMeta.Name != "Redis Plugin" || gotManifest.ID != "p1" {
		t.Fatalf("unexpected get result: manifest=%+v meta=%+v", gotManifest, gotMeta)
	}
	items, err := store.List(ctx)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 1 || items[0].ID != "p1" {
		t.Fatalf("unexpected list result: %+v", items)
	}
	if _, _, err := store.Lookup(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// unreachableClient points at a closed port so any command that actually
// reaches the network fails fast.
func unreachableClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
}

func TestRedisStoreRejectsKeyInjectionBeforeNetwork(t *testing.T) {
	store := NewRedisStore(RedisOptions{Client: unreachableClient()})
	ctx := context.Background()
	for _, id := range []string{"", "a:meta:b", "../x", "a b", "a\r\nFLUSHALL"} {
		err := store.Put(ctx, VerifiedManifest{ID: id}, Metadata{})
		if !errors.Is(err, ErrInvalidManifest) {
			t.Errorf("Put(%q): expected validation error, got %v", id, err)
		}
		if _, _, err := store.Lookup(ctx, id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Lookup(%q): expected ErrNotFound without touching redis, got %v", id, err)
		}
	}
	if err := store.Put(ctx, VerifiedManifest{ID: "p1"}, Metadata{ID: "p2"}); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected id mismatch error, got %v", err)
	}
}

func TestRedisStoreSurfacesBackendErrors(t *testing.T) {
	store := NewRedisStore(RedisOptions{Client: unreachableClient()})
	ctx := context.Background()
	if err := store.Put(ctx, VerifiedManifest{ID: "p1"}, Metadata{}); err == nil {
		t.Fatal("expected put error")
	}
	_, _, err := store.Lookup(ctx, "p1")
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("expected backend error distinct from not-found, got %v", err)
	}
	if _, err := store.List(ctx); err == nil {
		t.Fatal("expected list error")
	}
	if _, _, ok := store.Get(ctx, "p1"); ok {
		t.Fatal("expected Get to report missing on backend error")
	}

	var nilClient RedisStore
	if err := nilClient.Put(ctx, VerifiedManifest{ID: "p1"}, Metadata{}); err == nil {
		t.Fatal("expected error for store without client")
	}
}
