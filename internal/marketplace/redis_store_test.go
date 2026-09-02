package marketplace

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreLifecycle(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available")
	}
	store := NewRedisStore(RedisOptions{Client: client})
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
}
