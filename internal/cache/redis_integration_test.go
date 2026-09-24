package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/redis/go-redis/v9"
)

// integrationCache returns a RedisCache over AEROLLM_TEST_REDIS_ADDR with a
// unique key prefix whose keys are deleted when the test ends; it skips
// when the variable is unset.
func integrationCache(t *testing.T, ttl time.Duration) (*RedisCache, *redis.Client, string) {
	t.Helper()
	addr := os.Getenv("AEROLLM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("AEROLLM_TEST_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("redis unavailable: %v", err)
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	prefix := "aerollm-test:cache:" + hex.EncodeToString(b[:]) + ":"
	t.Cleanup(func() {
		var keys []string
		iter := client.Scan(ctx, 0, prefix+"*", 200).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		client.Close()
	})
	return NewRedisCacheWithOptions(client, RedisCacheOptions{TTL: ttl, KeyPrefix: prefix}), client, prefix
}

func chatReq(prompt string) *models.LLMRequest {
	return &models.LLMRequest{Model: "gpt-4o", Messages: []models.Message{{Role: models.RoleUser, Content: &prompt}}}
}

func TestRedisExactCacheGetSetIntegration(t *testing.T) {
	c, client, prefix := integrationCache(t, time.Hour)
	ctx := context.Background()
	key := KeyForRequestNS(TenantNamespace("sk-tenant-a"), chatReq("hello"))
	if hit, err := c.GetExactCtx(ctx, key); hit != nil || err != nil {
		t.Fatalf("expected miss, got %+v %v", hit, err)
	}
	resp := []byte(`{"id":"chatcmpl-1","choices":[]}`)
	if err := c.SetExactWithMeta(ctx, key, resp, EntryMeta{Model: "gpt-4o", TokenCount: 42, TTL: 5 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	hit, err := c.GetExactCtx(ctx, key)
	if err != nil || hit == nil || string(hit.Response) != string(resp) || hit.Model != "gpt-4o" || hit.TokenCount != 42 || hit.CreatedAt.IsZero() {
		t.Fatalf("unexpected hit %+v %v", hit, err)
	}
	// The key is stored under the prefix with the per-entry TTL.
	ttl, err := client.TTL(ctx, prefix+key).Result()
	if err != nil || ttl <= 0 || ttl > 5*time.Minute {
		t.Fatalf("expected per-entry TTL, got %v %v", ttl, err)
	}
	if n, _ := client.Exists(ctx, key).Result(); n != 0 {
		t.Fatal("unprefixed key must not be written")
	}
	// Binary (non-JSON) payloads round-trip.
	bin := []byte{0xff, 0x00, 0x01}
	binKey := KeyForRequestNS(TenantNamespace("sk-tenant-a"), chatReq("binary"))
	_ = c.SetExact(binKey, bin, 1)
	if hit, _ := c.GetExact(binKey); hit == nil || string(hit.Response) != string(bin) {
		t.Fatalf("binary payload: %+v", hit)
	}
	// Legacy raw values are still readable.
	legacyKey := KeyForRequest(chatReq("legacy"))
	client.Set(ctx, prefix+legacyKey, "raw-bytes", time.Minute)
	if hit, _ := c.GetExactCtx(ctx, legacyKey); hit == nil || string(hit.Response) != "raw-bytes" {
		t.Fatalf("legacy value: %+v", hit)
	}
	if err := c.DeleteExact(ctx, key); err != nil {
		t.Fatal(err)
	}
	if hit, _ := c.GetExactCtx(ctx, key); hit != nil {
		t.Fatal("deleted entry must miss")
	}
	st, err := c.ExactStats(ctx)
	if err != nil || st.Hits != 3 || st.Misses != 2 || st.Sets != 2 || st.Entries != 2 {
		t.Fatalf("unexpected stats %+v %v", st, err)
	}
}

func TestRedisExactCacheNamespacesScanAndClearIntegration(t *testing.T) {
	c, client, prefix := integrationCache(t, time.Hour)
	ctx := context.Background()
	nsA, nsB := TenantNamespace("sk-tenant-a"), TenantNamespace("sk-tenant-b")
	req := chatReq("same prompt")
	keyA, keyB := KeyForRequestNS(nsA, req), KeyForRequestNS(nsB, req)
	if keyA == keyB {
		t.Fatal("tenants must not share cache keys")
	}
	_ = c.SetExactWithMeta(ctx, keyA, []byte(`"a"`), EntryMeta{Model: "gpt-4o"})
	_ = c.SetExactWithMeta(ctx, keyB, []byte(`"b"`), EntryMeta{Model: "gpt-4o"})
	for i := 0; i < 23; i++ {
		_ = c.SetExactWithMeta(ctx, KeyForRequestNS(nsA, chatReq(fmt.Sprintf("p%d", i))), []byte(`"x"`), EntryMeta{Model: "m", TokenCount: i})
	}
	// A second deployment sharing the Redis under another prefix.
	other := NewRedisCacheWithOptions(client, RedisCacheOptions{TTL: time.Hour, KeyPrefix: prefix + "other:"})
	_ = other.SetExact(keyA, []byte(`"other"`), 1)

	// Inspect pages through every entry (SCAN) without payloads.
	seen := map[string]bool{}
	cursor := "0"
	for pages := 0; pages < 100; pages++ {
		entries, next, err := c.Inspect(ctx, cursor, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if len(e.Key) < len(ExactKeyPrefix) || e.Key[:len(ExactKeyPrefix)] != ExactKeyPrefix {
				t.Fatalf("inspect must return logical keys, got %q", e.Key)
			}
			if e.CreatedAt.IsZero() || e.ExpiresAt.IsZero() {
				t.Fatalf("missing timestamps %+v", e)
			}
			seen[e.Key] = true
		}
		cursor = next
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 25 || !seen[keyA] || !seen[keyB] {
		t.Fatalf("inspect saw %d keys, want 25 (the other deployment must be invisible)", len(seen))
	}
	if st, _ := c.ExactStats(ctx); st.Entries != 25 {
		t.Fatalf("stats must count this deployment's 25 entries, got %d", st.Entries)
	}

	n, err := c.ClearNamespace(ctx, nsA)
	if err != nil || n != 24 {
		t.Fatalf("clear namespace A: %d %v", n, err)
	}
	if hit, _ := c.GetExactCtx(ctx, keyA); hit != nil {
		t.Fatal("namespace A must be cleared")
	}
	if hit, _ := c.GetExactCtx(ctx, keyB); hit == nil || string(hit.Response) != `"b"` {
		t.Fatalf("namespace B must survive: %+v", hit)
	}
	if _, err := c.ClearNamespace(ctx, ""); err == nil {
		t.Fatal("empty namespace must be rejected")
	}
	if err := c.ClearExact(ctx); err != nil {
		t.Fatal(err)
	}
	if st, _ := c.ExactStats(ctx); st.Entries != 0 {
		t.Fatalf("clear must remove every entry, %d left", st.Entries)
	}
	if hit, _ := other.GetExactCtx(ctx, keyA); hit == nil || string(hit.Response) != `"other"` {
		t.Fatalf("clearing one prefix must not touch another deployment: %+v", hit)
	}
	stats, _ := c.Stats(ctx)
	if stats["exact_total_entries"] != 0 {
		t.Fatalf("stats map: %v", stats)
	}
}

func TestEscapeGlob(t *testing.T) {
	if got := escapeGlob(`a*b?c[d]e\f`); got != `a\*b\?c\[d\]e\\f` {
		t.Fatalf("escapeGlob: %q", got)
	}
	c := NewRedisCacheWithOptions(nil, RedisCacheOptions{KeyPrefix: "x*"})
	if got := c.pattern(ExactKeyPrefix); got != `x\*cache:exact:*` {
		t.Fatalf("pattern: %q", got)
	}
}
