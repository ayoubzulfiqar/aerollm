package cache

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/redis/go-redis/v9"
)

// RedisClient mirrors go-redis/v9 to keep cache.go buildable without tight coupling.
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	Close() error
}

// RedisCache wraps Redis for prompt caching.
type RedisCache struct {
	client RedisClient
	ttl    time.Duration
	sem    *SemanticCache
}

// NewRedisCache creates a new RedisCache.
func NewRedisCache(client RedisClient, ttl time.Duration) *RedisCache {
	return &RedisCache{
		client: client,
		ttl:    ttl,
		sem:    NewSemanticCache("sem"),
	}
}

// CacheEntry represents a cached LLM response.
type CacheEntry struct {
	Key         string
	Response    []byte
	CreatedAt   time.Time
	TTL         time.Duration
	TokenCount  int
	Semantic    bool
}

// KeyForRequest generates a deterministic cache key from an LLM request.
func KeyForRequest(req *models.LLMRequest) string {
	b, _ := json.Marshal(req)
	return fmt.Sprintf("%x", md5.Sum(b))
}

// GetExact retrieves an exact-match cached response.
func (c *RedisCache) GetExact(key string) (*CacheEntry, error) {
	if c.client == nil {
		return nil, nil
	}
	val, err := c.client.Get(context.Background(), key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &CacheEntry{
		Key:      key,
		Response: []byte(val),
	}, nil
}

// SetExact stores a response in the exact-match cache.
func (c *RedisCache) SetExact(key string, resp []byte, tokenCount int) error {
	if c.client == nil {
		return nil
	}
	_ = tokenCount
	return c.client.Set(context.Background(), key, string(resp), c.ttl).Err()
}

// GetSemantic retrieves a semantically similar cached response.
func (c *RedisCache) GetSemantic(key string, threshold float64) (*CacheEntry, error) {
	if c == nil || c.sem == nil {
		return nil, nil
	}
	hit, err := c.sem.Search(key, threshold)
	if err != nil || hit == nil {
		return nil, err
	}
	return &CacheEntry{
		Key:      hit.Key,
		Response: hit.Response,
		Semantic: true,
	}, nil
}

// SetSemantic stores a response in the semantic cache.
func (c *RedisCache) SetSemantic(key string, resp []byte, tokenCount int) error {
	if c == nil || c.sem == nil {
		return nil
	}
	_ = tokenCount
	return c.sem.Upsert(key, key, resp, c.ttl)
}

// ClearExact clears all entries from the exact-match cache.
// If the Redis client is nil (in-memory mode), this is a no-op.
func (c *RedisCache) ClearExact(ctx context.Context) error {
	if c.client == nil {
		return nil
	}
	// Use a pattern-based deletion. Exact cache keys follow the hash prefix "cache:exact:".
	keys, err := c.client.Keys(ctx, "cache:exact:*").Result()
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		return c.client.Del(ctx, keys...).Err()
	}
	return nil
}

// ClearSemantic clears all entries from the semantic cache.
func (c *RedisCache) ClearSemantic(ctx context.Context) error {
	if c.sem == nil {
		return nil
	}
	return c.sem.Clear()
}

// Stats returns statistics for both exact and semantic caches.
func (c *RedisCache) Stats(ctx context.Context) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// Exact cache stats from Redis
	if c.client != nil {
		keys, err := c.client.Keys(ctx, "cache:exact:*").Result()
		if err == nil {
			result["exact_total_entries"] = len(keys)
		} else {
			result["exact_total_entries"] = 0
			result["exact_error"] = err.Error()
		}
	} else {
		result["exact_total_entries"] = 0
	}

	// Semantic cache stats
	if c.sem != nil {
		semStats := c.sem.Stats()
		for k, v := range semStats {
			result["semantic_"+k] = v
		}
	}

	return result, nil
}

// Inspect returns a paginated list of cached entries for the exact cache.
// Only the hash, model (if available), and timestamp are returned — never the full payload.
func (c *RedisCache) Inspect(ctx context.Context, cursor string, pageSize int) ([]CacheInspectEntry, string, error) {
	if c.client == nil {
		return nil, "0", nil
	}

	keys, nextCursor, err := c.client.Scan(ctx, parseCursor(cursor), "cache:exact:*", int64(pageSize)).Result()
	if err != nil {
		return nil, "0", err
	}

	var entries []CacheInspectEntry
	for _, key := range keys {
		val, err := c.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		entry := CacheInspectEntry{
			Key:       key,
			CreatedAt: time.Now().UTC(), // Exact cache entries don't store timestamps inline; best-effort
		}

		// Try to extract metadata from the cached response (it's a marshaled CacheEntry JSON).
		var cached CacheEntry
		if json.Unmarshal([]byte(val), &cached) == nil {
			entry.TokenCount = cached.TokenCount
			entry.Semantic = cached.Semantic
		}

		entries = append(entries, entry)
	}

	return entries, strconv.FormatUint(nextCursor, 10), nil
}

// parseCursor safely converts a string cursor to uint64.
func parseCursor(s string) uint64 {
	if s == "" || s == "0" {
		return 0
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// CacheInspectEntry is a lightweight view of a cached entry for the inspect API.
type CacheInspectEntry struct {
	Key        string    `json:"key"`
	TokenCount int       `json:"token_count,omitempty"`
	Semantic   bool      `json:"semantic,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}
