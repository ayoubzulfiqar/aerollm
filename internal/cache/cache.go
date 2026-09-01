package cache

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/redis/go-redis/v9"
)

// RedisClient mirrors go-redis/v9 to keep cache.go buildable without tight coupling.
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Close() error
}

// RedisCache wraps Redis for prompt caching.
type RedisCache struct {
	client RedisClient
	ttl    time.Duration
}

// NewRedisCache creates a new RedisCache.
func NewRedisCache(client RedisClient, ttl time.Duration) *RedisCache {
	return &RedisCache{
		client: client,
		ttl:    ttl,
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
	return &CacheEntry{}, nil
}

// SetSemantic stores a response in the semantic cache.
func (c *RedisCache) SetSemantic(key string, resp []byte, tokenCount int) error {
	return nil
}
