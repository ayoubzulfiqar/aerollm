package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/redis/go-redis/v9"
)

// ExactKeyPrefix prefixes every exact-match cache key so that Stats, Clear
// and Inspect can find them with a SCAN pattern.
const ExactKeyPrefix = "cache:exact:"

// maxScanKeys bounds how many keys Stats/Clear will walk in one call.
const maxScanKeys = 1_000_000

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
	// prefix is prepended to every physical Redis key (see
	// RedisCacheOptions.KeyPrefix); API keys never include it.
	prefix string

	hits   atomic.Int64
	misses atomic.Int64
	sets   atomic.Int64
	errs   atomic.Int64
}

// NewRedisCache creates a new RedisCache. A nil client yields a cache that
// always misses (in-memory/offline mode).
func NewRedisCache(client RedisClient, ttl time.Duration) *RedisCache {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &RedisCache{
		client: client,
		ttl:    ttl,
		sem:    NewSemanticCache("sem"),
	}
}

// RedisCacheOptions configures NewRedisCacheWithOptions.
type RedisCacheOptions struct {
	// TTL is the default entry lifetime (default 1h).
	TTL time.Duration
	// KeyPrefix is prepended to every Redis key the cache reads or writes
	// (for example "prod:"), so several deployments — or test runs — can
	// share one Redis without seeing or clearing each other's entries.
	// Keys passed to and returned by the cache API never include it.
	KeyPrefix string
}

// NewRedisCacheWithOptions creates a RedisCache with explicit options. A nil
// client yields a cache that always misses.
func NewRedisCacheWithOptions(client RedisClient, opts RedisCacheOptions) *RedisCache {
	c := NewRedisCache(client, opts.TTL)
	c.prefix = opts.KeyPrefix
	return c
}

// physical maps a cache key to the Redis key that stores it.
func (c *RedisCache) physical(key string) string { return c.prefix + key }

// pattern returns a SCAN MATCH pattern for logical keys starting with p.
func (c *RedisCache) pattern(p string) string { return escapeGlob(c.prefix+p) + "*" }

// escapeGlob escapes Redis glob metacharacters.
func escapeGlob(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// CacheEntry represents a cached LLM response.
type CacheEntry struct {
	Key        string
	Response   []byte
	CreatedAt  time.Time
	TTL        time.Duration
	TokenCount int
	Model      string
	Semantic   bool
}

// EntryMeta carries optional metadata stored alongside an exact-cache entry.
type EntryMeta struct {
	Model      string
	TokenCount int
	// TTL overrides the cache default when positive.
	TTL time.Duration
}

// storedEntry is the envelope persisted in Redis. Values written by older
// versions (the raw response bytes) are still readable.
type storedEntry struct {
	Marker      int             `json:"aerollm_cache"`
	CreatedAt   time.Time       `json:"created_at"`
	TokenCount  int             `json:"token_count,omitempty"`
	Model       string          `json:"model,omitempty"`
	Response    json.RawMessage `json:"response,omitempty"`
	ResponseB64 []byte          `json:"response_b64,omitempty"`
}

const envelopeVersion = 1

// KeyForRequest generates a deterministic exact-cache key from an LLM
// request without a tenant namespace. Prefer KeyForRequestNS so that
// responses are never shared across tenants.
func KeyForRequest(req *models.LLMRequest) string {
	return KeyForRequestNS("", req)
}

// KeyForRequestNS generates a deterministic exact-cache key scoped to ns
// (for example TenantNamespace(apiKey)). The key covers every request field
// that can influence the output (model, messages including tool calls and
// tool results, sampling parameters, stop, tools, response_format and any
// field added to models.LLMRequest in the future). The stream flag is
// deliberately excluded: a streamed and a non-streamed request produce the
// same completion, so a cached full response can be replayed as a stream
// (see models.StreamChunksFromResponse).
//
// It returns "" if the request cannot be canonicalised (e.g. NaN
// parameters); GetExact/SetExact treat "" as uncacheable.
func KeyForRequestNS(ns string, req *models.LLMRequest) string {
	canonical, err := canonicalRequest(req)
	if err != nil {
		return ""
	}
	h := sha256.New()
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(ns)))
	h.Write(lenBuf[:])
	h.Write([]byte(ns))
	h.Write(canonical)
	digest := hex.EncodeToString(h.Sum(nil))
	if ns == "" {
		return ExactKeyPrefix + digest
	}
	return ExactKeyPrefix + namespaceTag(ns) + ":" + digest
}

// streamOnlyFields are request fields that change the transport, not the
// completion, and are therefore excluded from the cache key.
var streamOnlyFields = []string{"stream", "stream_options"}

func canonicalRequest(req *models.LLMRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.New("cache: nil request")
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	// Round-trip through a generic map (numbers kept verbatim) so that
	// transport-only fields can be dropped without naming every model
	// field; encoding/json sorts map keys, so the result is canonical.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]interface{}
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	for _, f := range streamOnlyFields {
		delete(m, f)
	}
	return json.Marshal(m)
}

// namespaceTag is a short, non-reversible label for a namespace used inside
// Redis key names (so raw API keys never appear in the keyspace).
func namespaceTag(ns string) string {
	sum := sha256.Sum256([]byte("aerollm-cache-ns\x00" + ns))
	return "ns" + hex.EncodeToString(sum[:8])
}

// TenantNamespace derives a cache namespace from a tenant secret such as an
// API key. The result is a hash and is safe to log or expose.
func TenantNamespace(apiKey string) string {
	if apiKey == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("aerollm-tenant\x00" + apiKey))
	return "t_" + hex.EncodeToString(sum[:12])
}

// ScopedNamespace combines a tenant namespace with a model so semantic
// lookups never return a response produced by a different model.
func ScopedNamespace(tenantNS, model string) string {
	return tenantNS + "|" + model
}

// IsCacheable reports whether a request may be served from / stored in the
// response caches. Requests asking for several choices (n > 1) or carrying
// no model/messages are not cached. stream=true is cacheable for lookups
// (the key ignores the stream flag) but callers must only store complete,
// fully assembled responses.
func IsCacheable(req *models.LLMRequest) bool {
	if req == nil || req.Model == "" || len(req.Messages) == 0 {
		return false
	}
	// Inspect "n" generically so this keeps working when the field is added
	// to models.LLMRequest.
	raw, err := json.Marshal(req)
	if err != nil {
		return false
	}
	var probe struct {
		N *float64 `json:"n"`
	}
	if json.Unmarshal(raw, &probe) == nil && probe.N != nil && *probe.N > 1 {
		return false
	}
	return true
}

// GetExact retrieves an exact-match cached response.
func (c *RedisCache) GetExact(key string) (*CacheEntry, error) {
	return c.GetExactCtx(context.Background(), key)
}

// GetExactCtx retrieves an exact-match cached response honoring ctx.
// A miss returns (nil, nil).
func (c *RedisCache) GetExactCtx(ctx context.Context, key string) (*CacheEntry, error) {
	if c == nil || c.client == nil || key == "" {
		return nil, nil
	}
	val, err := c.client.Get(ctx, c.physical(key)).Result()
	if errors.Is(err, redis.Nil) {
		c.misses.Add(1)
		return nil, nil
	}
	if err != nil {
		c.errs.Add(1)
		c.misses.Add(1)
		return nil, err
	}
	c.hits.Add(1)
	return decodeStored(key, []byte(val), c.ttl), nil
}

func decodeStored(key string, val []byte, ttl time.Duration) *CacheEntry {
	var env storedEntry
	if json.Unmarshal(val, &env) == nil && env.Marker == envelopeVersion {
		resp := []byte(env.Response)
		if len(env.ResponseB64) > 0 {
			resp = env.ResponseB64
		}
		return &CacheEntry{
			Key:        key,
			Response:   resp,
			CreatedAt:  env.CreatedAt,
			TTL:        ttl,
			TokenCount: env.TokenCount,
			Model:      env.Model,
		}
	}
	// Legacy value: the raw response bytes.
	return &CacheEntry{Key: key, Response: val, TTL: ttl}
}

// SetExact stores a response in the exact-match cache.
func (c *RedisCache) SetExact(key string, resp []byte, tokenCount int) error {
	return c.SetExactWithMeta(context.Background(), key, resp, EntryMeta{TokenCount: tokenCount})
}

// SetExactCtx stores a response in the exact-match cache honoring ctx.
func (c *RedisCache) SetExactCtx(ctx context.Context, key string, resp []byte, tokenCount int) error {
	return c.SetExactWithMeta(ctx, key, resp, EntryMeta{TokenCount: tokenCount})
}

// SetExactWithMeta stores a response with metadata used by Inspect.
func (c *RedisCache) SetExactWithMeta(ctx context.Context, key string, resp []byte, meta EntryMeta) error {
	if c == nil || c.client == nil || key == "" || len(resp) == 0 {
		return nil
	}
	env := storedEntry{
		Marker:     envelopeVersion,
		CreatedAt:  time.Now().UTC(),
		TokenCount: meta.TokenCount,
		Model:      meta.Model,
	}
	if json.Valid(resp) {
		env.Response = json.RawMessage(resp)
	} else {
		env.ResponseB64 = resp
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	ttl := c.ttl
	if meta.TTL > 0 {
		ttl = meta.TTL
	}
	if err := c.client.Set(ctx, c.physical(key), string(b), ttl).Err(); err != nil {
		c.errs.Add(1)
		return err
	}
	c.sets.Add(1)
	return nil
}

// DeleteExact removes a single exact-cache entry.
func (c *RedisCache) DeleteExact(ctx context.Context, key string) error {
	if c == nil || c.client == nil || key == "" {
		return nil
	}
	return c.client.Del(ctx, c.physical(key)).Err()
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

// scanDelete deletes every key matching pattern using SCAN (never KEYS,
// which blocks Redis) and returns how many keys were deleted.
func (c *RedisCache) scanDelete(ctx context.Context, pattern string) (int, error) {
	var cursor uint64
	deleted, seen := 0, 0
	for {
		keys, next, err := c.client.Scan(ctx, cursor, pattern, 500).Result()
		if err != nil {
			return deleted, err
		}
		if len(keys) > 0 {
			n, err := c.client.Del(ctx, keys...).Result()
			if err != nil {
				return deleted, err
			}
			deleted += int(n)
		}
		seen += len(keys)
		cursor = next
		if cursor == 0 || seen >= maxScanKeys {
			return deleted, nil
		}
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
	}
}

// ClearExact clears all entries from the exact-match cache.
// If the Redis client is nil (in-memory mode), this is a no-op.
func (c *RedisCache) ClearExact(ctx context.Context) error {
	if c == nil || c.client == nil {
		return nil
	}
	_, err := c.scanDelete(ctx, c.pattern(ExactKeyPrefix))
	return err
}

// ClearNamespace removes the exact-cache entries of one namespace (as passed
// to KeyForRequestNS) and returns how many keys were deleted.
func (c *RedisCache) ClearNamespace(ctx context.Context, ns string) (int, error) {
	if c == nil || c.client == nil {
		return 0, nil
	}
	if ns == "" {
		return 0, errors.New("cache: empty namespace")
	}
	return c.scanDelete(ctx, c.pattern(ExactKeyPrefix+namespaceTag(ns)+":"))
}

// ClearSemantic clears all entries from the (legacy) semantic cache.
func (c *RedisCache) ClearSemantic(ctx context.Context) error {
	if c == nil || c.sem == nil {
		return nil
	}
	return c.sem.Clear()
}

// ExactStats is a typed snapshot of exact-cache statistics.
type ExactStats struct {
	Entries   int     `json:"total_entries"`
	Truncated bool    `json:"truncated,omitempty"`
	Hits      int64   `json:"hits"`
	Misses    int64   `json:"misses"`
	Sets      int64   `json:"sets"`
	Errors    int64   `json:"errors"`
	HitRate   float64 `json:"hit_rate"`
}

// ExactStats counts exact-cache entries (via SCAN) and returns the
// process-local hit/miss counters.
func (c *RedisCache) ExactStats(ctx context.Context) (ExactStats, error) {
	var st ExactStats
	if c == nil {
		return st, nil
	}
	st.Hits, st.Misses, st.Sets, st.Errors = c.hits.Load(), c.misses.Load(), c.sets.Load(), c.errs.Load()
	if total := st.Hits + st.Misses; total > 0 {
		st.HitRate = float64(st.Hits) / float64(total)
	}
	if c.client == nil {
		return st, nil
	}
	var cursor uint64
	for {
		keys, next, err := c.client.Scan(ctx, cursor, c.pattern(ExactKeyPrefix), 1000).Result()
		if err != nil {
			return st, err
		}
		st.Entries += len(keys)
		cursor = next
		if cursor == 0 {
			return st, nil
		}
		if st.Entries >= maxScanKeys {
			st.Truncated = true
			return st, nil
		}
		if err := ctx.Err(); err != nil {
			return st, err
		}
	}
}

// Stats returns statistics for both exact and semantic caches.
// "exact_total_entries" is always an int.
func (c *RedisCache) Stats(ctx context.Context) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	st, err := c.ExactStats(ctx)
	result["exact_total_entries"] = st.Entries
	result["exact_hits"] = st.Hits
	result["exact_misses"] = st.Misses
	result["exact_hit_rate"] = st.HitRate
	if err != nil {
		result["exact_error"] = "stats unavailable"
	}
	if c != nil && c.sem != nil {
		for k, v := range c.sem.Stats() {
			result["semantic_"+k] = v
		}
	}
	return result, nil
}

// Inspect returns a paginated list of cached entries for the exact cache.
// Only the key, model, token count and creation time are returned — never
// the payload.
func (c *RedisCache) Inspect(ctx context.Context, cursor string, pageSize int) ([]CacheInspectEntry, string, error) {
	if c == nil || c.client == nil {
		return nil, "0", nil
	}
	if pageSize <= 0 {
		pageSize = 50
	}
	if pageSize > 500 {
		pageSize = 500
	}
	keys, nextCursor, err := c.client.Scan(ctx, parseCursor(cursor), c.pattern(ExactKeyPrefix), int64(pageSize)).Result()
	if err != nil {
		return nil, "0", err
	}

	entries := make([]CacheInspectEntry, 0, len(keys))
	for _, pkey := range keys {
		val, err := c.client.Get(ctx, pkey).Result()
		if err != nil {
			continue // expired between SCAN and GET
		}
		key := strings.TrimPrefix(pkey, c.prefix)
		ce := decodeStored(key, []byte(val), c.ttl)
		entry := CacheInspectEntry{
			Key:        key,
			Model:      ce.Model,
			TokenCount: ce.TokenCount,
			CreatedAt:  ce.CreatedAt,
		}
		if !ce.CreatedAt.IsZero() {
			entry.ExpiresAt = ce.CreatedAt.Add(c.ttl)
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
	Namespace  string    `json:"namespace,omitempty"`
	Model      string    `json:"model,omitempty"`
	TokenCount int       `json:"token_count,omitempty"`
	Semantic   bool      `json:"semantic,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`
}

// validThreshold normalises a similarity threshold into (0, 1].
func validThreshold(t, def float64) float64 {
	if math.IsNaN(t) || t <= 0 {
		return def
	}
	if t > 1 {
		return 1
	}
	return t
}
