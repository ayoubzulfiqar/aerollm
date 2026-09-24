package marketplace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"
)

// RedisOptions configures the Redis-backed store.
type RedisOptions struct {
	Client *redis.Client
	Prefix string
}

// RedisStore persists registry data in Redis.
//
// Keys are "<prefix>:manifest:<id>", "<prefix>:meta:<id>" and the set
// "<prefix>:index". IDs are validated with ValidatePluginID before use, so an
// ID can never contain ':' or other characters that would address a
// different key.
type RedisStore struct {
	client *redis.Client
	prefix string
}

// NewRedisStore creates a Redis-backed registry store.
func NewRedisStore(opts RedisOptions) *RedisStore {
	if opts.Prefix == "" {
		opts.Prefix = "aerollm:marketplace"
	}
	return &RedisStore{client: opts.Client, prefix: opts.Prefix}
}

func (s *RedisStore) manifestKey(id string) string {
	return fmt.Sprintf("%s:manifest:%s", s.prefix, id)
}
func (s *RedisStore) metaKey(id string) string {
	return fmt.Sprintf("%s:meta:%s", s.prefix, id)
}
func (s *RedisStore) indexKey() string {
	return fmt.Sprintf("%s:index", s.prefix)
}

func (s *RedisStore) ready() error {
	if s == nil || s.client == nil {
		return errors.New("marketplace: redis store has no client")
	}
	return nil
}

// Put stores a manifest and metadata entry atomically (MULTI/EXEC).
func (s *RedisStore) Put(ctx context.Context, manifest VerifiedManifest, meta Metadata) error {
	if err := s.ready(); err != nil {
		return err
	}
	if err := ValidatePluginID(manifest.ID); err != nil {
		return err
	}
	if meta.ID != "" && meta.ID != manifest.ID {
		return fmt.Errorf("%w: metadata id %q does not match manifest id %q", ErrInvalidManifest, meta.ID, manifest.ID)
	}
	meta.ID = manifest.ID
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	metaPayload, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	pipe := s.client.TxPipeline()
	pipe.Set(ctx, s.manifestKey(manifest.ID), payload, 0)
	pipe.Set(ctx, s.metaKey(manifest.ID), metaPayload, 0)
	pipe.SAdd(ctx, s.indexKey(), manifest.ID)
	if _, err = pipe.Exec(ctx); err != nil {
		return fmt.Errorf("marketplace: redis put %q: %w", manifest.ID, err)
	}
	return nil
}

// Lookup retrieves a manifest and metadata by plugin ID. It returns
// ErrNotFound for a missing plugin and a wrapped error for Redis or decoding
// failures, so callers can tell "absent" from "broken".
func (s *RedisStore) Lookup(ctx context.Context, pluginID string) (VerifiedManifest, Metadata, error) {
	if err := s.ready(); err != nil {
		return VerifiedManifest{}, Metadata{}, err
	}
	if ValidatePluginID(pluginID) != nil {
		return VerifiedManifest{}, Metadata{}, ErrNotFound
	}
	if ctx == nil {
		ctx = context.Background()
	}
	vals, err := s.client.MGet(ctx, s.manifestKey(pluginID), s.metaKey(pluginID)).Result()
	if err != nil {
		return VerifiedManifest{}, Metadata{}, fmt.Errorf("marketplace: redis get %q: %w", pluginID, err)
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] == nil {
		return VerifiedManifest{}, Metadata{}, ErrNotFound
	}
	manifestRaw, ok1 := vals[0].(string)
	metaRaw, ok2 := vals[1].(string)
	if !ok1 || !ok2 {
		return VerifiedManifest{}, Metadata{}, fmt.Errorf("marketplace: redis get %q: unexpected value type", pluginID)
	}
	var manifest VerifiedManifest
	if err := json.Unmarshal([]byte(manifestRaw), &manifest); err != nil {
		return VerifiedManifest{}, Metadata{}, fmt.Errorf("marketplace: decode manifest %q: %w", pluginID, err)
	}
	var meta Metadata
	if err := json.Unmarshal([]byte(metaRaw), &meta); err != nil {
		return VerifiedManifest{}, Metadata{}, fmt.Errorf("marketplace: decode metadata %q: %w", pluginID, err)
	}
	return manifest, meta, nil
}

// Get retrieves a manifest and metadata by plugin ID. Errors are reported as
// not-found; use Lookup to distinguish them.
func (s *RedisStore) Get(ctx context.Context, pluginID string) (VerifiedManifest, Metadata, bool) {
	m, meta, err := s.Lookup(ctx, pluginID)
	return m, meta, err == nil
}

// List returns metadata for all registry entries sorted by ID. Index entries
// whose metadata has disappeared are skipped; Redis and decoding errors are
// returned.
func (s *RedisStore) List(ctx context.Context) ([]Metadata, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ids, err := s.client.SMembers(ctx, s.indexKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("marketplace: redis list: %w", err)
	}
	valid := ids[:0]
	for _, id := range ids {
		if ValidatePluginID(id) == nil {
			valid = append(valid, id)
		}
	}
	sort.Strings(valid)
	out := make([]Metadata, 0, len(valid))
	const batch = 500
	for start := 0; start < len(valid); start += batch {
		end := start + batch
		if end > len(valid) {
			end = len(valid)
		}
		keys := make([]string, 0, end-start)
		for _, id := range valid[start:end] {
			keys = append(keys, s.metaKey(id))
		}
		vals, err := s.client.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("marketplace: redis list: %w", err)
		}
		for i, v := range vals {
			raw, ok := v.(string)
			if !ok {
				continue // deleted between SMEMBERS and MGET
			}
			var meta Metadata
			if err := json.Unmarshal([]byte(raw), &meta); err != nil {
				return nil, fmt.Errorf("marketplace: decode metadata %q: %w", valid[start+i], err)
			}
			out = append(out, meta)
		}
	}
	return out, nil
}

var _ Store = (*RedisStore)(nil)
