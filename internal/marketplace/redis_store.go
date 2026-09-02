package marketplace

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisOptions configures the Redis-backed store.
type RedisOptions struct {
	Client *redis.Client
	Prefix string
}

// RedisStore persists registry data in Redis.
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

// Put stores a manifest and metadata entry.
func (s *RedisStore) Put(ctx context.Context, manifest VerifiedManifest, meta Metadata) error {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	metaPayload, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	pipe := s.client.Pipeline()
	pipe.Set(ctx, s.manifestKey(manifest.ID), payload, 0)
	pipe.Set(ctx, s.metaKey(manifest.ID), metaPayload, 0)
	pipe.SAdd(ctx, s.indexKey(), manifest.ID)
	_, err = pipe.Exec(ctx)
	return err
}

// Get retrieves a manifest and metadata by plugin ID.
func (s *RedisStore) Get(_ context.Context, pluginID string) (VerifiedManifest, Metadata, bool) {
	manifestRaw, err := s.client.Get(context.Background(), s.manifestKey(pluginID)).Result()
	if err != nil {
		return VerifiedManifest{}, Metadata{}, false
	}
	metaRaw, err := s.client.Get(context.Background(), s.metaKey(pluginID)).Result()
	if err != nil {
		return VerifiedManifest{}, Metadata{}, false
	}
	var manifest VerifiedManifest
	if err := json.Unmarshal([]byte(manifestRaw), &manifest); err != nil {
		return VerifiedManifest{}, Metadata{}, false
	}
	var meta Metadata
	if err := json.Unmarshal([]byte(metaRaw), &meta); err != nil {
		return VerifiedManifest{}, Metadata{}, false
	}
	return manifest, meta, true
}

// List returns metadata for all registry entries.
func (s *RedisStore) List(_ context.Context) ([]Metadata, error) {
	ids, err := s.client.SMembers(context.Background(), s.indexKey()).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Metadata, 0, len(ids))
	for _, id := range ids {
		_, meta, ok := s.Get(context.Background(), id)
		if !ok {
			continue
		}
		out = append(out, meta)
	}
	return out, nil
}

var _ Store = (*RedisStore)(nil)
