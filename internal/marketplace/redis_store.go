package marketplace

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisOptions configures the Redis-backed store.
type RedisOptions struct {
	Client *redis.Client
	Prefix string
	// LockTTL is the lifetime of a per-plugin publish lock whose holder dies
	// without releasing it (default DefaultPublishLockTTL).
	LockTTL time.Duration
	// LockWait bounds how long a publish waits for the lock held by another
	// instance before failing with ErrLockTimeout (default
	// DefaultPublishLockWait).
	LockWait time.Duration
}

// RedisStore persists registry data in Redis.
//
// Keys are "<prefix>:manifest:<id>", "<prefix>:meta:<id>", the set
// "<prefix>:index", the publish locks "<prefix>:lock:publish:<id>" and the
// creator key sets "<prefix>:creator:<creator>:keys". IDs are validated with
// ValidatePluginID before use, so an ID can never contain ':' or other
// characters that would address a different key.
//
// RedisStore implements PublishLocker: publishes of one plugin are serialised
// across every instance sharing the Redis (SET NX PX lock with a random
// token, released by a token-checked Lua script), and the publish write is
// fenced on still holding that lock.
type RedisStore struct {
	client   *redis.Client
	prefix   string
	lockTTL  time.Duration
	lockWait time.Duration
}

// NewRedisStore creates a Redis-backed registry store.
func NewRedisStore(opts RedisOptions) *RedisStore {
	if opts.Prefix == "" {
		opts.Prefix = "aerollm:marketplace"
	}
	if opts.LockTTL <= 0 {
		opts.LockTTL = DefaultPublishLockTTL
	}
	if opts.LockWait <= 0 {
		opts.LockWait = DefaultPublishLockWait
	}
	return &RedisStore{client: opts.Client, prefix: opts.Prefix, lockTTL: opts.LockTTL, lockWait: opts.LockWait}
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
func (s *RedisStore) lockKey(id string) string {
	return fmt.Sprintf("%s:lock:publish:%s", s.prefix, id)
}
func (s *RedisStore) creatorKeysKey(creatorID string) string {
	return fmt.Sprintf("%s:creator:%s:keys", s.prefix, creatorID)
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

// releaseLockScript deletes the lock only when it still holds the caller's
// token, so a holder whose lock expired cannot release someone else's.
var releaseLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// fencedPutScript commits a publish only while the caller still holds the
// publish lock. With ARGV[6] == "1" it also enforces the store-wide creator
// key registry: the first key published for a creator is pinned, and later
// publishes must use a pinned key.
//
// KEYS: lock, manifest, meta, index, creator key set.
// ARGV: token, manifest JSON, metadata JSON, plugin id, public key (base64),
// pin flag.
// Returns 1 on success, 0 when the lock is no longer held, -1 when the
// creator key is not pinned.
var fencedPutScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
if ARGV[6] == "1" then
	if redis.call("SCARD", KEYS[5]) == 0 then
		redis.call("SADD", KEYS[5], ARGV[5])
	elseif redis.call("SISMEMBER", KEYS[5], ARGV[5]) == 0 then
		return -1
	end
end
redis.call("SET", KEYS[2], ARGV[2])
redis.call("SET", KEYS[3], ARGV[3])
redis.call("SADD", KEYS[4], ARGV[4])
return 1
`)

// LockPublish acquires the cross-instance publish lock for pluginID with
// SET NX PX and a random token. It retries with jittered backoff until the
// lock is free, ctx is done, or the configured LockWait elapses
// (ErrLockTimeout). The lock expires after LockTTL if never released.
func (s *RedisStore) LockPublish(ctx context.Context, pluginID string) (PublishLock, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if err := ValidatePluginID(pluginID); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("marketplace: lock token: %w", err)
	}
	token := hex.EncodeToString(raw[:])
	key := s.lockKey(pluginID)
	wait := time.NewTimer(s.lockWait)
	defer wait.Stop()
	backoff := 5 * time.Millisecond
	for {
		ok, err := s.client.SetNX(ctx, key, token, s.lockTTL).Result()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, fmt.Errorf("marketplace: redis lock %q: %w", pluginID, err)
		}
		if ok {
			return &redisPublishLock{store: s, key: key, token: token}, nil
		}
		// Full jitter keeps contending instances from retrying in lockstep.
		sleep := time.NewTimer(backoff/2 + mrand.N(backoff/2+1))
		select {
		case <-ctx.Done():
			sleep.Stop()
			return nil, ctx.Err()
		case <-wait.C:
			sleep.Stop()
			return nil, fmt.Errorf("%w: plugin %q", ErrLockTimeout, pluginID)
		case <-sleep.C:
		}
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
}

type redisPublishLock struct {
	store *RedisStore
	key   string
	token string

	once sync.Once
	err  error
}

// Unlock releases the lock with a token-checked script. The release gets its
// own short timeout and ignores cancellation of ctx, so a publish whose
// request was canceled still frees the lock promptly instead of holding it
// until the TTL expires.
func (l *redisPublishLock) Unlock(ctx context.Context) error {
	l.once.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
		defer cancel()
		if err := releaseLockScript.Run(rctx, l.store.client, []string{l.key}, l.token).Err(); err != nil {
			l.err = fmt.Errorf("marketplace: redis unlock: %w", err)
		}
	})
	return l.err
}

func (l *redisPublishLock) putFenced(ctx context.Context, manifest VerifiedManifest, meta Metadata, pinCreatorKey bool) error {
	s := l.store
	if err := ValidatePluginID(manifest.ID); err != nil {
		return err
	}
	if s.lockKey(manifest.ID) != l.key {
		return fmt.Errorf("%w: lock is not for plugin %q", ErrLockLost, manifest.ID)
	}
	if err := ValidatePluginID(manifest.CreatorID); err != nil {
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
	pin := "0"
	if pinCreatorKey {
		pin = "1"
	}
	keys := []string{l.key, s.manifestKey(manifest.ID), s.metaKey(manifest.ID), s.indexKey(), s.creatorKeysKey(manifest.CreatorID)}
	res, err := fencedPutScript.Run(ctx, s.client, keys, l.token, payload, metaPayload, manifest.ID,
		base64.StdEncoding.EncodeToString(manifest.PublicKey), pin).Int64()
	if err != nil {
		return fmt.Errorf("marketplace: redis put %q: %w", manifest.ID, err)
	}
	switch res {
	case 1:
		return nil
	case -1:
		return fmt.Errorf("%w: key is not pinned for creator %q", ErrUntrustedKey, manifest.CreatorID)
	default:
		return fmt.Errorf("%w: plugin %q", ErrLockLost, manifest.ID)
	}
}

// seedCreatorKeys adds already-stored creator keys to the shared creator key
// registry, so trust-on-first-use pins made before the registry existed are
// honoured by every instance.
func (s *RedisStore) seedCreatorKeys(ctx context.Context, keys map[string][][]byte) error {
	if err := s.ready(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	pipe := s.client.Pipeline()
	for creator, pubs := range keys {
		if ValidatePluginID(creator) != nil || len(pubs) == 0 {
			continue
		}
		members := make([]interface{}, 0, len(pubs))
		for _, pub := range pubs {
			members = append(members, base64.StdEncoding.EncodeToString(pub))
		}
		pipe.SAdd(ctx, s.creatorKeysKey(creator), members...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("marketplace: redis seed creator keys: %w", err)
	}
	return nil
}

var (
	_ Store            = (*RedisStore)(nil)
	_ PublishLocker    = (*RedisStore)(nil)
	_ creatorKeySeeder = (*RedisStore)(nil)
)
