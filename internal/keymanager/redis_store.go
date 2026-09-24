package keymanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis key layout (P = the configured prefix). Every key contains the hash
// tag "{km}" (or P's own hash tag, which takes precedence when P contains
// one), so all keys of a store live in one Redis Cluster slot and multi-key
// transactions work on clusters:
//
//	P{km}:key:<sha256 hex>      JSON VirtualKey (hash only, never plaintext)
//	P{km}:keys                  SET of all key hashes
//	P{km}:keyprefix:<prefix>    SET of key hashes with that display prefix
//	P{km}:user:<id>             JSON User
//	P{km}:team:<id>             JSON Team
const redisHashTag = "{km}:"

// Optimistic-transaction retry policy.
const (
	redisMaxAttempts = 64
	redisBaseBackoff = time.Millisecond
	redisMaxBackoff  = 32 * time.Millisecond
	redisMGetChunk   = 500
)

var errNoRedisClient = errors.New("keymanager: redis client not configured")

// stripedLock serializes same-process updates of one Redis key, so
// optimistic transactions only conflict across processes.
type stripedLock [64]sync.Mutex

func (l *stripedLock) lock(key string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	mu := &l[h.Sum32()%uint32(len(l))]
	mu.Lock()
	return mu.Unlock
}

// redisBase holds what all Redis-backed stores share.
type redisBase struct {
	client redis.UniversalClient
	ns     string
	locks  stripedLock
}

func newRedisBase(client redis.UniversalClient, prefix string) redisBase {
	return redisBase{client: client, ns: prefix + redisHashTag}
}

func (b *redisBase) ready() error {
	if b.client == nil {
		return errNoRedisClient
	}
	return nil
}

func backoff(ctx context.Context, attempt int) error {
	d := redisBaseBackoff << min(attempt, 5)
	d = min(d, redisMaxBackoff)
	d = d/2 + rand.N(d/2+1)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// watchUpdate runs a WATCH/MULTI optimistic transaction on key, retrying on
// conflicts. txf reads through tx and queues writes with tx.TxPipelined.
// Same-process updates of key are serialized first.
func (b *redisBase) watchUpdate(ctx context.Context, key string, txf func(tx *redis.Tx) error) error {
	if err := b.ready(); err != nil {
		return err
	}
	unlock := b.locks.lock(key)
	defer unlock()
	for attempt := 0; attempt < redisMaxAttempts; attempt++ {
		err := b.client.Watch(ctx, txf, key)
		if !errors.Is(err, redis.TxFailedErr) {
			return err
		}
		if err := backoff(ctx, attempt); err != nil {
			return err
		}
	}
	return ErrConflict
}

// RedisKeyStore is a KeyStore backed by Redis for multi-instance
// deployments: every gateway instance sees the same keys, revocations and
// spend. Only key hashes are stored. UpdateFunc is atomic across instances
// (WATCH/MULTI with retries), so concurrent spend from several instances is
// never lost.
type RedisKeyStore struct {
	redisBase
}

var (
	_ KeyStore         = (*RedisKeyStore)(nil)
	_ AtomicKeyUpdater = (*RedisKeyStore)(nil)
)

// NewRedisKeyStore returns a key store using client. prefix namespaces all
// Redis keys (e.g. "aerollm:"); see the key layout above.
func NewRedisKeyStore(client redis.UniversalClient, prefix string) *RedisKeyStore {
	return &RedisKeyStore{redisBase: newRedisBase(client, prefix)}
}

func (s *RedisKeyStore) docKey(hash string) string      { return s.ns + "key:" + hash }
func (s *RedisKeyStore) indexKey() string               { return s.ns + "keys" }
func (s *RedisKeyStore) prefixKey(prefix string) string { return s.ns + "keyprefix:" + prefix }
func (s *RedisKeyStore) encode(vk *VirtualKey) ([]byte, error) {
	data, err := json.Marshal(vk)
	if err != nil {
		return nil, fmt.Errorf("keymanager: encode key: %w", err)
	}
	return data, nil
}

func decodeKey(raw []byte) (*VirtualKey, error) {
	var vk VirtualKey
	if err := json.Unmarshal(raw, &vk); err != nil {
		return nil, fmt.Errorf("keymanager: decode key: %w", err)
	}
	normalizeStoredKey(&vk)
	return &vk, nil
}

// createKeyScript inserts a key document only if it does not exist and
// indexes it. KEYS: doc, index, prefix set. ARGV: json, hash, has-prefix.
var createKeyScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SADD', KEYS[2], ARGV[2])
if ARGV[3] == '1' then redis.call('SADD', KEYS[3], ARGV[2]) end
return 1
`)

// Create stores a new key; it fails with ErrKeyExists if the hash exists.
func (s *RedisKeyStore) Create(ctx context.Context, key *VirtualKey) error {
	if err := s.ready(); err != nil {
		return err
	}
	if key == nil || !isKeyHash(key.KeyHash) {
		return errors.New("keymanager: invalid key")
	}
	data, err := s.encode(key)
	if err != nil {
		return err
	}
	hasPrefix := "0"
	if key.Prefix != "" {
		hasPrefix = "1"
	}
	n, err := createKeyScript.Run(ctx, s.client,
		[]string{s.docKey(key.KeyHash), s.indexKey(), s.prefixKey(key.Prefix)},
		data, key.KeyHash, hasPrefix).Int()
	if err != nil {
		return fmt.Errorf("keymanager: redis create key: %w", err)
	}
	if n == 0 {
		return ErrKeyExists
	}
	return nil
}

// Get returns the key with the given hash.
func (s *RedisKeyStore) Get(ctx context.Context, keyHash string) (*VirtualKey, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if !isKeyHash(keyHash) {
		return nil, ErrKeyNotFound
	}
	raw, err := s.client.Get(ctx, s.docKey(keyHash)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("keymanager: redis get key: %w", err)
	}
	return decodeKey(raw)
}

// loadKeys fetches the documents of hashes (missing documents are skipped).
func (s *RedisKeyStore) loadKeys(ctx context.Context, hashes []string) ([]*VirtualKey, error) {
	out := make([]*VirtualKey, 0, len(hashes))
	for start := 0; start < len(hashes); start += redisMGetChunk {
		chunk := hashes[start:min(start+redisMGetChunk, len(hashes))]
		keys := make([]string, len(chunk))
		for i, h := range chunk {
			keys[i] = s.docKey(h)
		}
		vals, err := s.client.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("keymanager: redis mget keys: %w", err)
		}
		for _, v := range vals {
			str, ok := v.(string)
			if !ok {
				continue // deleted concurrently
			}
			vk, err := decodeKey([]byte(str))
			if err != nil {
				return nil, err
			}
			out = append(out, vk)
		}
	}
	return out, nil
}

// GetByPrefix returns the most recently created key with the display prefix.
func (s *RedisKeyStore) GetByPrefix(ctx context.Context, prefix string) (*VirtualKey, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if prefix == "" {
		return nil, ErrKeyNotFound
	}
	hashes, err := s.client.SMembers(ctx, s.prefixKey(prefix)).Result()
	if err != nil {
		return nil, fmt.Errorf("keymanager: redis prefix lookup: %w", err)
	}
	keys, err := s.loadKeys(ctx, hashes)
	if err != nil {
		return nil, err
	}
	var best *VirtualKey
	for _, k := range keys {
		if k.Prefix == prefix && (best == nil || k.CreatedAt.After(best.CreatedAt)) {
			best = k
		}
	}
	if best == nil {
		return nil, ErrKeyNotFound
	}
	return best, nil
}

// Update replaces a stored key (atomically, see UpdateFunc).
func (s *RedisKeyStore) Update(ctx context.Context, key *VirtualKey) error {
	if key == nil {
		return ErrKeyNotFound
	}
	_, err := s.UpdateFunc(ctx, key.KeyHash, func(vk *VirtualKey) error {
		*vk = *key.Clone()
		return nil
	})
	return err
}

// UpdateFunc atomically applies fn to a copy of the stored key and stores
// the result if fn returns nil. It uses WATCH/MULTI and retries on
// conflicts with other instances, so fn may run more than once and must
// only modify the key it is given. KeyHash is immutable.
func (s *RedisKeyStore) UpdateFunc(ctx context.Context, keyHash string, fn func(*VirtualKey) error) (*VirtualKey, error) {
	if !isKeyHash(keyHash) {
		return nil, ErrKeyNotFound
	}
	doc := s.docKey(keyHash)
	var result *VirtualKey
	err := s.watchUpdate(ctx, doc, func(tx *redis.Tx) error {
		raw, err := tx.Get(ctx, doc).Bytes()
		if errors.Is(err, redis.Nil) {
			return ErrKeyNotFound
		}
		if err != nil {
			return fmt.Errorf("keymanager: redis get key: %w", err)
		}
		cur, err := decodeKey(raw)
		if err != nil {
			return err
		}
		next := cur.Clone()
		if err := fn(next); err != nil {
			return err
		}
		next.KeyHash, next.HashedKey = keyHash, keyHash
		data, err := s.encode(next)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, doc, data, 0)
			if next.Prefix != cur.Prefix {
				if cur.Prefix != "" {
					p.SRem(ctx, s.prefixKey(cur.Prefix), keyHash)
				}
				if next.Prefix != "" {
					p.SAdd(ctx, s.prefixKey(next.Prefix), keyHash)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		result = next
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result.Clone(), nil
}

// Delete removes a key permanently (Manager.Delete only revokes).
func (s *RedisKeyStore) Delete(ctx context.Context, keyHash string) error {
	if !isKeyHash(keyHash) {
		return ErrKeyNotFound
	}
	doc := s.docKey(keyHash)
	return s.watchUpdate(ctx, doc, func(tx *redis.Tx) error {
		raw, err := tx.Get(ctx, doc).Bytes()
		if errors.Is(err, redis.Nil) {
			return ErrKeyNotFound
		}
		if err != nil {
			return fmt.Errorf("keymanager: redis get key: %w", err)
		}
		cur, err := decodeKey(raw)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Del(ctx, doc)
			p.SRem(ctx, s.indexKey(), keyHash)
			if cur.Prefix != "" {
				p.SRem(ctx, s.prefixKey(cur.Prefix), keyHash)
			}
			return nil
		})
		return err
	})
}

// List returns all keys.
func (s *RedisKeyStore) List(ctx context.Context) ([]*VirtualKey, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	hashes, err := s.client.SMembers(ctx, s.indexKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("keymanager: redis list keys: %w", err)
	}
	return s.loadKeys(ctx, hashes)
}

// RedisUserStore is a UserStore backed by Redis.
type RedisUserStore struct {
	redisBase
}

var _ UserStore = (*RedisUserStore)(nil)

// NewRedisUserStore returns a user store using client and prefix.
func NewRedisUserStore(client redis.UniversalClient, prefix string) *RedisUserStore {
	return &RedisUserStore{redisBase: newRedisBase(client, prefix)}
}

func (s *RedisUserStore) docKey(id string) string { return s.ns + "user:" + id }

// Create stores a new user (fails with ErrUserExists).
func (s *RedisUserStore) Create(ctx context.Context, u *User) error {
	if err := s.ready(); err != nil {
		return err
	}
	if u == nil || u.ID == "" {
		return errors.New("keymanager: invalid user")
	}
	data, err := json.Marshal(u)
	if err != nil {
		return fmt.Errorf("keymanager: encode user: %w", err)
	}
	ok, err := s.client.SetNX(ctx, s.docKey(u.ID), data, 0).Result()
	if err != nil {
		return fmt.Errorf("keymanager: redis create user: %w", err)
	}
	if !ok {
		return ErrUserExists
	}
	return nil
}

// Get returns a user.
func (s *RedisUserStore) Get(ctx context.Context, id string) (*User, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	raw, err := s.client.Get(ctx, s.docKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrUserNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("keymanager: redis get user: %w", err)
	}
	var u User
	if err := json.Unmarshal(raw, &u); err != nil {
		return nil, fmt.Errorf("keymanager: decode user: %w", err)
	}
	return &u, nil
}

// Update replaces an existing user (fails with ErrUserNotFound).
func (s *RedisUserStore) Update(ctx context.Context, u *User) error {
	if err := s.ready(); err != nil {
		return err
	}
	if u == nil || u.ID == "" {
		return errors.New("keymanager: invalid user")
	}
	data, err := json.Marshal(u)
	if err != nil {
		return fmt.Errorf("keymanager: encode user: %w", err)
	}
	ok, err := s.client.SetXX(ctx, s.docKey(u.ID), data, redis.KeepTTL).Result()
	if err != nil {
		return fmt.Errorf("keymanager: redis update user: %w", err)
	}
	if !ok {
		return ErrUserNotFound
	}
	return nil
}

// RedisTeamStore is a TeamStore backed by Redis. UpdateFunc is atomic
// across instances, so team spend accrued by several gateway instances is
// never lost.
type RedisTeamStore struct {
	redisBase
}

var (
	_ TeamStore         = (*RedisTeamStore)(nil)
	_ AtomicTeamUpdater = (*RedisTeamStore)(nil)
)

// NewRedisTeamStore returns a team store using client and prefix.
func NewRedisTeamStore(client redis.UniversalClient, prefix string) *RedisTeamStore {
	return &RedisTeamStore{redisBase: newRedisBase(client, prefix)}
}

func (s *RedisTeamStore) docKey(id string) string { return s.ns + "team:" + id }

// Create stores a new team (fails with ErrTeamExists).
func (s *RedisTeamStore) Create(ctx context.Context, t *Team) error {
	if err := s.ready(); err != nil {
		return err
	}
	if t == nil || t.ID == "" {
		return errors.New("keymanager: invalid team")
	}
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("keymanager: encode team: %w", err)
	}
	ok, err := s.client.SetNX(ctx, s.docKey(t.ID), data, 0).Result()
	if err != nil {
		return fmt.Errorf("keymanager: redis create team: %w", err)
	}
	if !ok {
		return ErrTeamExists
	}
	return nil
}

func decodeTeam(raw []byte) (*Team, error) {
	var t Team
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, fmt.Errorf("keymanager: decode team: %w", err)
	}
	return &t, nil
}

// Get returns a team.
func (s *RedisTeamStore) Get(ctx context.Context, id string) (*Team, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	raw, err := s.client.Get(ctx, s.docKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrTeamNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("keymanager: redis get team: %w", err)
	}
	return decodeTeam(raw)
}

// Update replaces an existing team; CreatedAt is preserved.
func (s *RedisTeamStore) Update(ctx context.Context, t *Team) error {
	if t == nil {
		return errors.New("keymanager: invalid team")
	}
	_, err := s.UpdateFunc(ctx, t.ID, func(cur *Team) error {
		*cur = cloneTeam(*t)
		return nil
	})
	return err
}

// UpdateFunc atomically applies fn to a copy of the team (WATCH/MULTI with
// retries; fn may run more than once). ID and CreatedAt are immutable.
func (s *RedisTeamStore) UpdateFunc(ctx context.Context, id string, fn func(*Team) error) (*Team, error) {
	if id == "" {
		return nil, ErrTeamNotFound
	}
	doc := s.docKey(id)
	var result *Team
	err := s.watchUpdate(ctx, doc, func(tx *redis.Tx) error {
		raw, err := tx.Get(ctx, doc).Bytes()
		if errors.Is(err, redis.Nil) {
			return ErrTeamNotFound
		}
		if err != nil {
			return fmt.Errorf("keymanager: redis get team: %w", err)
		}
		cur, err := decodeTeam(raw)
		if err != nil {
			return err
		}
		next := cloneTeamPtr(cur)
		if err := fn(next); err != nil {
			return err
		}
		next.ID, next.CreatedAt = cur.ID, cur.CreatedAt
		data, err := json.Marshal(next)
		if err != nil {
			return fmt.Errorf("keymanager: encode team: %w", err)
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, doc, data, 0)
			return nil
		})
		if err != nil {
			return err
		}
		result = next
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cloneTeamPtr(result), nil
}
