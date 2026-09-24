package keymanager

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used by the persist.Store-backed stores.
const (
	BucketKeys  = "keymanager_keys"
	BucketUsers = "keymanager_users"
	BucketTeams = "keymanager_teams"
)

// durableMap is an in-memory map mirrored to a persist.Store bucket.
//
// Every mutation is written to the persist.Store before it becomes visible
// in memory, so a failed write leaves the store unchanged and is returned
// to the caller. Mutations are serialized by wmu (the backend serializes
// writes anyway); readers only take mu briefly and never wait for I/O.
type durableMap[T any] struct {
	ps     persist.Store
	bucket string
	clone  func(T) T

	wmu sync.Mutex   // serializes mutations and their persistence
	mu  sync.RWMutex // guards m
	m   map[string]T
}

func loadDurableMap[T any](ps persist.Store, bucket string, clone func(T) T, fix func(id string, v *T) error) (*durableMap[T], error) {
	if ps == nil {
		return nil, errors.New("keymanager: nil persist store")
	}
	docs, err := persist.LoadAll[T](ps, bucket)
	if err != nil {
		return nil, fmt.Errorf("keymanager: load %s: %w", bucket, err)
	}
	for id, v := range docs {
		if fix != nil {
			if err := fix(id, &v); err != nil {
				return nil, fmt.Errorf("keymanager: load %s/%s: %w", bucket, id, err)
			}
			docs[id] = v
		}
	}
	return &durableMap[T]{ps: ps, bucket: bucket, clone: clone, m: docs}, nil
}

func (d *durableMap[T]) get(id string) (T, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	v, ok := d.m[id]
	if !ok {
		var zero T
		return zero, false
	}
	return d.clone(v), true
}

func (d *durableMap[T]) values() []T {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]T, 0, len(d.m))
	for _, v := range d.m {
		out = append(out, d.clone(v))
	}
	return out
}

// mutate applies fn to the current value (exists reports whether there is
// one) and persists and publishes the returned value.
func (d *durableMap[T]) mutate(id string, fn func(cur T, exists bool) (T, error)) (T, error) {
	var zero T
	d.wmu.Lock()
	defer d.wmu.Unlock()
	cur, exists := d.get(id)
	next, err := fn(cur, exists)
	if err != nil {
		return zero, err
	}
	next = d.clone(next)
	if err := d.ps.Put(d.bucket, id, next); err != nil {
		return zero, fmt.Errorf("keymanager: persist %s/%s: %w", d.bucket, id, err)
	}
	d.mu.Lock()
	d.m[id] = next
	d.mu.Unlock()
	return d.clone(next), nil
}

func (d *durableMap[T]) remove(id string) (bool, error) {
	d.wmu.Lock()
	defer d.wmu.Unlock()
	if _, ok := d.get(id); !ok {
		return false, nil
	}
	if err := d.ps.Delete(d.bucket, id); err != nil {
		return true, fmt.Errorf("keymanager: persist delete %s/%s: %w", d.bucket, id, err)
	}
	d.mu.Lock()
	delete(d.m, id)
	d.mu.Unlock()
	return true, nil
}

// normalizeStoredKey restores fields that are not serialized (HashedKey)
// and re-mirrors the typed rate limits into Metadata with their Go types
// (JSON decoding turns every number into float64).
func normalizeStoredKey(vk *VirtualKey) {
	vk.HashedKey = vk.KeyHash
	if vk.Metadata != nil {
		delete(vk.Metadata, MetadataRateLimitRPS)
		delete(vk.Metadata, MetadataRateLimitTPM)
		if vk.RateLimitRPS > 0 {
			vk.Metadata[MetadataRateLimitRPS] = vk.RateLimitRPS
		}
		if vk.RateLimitTPM > 0 {
			vk.Metadata[MetadataRateLimitTPM] = vk.RateLimitTPM
		}
	}
}

func cloneKeyValue(v *VirtualKey) *VirtualKey { return v.Clone() }

// PersistentKeyStore is a KeyStore that keeps keys in memory and writes
// every change through to a persist.Store (bucket BucketKeys), so issued
// keys, their limits, spend and revocations survive restarts. Only the
// SHA-256 hash of a key is stored. It implements AtomicKeyUpdater.
//
// The persist.Store must not be shared by several gateway processes (use
// RedisKeyStore for multi-instance deployments).
type PersistentKeyStore struct {
	d *durableMap[*VirtualKey]
}

var (
	_ KeyStore         = (*PersistentKeyStore)(nil)
	_ AtomicKeyUpdater = (*PersistentKeyStore)(nil)
)

// NewPersistentKeyStore loads all keys from ps and returns a write-through
// store. It fails if a stored document cannot be decoded, so keys are
// never silently lost.
func NewPersistentKeyStore(ps persist.Store) (*PersistentKeyStore, error) {
	d, err := loadDurableMap(ps, BucketKeys, cloneKeyValue, func(id string, v **VirtualKey) error {
		if *v == nil || (*v).KeyHash != id || !isKeyHash(id) {
			return errors.New("document does not match its key hash")
		}
		normalizeStoredKey(*v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &PersistentKeyStore{d: d}, nil
}

// Create stores a new key; it fails with ErrKeyExists if the hash exists.
func (s *PersistentKeyStore) Create(ctx context.Context, key *VirtualKey) error {
	if key == nil || !isKeyHash(key.KeyHash) {
		return errors.New("keymanager: invalid key")
	}
	_, err := s.d.mutate(key.KeyHash, func(_ *VirtualKey, exists bool) (*VirtualKey, error) {
		if exists {
			return nil, ErrKeyExists
		}
		c := key.Clone()
		c.HashedKey = c.KeyHash
		return c, nil
	})
	return err
}

// Get returns a copy of the key with the given hash.
func (s *PersistentKeyStore) Get(ctx context.Context, keyHash string) (*VirtualKey, error) {
	if vk, ok := s.d.get(keyHash); ok {
		return vk, nil
	}
	return nil, ErrKeyNotFound
}

// GetByPrefix returns the most recently created key with the display prefix.
func (s *PersistentKeyStore) GetByPrefix(ctx context.Context, prefix string) (*VirtualKey, error) {
	var best *VirtualKey
	if prefix != "" {
		for _, k := range s.d.values() {
			if k.Prefix == prefix && (best == nil || k.CreatedAt.After(best.CreatedAt)) {
				best = k
			}
		}
	}
	if best == nil {
		return nil, ErrKeyNotFound
	}
	return best, nil
}

// Update replaces a stored key.
func (s *PersistentKeyStore) Update(ctx context.Context, key *VirtualKey) error {
	if key == nil {
		return ErrKeyNotFound
	}
	_, err := s.d.mutate(key.KeyHash, func(_ *VirtualKey, exists bool) (*VirtualKey, error) {
		if !exists {
			return nil, ErrKeyNotFound
		}
		c := key.Clone()
		c.HashedKey = c.KeyHash
		return c, nil
	})
	return err
}

// UpdateFunc atomically applies fn to a copy of the stored key and persists
// the result if fn returns nil. KeyHash is immutable.
func (s *PersistentKeyStore) UpdateFunc(ctx context.Context, keyHash string, fn func(*VirtualKey) error) (*VirtualKey, error) {
	return s.d.mutate(keyHash, func(cur *VirtualKey, exists bool) (*VirtualKey, error) {
		if !exists {
			return nil, ErrKeyNotFound
		}
		if err := fn(cur); err != nil {
			return nil, err
		}
		cur.KeyHash, cur.HashedKey = keyHash, keyHash
		return cur, nil
	})
}

// Delete removes a key permanently (Manager.Delete only revokes).
func (s *PersistentKeyStore) Delete(ctx context.Context, keyHash string) error {
	ok, err := s.d.remove(keyHash)
	if err != nil {
		return err
	}
	if !ok {
		return ErrKeyNotFound
	}
	return nil
}

// List returns copies of all keys.
func (s *PersistentKeyStore) List(ctx context.Context) ([]*VirtualKey, error) {
	return s.d.values(), nil
}

// PersistentUserStore is a UserStore mirrored to a persist.Store (bucket
// BucketUsers).
type PersistentUserStore struct {
	d *durableMap[User]
}

var _ UserStore = (*PersistentUserStore)(nil)

func cloneUser(u User) User {
	u.Metadata = maps.Clone(u.Metadata)
	return u
}

// NewPersistentUserStore loads all users from ps.
func NewPersistentUserStore(ps persist.Store) (*PersistentUserStore, error) {
	d, err := loadDurableMap(ps, BucketUsers, cloneUser, func(id string, u *User) error {
		if u.ID != id {
			return errors.New("document does not match its id")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &PersistentUserStore{d: d}, nil
}

// Create stores a new user.
func (s *PersistentUserStore) Create(ctx context.Context, u *User) error {
	if u == nil || u.ID == "" {
		return errors.New("keymanager: invalid user")
	}
	_, err := s.d.mutate(u.ID, func(_ User, exists bool) (User, error) {
		if exists {
			return User{}, ErrUserExists
		}
		return *u, nil
	})
	return err
}

// Get returns a copy of a user.
func (s *PersistentUserStore) Get(ctx context.Context, id string) (*User, error) {
	u, ok := s.d.get(id)
	if !ok {
		return nil, ErrUserNotFound
	}
	return &u, nil
}

// Update replaces an existing user.
func (s *PersistentUserStore) Update(ctx context.Context, u *User) error {
	if u == nil {
		return errors.New("keymanager: invalid user")
	}
	_, err := s.d.mutate(u.ID, func(_ User, exists bool) (User, error) {
		if !exists {
			return User{}, ErrUserNotFound
		}
		return *u, nil
	})
	return err
}

// PersistentTeamStore is a TeamStore mirrored to a persist.Store (bucket
// BucketTeams). It implements AtomicTeamUpdater, so team spend accrual is
// atomic and durable.
type PersistentTeamStore struct {
	d *durableMap[Team]
}

var (
	_ TeamStore         = (*PersistentTeamStore)(nil)
	_ AtomicTeamUpdater = (*PersistentTeamStore)(nil)
)

// NewPersistentTeamStore loads all teams from ps.
func NewPersistentTeamStore(ps persist.Store) (*PersistentTeamStore, error) {
	d, err := loadDurableMap(ps, BucketTeams, cloneTeam, func(id string, t *Team) error {
		if t.ID != id {
			return errors.New("document does not match its id")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &PersistentTeamStore{d: d}, nil
}

// Create stores a new team.
func (s *PersistentTeamStore) Create(ctx context.Context, t *Team) error {
	if t == nil || t.ID == "" {
		return errors.New("keymanager: invalid team")
	}
	_, err := s.d.mutate(t.ID, func(_ Team, exists bool) (Team, error) {
		if exists {
			return Team{}, ErrTeamExists
		}
		return *t, nil
	})
	return err
}

// Get returns a copy of a team.
func (s *PersistentTeamStore) Get(ctx context.Context, id string) (*Team, error) {
	t, ok := s.d.get(id)
	if !ok {
		return nil, ErrTeamNotFound
	}
	return &t, nil
}

// Update replaces an existing team; CreatedAt is preserved.
func (s *PersistentTeamStore) Update(ctx context.Context, t *Team) error {
	if t == nil {
		return errors.New("keymanager: invalid team")
	}
	_, err := s.d.mutate(t.ID, func(cur Team, exists bool) (Team, error) {
		if !exists {
			return Team{}, ErrTeamNotFound
		}
		next := *t
		next.CreatedAt = cur.CreatedAt
		return next, nil
	})
	return err
}

// UpdateFunc atomically applies fn to a copy of the team and persists it.
func (s *PersistentTeamStore) UpdateFunc(ctx context.Context, id string, fn func(*Team) error) (*Team, error) {
	t, err := s.d.mutate(id, func(cur Team, exists bool) (Team, error) {
		if !exists {
			return Team{}, ErrTeamNotFound
		}
		next := cloneTeam(cur)
		if err := fn(&next); err != nil {
			return Team{}, err
		}
		next.ID, next.CreatedAt = cur.ID, cur.CreatedAt
		return next, nil
	})
	if err != nil {
		return nil, err
	}
	return &t, nil
}
