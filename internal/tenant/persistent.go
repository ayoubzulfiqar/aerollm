package tenant

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used by the persistent tenant stores.
const (
	BucketAPIKeys       = "tenant_api_keys"
	BucketOrganizations = "tenant_orgs"
	BucketTeams         = "tenant_teams"
	BucketUsers         = "tenant_users"
	BucketQuotas        = "tenant_quotas"
)

// LastUsedPersistInterval is the minimum interval between persisted
// LastUsedAt updates of one API key (seconds), so key resolution does not
// write on every request.
const LastUsedPersistInterval = 60

// ErrPersistence is matched (errors.Is) by errors caused by the persistence
// backend.
var ErrPersistence = errors.New("tenant: persistence failure")

// storeDisk mirrors an InMemoryStore to a persist.Store. Used under the
// store's lock; all methods are no-ops on a nil receiver.
type storeDisk struct {
	ps       persist.Store
	lastUsed map[string]int64 // persisted LastUsedAt per key ID
	failures uint64
	lastErr  error
	onError  func(error)
}

func (d *storeDisk) put(bucket, key string, v any) error {
	if d == nil {
		return nil
	}
	if err := d.ps.Put(bucket, key, v); err != nil {
		return fmt.Errorf("%w: %s/%s: %v", ErrPersistence, bucket, key, err)
	}
	return nil
}

func (d *storeDisk) putAPIKey(k *APIKey) error {
	if d == nil {
		return nil
	}
	if err := d.put(BucketAPIKeys, k.ID, k); err != nil {
		return err
	}
	d.lastUsed[k.ID] = k.LastUsedAt
	return nil
}

// touchAPIKey persists k's LastUsedAt when it advanced by at least
// LastUsedPersistInterval. On failure it records the error and returns the
// error handler to call outside the lock.
func (d *storeDisk) touchAPIKey(k *APIKey) (func(error), error) {
	if d == nil || k.LastUsedAt-d.lastUsed[k.ID] < LastUsedPersistInterval {
		return nil, nil
	}
	if err := d.putAPIKey(k.clone()); err != nil {
		d.failures++
		d.lastErr = err
		return d.onError, err
	}
	return nil, nil
}

func loadBucket[T any](ps persist.Store, bucket string) (map[string]T, error) {
	docs, err := persist.LoadAll[T](ps, bucket)
	if err != nil {
		return nil, fmt.Errorf("tenant: load %s: %w", bucket, err)
	}
	return docs, nil
}

// NewPersistentStore returns an InMemoryStore loaded from ps that writes
// every change through to it (buckets BucketAPIKeys, BucketOrganizations,
// BucketTeams, BucketUsers). Only key hashes are persisted.
func NewPersistentStore(ps persist.Store) (*InMemoryStore, error) {
	s := NewInMemoryStore()
	if err := s.EnablePersistence(ps); err != nil {
		return nil, err
	}
	return s, nil
}

// EnablePersistence loads the tenants and API keys stored in ps and writes
// every later change through to it. Entries already in memory are written
// to ps and take precedence. It fails on undecodable or inconsistent
// documents (e.g. two keys with the same hash), so nothing is silently
// dropped.
func (s *InMemoryStore) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("tenant: nil persist store")
	}
	keys, err := loadBucket[*APIKey](ps, BucketAPIKeys)
	if err != nil {
		return err
	}
	orgs, err := loadBucket[*Organization](ps, BucketOrganizations)
	if err != nil {
		return err
	}
	teams, err := loadBucket[*Team](ps, BucketTeams)
	if err != nil {
		return err
	}
	users, err := loadBucket[*User](ps, BucketUsers)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disk != nil {
		return errors.New("tenant: persistence already enabled")
	}
	d := &storeDisk{ps: ps, lastUsed: make(map[string]int64)}

	byHash := make(map[string]string, len(keys))
	for id, k := range keys {
		if k == nil || k.ID != id || k.HashedKey == "" {
			return fmt.Errorf("tenant: stored api key %q is invalid", id)
		}
		k.HashedKey = strings.ToLower(k.HashedKey)
		if other, dup := byHash[k.HashedKey]; dup {
			return fmt.Errorf("tenant: stored api keys %q and %q share a hash", other, id)
		}
		byHash[k.HashedKey] = id
		d.lastUsed[id] = k.LastUsedAt
	}
	for id, o := range orgs {
		if o == nil || string(o.ID) != id {
			return fmt.Errorf("tenant: stored organization %q is invalid", id)
		}
	}
	for id, t := range teams {
		if t == nil || string(t.ID) != id {
			return fmt.Errorf("tenant: stored team %q is invalid", id)
		}
	}
	for id, u := range users {
		if u == nil || string(u.ID) != id {
			return fmt.Errorf("tenant: stored user %q is invalid", id)
		}
	}

	// Write what is already in memory, then adopt stored entries that are
	// not shadowed by it.
	for id, k := range s.apiKeys {
		if other, ok := byHash[k.HashedKey]; ok && other != id {
			return fmt.Errorf("tenant: api key %q conflicts with stored key %q", id, other)
		}
		if err := d.putAPIKey(k); err != nil {
			return err
		}
	}
	for id, o := range s.orgs {
		if err := d.put(BucketOrganizations, string(id), o); err != nil {
			return err
		}
	}
	for id, t := range s.teams {
		if err := d.put(BucketTeams, string(id), t); err != nil {
			return err
		}
	}
	for id, u := range s.users {
		if err := d.put(BucketUsers, string(id), u); err != nil {
			return err
		}
	}
	for id, k := range keys {
		if _, ok := s.apiKeys[id]; !ok {
			s.apiKeys[id] = k
			s.byHash[k.HashedKey] = id
		}
	}
	for id, o := range orgs {
		if _, ok := s.orgs[TenantID(id)]; !ok {
			s.orgs[TenantID(id)] = o
		}
	}
	for id, t := range teams {
		if _, ok := s.teams[TenantID(id)]; !ok {
			s.teams[TenantID(id)] = t
		}
	}
	for id, u := range users {
		if _, ok := s.users[TenantID(id)]; !ok {
			s.users[TenantID(id)] = u
		}
	}
	s.disk = d
	return nil
}

// SetErrorHandler registers fn to be called (outside the store's lock) when
// a best-effort write fails (currently: persisting an API key's LastUsedAt
// during ResolveByAPIKey). All other write failures are returned directly.
func (s *InMemoryStore) SetErrorHandler(fn func(error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disk != nil {
		s.disk.onError = fn
	}
}

// PersistErrors returns how many best-effort writes failed and the most
// recent error (0, nil without persistence).
func (s *InMemoryStore) PersistErrors() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.disk == nil {
		return 0, nil
	}
	return s.disk.failures, s.disk.lastErr
}

// Persistent reports whether the store writes through to a persist.Store.
func (s *InMemoryStore) Persistent() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.disk != nil
}

// NewPersistentQuotaStore returns a quota store loaded from ps (bucket
// BucketQuotas) that writes every change through to it.
func NewPersistentQuotaStore(ps persist.Store) (*InMemoryQuotaStore, error) {
	s := NewInMemoryQuotaStore()
	if err := s.EnablePersistence(ps); err != nil {
		return nil, err
	}
	return s, nil
}

// EnablePersistence loads the quotas stored in ps and writes every later
// change through to it. Quotas already in memory are written to ps and take
// precedence. Invalid stored quotas make it fail.
func (s *InMemoryQuotaStore) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("tenant: nil persist store")
	}
	stored, err := loadBucket[Quota](ps, BucketQuotas)
	if err != nil {
		return err
	}
	for id, q := range stored {
		if q.ID != id {
			return fmt.Errorf("tenant: stored quota %q does not match its id", id)
		}
		if err := q.validate(); err != nil {
			return fmt.Errorf("tenant: stored quota %q: %w", id, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disk != nil {
		return errors.New("tenant: persistence already enabled")
	}
	for id, q := range s.quotas {
		if err := ps.Put(BucketQuotas, id, q); err != nil {
			return fmt.Errorf("%w: quota %s: %v", ErrPersistence, id, err)
		}
	}
	for id, q := range stored {
		if _, ok := s.quotas[id]; !ok {
			c := q
			s.quotas[id] = &c
		}
	}
	s.disk = ps
	return nil
}
