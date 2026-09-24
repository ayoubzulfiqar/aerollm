// Package persist is a small durable document store used by the gateway's
// in-process stores (virtual keys, secrets, flags, incidents, batches, ...)
// so their state survives restarts.
//
// Values are JSON documents addressed by (bucket, key). The bbolt backend is
// single-process: two gateway instances must not share one file (use the
// Redis-backed stores for multi-instance deployments where available).
package persist

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ErrClosed is returned by operations on a closed store.
var ErrClosed = errors.New("persist: store closed")

// Store persists JSON documents in named buckets. Implementations are safe
// for concurrent use.
type Store interface {
	// Put stores v (JSON-encoded) under bucket/key, replacing any value.
	Put(bucket, key string, v any) error
	// Get decodes the value under bucket/key into v. It reports false (and
	// no error) when the key does not exist.
	Get(bucket, key string, v any) (bool, error)
	// Delete removes bucket/key; deleting a missing key is not an error.
	Delete(bucket, key string) error
	// ForEach calls fn for every key in bucket in ascending key order. A
	// non-nil error from fn stops the iteration and is returned.
	ForEach(bucket string, fn func(key string, raw json.RawMessage) error) error
	// Close releases the store.
	Close() error
}

func validate(bucket, key string) error {
	if bucket == "" {
		return errors.New("persist: empty bucket")
	}
	if key == "" {
		return errors.New("persist: empty key")
	}
	return nil
}

// LoadAll decodes every document in bucket into a map keyed by key.
// Documents that fail to decode are skipped and reported in the returned
// error (after all valid documents have been loaded).
func LoadAll[T any](s Store, bucket string) (map[string]T, error) {
	out := make(map[string]T)
	var bad []string
	err := s.ForEach(bucket, func(key string, raw json.RawMessage) error {
		var v T
		if err := json.Unmarshal(raw, &v); err != nil {
			bad = append(bad, key)
			return nil
		}
		out[key] = v
		return nil
	})
	if err != nil {
		return out, err
	}
	if len(bad) > 0 {
		return out, fmt.Errorf("persist: %d undecodable documents in %s (%s)", len(bad), bucket, strings.Join(bad, ","))
	}
	return out, nil
}

// Bolt is a bbolt-backed Store.
type Bolt struct {
	db *bolt.DB
}

// OpenBolt opens (creating if needed) a bbolt file at path with 0600
// permissions. The parent directory is created with 0700. It fails after a
// short timeout if another process holds the file lock.
func OpenBolt(path string) (*Bolt, error) {
	if path == "" {
		return nil, errors.New("persist: empty path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("persist: create dir: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("persist: open %s: %w", path, err)
	}
	// Writes go through db.Batch, which coalesces concurrent writers into
	// one transaction (one fsync); a short delay bounds the added latency.
	db.MaxBatchDelay = 5 * time.Millisecond
	return &Bolt{db: db}, nil
}

// Put implements Store.
func (b *Bolt) Put(bucket, key string, v any) error {
	if err := validate(bucket, key); err != nil {
		return err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("persist: encode %s/%s: %w", bucket, key, err)
	}
	// Batch may run the function more than once; Put is idempotent.
	return b.db.Batch(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return bk.Put([]byte(key), data)
	})
}

// Get implements Store.
func (b *Bolt) Get(bucket, key string, v any) (bool, error) {
	if err := validate(bucket, key); err != nil {
		return false, err
	}
	var raw []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(bucket))
		if bk == nil {
			return nil
		}
		if val := bk.Get([]byte(key)); val != nil {
			raw = append([]byte(nil), val...)
		}
		return nil
	})
	if err != nil || raw == nil {
		return false, err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return true, fmt.Errorf("persist: decode %s/%s: %w", bucket, key, err)
	}
	return true, nil
}

// Delete implements Store.
func (b *Bolt) Delete(bucket, key string) error {
	if err := validate(bucket, key); err != nil {
		return err
	}
	return b.db.Batch(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(bucket))
		if bk == nil {
			return nil
		}
		return bk.Delete([]byte(key))
	})
}

// ForEach implements Store. fn runs outside the bbolt transaction, so it
// may call back into the store.
func (b *Bolt) ForEach(bucket string, fn func(key string, raw json.RawMessage) error) error {
	if bucket == "" {
		return errors.New("persist: empty bucket")
	}
	type kv struct {
		k string
		v []byte
	}
	var items []kv
	err := b.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(bucket))
		if bk == nil {
			return nil
		}
		return bk.ForEach(func(k, v []byte) error {
			items = append(items, kv{string(k), append([]byte(nil), v...)})
			return nil
		})
	})
	if err != nil {
		return err
	}
	for _, it := range items {
		if err := fn(it.k, it.v); err != nil {
			return err
		}
	}
	return nil
}

// Close implements Store.
func (b *Bolt) Close() error { return b.db.Close() }

// Memory is an in-memory Store for tests and ephemeral deployments.
type Memory struct {
	mu      sync.RWMutex
	buckets map[string]map[string][]byte
	closed  bool
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{buckets: make(map[string]map[string][]byte)} }

// Put implements Store.
func (m *Memory) Put(bucket, key string, v any) error {
	if err := validate(bucket, key); err != nil {
		return err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("persist: encode %s/%s: %w", bucket, key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	bk := m.buckets[bucket]
	if bk == nil {
		bk = make(map[string][]byte)
		m.buckets[bucket] = bk
	}
	bk[key] = data
	return nil
}

// Get implements Store.
func (m *Memory) Get(bucket, key string, v any) (bool, error) {
	if err := validate(bucket, key); err != nil {
		return false, err
	}
	m.mu.RLock()
	raw, ok := m.buckets[bucket][key]
	closed := m.closed
	m.mu.RUnlock()
	if closed {
		return false, ErrClosed
	}
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return true, fmt.Errorf("persist: decode %s/%s: %w", bucket, key, err)
	}
	return true, nil
}

// Delete implements Store.
func (m *Memory) Delete(bucket, key string) error {
	if err := validate(bucket, key); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	delete(m.buckets[bucket], key)
	return nil
}

// ForEach implements Store.
func (m *Memory) ForEach(bucket string, fn func(key string, raw json.RawMessage) error) error {
	if bucket == "" {
		return errors.New("persist: empty bucket")
	}
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return ErrClosed
	}
	keys := make([]string, 0, len(m.buckets[bucket]))
	vals := make(map[string][]byte, len(m.buckets[bucket]))
	for k, v := range m.buckets[bucket] {
		keys = append(keys, k)
		vals[k] = append([]byte(nil), v...)
	}
	m.mu.RUnlock()
	sort.Strings(keys)
	for _, k := range keys {
		if err := fn(k, vals[k]); err != nil {
			return err
		}
	}
	return nil
}

// Close implements Store.
func (m *Memory) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}
