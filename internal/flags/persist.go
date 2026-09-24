package flags

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used when persistence is enabled.
const (
	BucketFlags    = "flags.flags"
	BucketRollouts = "flags.rollouts"
)

// ErrPersistence is returned (wrapped) when a mutation could not be written
// to the durable store. The in-memory state is left unchanged in that case.
var ErrPersistence = errors.New("flags: persistence failed")

// NewStoreWithPersistence returns a store backed by ps. See
// EnablePersistence for the loading semantics. When the returned store is
// non-nil and err is non-nil, some persisted documents were invalid and
// skipped; the store is usable and write-through is enabled. A nil store
// means persistence could not be enabled at all.
func NewStoreWithPersistence(ps persist.Store) (*Store, error) {
	s := NewStore()
	if err := s.EnablePersistence(ps); err != nil {
		if s.persistenceEnabled() {
			return s, err
		}
		return nil, err
	}
	return s, nil
}

func (s *Store) persistenceEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ps != nil
}

// EnablePersistence loads every flag and rollout policy stored in ps into
// the store and, from then on, writes each mutation through to ps before
// applying it in memory. Persisted documents take precedence over entries
// already in memory; in-memory entries missing from ps are written to it.
//
// Documents that fail to decode or validate are skipped; they are reported
// in the returned error (wrapping ErrInvalidFlag) after persistence has been
// enabled. Any other error (nil ps, read/write failure, persistence already
// enabled) leaves the store unchanged and in-memory only. It is meant to be
// called once at startup.
func (s *Store) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("flags: nil persist store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ps != nil {
		return errors.New("flags: persistence already enabled")
	}

	var skipped []error
	loadedFlags := make(map[string]FeatureFlag)
	err := ps.ForEach(BucketFlags, func(key string, raw json.RawMessage) error {
		var f FeatureFlag
		if err := json.Unmarshal(raw, &f); err != nil {
			skipped = append(skipped, fmt.Errorf("flag %q: undecodable", key))
			return nil
		}
		if f.Key != key {
			skipped = append(skipped, fmt.Errorf("flag %q: key mismatch", key))
			return nil
		}
		if err := f.Validate(); err != nil {
			skipped = append(skipped, fmt.Errorf("flag %q: %w", key, err))
			return nil
		}
		if len(loadedFlags) >= MaxFlags {
			skipped = append(skipped, fmt.Errorf("flag %q: %w", key, ErrStoreFull))
			return nil
		}
		loadedFlags[key] = cloneFlag(f)
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: load flags: %w", ErrPersistence, err)
	}
	loadedRollouts := make(map[string]RolloutPolicy)
	err = ps.ForEach(BucketRollouts, func(key string, raw json.RawMessage) error {
		var p RolloutPolicy
		if err := json.Unmarshal(raw, &p); err != nil {
			skipped = append(skipped, fmt.Errorf("rollout %q: undecodable", key))
			return nil
		}
		if len(loadedRollouts) >= MaxFlags {
			skipped = append(skipped, fmt.Errorf("rollout %q: %w", key, ErrStoreFull))
			return nil
		}
		loadedRollouts[key] = p
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: load rollouts: %w", ErrPersistence, err)
	}

	// Write in-memory entries that ps does not know about yet, in a stable
	// order, before switching over.
	for _, key := range sortedKeys(s.flags) {
		if _, ok := loadedFlags[key]; ok {
			continue
		}
		if len(loadedFlags) >= MaxFlags {
			break
		}
		f := s.flags[key]
		if err := ps.Put(BucketFlags, key, f); err != nil {
			return fmt.Errorf("%w: write flag %q: %w", ErrPersistence, key, err)
		}
		loadedFlags[key] = f
	}
	for _, key := range sortedKeys(s.rollouts) {
		if _, ok := loadedRollouts[key]; ok || key == "" {
			continue
		}
		if len(loadedRollouts) >= MaxFlags {
			break
		}
		p := s.rollouts[key]
		if err := ps.Put(BucketRollouts, key, p); err != nil {
			return fmt.Errorf("%w: write rollout %q: %w", ErrPersistence, key, err)
		}
		loadedRollouts[key] = p
	}

	s.flags = loadedFlags
	s.rollouts = loadedRollouts
	s.ps = ps
	if len(skipped) > 0 {
		return fmt.Errorf("%w: skipped %d invalid persisted documents: %w", ErrInvalidFlag, len(skipped), errors.Join(skipped...))
	}
	return nil
}

// putLocked writes v to ps when persistence is enabled. s.mu must be held.
func (s *Store) putLocked(bucket, key string, v any) error {
	if s.ps == nil {
		return nil
	}
	if err := s.ps.Put(bucket, key, v); err != nil {
		return fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	return nil
}

// deleteLocked removes bucket/key from ps when persistence is enabled.
// s.mu must be held.
func (s *Store) deleteLocked(bucket, key string) error {
	if s.ps == nil || key == "" {
		return nil
	}
	if err := s.ps.Delete(bucket, key); err != nil {
		return fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
