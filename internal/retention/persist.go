package retention

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// BucketPolicies holds one document per retention policy, keyed by policy
// ID, in the policy's API JSON form (the exact "ttl_duration" string is what
// round-trips the TTL). Registered targets and sweep reports are runtime
// state and are never persisted.
const BucketPolicies = "retention.policies"

// NewRetentionStoreWithPersistence returns a RetentionStore backed by ps (see
// EnablePersistence). When err wraps ErrPersistence the store could not be
// loaded and is nil; otherwise the store is usable and a non-nil err
// (wrapping ErrInvalid) lists persisted documents that were skipped.
func NewRetentionStoreWithPersistence(ps persist.Store) (*RetentionStore, error) {
	s := NewRetentionStore()
	if err := s.EnablePersistence(ps); err != nil {
		if ps == nil || errors.Is(err, ErrPersistence) {
			return nil, err
		}
		return s, err
	}
	return s, nil
}

// EnablePersistence loads policies from ps and writes every later policy
// mutation through to it before changing memory.
//
// Persisted policies win over in-memory policies with the same ID; in-memory
// policies missing from ps are written to it. Documents that fail to decode
// or validate are skipped and left untouched in ps; they are reported in the
// returned error, which then wraps ErrInvalid while persistence is still
// enabled. A failure to read or write ps returns an error wrapping
// ErrPersistence and leaves the store unchanged and non-persistent. Calling
// it twice, or with a nil ps, is an error.
func (s *RetentionStore) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("retention: nil persist store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ps != nil {
		return errors.New("retention: persistence already enabled")
	}

	var skipped []error
	loaded := make(map[string]RetentionPolicy, len(s.policies))
	err := ps.ForEach(BucketPolicies, func(key string, raw json.RawMessage) error {
		var p RetentionPolicy
		if err := json.Unmarshal(raw, &p); err != nil {
			skipped = append(skipped, fmt.Errorf("%s/%s: undecodable document", BucketPolicies, key))
			return nil
		}
		if err := p.Validate(); err != nil {
			skipped = append(skipped, fmt.Errorf("%s/%s: %w", BucketPolicies, key, err))
			return nil
		}
		if key != p.ID {
			skipped = append(skipped, fmt.Errorf("%s/%s: document key does not match id %q", BucketPolicies, key, p.ID))
			return nil
		}
		if len(loaded) >= MaxPolicies {
			skipped = append(skipped, fmt.Errorf("%s/%s: %w", BucketPolicies, key, ErrStoreFull))
			return nil
		}
		loaded[key] = p
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: load %s: %w", ErrPersistence, BucketPolicies, err)
	}

	var missing []string
	for id := range s.policies {
		if _, ok := loaded[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(loaded)+len(missing) > MaxPolicies {
		return fmt.Errorf("%w: persisted and in-memory policies exceed %d", ErrPersistence, MaxPolicies)
	}
	sort.Strings(missing)
	for _, id := range missing {
		p := s.policies[id]
		if err := ps.Put(BucketPolicies, id, p); err != nil {
			return fmt.Errorf("%w: put %s/%s: %w", ErrPersistence, BucketPolicies, id, err)
		}
		loaded[id] = p
	}

	s.policies = loaded
	s.ps = ps
	if len(skipped) > 0 {
		return fmt.Errorf("%w: skipped %d persisted document(s): %w", ErrInvalid, len(skipped), errors.Join(skipped...))
	}
	return nil
}

// putLocked writes p through to the durable store, if any. s.mu must be held.
func (s *RetentionStore) putLocked(p RetentionPolicy) error {
	if s.ps == nil {
		return nil
	}
	if err := s.ps.Put(BucketPolicies, p.ID, p); err != nil {
		return fmt.Errorf("%w: put %s/%s: %w", ErrPersistence, BucketPolicies, p.ID, err)
	}
	return nil
}
