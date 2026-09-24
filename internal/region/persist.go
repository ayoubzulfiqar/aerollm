package region

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used when persistence is enabled. Each document is keyed by the
// object's ID and holds its JSON representation. Only configuration is
// persisted; Resolve decisions are derived and never stored.
const (
	BucketRegions  = "region.regions"
	BucketPolicies = "region.residency"
	BucketRules    = "region.routes"
)

// NewStoreWithPersistence returns a Store backed by ps (see
// EnablePersistence). When err wraps ErrPersistence the store could not be
// loaded and is nil; otherwise the store is usable and a non-nil err
// (wrapping ErrInvalid) lists persisted documents that were skipped.
func NewStoreWithPersistence(ps persist.Store) (*Store, error) {
	s := NewStore()
	if err := s.EnablePersistence(ps); err != nil {
		if ps == nil || errors.Is(err, ErrPersistence) {
			return nil, err
		}
		return s, err
	}
	return s, nil
}

// EnablePersistence loads regions, residency policies and route rules from
// ps and writes every later mutation through to it before changing memory.
//
// Persisted documents win over in-memory objects with the same ID; in-memory
// objects missing from ps are written to it. Documents that fail to decode
// or validate (including policies/rules naming an unknown region) are skipped
// and left untouched in ps; they are reported in the returned error, which
// then wraps ErrInvalid while persistence is still enabled. A failure to
// read or write ps returns an error wrapping ErrPersistence and leaves the
// store unchanged and non-persistent. Calling it twice, or with a nil ps, is
// an error.
func (s *Store) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("region: nil persist store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ps != nil {
		return errors.New("region: persistence already enabled")
	}

	var skipped []error
	skip := func(bucket, key string, err error) {
		skipped = append(skipped, fmt.Errorf("%s/%s: %w", bucket, key, err))
	}

	rawRegions, err := loadBucket[Region](ps, BucketRegions, skip)
	if err != nil {
		return err
	}
	rawPolicies, err := loadBucket[ResidencyPolicy](ps, BucketPolicies, skip)
	if err != nil {
		return err
	}
	rawRules, err := loadBucket[RouteRule](ps, BucketRules, skip)
	if err != nil {
		return err
	}

	// Validate persisted documents (regions first: policies and rules
	// reference them), then merge in-memory objects that ps lacks.
	regions := make(map[string]Region, len(rawRegions)+len(s.regions))
	var primaries []string
	for _, kv := range rawRegions {
		r := kv.val
		if err := validateLoaded(kv.key, r.ID, validateRegion(&r)); err != nil {
			skip(BucketRegions, kv.key, err)
			continue
		}
		if len(regions) >= MaxRegions {
			skip(BucketRegions, kv.key, ErrStoreFull)
			continue
		}
		regions[r.ID] = r
		if r.Primary {
			primaries = append(primaries, r.ID)
		}
	}
	var toWrite []pendingPut
	for id, r := range s.regions {
		if _, ok := regions[id]; ok {
			continue
		}
		if len(regions) >= MaxRegions {
			return fmt.Errorf("%w: persisted and in-memory regions exceed %d", ErrPersistence, MaxRegions)
		}
		regions[id] = r
		if r.Primary {
			primaries = append(primaries, id)
		}
		toWrite = append(toWrite, pendingPut{BucketRegions, id, r})
	}
	// At most one primary: keep the lowest ID and persist the demotions.
	if len(primaries) > 1 {
		sort.Strings(primaries)
		for _, id := range primaries[1:] {
			r := regions[id]
			r.Primary = false
			regions[id] = r
			toWrite = append(toWrite, pendingPut{BucketRegions, id, r})
		}
	}

	policies := make(map[string]ResidencyPolicy, len(rawPolicies)+len(s.policies))
	for _, kv := range rawPolicies {
		p := kv.val
		if err := validateLoaded(kv.key, p.ID, validatePolicy(&p, regions)); err != nil {
			skip(BucketPolicies, kv.key, err)
			continue
		}
		if len(policies) >= MaxPolicies {
			skip(BucketPolicies, kv.key, ErrStoreFull)
			continue
		}
		policies[p.ID] = p
	}
	for id, p := range s.policies {
		if _, ok := policies[id]; ok {
			continue
		}
		if len(policies) >= MaxPolicies {
			return fmt.Errorf("%w: persisted and in-memory policies exceed %d", ErrPersistence, MaxPolicies)
		}
		policies[id] = p
		toWrite = append(toWrite, pendingPut{BucketPolicies, id, p})
	}

	rules := make(map[string]RouteRule, len(rawRules)+len(s.rules))
	for _, kv := range rawRules {
		r := kv.val
		if err := validateLoaded(kv.key, r.ID, validateRule(&r, regions)); err != nil {
			skip(BucketRules, kv.key, err)
			continue
		}
		if len(rules) >= MaxRules {
			skip(BucketRules, kv.key, ErrStoreFull)
			continue
		}
		rules[r.ID] = r
	}
	for id, r := range s.rules {
		if _, ok := rules[id]; ok {
			continue
		}
		if len(rules) >= MaxRules {
			return fmt.Errorf("%w: persisted and in-memory rules exceed %d", ErrPersistence, MaxRules)
		}
		rules[id] = cloneRule(r)
		toWrite = append(toWrite, pendingPut{BucketRules, id, r})
	}

	sort.Slice(toWrite, func(i, j int) bool {
		if toWrite[i].bucket != toWrite[j].bucket {
			return toWrite[i].bucket < toWrite[j].bucket
		}
		return toWrite[i].key < toWrite[j].key
	})
	for _, w := range toWrite {
		if err := ps.Put(w.bucket, w.key, w.val); err != nil {
			return fmt.Errorf("%w: put %s/%s: %w", ErrPersistence, w.bucket, w.key, err)
		}
	}

	s.regions, s.policies, s.rules = regions, policies, rules
	s.ps = ps
	if len(skipped) > 0 {
		return fmt.Errorf("%w: skipped %d persisted document(s): %w", ErrInvalid, len(skipped), errors.Join(skipped...))
	}
	return nil
}

type pendingPut struct {
	bucket, key string
	val         any
}

type keyed[T any] struct {
	key string
	val T
}

// loadBucket decodes every document in bucket in key order. Undecodable
// documents are reported through skip; a store-level failure is returned
// wrapped in ErrPersistence.
func loadBucket[T any](ps persist.Store, bucket string, skip func(bucket, key string, err error)) ([]keyed[T], error) {
	var out []keyed[T]
	err := ps.ForEach(bucket, func(key string, raw json.RawMessage) error {
		var v T
		if err := json.Unmarshal(raw, &v); err != nil {
			skip(bucket, key, fmt.Errorf("%w: undecodable document", ErrInvalid))
			return nil
		}
		out = append(out, keyed[T]{key, v})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: load %s: %w", ErrPersistence, bucket, err)
	}
	return out, nil
}

// validateLoaded combines a validation result with the key/ID consistency
// check for a persisted document.
func validateLoaded(key, id string, verr error) error {
	if verr != nil {
		return verr
	}
	if key != id {
		return invalid("document key %q does not match id %q", key, id)
	}
	return nil
}

// putLocked writes v through to the durable store, if any. s.mu must be held.
func (s *Store) putLocked(bucket, key string, v any) error {
	if s.ps == nil {
		return nil
	}
	if err := s.ps.Put(bucket, key, v); err != nil {
		return fmt.Errorf("%w: put %s/%s: %w", ErrPersistence, bucket, key, err)
	}
	return nil
}

// deleteLocked removes key from the durable store, if any. s.mu must be held.
func (s *Store) deleteLocked(bucket, key string) error {
	if s.ps == nil {
		return nil
	}
	if err := s.ps.Delete(bucket, key); err != nil {
		return fmt.Errorf("%w: delete %s/%s: %w", ErrPersistence, bucket, key, err)
	}
	return nil
}

// persistRegionsLocked writes regions in order. When a write fails, the
// documents already written are restored (best effort) to the state that
// matches memory, so a failed upsert changes neither memory nor ps.
func (s *Store) persistRegionsLocked(regions []Region) error {
	if s.ps == nil {
		return nil
	}
	for i, r := range regions {
		if err := s.putLocked(BucketRegions, r.ID, r); err != nil {
			errs := []error{err}
			for _, done := range regions[:i] {
				var rerr error
				if prev, ok := s.regions[done.ID]; ok {
					rerr = s.ps.Put(BucketRegions, done.ID, prev)
				} else {
					rerr = s.ps.Delete(BucketRegions, done.ID)
				}
				if rerr != nil {
					errs = append(errs, fmt.Errorf("rollback %s: %w", done.ID, rerr))
				}
			}
			return errors.Join(errs...)
		}
	}
	return nil
}
