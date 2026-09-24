package incident

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// BucketIncidents is the persist bucket holding incidents keyed by ID.
const BucketIncidents = "incident.incidents"

// ErrPersistence is returned (wrapped) when a mutation could not be written
// to the durable store. The in-memory state is left unchanged in that case.
var ErrPersistence = errors.New("incident: persistence failed")

// NewStoreWithPersistence returns a store (holding at most
// DefaultMaxIncidents) backed by ps; see EnablePersistence. When the
// returned store is non-nil and err is non-nil, some persisted documents
// were invalid and skipped; the store is usable and write-through is on. A
// nil store means persistence could not be enabled.
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

// EnablePersistence loads every incident stored in ps into the store and,
// from then on, writes each mutation (create, update, transition, delete,
// eviction) through to ps before applying it in memory. Persisted incidents
// take precedence over in-memory ones with the same ID; in-memory incidents
// missing from ps are written to it. The change hook is not invoked for
// loaded incidents.
//
// Documents that fail to decode or validate, or that exceed the store limit,
// are skipped and reported in the returned error (wrapping ErrInvalid) after
// persistence has been enabled. Any other error (nil ps, read/write failure,
// already enabled) leaves the store unchanged and in-memory only. Call it
// once at startup.
func (s *Store) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("incident: nil persist store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ps != nil {
		return errors.New("incident: persistence already enabled")
	}

	var skipped []error
	var loaded []Incident
	err := ps.ForEach(BucketIncidents, func(key string, raw json.RawMessage) error {
		var inc Incident
		if err := json.Unmarshal(raw, &inc); err != nil {
			skipped = append(skipped, fmt.Errorf("incident %q: undecodable", key))
			return nil
		}
		if err := validateStored(key, &inc); err != nil {
			skipped = append(skipped, fmt.Errorf("incident %q: %w", key, err))
			return nil
		}
		loaded = append(loaded, inc)
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: load incidents: %w", ErrPersistence, err)
	}

	// Keep the most recently updated incidents when the store limit is
	// smaller than what was persisted.
	sort.Slice(loaded, func(i, j int) bool {
		if !loaded[i].UpdatedAt.Equal(loaded[j].UpdatedAt) {
			return loaded[i].UpdatedAt.After(loaded[j].UpdatedAt)
		}
		return loaded[i].ID < loaded[j].ID
	})
	next := make(map[string]Incident, len(loaded))
	for _, inc := range loaded {
		if len(next) >= s.max {
			skipped = append(skipped, fmt.Errorf("incident %q: %w", inc.ID, ErrStoreFull))
			continue
		}
		next[inc.ID] = inc
	}

	ids := make([]string, 0, len(s.incidents))
	for id := range s.incidents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if _, ok := next[id]; ok || len(next) >= s.max {
			continue
		}
		inc := s.incidents[id]
		if err := ps.Put(BucketIncidents, id, inc); err != nil {
			return fmt.Errorf("%w: write incident %q: %w", ErrPersistence, id, err)
		}
		next[id] = inc
	}

	s.incidents = next
	s.ps = ps
	if len(skipped) > 0 {
		return fmt.Errorf("%w: skipped %d invalid persisted documents: %w", ErrInvalid, len(skipped), errors.Join(skipped...))
	}
	return nil
}

// validateStored checks a persisted incident with the same rules the API
// enforces; stored incidents must carry a valid ID and status.
func validateStored(key string, inc *Incident) error {
	if inc.ID != key {
		return errors.New("id does not match key")
	}
	if !idPattern.MatchString(inc.ID) {
		return fmt.Errorf("%w: invalid id", ErrInvalid)
	}
	if inc.Status == "" {
		return fmt.Errorf("%w: missing status", ErrInvalid)
	}
	return normalize(inc)
}

// putLocked writes inc when persistence is enabled. s.mu must be held.
func (s *Store) putLocked(inc Incident) error {
	if s.ps == nil {
		return nil
	}
	if err := s.ps.Put(BucketIncidents, inc.ID, inc); err != nil {
		return fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	return nil
}

// deleteLocked removes id when persistence is enabled. s.mu must be held.
func (s *Store) deleteLocked(id string) error {
	if s.ps == nil {
		return nil
	}
	if err := s.ps.Delete(BucketIncidents, id); err != nil {
		return fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	return nil
}

// persistCreateLocked evicts victim (if any) and stores inc durably. If
// storing inc fails after the victim was removed, the victim is restored
// on a best-effort basis so the durable state matches the (unchanged)
// in-memory state. s.mu must be held.
func (s *Store) persistCreateLocked(inc Incident, victim string) error {
	if s.ps == nil {
		return nil
	}
	if victim != "" {
		if err := s.deleteLocked(victim); err != nil {
			return err
		}
	}
	if err := s.putLocked(inc); err != nil {
		if victim != "" {
			if rerr := s.ps.Put(BucketIncidents, victim, s.incidents[victim]); rerr != nil {
				slog.Warn("incident: could not restore evicted incident", "id", victim, "error", rerr)
			}
		}
		return err
	}
	return nil
}
