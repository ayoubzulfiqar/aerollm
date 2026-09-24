package eval

import (
	"context"
	"sort"
	"sync"
)

// DefaultMaxScoreRecords is the default capacity of an InMemoryScoreStore.
const DefaultMaxScoreRecords = 100000

// InMemoryScoreStore keeps scores in memory. It holds at most its capacity
// of records; when full, the oldest inserted records are dropped.
type InMemoryScoreStore struct {
	mu         sync.RWMutex
	records    []ScoreRecord
	maxRecords int
}

// NewInMemoryScoreStore creates a store with DefaultMaxScoreRecords capacity.
func NewInMemoryScoreStore() *InMemoryScoreStore {
	return NewInMemoryScoreStoreWithCap(DefaultMaxScoreRecords)
}

// NewInMemoryScoreStoreWithCap creates a store holding at most maxRecords
// records (values < 1 use DefaultMaxScoreRecords).
func NewInMemoryScoreStoreWithCap(maxRecords int) *InMemoryScoreStore {
	if maxRecords < 1 {
		maxRecords = DefaultMaxScoreRecords
	}
	return &InMemoryScoreStore{records: make([]ScoreRecord, 0), maxRecords: maxRecords}
}

// AppendScore stores a score, evicting the oldest records when full.
func (s *InMemoryScoreStore) AppendScore(ctx context.Context, record ScoreRecord) error {
	_ = ctx
	if s == nil {
		return ErrNoScoreStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := s.maxRecords
	if limit < 1 {
		limit = DefaultMaxScoreRecords
	}
	if len(s.records) >= limit {
		// Drop ~10% at once so eviction is amortized O(1) per append.
		drop := limit / 10
		if drop < 1 {
			drop = 1
		}
		if drop > len(s.records) {
			drop = len(s.records)
		}
		n := copy(s.records, s.records[drop:])
		clear(s.records[n:])
		s.records = s.records[:n]
	}
	s.records = append(s.records, record)
	return nil
}

// ListScores returns filtered scores in chronological (RecordedAt) order.
// When filter.Limit > 0, only the Limit most recent matches are returned.
func (s *InMemoryScoreStore) ListScores(ctx context.Context, filter ScoreFilter) ([]ScoreRecord, error) {
	_ = ctx
	if s == nil {
		return nil, ErrNoScoreStore
	}
	s.mu.RLock()
	out := make([]ScoreRecord, 0, len(s.records))
	for _, r := range s.records {
		if filter.Model != "" && r.Model != filter.Model {
			continue
		}
		if filter.Provider != "" && r.Provider != filter.Provider {
			continue
		}
		if filter.PromptVersion != "" && r.PromptVersion != filter.PromptVersion {
			continue
		}
		out = append(out, r)
	}
	s.mu.RUnlock()
	sort.SliceStable(out, func(i, j int) bool { return out[i].RecordedAt < out[j].RecordedAt })
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[len(out)-filter.Limit:]
	}
	return out, nil
}

// Len returns the number of stored records.
func (s *InMemoryScoreStore) Len() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}
