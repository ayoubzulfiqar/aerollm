package eval

import (
	"context"
	"sort"
	"sync"
)

// InMemoryScoreStore keeps scores in memory.
type InMemoryScoreStore struct {
	mu      sync.RWMutex
	records []ScoreRecord
}

// NewInMemoryScoreStore creates a store.
func NewInMemoryScoreStore() *InMemoryScoreStore {
	return &InMemoryScoreStore{records: make([]ScoreRecord, 0)}
}

// AppendScore stores a score.
func (s *InMemoryScoreStore) AppendScore(ctx context.Context, record ScoreRecord) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record)
	return nil
}

// ListScores returns filtered scores.
func (s *InMemoryScoreStore) ListScores(ctx context.Context, filter ScoreFilter) ([]ScoreRecord, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	sort.SliceStable(out, func(i, j int) bool { return out[i].RecordedAt < out[j].RecordedAt })
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}
