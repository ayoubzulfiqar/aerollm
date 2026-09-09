package batch

import (
	"context"
	"errors"
	"sync"
)

// ErrBatchNotFound is returned when a batch ID does not exist in the store.
var ErrBatchNotFound = errors.New("batch not found")

// InMemoryStore is a thread-safe, in-memory implementation of BatchStore.
// For production, swap with a Redis-backed or database-backed store.
type InMemoryStore struct {
	mu    sync.RWMutex
	batches map[string]*Batch
}

// NewInMemoryStore creates a new InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		batches: make(map[string]*Batch),
	}
}

// SaveBatch persists a new batch.
func (s *InMemoryStore) SaveBatch(ctx context.Context, b *Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches[b.ID] = b
	return nil
}

// GetBatch retrieves a batch by ID.
func (s *InMemoryStore) GetBatch(ctx context.Context, id string) (*Batch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.batches[id]
	if !ok {
		return nil, ErrBatchNotFound
	}
	return b, nil
}

// GetBatchWithOk retrieves a batch by ID, returning a found flag for internal use.
func (s *InMemoryStore) GetBatchWithOk(ctx context.Context, id string) (*Batch, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.batches[id]
	if !ok {
		return nil, false, nil
	}
	return b, true, nil
}

// UpdateBatch persists updates to an existing batch.
func (s *InMemoryStore) UpdateBatch(ctx context.Context, b *Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.batches[b.ID]; !ok {
		return ErrBatchNotFound
	}
	s.batches[b.ID] = b
	return nil
}

// ListBatches returns all batches sorted by creation time.
func (s *InMemoryStore) ListBatches(ctx context.Context) ([]*Batch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Batch, 0, len(s.batches))
	for _, b := range s.batches {
		result = append(result, b)
	}
	// Sort by CreatedAt descending (newest first).
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].CreatedAt.After(result[i].CreatedAt) {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result, nil
}

// Ensure InMemoryStore satisfies BatchStore.
var _ BatchStore = (*InMemoryStore)(nil)
