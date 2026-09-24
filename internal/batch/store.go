package batch

import (
	"context"
	"errors"
	"sort"
	"sync"
)

// ErrBatchNotFound is returned when a batch ID does not exist in the store.
var ErrBatchNotFound = errors.New("batch not found")

// InMemoryStore is a thread-safe, in-memory implementation of BatchStore.
// It stores and returns deep copies, so callers can never race with the
// processor by mutating a shared *Batch. For production, swap with a
// Redis-backed or database-backed store.
type InMemoryStore struct {
	mu      sync.RWMutex
	batches map[string]*Batch
}

// NewInMemoryStore creates a new InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		batches: make(map[string]*Batch),
	}
}

// SaveBatch persists a new batch (a copy of b).
func (s *InMemoryStore) SaveBatch(ctx context.Context, b *Batch) error {
	if b == nil || b.ID == "" {
		return errors.New("batch: invalid batch")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches[b.ID] = b.Clone()
	return nil
}

// GetBatch retrieves a copy of a batch by ID.
func (s *InMemoryStore) GetBatch(ctx context.Context, id string) (*Batch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.batches[id]
	if !ok {
		return nil, ErrBatchNotFound
	}
	return b.Clone(), nil
}

// GetBatchWithOk retrieves a batch by ID, returning a found flag for internal use.
func (s *InMemoryStore) GetBatchWithOk(ctx context.Context, id string) (*Batch, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.batches[id]
	if !ok {
		return nil, false, nil
	}
	return b.Clone(), true, nil
}

// UpdateBatch persists updates to an existing batch.
func (s *InMemoryStore) UpdateBatch(ctx context.Context, b *Batch) error {
	if b == nil {
		return ErrBatchNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.batches[b.ID]; !ok {
		return ErrBatchNotFound
	}
	s.batches[b.ID] = b.Clone()
	return nil
}

// DeleteBatch removes a batch record.
func (s *InMemoryStore) DeleteBatch(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.batches[id]; !ok {
		return ErrBatchNotFound
	}
	delete(s.batches, id)
	return nil
}

// ListBatches returns copies of all batches sorted by creation time,
// newest first (ties broken by ID for a stable order).
func (s *InMemoryStore) ListBatches(ctx context.Context) ([]*Batch, error) {
	s.mu.RLock()
	result := make([]*Batch, 0, len(s.batches))
	for _, b := range s.batches {
		result = append(result, b.Clone())
	}
	s.mu.RUnlock()
	sortNewestFirst(result)
	return result, nil
}

func sortNewestFirst(bs []*Batch) {
	sort.Slice(bs, func(i, j int) bool {
		if !bs[i].CreatedAt.Equal(bs[j].CreatedAt) {
			return bs[i].CreatedAt.After(bs[j].CreatedAt)
		}
		return bs[i].ID > bs[j].ID
	})
}

// Ensure InMemoryStore satisfies BatchStore.
var _ BatchStore = (*InMemoryStore)(nil)
