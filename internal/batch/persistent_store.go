package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// BatchBucket is the persist.Store bucket holding batch records.
const BatchBucket = "batch_batches"

// batchDoc is the persisted form of a batch. Owner is stored explicitly
// because Batch.Owner is excluded from the batch's JSON form.
type batchDoc struct {
	Batch *Batch `json:"batch"`
	Owner string `json:"owner,omitempty"`
}

// PersistentStore is a BatchStore that serves reads from memory and writes
// every change through to a persist.Store, so batch records (status,
// counts, owner, metadata) survive restarts. Combine it with
// BatchProcessorConfig.PersistentWorkDir (so input and result files
// survive too) and call BatchProcessor.Recover at startup to resume or
// fail batches interrupted by the restart. Write failures are returned
// and leave the in-memory view unchanged.
type PersistentStore struct {
	ps  persist.Store
	mem *InMemoryStore

	// Per-batch locks serialise the write-through of one batch so memory
	// and disk agree on its last write; different batches write
	// concurrently (and a batching persist.Store commits them together).
	locksMu sync.Mutex
	locks   map[string]*sync.Mutex
}

func (s *PersistentStore) lock(id string) func() {
	s.locksMu.Lock()
	m := s.locks[id]
	if m == nil {
		m = &sync.Mutex{}
		s.locks[id] = m
	}
	s.locksMu.Unlock()
	m.Lock()
	return m.Unlock
}

// NewPersistentStore loads the batches stored in ps. Undecodable records
// are skipped and reported in the returned error (the store is still
// returned and usable); a read failure returns a nil store.
func NewPersistentStore(ps persist.Store) (*PersistentStore, error) {
	if ps == nil {
		return nil, errors.New("batch: nil persist store")
	}
	s := &PersistentStore{ps: ps, mem: NewInMemoryStore(), locks: map[string]*sync.Mutex{}}
	var bad []string
	err := ps.ForEach(BatchBucket, func(id string, raw json.RawMessage) error {
		var d batchDoc
		if json.Unmarshal(raw, &d) != nil || d.Batch == nil || d.Batch.ID != id {
			bad = append(bad, id)
			return nil
		}
		d.Batch.Owner = d.Owner
		s.mem.batches[id] = d.Batch
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("batch: load batches: %w", err)
	}
	if len(bad) > 0 {
		return s, fmt.Errorf("batch: skipped %d undecodable batch records (%s)", len(bad), strings.Join(bad, ","))
	}
	return s, nil
}

func (s *PersistentStore) put(b *Batch) error {
	if err := s.ps.Put(BatchBucket, b.ID, batchDoc{Batch: b, Owner: b.Owner}); err != nil {
		return fmt.Errorf("batch: persist %s: %w", b.ID, err)
	}
	return nil
}

// SaveBatch implements BatchStore.
func (s *PersistentStore) SaveBatch(ctx context.Context, b *Batch) error {
	if b == nil || b.ID == "" {
		return errors.New("batch: invalid batch")
	}
	defer s.lock(b.ID)()
	if err := s.put(b); err != nil {
		return err
	}
	return s.mem.SaveBatch(ctx, b)
}

// GetBatch implements BatchStore.
func (s *PersistentStore) GetBatch(ctx context.Context, id string) (*Batch, error) {
	return s.mem.GetBatch(ctx, id)
}

// UpdateBatch implements BatchStore.
func (s *PersistentStore) UpdateBatch(ctx context.Context, b *Batch) error {
	if b == nil {
		return ErrBatchNotFound
	}
	defer s.lock(b.ID)()
	if _, ok, _ := s.mem.GetBatchWithOk(ctx, b.ID); !ok {
		return ErrBatchNotFound
	}
	if err := s.put(b); err != nil {
		return err
	}
	return s.mem.UpdateBatch(ctx, b)
}

// DeleteBatch removes a batch record.
func (s *PersistentStore) DeleteBatch(ctx context.Context, id string) error {
	unlock := s.lock(id)
	defer unlock()
	if _, ok, _ := s.mem.GetBatchWithOk(ctx, id); !ok {
		return ErrBatchNotFound
	}
	if err := s.ps.Delete(BatchBucket, id); err != nil {
		return fmt.Errorf("batch: delete %s: %w", id, err)
	}
	if err := s.mem.DeleteBatch(ctx, id); err != nil {
		return err
	}
	s.locksMu.Lock()
	delete(s.locks, id)
	s.locksMu.Unlock()
	return nil
}

// ListBatches implements BatchStore (newest first).
func (s *PersistentStore) ListBatches(ctx context.Context) ([]*Batch, error) {
	return s.mem.ListBatches(ctx)
}

var _ BatchStore = (*PersistentStore)(nil)
