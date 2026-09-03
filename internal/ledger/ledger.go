package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// LedgerRecord represents an append-only audit entry.
type LedgerRecord struct {
	Timestamp    time.Time
	PrevHash     string
	RequestHash  string
	ResponseHash string
	ChainHash    string
	Metadata     map[string]interface{}
	RequestPayload  string
	ResponsePayload string
}

// LedgerStore persists ledger records.
type LedgerStore interface {
	Append(ctx context.Context, record LedgerRecord) error
	Latest(ctx context.Context) (*LedgerRecord, error)
	All(ctx context.Context) ([]LedgerRecord, error)
}

// InMemoryLedgerStore implements LedgerStore in memory for development/testing.
type InMemoryLedgerStore struct {
	mu      sync.RWMutex
	records []LedgerRecord
	last    *LedgerRecord
}

// NewInMemoryLedgerStore creates a new in-memory ledger store.
func NewInMemoryLedgerStore() *InMemoryLedgerStore {
	return &InMemoryLedgerStore{records: make([]LedgerRecord, 0)}
}

// Append stores a new record.
func (s *InMemoryLedgerStore) Append(ctx context.Context, record LedgerRecord) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record)
	s.last = &record
	return nil
}

// Latest returns the last stored record.
func (s *InMemoryLedgerStore) Latest(ctx context.Context) (*LedgerRecord, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.last == nil {
		return nil, fmt.Errorf("no ledger entries")
	}
	out := *s.last
	return &out, nil
}

// All returns all stored records.
func (s *InMemoryLedgerStore) All(ctx context.Context) ([]LedgerRecord, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]LedgerRecord, len(s.records))
	copy(out, s.records)
	return out, nil
}

// ComputeChainHash computes the chained hash for a new record.
func ComputeChainHash(prevHash, requestPayload, responsePayload string) string {
	if prevHash == "" {
		prevHash = "genesis"
	}
	sum := sha256.Sum256([]byte(prevHash + requestPayload + responsePayload))
	return hex.EncodeToString(sum[:])
}

// RecordRequestResponse creates a new LedgerRecord and returns its chain hash.
func RecordRequestResponse(store LedgerStore, prevHash, requestPayload, responsePayload string, metadata map[string]interface{}) (string, error) {
	chainHash := ComputeChainHash(prevHash, requestPayload, responsePayload)
	reqHashBytes := sha256.Sum256([]byte(requestPayload))
	respHashBytes := sha256.Sum256([]byte(responsePayload))
	record := LedgerRecord{
		Timestamp:    time.Now().UTC(),
		PrevHash:     prevHash,
		RequestHash:  hex.EncodeToString(reqHashBytes[:]),
		ResponseHash: hex.EncodeToString(respHashBytes[:]),
		ChainHash:    chainHash,
		Metadata:     metadata,
		RequestPayload:  requestPayload,
		ResponsePayload: responsePayload,
	}
	if err := store.Append(nil, record); err != nil {
		return "", err
	}
	return chainHash, nil
}
