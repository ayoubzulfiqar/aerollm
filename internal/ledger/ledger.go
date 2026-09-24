// Package ledger implements an append-only, hash-chained audit ledger of
// request/response pairs.
//
// Every record carries ChainHash = ComputeChainHash(PrevHash, request,
// response), where PrevHash is the ChainHash of the preceding record (""
// for the genesis record). Verify walks the chain and detects modified
// payloads, re-ordered, removed or inserted records. Timestamps and Metadata
// are NOT covered by the chain hash (Metadata is used for annotations such as
// signatures added after hashing).
package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"
)

// LedgerRecord represents an append-only audit entry.
type LedgerRecord struct {
	Timestamp       time.Time
	PrevHash        string
	RequestHash     string
	ResponseHash    string
	ChainHash       string
	Metadata        map[string]interface{}
	RequestPayload  string
	ResponsePayload string
}

// LedgerStore persists ledger records.
type LedgerStore interface {
	Append(ctx context.Context, record LedgerRecord) error
	Latest(ctx context.Context) (*LedgerRecord, error)
	All(ctx context.Context) ([]LedgerRecord, error)
}

// ChainedAppender is implemented by stores that can compute the chain link
// atomically. Callers should prefer it over Latest+ComputeChainHash+Append,
// which races under concurrency and forks the chain.
type ChainedAppender interface {
	AppendChainedWithMetadata(ctx context.Context, requestPayload, responsePayload string, metadata map[string]interface{}) (LedgerRecord, error)
}

// DefaultMaxRecords is the default number of records an InMemoryLedgerStore
// retains (see Options.MaxRecords).
const DefaultMaxRecords = 10000

// Options configures an InMemoryLedgerStore.
type Options struct {
	// MaxRecords bounds the number of retained records; older records are
	// pruned (0 = unbounded). Pruning keeps verification meaningful: the
	// ChainHash of the newest pruned record is kept as an anchor and Verify
	// checks that the retained records chain from it. Pruned records
	// themselves can no longer be verified.
	MaxRecords int
	// AutoChain makes Append ignore the caller's PrevHash/ChainHash/
	// RequestHash/ResponseHash and compute them under the store lock, so
	// concurrent appends always form a single linear chain. When false,
	// Append stores records verbatim (use for importing existing chains).
	AutoChain bool
}

// InMemoryLedgerStore implements LedgerStore in memory. It is safe for
// concurrent use.
type InMemoryLedgerStore struct {
	mu         sync.RWMutex
	records    []LedgerRecord
	maxRecords int
	autoChain  bool
	pruned     uint64
	anchor     string // ChainHash of the newest pruned record
}

// NewInMemoryLedgerStore creates a store with AutoChain enabled and
// MaxRecords = DefaultMaxRecords.
func NewInMemoryLedgerStore() *InMemoryLedgerStore {
	return NewInMemoryLedgerStoreWithOptions(Options{MaxRecords: DefaultMaxRecords, AutoChain: true})
}

// NewInMemoryLedgerStoreWithOptions creates a store with explicit options.
func NewInMemoryLedgerStoreWithOptions(opts Options) *InMemoryLedgerStore {
	if opts.MaxRecords < 0 {
		opts.MaxRecords = 0
	}
	return &InMemoryLedgerStore{records: make([]LedgerRecord, 0), maxRecords: opts.MaxRecords, autoChain: opts.AutoChain}
}

// SetMaxRecords changes the retention bound (0 = unbounded), pruning
// immediately if necessary.
func (s *InMemoryLedgerStore) SetMaxRecords(n int) {
	if n < 0 {
		n = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxRecords = n
	s.pruneLocked()
}

func (s *InMemoryLedgerStore) pruneLocked() {
	if s.maxRecords <= 0 || len(s.records) <= s.maxRecords {
		return
	}
	drop := len(s.records) - s.maxRecords
	s.anchor = s.records[drop-1].ChainHash
	s.pruned += uint64(drop)
	clear(s.records[:drop]) // release payload memory
	s.records = s.records[drop:]
}

func cloneRecord(r LedgerRecord) LedgerRecord {
	r.Metadata = maps.Clone(r.Metadata)
	return r
}

func hashHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (s *InMemoryLedgerStore) lastHashLocked() string {
	if n := len(s.records); n > 0 {
		return s.records[n-1].ChainHash
	}
	return s.anchor
}

// chainLocked fills the hash fields of rec so it extends the current chain.
func (s *InMemoryLedgerStore) chainLocked(rec *LedgerRecord) {
	rec.PrevHash = s.lastHashLocked()
	rec.RequestHash = hashHex(rec.RequestPayload)
	rec.ResponseHash = hashHex(rec.ResponsePayload)
	rec.ChainHash = ComputeChainHash(rec.PrevHash, rec.RequestPayload, rec.ResponsePayload)
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}
}

// Append stores a new record. With AutoChain (the default for
// NewInMemoryLedgerStore) the hash fields are computed atomically; otherwise
// the record is stored as given.
func (s *InMemoryLedgerStore) Append(ctx context.Context, record LedgerRecord) error {
	record = cloneRecord(record)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.autoChain {
		s.chainLocked(&record)
	}
	s.records = append(s.records, record)
	s.pruneLocked()
	return nil
}

// SealFunc may annotate a freshly chained record (e.g. add a signature to its
// Metadata) before it is stored. It runs under the store lock and must not
// call back into the store. Returning an error aborts the append.
type SealFunc func(rec *LedgerRecord) error

// AppendChained atomically links a new request/response record to the
// current head of the chain and returns the stored record.
func (s *InMemoryLedgerStore) AppendChained(ctx context.Context, requestPayload, responsePayload string) (LedgerRecord, error) {
	return s.AppendChainedSealed(ctx, requestPayload, responsePayload, nil, nil)
}

// AppendChainedWithMetadata is AppendChained with record metadata.
func (s *InMemoryLedgerStore) AppendChainedWithMetadata(ctx context.Context, requestPayload, responsePayload string, metadata map[string]interface{}) (LedgerRecord, error) {
	return s.AppendChainedSealed(ctx, requestPayload, responsePayload, metadata, nil)
}

// AppendChainedSealed is AppendChainedWithMetadata with an optional seal
// hook applied before the record is stored.
func (s *InMemoryLedgerStore) AppendChainedSealed(ctx context.Context, requestPayload, responsePayload string, metadata map[string]interface{}, seal SealFunc) (LedgerRecord, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return LedgerRecord{}, err
		}
	}
	rec := LedgerRecord{
		Timestamp:       time.Now().UTC(),
		RequestPayload:  requestPayload,
		ResponsePayload: responsePayload,
		Metadata:        maps.Clone(metadata),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chainLocked(&rec)
	if seal != nil {
		if err := seal(&rec); err != nil {
			return LedgerRecord{}, err
		}
	}
	s.records = append(s.records, rec)
	s.pruneLocked()
	return cloneRecord(rec), nil
}

// Latest returns a copy of the last stored record.
func (s *InMemoryLedgerStore) Latest(ctx context.Context) (*LedgerRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.records) == 0 {
		return nil, fmt.Errorf("no ledger entries")
	}
	out := cloneRecord(s.records[len(s.records)-1])
	return &out, nil
}

// All returns copies of all retained records, oldest first.
func (s *InMemoryLedgerStore) All(ctx context.Context) ([]LedgerRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]LedgerRecord, len(s.records))
	for i, r := range s.records {
		out[i] = cloneRecord(r)
	}
	return out, nil
}

// Len returns the number of retained records.
func (s *InMemoryLedgerStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}

// Pruned returns how many records have been pruned and the anchor hash the
// retained chain starts from.
func (s *InMemoryLedgerStore) Pruned() (count uint64, anchor string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pruned, s.anchor
}

// Verify walks the retained chain and returns a *ChainError describing the
// first inconsistency, or nil if the chain is intact.
func (s *InMemoryLedgerStore) Verify(ctx context.Context) error {
	s.mu.RLock()
	anchor := s.anchor
	records := make([]LedgerRecord, len(s.records))
	copy(records, s.records)
	s.mu.RUnlock()
	return VerifyRecords(anchor, records)
}

// ChainError describes a ledger integrity violation.
type ChainError struct {
	Index  int
	Reason string
}

func (e *ChainError) Error() string {
	return fmt.Sprintf("ledger: chain broken at record %d: %s", e.Index, e.Reason)
}

// ErrChainBroken is matched (errors.Is) by every *ChainError.
var ErrChainBroken = errors.New("ledger: chain broken")

// Is lets errors.Is(err, ErrChainBroken) match.
func (e *ChainError) Is(target error) bool { return target == ErrChainBroken }

// VerifyRecords verifies that records form an intact chain starting from
// anchor ("" for a chain that starts at genesis).
func VerifyRecords(anchor string, records []LedgerRecord) error {
	prev := anchor
	for i, r := range records {
		if r.PrevHash != prev {
			return &ChainError{Index: i, Reason: "previous-hash link mismatch"}
		}
		if r.RequestHash != "" && r.RequestHash != hashHex(r.RequestPayload) {
			return &ChainError{Index: i, Reason: "request hash mismatch"}
		}
		if r.ResponseHash != "" && r.ResponseHash != hashHex(r.ResponsePayload) {
			return &ChainError{Index: i, Reason: "response hash mismatch"}
		}
		if r.ChainHash != ComputeChainHash(r.PrevHash, r.RequestPayload, r.ResponsePayload) {
			return &ChainError{Index: i, Reason: "chain hash mismatch"}
		}
		prev = r.ChainHash
	}
	return nil
}

// ComputeChainHash computes the chained hash for a new record. The inputs
// are length-prefixed and domain-separated, so moving bytes between the
// request and response (or the previous hash) changes the result.
func ComputeChainHash(prevHash, requestPayload, responsePayload string) string {
	if prevHash == "" {
		prevHash = "genesis"
	}
	h := sha256.New()
	h.Write([]byte("aerollm-ledger-v1"))
	for _, part := range []string{prevHash, requestPayload, responsePayload} {
		var l [8]byte
		binary.BigEndian.PutUint64(l[:], uint64(len(part)))
		h.Write(l[:])
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RecordRequestResponse appends a record and returns its chain hash. If the
// store implements ChainedAppender the link is computed atomically and
// prevHash is ignored; otherwise prevHash must be the current head's
// ChainHash.
func RecordRequestResponse(store LedgerStore, prevHash, requestPayload, responsePayload string, metadata map[string]interface{}) (string, error) {
	if store == nil {
		return "", errors.New("ledger: nil store")
	}
	ctx := context.Background()
	if ca, ok := store.(ChainedAppender); ok {
		rec, err := ca.AppendChainedWithMetadata(ctx, requestPayload, responsePayload, metadata)
		if err != nil {
			return "", err
		}
		return rec.ChainHash, nil
	}
	chainHash := ComputeChainHash(prevHash, requestPayload, responsePayload)
	record := LedgerRecord{
		Timestamp:       time.Now().UTC(),
		PrevHash:        prevHash,
		RequestHash:     hashHex(requestPayload),
		ResponseHash:    hashHex(responsePayload),
		ChainHash:       chainHash,
		Metadata:        metadata,
		RequestPayload:  requestPayload,
		ResponsePayload: responsePayload,
	}
	if err := store.Append(ctx, record); err != nil {
		return "", err
	}
	return chainHash, nil
}
