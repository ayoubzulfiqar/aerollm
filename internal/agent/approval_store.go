package agent

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used by LocalApprovalStore in its persist.Store.
const (
	approvalBucket      = "agent_approvals"
	approvalClaimBucket = "agent_approval_claims"
)

// DefaultMaxLocalApprovals bounds the records held by a LocalApprovalStore.
const DefaultMaxLocalApprovals = 10000

// approvalSweepInterval is the minimum spacing of full expiry sweeps.
const approvalSweepInterval = time.Minute

// ErrApprovalStoreFull is returned by LocalApprovalStore.Save when the store
// holds its maximum number of live pending approvals.
var ErrApprovalStoreFull = errors.New("approval store is full")

// storedApproval is the persisted form of an approval record. Expires is the
// storage expiry (the equivalent of the Redis key TTL), reset on every save.
type storedApproval struct {
	Record  ApprovalRecord `json:"record"`
	Expires time.Time      `json:"expires"`
}

// storedClaim is the persisted form of an approval claim.
type storedClaim struct {
	Expires time.Time `json:"expires"`
}

type approvalEntry struct {
	id      string
	expires time.Time
	pending bool
}

// LocalApprovalStore is an ApprovalStore (and ApprovalClaimer) for
// single-instance deployments that do not run Redis. It mirrors
// RedisApprovalStore semantics:
//
//   - records are stored JSON-encoded (Error does not round-trip; it is
//     restored from ErrorMessage on Get) and expire ttl after their last
//     Save/Update, after which Get returns ErrApprovalNotFound;
//   - Claim succeeds exactly once per approval (until the claim's ttl), so an
//     approval is resolved at most once, including across restarts when the
//     store is backed by a durable persist.Store.
//
// Build it with NewMemoryApprovalStore (process memory) or
// NewPersistentApprovalStore (e.g. a persist.OpenBolt file). It must not be
// shared by several gateway instances: use RedisApprovalStore for that.
type LocalApprovalStore struct {
	ps         persist.Store
	ttl        time.Duration
	maxRecords int
	now        func() time.Time

	mu        sync.Mutex
	entries   map[string]*list.Element // id -> element in order (value *approvalEntry)
	order     *list.List               // save order, oldest first
	claims    map[string]time.Time
	lastSweep time.Time
}

// NewMemoryApprovalStore returns an in-process approval store. A
// non-positive ttl defaults to DefaultApprovalTTL.
func NewMemoryApprovalStore(ttl time.Duration) *LocalApprovalStore {
	return newLocalApprovalStore(persist.NewMemory(), ttl)
}

// NewPersistentApprovalStore returns an approval store that writes through
// to ps (buckets "agent_approvals" and "agent_approval_claims"), so pending
// approvals survive a restart. Existing documents are loaded and expired
// ones purged. A non-positive ttl defaults to DefaultApprovalTTL. ps stays
// owned by the caller (the store never closes it).
func NewPersistentApprovalStore(ps persist.Store, ttl time.Duration) (*LocalApprovalStore, error) {
	if ps == nil {
		return nil, errors.New("agent: nil persist store")
	}
	s := newLocalApprovalStore(ps, ttl)
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func newLocalApprovalStore(ps persist.Store, ttl time.Duration) *LocalApprovalStore {
	if ttl <= 0 {
		ttl = DefaultApprovalTTL
	}
	return &LocalApprovalStore{
		ps:         ps,
		ttl:        ttl,
		maxRecords: DefaultMaxLocalApprovals,
		now:        func() time.Time { return time.Now().UTC() },
		entries:    make(map[string]*list.Element),
		order:      list.New(),
		claims:     make(map[string]time.Time),
	}
}

// SetMaxRecords changes the record cap (non-positive restores
// DefaultMaxLocalApprovals). Call it before the store is used.
func (s *LocalApprovalStore) SetMaxRecords(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n <= 0 {
		n = DefaultMaxLocalApprovals
	}
	s.maxRecords = n
}

// load rebuilds the in-memory index from the persist store, deleting
// expired documents.
func (s *LocalApprovalStore) load() error {
	now := s.now()
	type loaded struct {
		entry   approvalEntry
		savedAt time.Time
	}
	var records []loaded
	var expired []string
	err := s.ps.ForEach(approvalBucket, func(key string, raw json.RawMessage) error {
		var doc storedApproval
		if err := json.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("agent: decode approval %q: %w", key, err)
		}
		if !doc.Expires.After(now) {
			expired = append(expired, key)
			return nil
		}
		records = append(records, loaded{
			entry:   approvalEntry{id: key, expires: doc.Expires, pending: doc.Record.Status == ApprovalStatusPending},
			savedAt: doc.Expires.Add(-s.ttl),
		})
		return nil
	})
	if err != nil {
		return fmt.Errorf("agent: load approvals: %w", err)
	}
	var expiredClaims []string
	claims := make(map[string]time.Time)
	err = s.ps.ForEach(approvalClaimBucket, func(key string, raw json.RawMessage) error {
		var c storedClaim
		if err := json.Unmarshal(raw, &c); err != nil {
			return fmt.Errorf("agent: decode approval claim %q: %w", key, err)
		}
		if !c.Expires.After(now) {
			expiredClaims = append(expiredClaims, key)
			return nil
		}
		claims[key] = c.Expires
		return nil
	})
	if err != nil {
		return fmt.Errorf("agent: load approval claims: %w", err)
	}
	for _, id := range expired {
		if err := s.ps.Delete(approvalBucket, id); err != nil {
			return fmt.Errorf("agent: purge expired approval: %w", err)
		}
	}
	for _, id := range expiredClaims {
		if err := s.ps.Delete(approvalClaimBucket, id); err != nil {
			return fmt.Errorf("agent: purge expired approval claim: %w", err)
		}
	}
	// Rebuild the save order (oldest first) from the storage expiries.
	sort.Slice(records, func(i, j int) bool {
		if !records[i].savedAt.Equal(records[j].savedAt) {
			return records[i].savedAt.Before(records[j].savedAt)
		}
		return records[i].entry.id < records[j].entry.id
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range records {
		e := r.entry
		s.entries[e.id] = s.order.PushBack(&e)
	}
	s.claims = claims
	s.lastSweep = now
	return nil
}

// Save stores an approval record, replacing any record with the same ID and
// resetting its expiry.
func (s *LocalApprovalStore) Save(ctx context.Context, record ApprovalRecord) error {
	if s == nil || s.ps == nil {
		return ErrNoApprovalStore
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validApprovalID(record.ApprovalID) {
		return fmt.Errorf("invalid approval id")
	}
	if record.Error != nil && record.ErrorMessage == "" {
		record.ErrorMessage = record.Error.Error()
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeSweepLocked(now)
	if _, exists := s.entries[record.ApprovalID]; !exists && len(s.entries) >= s.maxRecords {
		if err := s.makeRoomLocked(now); err != nil {
			return err
		}
	}
	expires := now.Add(s.ttl)
	if err := s.ps.Put(approvalBucket, record.ApprovalID, storedApproval{Record: record, Expires: expires}); err != nil {
		return fmt.Errorf("agent: persist approval: %w", err)
	}
	entry := &approvalEntry{id: record.ApprovalID, expires: expires, pending: record.Status == ApprovalStatusPending}
	if el, ok := s.entries[record.ApprovalID]; ok {
		s.order.Remove(el)
	}
	s.entries[record.ApprovalID] = s.order.PushBack(entry)
	return nil
}

// Update updates an approval record (same as Save).
func (s *LocalApprovalStore) Update(ctx context.Context, record ApprovalRecord) error {
	return s.Save(ctx, record)
}

// Get retrieves an approval record. It returns ErrApprovalNotFound when the
// record does not exist or has expired.
func (s *LocalApprovalStore) Get(ctx context.Context, approvalID string) (*ApprovalRecord, error) {
	if s == nil || s.ps == nil {
		return nil, ErrNoApprovalStore
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validApprovalID(approvalID) {
		return nil, ErrApprovalNotFound
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.entries[approvalID]
	if !ok {
		return nil, ErrApprovalNotFound
	}
	if !el.Value.(*approvalEntry).expires.After(now) {
		s.dropLocked(approvalID)
		return nil, ErrApprovalNotFound
	}
	var doc storedApproval
	found, err := s.ps.Get(approvalBucket, approvalID, &doc)
	if err != nil {
		return nil, fmt.Errorf("agent: read approval: %w", err)
	}
	if !found {
		s.order.Remove(el)
		delete(s.entries, approvalID)
		return nil, ErrApprovalNotFound
	}
	record := doc.Record
	if record.ErrorMessage != "" {
		record.Error = errors.New(record.ErrorMessage)
	}
	return &record, nil
}

// Claim atomically marks the approval as being resolved. It returns true for
// the first caller only (until the claim expires ttl later) and
// ErrApprovalNotFound for unknown or expired approvals.
func (s *LocalApprovalStore) Claim(ctx context.Context, approvalID string) (bool, error) {
	if s == nil || s.ps == nil {
		return false, ErrNoApprovalStore
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.entries[approvalID]
	if !ok || !el.Value.(*approvalEntry).expires.After(now) {
		return false, ErrApprovalNotFound
	}
	if exp, claimed := s.claims[approvalID]; claimed && exp.After(now) {
		return false, nil
	}
	expires := now.Add(s.ttl)
	if err := s.ps.Put(approvalClaimBucket, approvalID, storedClaim{Expires: expires}); err != nil {
		return false, fmt.Errorf("agent: persist approval claim: %w", err)
	}
	s.claims[approvalID] = expires
	return true, nil
}

// Len returns the number of live (unexpired) approval records.
func (s *LocalApprovalStore) Len() int {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, el := range s.entries {
		if el.Value.(*approvalEntry).expires.After(now) {
			n++
		}
	}
	return n
}

// maybeSweepLocked removes expired records and claims at most once per
// approvalSweepInterval. Persistence errors are ignored here: the documents
// are expired and are retried on the next sweep or purged on load.
func (s *LocalApprovalStore) maybeSweepLocked(now time.Time) {
	if now.Sub(s.lastSweep) < approvalSweepInterval {
		return
	}
	s.sweepLocked(now)
}

func (s *LocalApprovalStore) sweepLocked(now time.Time) {
	s.lastSweep = now
	for id, el := range s.entries {
		if !el.Value.(*approvalEntry).expires.After(now) {
			s.dropLocked(id)
		}
	}
	for id, exp := range s.claims {
		if !exp.After(now) {
			if s.ps.Delete(approvalClaimBucket, id) == nil {
				delete(s.claims, id)
			}
		}
	}
}

// makeRoomLocked frees one slot: expired records go first, then the oldest
// resolved record. Pending approvals are never evicted.
func (s *LocalApprovalStore) makeRoomLocked(now time.Time) error {
	s.sweepLocked(now)
	if len(s.entries) < s.maxRecords {
		return nil
	}
	for el := s.order.Front(); el != nil; el = el.Next() {
		e := el.Value.(*approvalEntry)
		if !e.pending {
			if err := s.ps.Delete(approvalBucket, e.id); err != nil {
				return fmt.Errorf("agent: evict approval: %w", err)
			}
			s.order.Remove(el)
			delete(s.entries, e.id)
			return nil
		}
	}
	return ErrApprovalStoreFull
}

// dropLocked deletes an approval (and its claim) from the store and index.
// The index entry is kept when the persisted delete fails, so it is retried.
func (s *LocalApprovalStore) dropLocked(id string) {
	if err := s.ps.Delete(approvalBucket, id); err != nil {
		return
	}
	if el, ok := s.entries[id]; ok {
		s.order.Remove(el)
		delete(s.entries, id)
	}
	if _, ok := s.claims[id]; ok && s.ps.Delete(approvalClaimBucket, id) == nil {
		delete(s.claims, id)
	}
}

var (
	_ ApprovalStore   = (*LocalApprovalStore)(nil)
	_ ApprovalClaimer = (*LocalApprovalStore)(nil)
)
