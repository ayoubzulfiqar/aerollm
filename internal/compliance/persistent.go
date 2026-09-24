package compliance

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used by the persistent compliance stores.
const (
	BucketHTTPRules = "compliance_http_rules"
	BucketAudit     = "compliance_audit"
)

// ErrPersistence is matched (errors.Is) by errors caused by the persistence
// backend rather than by invalid input.
var ErrPersistence = errors.New("compliance: persistence failure")

// NewPersistentHTTPPolicyStore returns a policy store that loads its rules
// from ps (bucket BucketHTTPRules) and writes every change through to it.
func NewPersistentHTTPPolicyStore(ps persist.Store) (*HTTPPolicyStore, error) {
	s := NewHTTPPolicyStore()
	if err := s.EnablePersistence(ps); err != nil {
		return nil, err
	}
	return s, nil
}

// EnablePersistence loads the rules stored in ps and writes every later
// change through to it. Rules already in memory are written to ps and take
// precedence over stored rules with the same ID. Stored rules that no longer
// validate make it fail, so a policy is never silently dropped.
func (s *HTTPPolicyStore) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("compliance: nil persist store")
	}
	stored, err := persist.LoadAll[HTTPPolicyRule](ps, BucketHTTPRules)
	if err != nil {
		return fmt.Errorf("compliance: load policy rules: %w", err)
	}
	for id, rule := range stored {
		if err := normalizeRule(&rule); err != nil || rule.ID != id {
			return fmt.Errorf("compliance: stored policy rule %q is invalid: %v", id, err)
		}
		stored[id] = rule
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disk != nil {
		return errors.New("compliance: persistence already enabled")
	}
	for id, rule := range s.rules {
		if err := ps.Put(BucketHTTPRules, id, rule); err != nil {
			return fmt.Errorf("%w: %v", ErrPersistence, err)
		}
	}
	for id, rule := range stored {
		if _, ok := s.rules[id]; !ok {
			s.rules[id] = rule
		}
	}
	s.disk = ps
	return nil
}

// storedAuditEvent is the persisted form of an AuditEvent (input already
// sanitized).
type storedAuditEvent struct {
	Seq       uint64                 `json:"seq"`
	Timestamp time.Time              `json:"timestamp"`
	Policy    string                 `json:"policy"`
	Decision  string                 `json:"decision"`
	Input     map[string]interface{} `json:"input,omitempty"`
	Reason    string                 `json:"reason,omitempty"`
}

func auditKey(seq uint64) string { return fmt.Sprintf("%020d", seq) }

// auditDisk mirrors a MemoryAuditLogger to a persist.Store. Used under the
// logger's lock.
type auditDisk struct {
	ps       persist.Store
	seqs     []uint64 // sequence numbers of the retained events, oldest first
	pending  []uint64 // evicted events whose deletion failed (retried)
	nextSeq  uint64
	failures uint64
	lastErr  error
	onError  func(error)
}

func (d *auditDisk) put(ev *AuditEvent) error {
	doc := storedAuditEvent{Seq: d.nextSeq, Timestamp: ev.Timestamp, Policy: ev.Policy, Decision: ev.Decision, Input: ev.Input, Reason: ev.Reason}
	if _, err := json.Marshal(doc.Input); err != nil {
		// Keep the decision on record even if the input is not encodable.
		doc.Input = map[string]interface{}{"_unserializable_input": true}
	}
	if err := d.ps.Put(BucketAudit, auditKey(doc.Seq), doc); err != nil {
		return fmt.Errorf("%w: store audit event: %v", ErrPersistence, err)
	}
	d.seqs = append(d.seqs, d.nextSeq)
	d.nextSeq++
	return nil
}

// evict drops the n oldest retained events from ps (and retries earlier
// failed deletions). Events whose deletion fails are retried later.
func (d *auditDisk) evict(n int) error {
	n = min(n, len(d.seqs))
	todo := append(d.pending, d.seqs[:n]...)
	d.seqs = d.seqs[n:]
	d.pending = nil
	var firstErr error
	for _, seq := range todo {
		if err := d.ps.Delete(BucketAudit, auditKey(seq)); err != nil {
			d.pending = append(d.pending, seq)
			if firstErr == nil {
				firstErr = fmt.Errorf("%w: delete evicted audit event: %v", ErrPersistence, err)
			}
		}
	}
	return firstErr
}

// NewPersistentAuditLogger returns an audit logger holding up to capacity
// events (minimum 1) that loads the most recent persisted events from ps
// (bucket BucketAudit) and writes every event through to it. Older
// persisted events beyond capacity are deleted.
func NewPersistentAuditLogger(ps persist.Store, capacity int) (*MemoryAuditLogger, error) {
	if ps == nil {
		return nil, errors.New("compliance: nil persist store")
	}
	m := NewMemoryAuditLoggerWithCapacity(capacity)
	d := &auditDisk{ps: ps}
	var events []*AuditEvent
	err := ps.ForEach(BucketAudit, func(key string, raw json.RawMessage) error {
		seq, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return fmt.Errorf("compliance: invalid audit key %q", key)
		}
		var doc storedAuditEvent
		if err := json.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("compliance: decode audit event %d: %w", seq, err)
		}
		events = append(events, &AuditEvent{Timestamp: doc.Timestamp, Policy: doc.Policy, Decision: doc.Decision, Input: doc.Input, Reason: doc.Reason})
		d.seqs = append(d.seqs, seq)
		d.nextSeq = seq + 1
		return nil
	})
	if err != nil {
		return nil, err
	}
	if extra := len(events) - m.capacity; extra > 0 {
		if err := d.evict(extra); err != nil {
			return nil, err
		}
		events = events[extra:]
	}
	m.events = append(m.events, events...)
	m.disk = d
	return m, nil
}

// SetErrorHandler registers fn to be called (outside the logger's lock)
// with every persistence error of Log/Clear. fn must not block for long.
func (m *MemoryAuditLogger) SetErrorHandler(fn func(error)) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.disk != nil {
		m.disk.onError = fn
	}
}

// PersistErrors returns how many persistence operations failed and the
// most recent error (0, nil for in-memory loggers).
func (m *MemoryAuditLogger) PersistErrors() (uint64, error) {
	if m == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.disk == nil {
		return 0, nil
	}
	return m.disk.failures, m.disk.lastErr
}

// Persistent reports whether the logger writes through to a persist.Store.
func (m *MemoryAuditLogger) Persistent() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.disk != nil
}
