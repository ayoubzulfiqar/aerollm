package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used by a persistent ledger store.
const (
	BucketRecords = "ledger_records"
	BucketMeta    = "ledger_meta"
	metaStateKey  = "state"
)

// maxDeletesPerAppend bounds the persisted records deleted per append, so
// pruning is spread over appends instead of causing latency spikes. It is
// larger than one so deletion outpaces pruning (one record per append at
// steady state).
const maxDeletesPerAppend = 2

// storedRecord is the persisted form of a LedgerRecord.
type storedRecord struct {
	Seq             uint64                 `json:"seq"`
	Version         int                    `json:"version,omitempty"`
	Timestamp       time.Time              `json:"timestamp"`
	PrevHash        string                 `json:"prev_hash"`
	RequestHash     string                 `json:"request_hash,omitempty"`
	ResponseHash    string                 `json:"response_hash,omitempty"`
	ChainHash       string                 `json:"chain_hash"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
	RequestPayload  string                 `json:"request_payload"`
	ResponsePayload string                 `json:"response_payload"`
}

func (r storedRecord) record() LedgerRecord {
	return LedgerRecord{
		Timestamp: r.Timestamp, PrevHash: r.PrevHash, RequestHash: r.RequestHash,
		ResponseHash: r.ResponseHash, ChainHash: r.ChainHash, Metadata: r.Metadata,
		RequestPayload: r.RequestPayload, ResponsePayload: r.ResponsePayload, Version: r.Version,
	}
}

// ledgerState is the persisted pruning state: records with a sequence
// number below FirstSeq were pruned; Anchor is the ChainHash of the newest
// pruned record and Pruned counts pruned records.
type ledgerState struct {
	FirstSeq uint64 `json:"first_seq"`
	Anchor   string `json:"anchor"`
	Pruned   uint64 `json:"pruned"`
}

// seqKey renders a sequence number so that lexical order is numeric order.
func seqKey(seq uint64) string { return fmt.Sprintf("%020d", seq) }

// ledgerDisk mirrors an InMemoryLedgerStore to a persist.Store. It is only
// used under the owning store's lock.
type ledgerDisk struct {
	ps        persist.Store
	seqs      []uint64 // sequence number of each retained record
	nextSeq   uint64
	metaFirst uint64 // FirstSeq persisted in the meta document
	diskFirst uint64 // lowest sequence number that may still be on disk
	batch     uint64 // how far memory may run ahead of metaFirst
}

func (d *ledgerDisk) put(rec LedgerRecord) error {
	seq := d.nextSeq
	doc := storedRecord{
		Seq: seq, Version: rec.Version, Timestamp: rec.Timestamp, PrevHash: rec.PrevHash,
		RequestHash: rec.RequestHash, ResponseHash: rec.ResponseHash, ChainHash: rec.ChainHash,
		Metadata: rec.Metadata, RequestPayload: rec.RequestPayload, ResponsePayload: rec.ResponsePayload,
	}
	if err := d.ps.Put(BucketRecords, seqKey(seq), doc); err != nil {
		return fmt.Errorf("ledger: persist record %d: %w", seq, err)
	}
	d.nextSeq++
	d.seqs = append(d.seqs, seq)
	return nil
}

// compact advances the persisted pruning state and deletes a few pruned
// records. anchor/pruned describe the in-memory pruning state.
func (d *ledgerDisk) compact(anchor string, pruned uint64) error {
	first := d.nextSeq // everything is pruned when nothing is retained
	if len(d.seqs) > 0 {
		first = d.seqs[0]
	}
	if first > d.metaFirst && first-d.metaFirst >= d.batch {
		st := ledgerState{FirstSeq: first, Anchor: anchor, Pruned: pruned}
		if err := d.ps.Put(BucketMeta, metaStateKey, st); err != nil {
			return fmt.Errorf("persist pruning state: %w", err)
		}
		d.metaFirst = first
	}
	for i := 0; i < maxDeletesPerAppend && d.diskFirst < d.metaFirst; i++ {
		if err := d.ps.Delete(BucketRecords, seqKey(d.diskFirst)); err != nil {
			return fmt.Errorf("delete pruned record %d: %w", d.diskFirst, err)
		}
		d.diskFirst++
	}
	return nil
}

// NewPersistentLedgerStore returns an auto-chaining ledger whose records
// are written to ps (buckets BucketRecords and BucketMeta) before they are
// acknowledged, so the chain survives restarts. The store is append-only:
// existing records are never rewritten.
//
// maxRecords bounds the records retained in memory and on disk (0 =
// unbounded; DefaultMaxRecords is a sensible bound). Pruning keeps the same
// anchor semantics as the in-memory store: the ChainHash of the newest
// pruned record is persisted and Verify checks that the retained records
// chain from it. Pruned records are deleted from ps gradually, a few per
// append.
//
// Existing records are loaded as stored (Verify reports any tampering);
// the constructor fails if a stored document cannot be decoded. The
// persist.Store must not be shared by several gateway processes.
func NewPersistentLedgerStore(ps persist.Store, maxRecords int) (*InMemoryLedgerStore, error) {
	if ps == nil {
		return nil, errors.New("ledger: nil persist store")
	}
	if maxRecords < 0 {
		maxRecords = 0
	}
	var st ledgerState
	if _, err := ps.Get(BucketMeta, metaStateKey, &st); err != nil {
		return nil, fmt.Errorf("ledger: load pruning state: %w", err)
	}
	s := NewInMemoryLedgerStoreWithOptions(Options{MaxRecords: maxRecords, AutoChain: true})
	d := &ledgerDisk{ps: ps, nextSeq: st.FirstSeq, metaFirst: st.FirstSeq, diskFirst: st.FirstSeq}
	d.batch = uint64(max(1, min(maxRecords/16, 256)))
	s.anchor, s.pruned = st.Anchor, st.Pruned

	lowest, seenAny := uint64(0), false
	err := ps.ForEach(BucketRecords, func(key string, raw json.RawMessage) error {
		seq, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return fmt.Errorf("ledger: invalid record key %q", key)
		}
		if !seenAny {
			lowest, seenAny = seq, true
		}
		if seq < st.FirstSeq {
			return nil // pruned; deleted by compaction
		}
		var doc storedRecord
		if err := json.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("ledger: decode record %d: %w", seq, err)
		}
		if doc.Seq != seq {
			return fmt.Errorf("ledger: record %d has mismatched sequence %d", seq, doc.Seq)
		}
		s.records = append(s.records, doc.record())
		d.seqs = append(d.seqs, seq)
		d.nextSeq = seq + 1
		return nil
	})
	if err != nil {
		return nil, err
	}
	if seenAny && lowest < d.diskFirst {
		d.diskFirst = lowest
	}
	s.disk = d
	s.pruneLocked()
	return s, nil
}

// Persistent reports whether the store writes through to a persist.Store.
func (s *InMemoryLedgerStore) Persistent() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.disk != nil
}
