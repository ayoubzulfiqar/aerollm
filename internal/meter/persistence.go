package meter

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// RecordBucket is the persist.Store bucket holding usage record chunks.
const RecordBucket = "meter_usage_records"

// Persistence defaults.
const (
	DefaultPersistMaxAge        = 30 * 24 * time.Hour
	DefaultPersistFlushInterval = time.Second
	persistChunkMax             = 1000
)

// PersistenceOptions configures Recorder.EnablePersistence. Zero values
// select the defaults.
type PersistenceOptions struct {
	// MaxAge drops persisted records older than this (default 30 days).
	MaxAge time.Duration
	// MaxRecords bounds the number of persisted records (default: the
	// recorder capacity). The oldest chunks are deleted first.
	MaxRecords int
	// FlushInterval is how often recorded usage is written in one batch
	// (default 1s). At most one interval of records is lost on a crash;
	// Close flushes everything.
	FlushInterval time.Duration
}

// PersistenceStats reports the state of the persisted record log.
type PersistenceStats struct {
	Enabled   bool      `json:"enabled"`
	Chunks    int       `json:"chunks"`
	Records   int       `json:"records"`
	Pending   int       `json:"pending"`
	Dropped   int64     `json:"dropped"`
	Corrupt   int       `json:"corrupt_chunks"`
	LastFlush time.Time `json:"last_flush,omitempty"`
	LastError string    `json:"last_error,omitempty"`
}

type chunkDoc[T any] struct {
	Items []T `json:"items"`
}

type chunkMeta struct {
	key    string
	count  int
	newest time.Time
}

// chunkLog persists an append-only stream of items as chunk documents (one
// per flush, at most persistChunkMax items each) with age and count
// retention. Items are buffered in memory and written by a background
// flusher, so recording never waits for disk I/O.
type chunkLog[T any] struct {
	ps       persist.Store
	bucket   string
	maxAge   time.Duration
	maxItems int
	timeOf   func(T) time.Time
	now      func() time.Time

	mu      sync.Mutex // guards pending and dropped
	pending []T
	dropped int64

	fmu       sync.Mutex // serialises flushes; guards the fields below
	chunks    []chunkMeta
	seq       uint64
	corrupt   int
	lastErr   error
	lastFlush time.Time

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// openChunkLog loads the stored chunks (oldest first) and returns the
// retained items: newer than maxAge, at most maxItems, oldest first.
func openChunkLog[T any](ps persist.Store, bucket string, maxAge time.Duration, maxItems int, timeOf func(T) time.Time) (*chunkLog[T], []T, error) {
	l := &chunkLog[T]{ps: ps, bucket: bucket, maxAge: maxAge, maxItems: maxItems, timeOf: timeOf, now: time.Now}
	cutoff := l.now().Add(-maxAge)
	var items []T
	err := ps.ForEach(bucket, func(key string, raw json.RawMessage) error {
		var doc chunkDoc[T]
		if json.Unmarshal(raw, &doc) != nil {
			// Undecodable chunks are tracked with a zero timestamp so the
			// next retention pass removes them.
			l.corrupt++
			l.chunks = append(l.chunks, chunkMeta{key: key})
			return nil
		}
		m := chunkMeta{key: key, count: len(doc.Items)}
		for _, it := range doc.Items {
			ts := timeOf(it)
			if ts.After(m.newest) {
				m.newest = ts
			}
			if !ts.Before(cutoff) {
				items = append(items, it)
			}
		}
		l.chunks = append(l.chunks, m)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if len(items) > maxItems {
		items = items[len(items)-maxItems:]
	}
	return l, items, nil
}

func (l *chunkLog[T]) add(it T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.pending) >= l.maxItems {
		// Persistence is failing or far behind: drop the oldest unflushed
		// items (a tenth at a time, so this stays amortised O(1)).
		drop := max(1, l.maxItems/10)
		l.pending = l.pending[drop:]
		l.dropped += int64(drop)
	}
	l.pending = append(l.pending, it)
}

// flush writes pending items and applies retention. On a write failure the
// unwritten items are kept for the next flush and the error is returned.
func (l *chunkLog[T]) flush() error {
	l.fmu.Lock()
	defer l.fmu.Unlock()
	l.mu.Lock()
	pending := l.pending
	l.pending = nil
	l.mu.Unlock()

	var err error
	for len(pending) > 0 {
		n := min(len(pending), persistChunkMax)
		chunk := pending[:n]
		l.seq++
		key := fmt.Sprintf("%020d-%08d", l.now().UnixNano(), l.seq)
		if err = l.ps.Put(l.bucket, key, chunkDoc[T]{Items: chunk}); err != nil {
			break
		}
		m := chunkMeta{key: key, count: n}
		for _, it := range chunk {
			if ts := l.timeOf(it); ts.After(m.newest) {
				m.newest = ts
			}
		}
		l.chunks = append(l.chunks, m)
		pending = pending[n:]
	}
	if len(pending) > 0 {
		l.mu.Lock()
		l.pending = append(pending, l.pending...)
		if over := len(l.pending) - l.maxItems; over > 0 {
			l.pending = l.pending[over:]
			l.dropped += int64(over)
		}
		l.mu.Unlock()
	}
	if rerr := l.retainLocked(); err == nil {
		err = rerr
	}
	l.lastErr = err
	if err == nil {
		l.lastFlush = l.now()
	}
	return err
}

// retainLocked deletes chunks whose entries are all older than maxAge and
// the oldest chunks beyond maxItems.
func (l *chunkLog[T]) retainLocked() error {
	cutoff := l.now().Add(-l.maxAge)
	total := 0
	for _, c := range l.chunks {
		total += c.count
	}
	var errs []error
	kept := l.chunks[:0]
	for i, c := range l.chunks {
		if c.newest.Before(cutoff) || total > l.maxItems {
			if err := l.ps.Delete(l.bucket, c.key); err != nil {
				errs = append(errs, err)
				kept = append(kept, l.chunks[i:]...)
				break
			}
			total -= c.count
			if c.count == 0 && c.newest.IsZero() && l.corrupt > 0 {
				l.corrupt--
			}
			continue
		}
		kept = append(kept, c)
	}
	l.chunks = kept
	return errors.Join(errs...)
}

// clear drops pending items and deletes every stored chunk; a failed
// delete is reported through stats and retried by the next clear.
func (l *chunkLog[T]) clear() {
	l.fmu.Lock()
	defer l.fmu.Unlock()
	l.mu.Lock()
	l.pending = nil
	l.mu.Unlock()
	var errs []error
	kept := l.chunks[:0]
	for _, c := range l.chunks {
		if err := l.ps.Delete(l.bucket, c.key); err != nil {
			errs = append(errs, err)
			kept = append(kept, c)
		}
	}
	l.chunks = kept
	l.corrupt = 0
	l.lastErr = errors.Join(errs...)
}

func (l *chunkLog[T]) start(interval time.Duration) {
	l.stop = make(chan struct{})
	l.done = make(chan struct{})
	go func() {
		defer close(l.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-t.C:
				_ = l.flush()
			}
		}
	}()
}

// close stops the flusher and writes what is pending.
func (l *chunkLog[T]) close() error {
	l.closeOnce.Do(func() {
		if l.stop != nil {
			close(l.stop)
			<-l.done
		}
	})
	return l.flush()
}

func (l *chunkLog[T]) stats() PersistenceStats {
	l.fmu.Lock()
	st := PersistenceStats{Enabled: true, Chunks: len(l.chunks), Corrupt: l.corrupt, LastFlush: l.lastFlush}
	for _, c := range l.chunks {
		st.Records += c.count
	}
	if l.lastErr != nil {
		st.LastError = l.lastErr.Error()
	}
	l.fmu.Unlock()
	l.mu.Lock()
	st.Pending, st.Dropped = len(l.pending), l.dropped
	l.mu.Unlock()
	return st
}

// EnablePersistence makes the recorder durable: records stored in ps
// (bucket RecordBucket) are loaded into the ring buffer (newest
// opts.MaxRecords within opts.MaxAge) and every record added afterwards is
// written in the background every FlushInterval. Records already retained
// in memory are persisted too. Call Close on shutdown to flush. Write
// errors are reported by Flush, Close and PersistenceStats; Record never
// blocks on disk. API keys are persisted as recorded (the gateway records
// key IDs, not raw keys).
func (r *Recorder) EnablePersistence(ps persist.Store, opts PersistenceOptions) error {
	if r == nil {
		return errors.New("meter: nil recorder")
	}
	if ps == nil {
		return errors.New("meter: nil persist store")
	}
	r.mu.Lock()
	if r.max <= 0 {
		r.max = DefaultMaxRecords
	}
	capacity, enabled := r.max, r.plog != nil || r.persistUsed
	r.mu.Unlock()
	if enabled {
		return errors.New("meter: persistence already enabled")
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = DefaultPersistMaxAge
	}
	if opts.MaxRecords <= 0 {
		opts.MaxRecords = capacity
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultPersistFlushInterval
	}
	l, loaded, err := openChunkLog(ps, RecordBucket, opts.MaxAge, opts.MaxRecords, func(u UsageRecord) time.Time { return u.Timestamp })
	if err != nil {
		return fmt.Errorf("meter: load persisted records: %w", err)
	}
	r.mu.Lock()
	if r.plog != nil || r.persistUsed {
		r.mu.Unlock()
		return errors.New("meter: persistence already enabled")
	}
	existing := make([]UsageRecord, 0, r.size)
	for i := 0; i < r.size; i++ {
		existing = append(existing, r.records[(r.head+i)%len(r.records)])
	}
	r.records, r.head, r.size = make([]UsageRecord, 0, min(r.max, 256)), 0, 0
	for _, u := range loaded {
		r.insertLocked(u)
	}
	for _, u := range existing {
		r.insertLocked(u)
		l.add(u)
	}
	r.plog, r.persistUsed = l, true
	r.mu.Unlock()
	l.start(opts.FlushInterval)
	return nil
}

// Flush writes records that are not yet persisted and applies retention.
// It is a no-op without persistence.
func (r *Recorder) Flush() error {
	if l := r.persistLog(); l != nil {
		return l.flush()
	}
	return nil
}

// Close stops background persistence after a final flush. The recorder
// keeps working in memory; records added after Close are not persisted and
// persistence cannot be enabled again.
func (r *Recorder) Close() error {
	l := r.persistLog()
	if l == nil {
		return nil
	}
	err := l.close()
	r.mu.Lock()
	r.plog = nil
	r.mu.Unlock()
	return err
}

// PersistenceStats reports the persisted record log (Enabled is false
// without persistence).
func (r *Recorder) PersistenceStats() PersistenceStats {
	if l := r.persistLog(); l != nil {
		return l.stats()
	}
	return PersistenceStats{}
}

func (r *Recorder) persistLog() *chunkLog[UsageRecord] {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.plog
}
