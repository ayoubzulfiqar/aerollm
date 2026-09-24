package analytics

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// CostEntryBucket is the persist.Store bucket holding cost entry chunks.
const CostEntryBucket = "analytics_cost_entries"

// Persistence defaults.
const (
	DefaultPersistMaxAge        = 30 * 24 * time.Hour
	DefaultPersistFlushInterval = time.Second
	persistChunkMax             = 1000
)

// PersistenceOptions configures AnalyticsEngine.EnablePersistence. Zero
// values select the defaults.
type PersistenceOptions struct {
	// MaxAge drops persisted entries older than this (default 30 days).
	MaxAge time.Duration
	// MaxEntries bounds the number of persisted entries (default: the
	// engine capacity). The oldest chunks are deleted first.
	MaxEntries int
	// FlushInterval is how often recorded entries are written in one batch
	// (default 1s). At most one interval of entries is lost on a crash;
	// Close flushes everything.
	FlushInterval time.Duration
}

// PersistenceStats reports the state of the persisted entry log.
type PersistenceStats struct {
	Enabled   bool      `json:"enabled"`
	Chunks    int       `json:"chunks"`
	Entries   int       `json:"entries"`
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
		st.Entries += c.count
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

// EnablePersistence makes the engine durable: entries stored in ps
// (bucket CostEntryBucket) are loaded into the ring buffer (newest
// opts.MaxEntries within opts.MaxAge) and every entry recorded afterwards
// is written in the background every FlushInterval, so spend reports
// survive restarts. Entries already recorded in memory are persisted too.
// Call Close on shutdown to flush. Persistence write errors are reported
// by Flush, Close and PersistenceStats; recording never blocks on disk.
func (a *AnalyticsEngine) EnablePersistence(ps persist.Store, opts PersistenceOptions) error {
	if ps == nil {
		return errors.New("analytics: nil persist store")
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = DefaultPersistMaxAge
	}
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = a.max
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultPersistFlushInterval
	}
	a.mu.RLock()
	enabled := a.plog != nil || a.persistUsed
	a.mu.RUnlock()
	if enabled {
		return errors.New("analytics: persistence already enabled")
	}
	l, loaded, err := openChunkLog(ps, CostEntryBucket, opts.MaxAge, opts.MaxEntries, func(e CostEntry) time.Time { return e.Timestamp })
	if err != nil {
		return fmt.Errorf("analytics: load persisted entries: %w", err)
	}
	a.mu.Lock()
	if a.plog != nil || a.persistUsed {
		a.mu.Unlock()
		return errors.New("analytics: persistence already enabled")
	}
	existing := make([]CostEntry, 0, a.size)
	for i := 0; i < a.size; i++ {
		existing = append(existing, *a.at(i))
	}
	a.buf, a.head, a.size = nil, 0, 0
	for _, e := range loaded {
		a.insertLocked(e)
	}
	for _, e := range existing {
		a.insertLocked(e)
		l.add(e)
	}
	a.plog, a.persistUsed = l, true
	a.mu.Unlock()
	l.start(opts.FlushInterval)
	return nil
}

// Flush writes recorded entries that are not yet persisted and applies
// retention. It is a no-op without persistence.
func (a *AnalyticsEngine) Flush() error {
	if l := a.persistLog(); l != nil {
		return l.flush()
	}
	return nil
}

// Close stops background persistence after a final flush. The engine keeps
// working in memory; entries recorded after Close are not persisted and
// persistence cannot be enabled again.
func (a *AnalyticsEngine) Close() error {
	l := a.persistLog()
	if l == nil {
		return nil
	}
	err := l.close()
	a.mu.Lock()
	a.plog = nil
	a.mu.Unlock()
	return err
}

// PersistenceStats reports the persisted entry log (Enabled is false
// without persistence).
func (a *AnalyticsEngine) PersistenceStats() PersistenceStats {
	if l := a.persistLog(); l != nil {
		return l.stats()
	}
	return PersistenceStats{}
}

func (a *AnalyticsEngine) persistLog() *chunkLog[CostEntry] {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.plog
}
