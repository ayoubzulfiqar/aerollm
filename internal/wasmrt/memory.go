package wasmrt

import (
	"bytes"
	"context"
	"sync"

	"github.com/tetratelabs/wazero/experimental"
)

// memTracker enforces the per-run memory cap and records the peak size of
// the guest's linear memory. wazero calls into it (through linearMemory) on
// instantiation and on every memory.grow, so a growth request past the cap
// fails inside the guest exactly like an out-of-memory condition, and the
// run can be reported as ErrMemoryLimit rather than as a generic crash.
type memTracker struct {
	mu    sync.Mutex
	limit uint64 // bytes
	peak  uint64 // bytes
	hit   bool
}

func (t *memTracker) exceeded() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.hit
}

func (t *memTracker) peakBytes() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.peak
}

// withAllocator installs t as the allocator for every memory instantiated
// with the returned context.
func withAllocator(ctx context.Context, t *memTracker) context.Context {
	return experimental.WithMemoryAllocator(ctx, experimental.MemoryAllocatorFunc(
		func(capBytes, _ uint64) experimental.LinearMemory {
			return &linearMemory{t: t, capHint: min(capBytes, t.limit)}
		}))
}

// linearMemory is a []byte-backed experimental.LinearMemory that refuses to
// grow past its tracker's limit.
type linearMemory struct {
	t         *memTracker
	buf       []byte
	capHint   uint64
	allocated bool
}

// Reallocate implements experimental.LinearMemory. Returning nil makes the
// guest's memory.grow fail (it returns -1 to the guest).
func (m *linearMemory) Reallocate(size uint64) []byte {
	m.t.mu.Lock()
	defer m.t.mu.Unlock()
	if size > m.t.limit {
		m.t.hit = true
		if m.allocated {
			return nil
		}
		// wazero cannot fail the initial allocation gracefully (it would
		// panic). It is still bounded by the runtime-wide ceiling enforced at
		// decode time, and exec rejects the run with ErrMemoryLimit before
		// "_start" executes.
	}
	if size > m.t.peak {
		m.t.peak = size
	}
	if !m.allocated {
		m.allocated = true
		m.buf = make([]byte, size, max(size, m.capHint))
		return m.buf
	}
	if size <= uint64(cap(m.buf)) {
		// The tail beyond len was zeroed on allocation and never written.
		m.buf = m.buf[:size]
		return m.buf
	}
	m.buf = append(m.buf, make([]byte, int(size)-len(m.buf))...)
	return m.buf
}

// Free implements experimental.LinearMemory.
func (m *linearMemory) Free() {
	m.t.mu.Lock()
	m.buf = nil
	m.t.mu.Unlock()
}

// cappedWriter collects guest output up to limit bytes. With truncate set
// the excess is silently dropped (stderr); otherwise the first write that
// would exceed the limit records an overflow, invokes onOverflow (which
// aborts the run) and fails.
type cappedWriter struct {
	mu         sync.Mutex
	buf        bytes.Buffer
	limit      int
	truncate   bool
	overflow   bool
	onOverflow func()
}

type errWriteLimit struct{}

func (errWriteLimit) Error() string { return "wasmrt: output limit reached" }

func (w *cappedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	room := w.limit - w.buf.Len()
	if len(p) <= room {
		return w.buf.Write(p)
	}
	if w.truncate {
		if room > 0 {
			w.buf.Write(p[:room])
		}
		w.overflow = true
		return len(p), nil // pretend success so the guest carries on
	}
	if !w.overflow {
		w.overflow = true
		if w.onOverflow != nil {
			w.onOverflow()
		}
	}
	return 0, errWriteLimit{}
}

func (w *cappedWriter) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Clone(w.buf.Bytes())
}

func (w *cappedWriter) overflowed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.overflow
}
