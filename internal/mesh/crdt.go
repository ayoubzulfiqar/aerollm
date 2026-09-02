package mesh

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// PNCounter is a CRDT increment/decrement counter.
type PNCounter struct {
	mu       sync.RWMutex
	positive map[string]int64
	negative map[string]int64
}

// NewPNCounter creates a new PN-Counter.
func NewPNCounter() *PNCounter {
	return &PNCounter{positive: make(map[string]int64), negative: make(map[string]int64)}
}

// Increment adds delta to the positive count.
func (c *PNCounter) Increment(key string, delta int64) {
	if delta <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.positive[key] += delta
}

// Decrement subtracts delta from the positive count via negative tally.
func (c *PNCounter) Decrement(key string, delta int64) {
	if delta <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.negative[key] += delta
}

// Value returns the current counter value.
func (c *PNCounter) Value(key string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.positive[key] - c.negative[key]
}

// Merge combines remote state into the local CRDT.
func (c *PNCounter) Merge(_ context.Context, remote json.RawMessage) error {
	var incoming map[string]struct {
		P int64 `json:"p"`
		N int64 `json:"n"`
	}
	if err := json.Unmarshal(remote, &incoming); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range incoming {
		if v.P > c.positive[k] {
			c.positive[k] = v.P
		}
		if v.N > c.negative[k] {
			c.negative[k] = v.N
		}
	}
	return nil
}

// Snapshot returns the current CRDT state.
func (c *PNCounter) Snapshot() (json.RawMessage, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]struct {
		P int64 `json:"p"`
		N int64 `json:"n"`
	}, len(c.positive))
	for k, p := range c.positive {
		out[k] = struct {
			P int64 `json:"p"`
			N int64 `json:"n"`
		}{P: p, N: c.negative[k]}
	}
	return json.Marshal(out)
}

// Type returns the CRDT type name.
func (c *PNCounter) Type() string { return "pn-counter" }

// LWWRWRegister is a last-writer-wins register.
type LWWRWRegister struct {
	mu      sync.RWMutex
	node    string
	timestamp int64
	value    []byte
}

// NewLWWRWRegister creates a new LWW register.
func NewLWWRWRegister(node string) *LWWRWRegister {
	return &LWWRWRegister{node: node}
}

// Set assigns a value with logical timestamp.
func (r *LWWRWRegister) Set(ts int64, value []byte) {
	if ts < r.timestamp {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.timestamp = ts
	r.value = value
}

// Get returns the current value and timestamp.
func (r *LWWRWRegister) Get() (int64, []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.timestamp, r.value
}

// Merge applies remote state if newer.
func (r *LWWRWRegister) Merge(_ context.Context, remote json.RawMessage) error {
	var incoming struct {
		Node     string `json:"node"`
		Timestamp int64  `json:"timestamp"`
		Value    []byte `json:"value"`
	}
	if err := json.Unmarshal(remote, &incoming); err != nil {
		return err
	}
	r.Set(incoming.Timestamp, incoming.Value)
	return nil
}

// Snapshot returns the current register state.
func (r *LWWRWRegister) Snapshot() (json.RawMessage, error) {
	ts, value := r.Get()
	return json.Marshal(struct {
		Node      string `json:"node"`
		Timestamp int64  `json:"timestamp"`
		Value     []byte `json:"value"`
	}{Node: r.node, Timestamp: ts, Value: value})
}

// Type returns the CRDT type name.
func (r *LWWRWRegister) Type() string { return "lww-register" }

// LWWElementSet is a set with last-writer-wins element semantics.
type LWWElementSet struct {
	mu       sync.RWMutex
	node     string
	elements map[string]int64
	removed  map[string]int64
	values   map[string][]byte
}

// NewLWWElementSet creates a new LWW set.
func NewLWWElementSet(node string) *LWWElementSet {
	return &LWWElementSet{
		node:     node,
		elements: make(map[string]int64),
		removed:  make(map[string]int64),
		values:   make(map[string][]byte),
	}
}

// Add inserts an element with timestamp.
func (s *LWWElementSet) Add(key string, ts int64, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ts < s.removed[key] {
		return
	}
	s.elements[key] = ts
	s.values[key] = value
}

// Remove marks an element removed at ts.
func (s *LWWElementSet) Remove(key string, ts int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ts < s.elements[key] {
		return
	}
	s.removed[key] = ts
}

// Elements returns current live elements.
func (s *LWWElementSet) Elements() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]byte, len(s.elements))
	for k, ts := range s.elements {
		if ts >= s.removed[k] {
			out[k] = s.values[k]
		}
	}
	return out
}

// Merge applies remote add/remove operations.
func (s *LWWElementSet) Merge(_ context.Context, remote json.RawMessage) error {
	var incoming []struct {
		Key       string `json:"key"`
		Timestamp int64  `json:"timestamp"`
		Value     []byte `json:"value"`
		Removed   bool   `json:"removed"`
	}
	if err := json.Unmarshal(remote, &incoming); err != nil {
		return err
	}
	for _, op := range incoming {
		if op.Removed {
			s.Remove(op.Key, op.Timestamp)
			continue
		}
		s.Add(op.Key, op.Timestamp, op.Value)
	}
	return nil
}

// Snapshot returns the current set state.
func (s *LWWElementSet) Snapshot() (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	type entry struct {
		Key       string `json:"key"`
		Timestamp int64  `json:"timestamp"`
		Value     []byte `json:"value"`
		Removed   bool   `json:"removed"`
	}
	out := make([]entry, 0, len(s.elements))
	for k, ts := range s.elements {
		out = append(out, entry{Key: k, Timestamp: ts, Value: s.values[k], Removed: ts < s.removed[k]})
	}
	return json.Marshal(out)
}

// Type returns the CRDT type name.
func (s *LWWElementSet) Type() string { return "lww-element-set" }

// VectorClock is a minimal logical clock.
type VectorClock struct {
	mu    sync.RWMutex
	clock map[string]int64
}

// NewVectorClock creates a new vector clock.
func NewVectorClock() *VectorClock {
	return &VectorClock{clock: make(map[string]int64)}
}

// Tick increments the local node counter.
func (v *VectorClock) Tick(node string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.clock[node]++
}

// Merge takes the max per node.
func (v *VectorClock) Merge(remote map[string]int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for node, ts := range remote {
		if ts > v.clock[node] {
			v.clock[node] = ts
		}
	}
}

// Clock returns a copy of the current clock.
func (v *VectorClock) Clock() map[string]int64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make(map[string]int64, len(v.clock))
	for k, vv := range v.clock {
		out[k] = vv
	}
	return out
}

// MergeVectorClock merges remote clock state.
func MergeVectorClock(dst, src map[string]int64) map[string]int64 {
	if dst == nil {
		dst = make(map[string]int64, len(src))
	}
	for node, ts := range src {
		if ts > dst[node] {
			dst[node] = ts
		}
	}
	return dst
}

// Now returns a timestamp suitable for LWW ops.
func Now() int64 {
	return time.Now().UnixNano()
}

var timeNow = time.Now

// LogicalClock returns a monotonic logical timestamp.
func LogicalClock(prev int64) int64 {
	now := timeNow().UnixNano()
	if now <= prev {
		return prev + 1
	}
	return now
}
