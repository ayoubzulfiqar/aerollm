package mesh

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Limits applied to remote CRDT payloads. A peer can only make a replica grow
// by at most MaxMergeEntries entries per merge, and a single payload may not
// exceed MaxMergePayloadBytes.
const (
	// MaxMergePayloadBytes caps the size of a remote CRDT payload accepted by Merge.
	MaxMergePayloadBytes = 8 << 20
	// MaxMergeEntries caps the number of entries in a single remote CRDT payload.
	MaxMergeEntries = 100_000
	// MaxNodeIDLen caps node identifiers carried in CRDT payloads.
	MaxNodeIDLen = 256
	// MaxCRDTKeyLen caps element / counter keys carried in CRDT payloads.
	MaxCRDTKeyLen = 1024
)

var (
	// ErrPayloadTooLarge is returned when a remote payload exceeds MaxMergePayloadBytes
	// or carries more than MaxMergeEntries entries.
	ErrPayloadTooLarge = errors.New("mesh: crdt payload too large")
	// ErrInvalidPayload is returned when a remote payload is malformed or carries
	// out-of-range values (negative counters or timestamps, oversized ids, ...).
	ErrInvalidPayload = errors.New("mesh: invalid crdt payload")
)

func checkPayload(remote json.RawMessage) error {
	if len(remote) == 0 {
		return fmt.Errorf("%w: empty payload", ErrInvalidPayload)
	}
	if len(remote) > MaxMergePayloadBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrPayloadTooLarge, len(remote), MaxMergePayloadBytes)
	}
	return nil
}

func checkNodeID(node string) error {
	if node == "" {
		return fmt.Errorf("%w: empty node id", ErrInvalidPayload)
	}
	if len(node) > MaxNodeIDLen {
		return fmt.Errorf("%w: node id longer than %d bytes", ErrInvalidPayload, MaxNodeIDLen)
	}
	return nil
}

func checkKey(key string) error {
	if len(key) > MaxCRDTKeyLen {
		return fmt.Errorf("%w: key longer than %d bytes", ErrInvalidPayload, MaxCRDTKeyLen)
	}
	return nil
}

// randomNodeID returns a random 128-bit hex identifier.
func randomNodeID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on supported platforms; fall back to a
		// time-derived id rather than panicking.
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

// cloneBytes copies b, normalising empty slices to nil so that "no value" has a
// single canonical representation (and therefore a single JSON encoding).
func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

// satAdd adds two non-negative int64 values, saturating at math.MaxInt64.
func satAdd(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// ---------------------------------------------------------------------------
// PN-Counter
// ---------------------------------------------------------------------------

type pnPair struct {
	P int64 `json:"p"`
	N int64 `json:"n"`
}

// PNCounter is a state-based increment/decrement counter CRDT holding one
// counter per key.
//
// Every replica only ever grows its own (node) slot; Merge takes the per-node
// maximum, which is commutative, associative and idempotent. Tallies saturate
// at math.MaxInt64 instead of overflowing.
//
// Snapshot format: {"<key>": {"<node>": {"p": <int>, "n": <int>}}}.
type PNCounter struct {
	mu    sync.RWMutex
	node  string
	state map[string]map[string]pnPair
}

// NewPNCounter creates a PN-Counter with a random, unique node identifier.
func NewPNCounter() *PNCounter {
	return NewPNCounterForNode("")
}

// NewPNCounterForNode creates a PN-Counter whose local increments are attributed
// to node. Every replica participating in the same counter must use a distinct
// node id; an empty node gets a random id.
func NewPNCounterForNode(node string) *PNCounter {
	if node == "" {
		node = randomNodeID()
	}
	return &PNCounter{node: node, state: make(map[string]map[string]pnPair)}
}

// Node returns the identifier local updates are attributed to.
func (c *PNCounter) Node() string { return c.node }

func (c *PNCounter) slot(key string) map[string]pnPair {
	m, ok := c.state[key]
	if !ok {
		m = make(map[string]pnPair)
		c.state[key] = m
	}
	return m
}

// Increment adds delta (> 0) to key. Non-positive deltas are ignored.
func (c *PNCounter) Increment(key string, delta int64) {
	if delta <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.slot(key)
	p := m[c.node]
	p.P = satAdd(p.P, delta)
	m[c.node] = p
}

// Decrement subtracts delta (> 0) from key. Non-positive deltas are ignored.
func (c *PNCounter) Decrement(key string, delta int64) {
	if delta <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.slot(key)
	p := m[c.node]
	p.N = satAdd(p.N, delta)
	m[c.node] = p
}

// Value returns the current value of key (sum of increments minus sum of
// decrements across all nodes).
func (c *PNCounter) Value(key string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var p, n int64
	for _, pair := range c.state[key] {
		p = satAdd(p, pair.P)
		n = satAdd(n, pair.N)
	}
	// Both sums are in [0, MaxInt64], so the difference cannot overflow.
	return p - n
}

// Merge combines remote state (a Snapshot from another replica) into the local
// counter. The payload is validated completely before anything is applied.
func (c *PNCounter) Merge(_ context.Context, remote json.RawMessage) error {
	if err := checkPayload(remote); err != nil {
		return err
	}
	var incoming map[string]map[string]pnPair
	if err := json.Unmarshal(remote, &incoming); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	entries := 0
	for key, nodes := range incoming {
		if err := checkKey(key); err != nil {
			return err
		}
		for node, pair := range nodes {
			entries++
			if entries > MaxMergeEntries {
				return fmt.Errorf("%w: more than %d entries", ErrPayloadTooLarge, MaxMergeEntries)
			}
			if err := checkNodeID(node); err != nil {
				return err
			}
			if pair.P < 0 || pair.N < 0 {
				return fmt.Errorf("%w: negative tally for key %q node %q", ErrInvalidPayload, key, node)
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, nodes := range incoming {
		if len(nodes) == 0 {
			continue
		}
		m := c.slot(key)
		for node, in := range nodes {
			cur := m[node]
			if in.P > cur.P {
				cur.P = in.P
			}
			if in.N > cur.N {
				cur.N = in.N
			}
			m[node] = cur
		}
	}
	return nil
}

// Snapshot returns the full CRDT state as canonical JSON.
func (c *PNCounter) Snapshot() (json.RawMessage, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]map[string]pnPair, len(c.state))
	for key, nodes := range c.state {
		cp := make(map[string]pnPair, len(nodes))
		for node, pair := range nodes {
			cp[node] = pair
		}
		out[key] = cp
	}
	return json.Marshal(out)
}

// LocalSnapshot implements MeshState.
func (c *PNCounter) LocalSnapshot(_ context.Context) (json.RawMessage, error) { return c.Snapshot() }

// Type returns the CRDT type name.
func (c *PNCounter) Type() string { return "pn-counter" }

// ---------------------------------------------------------------------------
// LWW register
// ---------------------------------------------------------------------------

// lwwNewer reports whether the write (ts, node, val) beats (curTS, curNode, curVal).
// Ordering is by timestamp, then node id, then value bytes, so concurrent writes
// with equal timestamps resolve identically on every replica.
func lwwNewer(ts int64, node string, val []byte, curTS int64, curNode string, curVal []byte) bool {
	if ts != curTS {
		return ts > curTS
	}
	if node != curNode {
		return node > curNode
	}
	return bytes.Compare(val, curVal) > 0
}

// LWWRWRegister is a last-writer-wins register.
//
// Writes are totally ordered by (timestamp, writer node id, value bytes); the
// greatest write wins. Negative timestamps are rejected.
//
// Snapshot format: {"node": "<writer>", "timestamp": <int>, "value": <base64>}.
type LWWRWRegister struct {
	mu        sync.RWMutex
	node      string // local node id stamped on local writes
	timestamp int64
	writer    string // node id that produced the current value
	value     []byte
}

// NewLWWRWRegister creates a new LWW register. node identifies this replica for
// deterministic tie-breaking; an empty node gets a random id.
func NewLWWRWRegister(node string) *LWWRWRegister {
	if node == "" {
		node = randomNodeID()
	}
	return &LWWRWRegister{node: node}
}

// Set assigns value at logical timestamp ts. The write is ignored if it does not
// beat the current value (see LWWRWRegister ordering) or if ts is negative.
func (r *LWWRWRegister) Set(ts int64, value []byte) {
	r.apply(ts, r.node, value)
}

func (r *LWWRWRegister) apply(ts int64, node string, value []byte) bool {
	if ts < 0 {
		return false
	}
	value = cloneBytes(value)
	r.mu.Lock()
	defer r.mu.Unlock()
	if !lwwNewer(ts, node, value, r.timestamp, r.writer, r.value) {
		return false
	}
	r.timestamp = ts
	r.writer = node
	r.value = value
	return true
}

// Get returns the current timestamp and a copy of the current value.
func (r *LWWRWRegister) Get() (int64, []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.timestamp, cloneBytes(r.value)
}

// Writer returns the node id that produced the current value ("" if unset).
func (r *LWWRWRegister) Writer() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.writer
}

type lwwRegisterWire struct {
	Node      string `json:"node"`
	Timestamp int64  `json:"timestamp"`
	Value     []byte `json:"value"`
}

// Merge applies remote state if it wins under the register ordering.
func (r *LWWRWRegister) Merge(_ context.Context, remote json.RawMessage) error {
	if err := checkPayload(remote); err != nil {
		return err
	}
	var in lwwRegisterWire
	if err := json.Unmarshal(remote, &in); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if in.Timestamp < 0 {
		return fmt.Errorf("%w: negative timestamp", ErrInvalidPayload)
	}
	if len(in.Node) > MaxNodeIDLen {
		return fmt.Errorf("%w: node id longer than %d bytes", ErrInvalidPayload, MaxNodeIDLen)
	}
	r.apply(in.Timestamp, in.Node, in.Value)
	return nil
}

// Snapshot returns the current register state.
func (r *LWWRWRegister) Snapshot() (json.RawMessage, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return json.Marshal(lwwRegisterWire{Node: r.writer, Timestamp: r.timestamp, Value: r.value})
}

// LocalSnapshot implements MeshState.
func (r *LWWRWRegister) LocalSnapshot(_ context.Context) (json.RawMessage, error) {
	return r.Snapshot()
}

// Type returns the CRDT type name.
func (r *LWWRWRegister) Type() string { return "lww-register" }

// ---------------------------------------------------------------------------
// LWW element set
// ---------------------------------------------------------------------------

type lwwAdd struct {
	ts    int64
	node  string
	value []byte
}

// LWWElementSet is a last-writer-wins element set.
//
// Each key keeps the greatest add (ordered by timestamp, node id, value bytes)
// and the greatest remove timestamp. A key is present when its add timestamp is
// greater than or equal to its remove timestamp: on equal timestamps the add
// wins ("add-wins bias"). Tombstones are retained so that removals converge.
//
// Snapshot format: a JSON array of operations sorted by key, adds before
// removes: [{"key":..., "timestamp":..., "node":..., "value":...},
// {"key":..., "timestamp":..., "removed":true}].
type LWWElementSet struct {
	mu      sync.RWMutex
	node    string
	adds    map[string]lwwAdd
	removes map[string]int64
}

// NewLWWElementSet creates a new LWW set. node identifies this replica for
// deterministic tie-breaking; an empty node gets a random id.
func NewLWWElementSet(node string) *LWWElementSet {
	if node == "" {
		node = randomNodeID()
	}
	return &LWWElementSet{
		node:    node,
		adds:    make(map[string]lwwAdd),
		removes: make(map[string]int64),
	}
}

func (s *LWWElementSet) applyAddLocked(key string, ts int64, node string, value []byte) {
	cur, ok := s.adds[key]
	if ok && !lwwNewer(ts, node, value, cur.ts, cur.node, cur.value) {
		return
	}
	s.adds[key] = lwwAdd{ts: ts, node: node, value: value}
}

func (s *LWWElementSet) applyRemoveLocked(key string, ts int64) {
	if cur, ok := s.removes[key]; ok && cur >= ts {
		return
	}
	s.removes[key] = ts
}

// Add inserts key with value at timestamp ts. Negative timestamps are ignored.
func (s *LWWElementSet) Add(key string, ts int64, value []byte) {
	if ts < 0 {
		return
	}
	value = cloneBytes(value)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyAddLocked(key, ts, s.node, value)
}

// Remove marks key removed at timestamp ts. Negative timestamps are ignored.
func (s *LWWElementSet) Remove(key string, ts int64) {
	if ts < 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyRemoveLocked(key, ts)
}

func (s *LWWElementSet) liveLocked(key string) (lwwAdd, bool) {
	a, ok := s.adds[key]
	if !ok {
		return lwwAdd{}, false
	}
	if rm, removed := s.removes[key]; removed && rm > a.ts {
		return lwwAdd{}, false
	}
	return a, true
}

// Lookup returns a copy of the value stored under key and whether key is live.
func (s *LWWElementSet) Lookup(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.liveLocked(key)
	if !ok {
		return nil, false
	}
	return cloneBytes(a.value), true
}

// Elements returns the current live elements (values are copies).
func (s *LWWElementSet) Elements() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]byte, len(s.adds))
	for k := range s.adds {
		if a, ok := s.liveLocked(k); ok {
			out[k] = cloneBytes(a.value)
		}
	}
	return out
}

type lwwSetOp struct {
	Key       string `json:"key"`
	Timestamp int64  `json:"timestamp"`
	Node      string `json:"node,omitempty"`
	Value     []byte `json:"value,omitempty"`
	Removed   bool   `json:"removed,omitempty"`
}

// Merge applies remote add/remove operations (a Snapshot from another replica).
// The payload is validated completely before anything is applied.
func (s *LWWElementSet) Merge(_ context.Context, remote json.RawMessage) error {
	if err := checkPayload(remote); err != nil {
		return err
	}
	var incoming []lwwSetOp
	if err := json.Unmarshal(remote, &incoming); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if len(incoming) > MaxMergeEntries {
		return fmt.Errorf("%w: more than %d entries", ErrPayloadTooLarge, MaxMergeEntries)
	}
	for _, op := range incoming {
		if err := checkKey(op.Key); err != nil {
			return err
		}
		if op.Timestamp < 0 {
			return fmt.Errorf("%w: negative timestamp for key %q", ErrInvalidPayload, op.Key)
		}
		if len(op.Node) > MaxNodeIDLen {
			return fmt.Errorf("%w: node id longer than %d bytes", ErrInvalidPayload, MaxNodeIDLen)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, op := range incoming {
		if op.Removed {
			s.applyRemoveLocked(op.Key, op.Timestamp)
			continue
		}
		s.applyAddLocked(op.Key, op.Timestamp, op.Node, cloneBytes(op.Value))
	}
	return nil
}

// Snapshot returns the full set state (adds and tombstones) as canonical JSON.
func (s *LWWElementSet) Snapshot() (json.RawMessage, error) {
	s.mu.RLock()
	out := make([]lwwSetOp, 0, len(s.adds)+len(s.removes))
	for k, a := range s.adds {
		out = append(out, lwwSetOp{Key: k, Timestamp: a.ts, Node: a.node, Value: a.value})
	}
	for k, ts := range s.removes {
		out = append(out, lwwSetOp{Key: k, Timestamp: ts, Removed: true})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return !out[i].Removed && out[j].Removed
	})
	return json.Marshal(out)
}

// LocalSnapshot implements MeshState.
func (s *LWWElementSet) LocalSnapshot(_ context.Context) (json.RawMessage, error) {
	return s.Snapshot()
}

// Type returns the CRDT type name.
func (s *LWWElementSet) Type() string { return "lww-element-set" }

var (
	_ MeshState = (*PNCounter)(nil)
	_ MeshState = (*LWWRWRegister)(nil)
	_ MeshState = (*LWWElementSet)(nil)
)

// ---------------------------------------------------------------------------
// Vector clock
// ---------------------------------------------------------------------------

// ClockOrdering is the causal relation between two vector clocks.
type ClockOrdering int

const (
	// ClockEqual means both clocks are identical.
	ClockEqual ClockOrdering = iota
	// ClockBefore means the first clock happened-before the second.
	ClockBefore
	// ClockAfter means the first clock happened-after the second.
	ClockAfter
	// ClockConcurrent means neither clock dominates the other.
	ClockConcurrent
)

// String implements fmt.Stringer.
func (o ClockOrdering) String() string {
	switch o {
	case ClockEqual:
		return "equal"
	case ClockBefore:
		return "before"
	case ClockAfter:
		return "after"
	case ClockConcurrent:
		return "concurrent"
	default:
		return "unknown"
	}
}

// CompareVectorClocks returns the causal relation of a relative to b. Missing
// and negative entries are treated as zero.
func CompareVectorClocks(a, b map[string]int64) ClockOrdering {
	less, greater := false, false
	cmp := func(x, y int64) {
		if x < 0 {
			x = 0
		}
		if y < 0 {
			y = 0
		}
		switch {
		case x < y:
			less = true
		case x > y:
			greater = true
		}
	}
	for node, av := range a {
		cmp(av, b[node])
	}
	for node, bv := range b {
		if _, seen := a[node]; !seen {
			cmp(0, bv)
		}
	}
	switch {
	case less && greater:
		return ClockConcurrent
	case less:
		return ClockBefore
	case greater:
		return ClockAfter
	default:
		return ClockEqual
	}
}

// VectorClock is a thread-safe vector clock.
type VectorClock struct {
	mu    sync.RWMutex
	clock map[string]int64
}

// NewVectorClock creates a new vector clock.
func NewVectorClock() *VectorClock {
	return &VectorClock{clock: make(map[string]int64)}
}

// Tick increments the counter for node (saturating at math.MaxInt64). Empty
// node ids are ignored.
func (v *VectorClock) Tick(node string) {
	if node == "" {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.clock[node] = satAdd(v.clock[node], 1)
}

// Merge takes the per-node maximum. Negative entries and empty node ids are
// ignored.
func (v *VectorClock) Merge(remote map[string]int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for node, ts := range remote {
		if node == "" || ts <= 0 {
			continue
		}
		if ts > v.clock[node] {
			v.clock[node] = ts
		}
	}
}

// Compare returns the causal relation of this clock relative to other.
func (v *VectorClock) Compare(other map[string]int64) ClockOrdering {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return CompareVectorClocks(v.clock, other)
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

// MergeVectorClock merges src into dst (per-node maximum) and returns dst.
// Negative entries and empty node ids in src are ignored.
func MergeVectorClock(dst, src map[string]int64) map[string]int64 {
	if dst == nil {
		dst = make(map[string]int64, len(src))
	}
	for node, ts := range src {
		if node == "" || ts <= 0 {
			continue
		}
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

// LogicalClock returns a timestamp strictly greater than prev, preferring the
// wall clock when it is ahead.
func LogicalClock(prev int64) int64 {
	now := timeNow().UnixNano()
	if now <= prev {
		if prev == math.MaxInt64 {
			return prev
		}
		return prev + 1
	}
	return now
}
