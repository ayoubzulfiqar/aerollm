package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

// StateStore is the interface for the embedded state store.
type StateStore interface {
	SaveAgentState(ctx context.Context, sessionID string, state []byte) error
	GetAgentState(ctx context.Context, sessionID string) ([]byte, error)
	DeleteAgentState(ctx context.Context, sessionID string) error
	StoreShortTermMemory(ctx context.Context, sessionID string, vectors []Vector) error
	SearchShortTermMemory(ctx context.Context, sessionID string, queryVector []float64, topK int) ([]ScoredVector, error)
	Close() error
}

// Vector is a dense embedding.
type Vector struct {
	ID   string
	Data []float64
	Meta map[string]string
}

// ScoredVector is a vector with similarity score.
type ScoredVector struct {
	Vector Vector
	Score  float64
}

var (
	// ErrStoreClosed is returned by operations on a closed (or nil) store.
	ErrStoreClosed = errors.New("state store is closed")
	// ErrEmptySessionID is returned when a session ID is empty.
	ErrEmptySessionID = errors.New("session id is required")
	// ErrEmptyVectorID is returned when a vector has no ID.
	ErrEmptyVectorID = errors.New("vector id is required")
	// ErrInvalidVector is returned when a vector contains NaN or Inf values.
	ErrInvalidVector = errors.New("vector contains NaN or Inf values")
)

const (
	bucketAgentState      = "agent_state"
	bucketShortTermMemory = "short_term_memory"

	// DefaultMaxVectorsPerSession bounds the short-term memory of one session.
	// When exceeded, the oldest vectors are evicted first (FIFO).
	DefaultMaxVectorsPerSession = 10000

	// defaultOpenTimeout bounds how long Open waits for the bbolt file lock,
	// so a DB held by another process fails instead of hanging forever.
	defaultOpenTimeout = 5 * time.Second
)

// flatIndex is a lightweight flat index for short-term memory.
type flatIndex struct {
	mu         sync.RWMutex
	bySession  map[string][]Vector
	maxPerSess int
}

func newFlatIndex(maxPerSession int) *flatIndex {
	if maxPerSession <= 0 {
		maxPerSession = DefaultMaxVectorsPerSession
	}
	return &flatIndex{bySession: make(map[string][]Vector), maxPerSess: maxPerSession}
}

func cloneVector(v Vector) Vector {
	out := Vector{ID: v.ID}
	if v.Data != nil {
		out.Data = append([]float64(nil), v.Data...)
	}
	if v.Meta != nil {
		out.Meta = make(map[string]string, len(v.Meta))
		for k, val := range v.Meta {
			out.Meta[k] = val
		}
	}
	return out
}

// upsertAll inserts/replaces vectors for a session, applies FIFO eviction and
// returns a deep copy of the resulting session set (for persistence).
func (f *flatIndex) upsertAll(sessionID string, vs []Vector) []Vector {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.bySession[sessionID]
	for _, v := range vs {
		v = cloneVector(v)
		replaced := false
		for i := range cur {
			if cur[i].ID == v.ID {
				cur[i] = v
				replaced = true
				break
			}
		}
		if !replaced {
			cur = append(cur, v)
		}
	}
	if over := len(cur) - f.maxPerSess; over > 0 {
		cur = append([]Vector(nil), cur[over:]...)
	}
	f.bySession[sessionID] = cur
	snapshot := make([]Vector, len(cur))
	for i, v := range cur {
		snapshot[i] = cloneVector(v)
	}
	return snapshot
}

// load replaces a session's vectors (used when loading persisted state).
func (f *flatIndex) load(sessionID string, vs []Vector) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if over := len(vs) - f.maxPerSess; over > 0 {
		vs = vs[over:]
	}
	f.bySession[sessionID] = vs
}

func (f *flatIndex) search(sessionID string, query []float64, topK int) []ScoredVector {
	if !usableQuery(query) {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	vecs := f.bySession[sessionID]
	if len(vecs) == 0 {
		return nil
	}
	scores := make([]ScoredVector, 0, len(vecs))
	for _, v := range vecs {
		if len(v.Data) != len(query) {
			continue // dimension mismatch: not comparable
		}
		scores = append(scores, ScoredVector{Vector: cloneVector(v), Score: cosineSimilarity(query, v.Data)})
	}
	sortScoredVectors(scores)
	if topK > 0 && len(scores) > topK {
		scores = scores[:topK]
	}
	return scores
}

// usableQuery reports whether a query vector is non-empty, finite and non-zero.
func usableQuery(q []float64) bool {
	if len(q) == 0 {
		return false
	}
	nonZero := false
	for _, x := range q {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return false
		}
		if x != 0 {
			nonZero = true
		}
	}
	return nonZero
}

func validateVector(v Vector) error {
	if v.ID == "" {
		return ErrEmptyVectorID
	}
	for _, x := range v.Data {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return fmt.Errorf("vector %q: %w", v.ID, ErrInvalidVector)
		}
	}
	return nil
}

// BboltStateStore uses bbolt for the KV layer and a flat index for vector search.
type BboltStateStore struct {
	mu       sync.RWMutex // guards closed; held (read) for the duration of DB ops
	closed   bool
	db       *bbolt.DB
	idx      *flatIndex
	basePath string
}

// OpenBboltStateStore opens or creates a bbolt-backed state store. Persisted
// short-term memory is loaded back into the in-memory vector index.
func OpenBboltStateStore(basePath string) (*BboltStateStore, error) {
	if basePath == "" {
		return nil, errors.New("state store base path is required")
	}
	if err := os.MkdirAll(basePath, 0o700); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	dbPath := filepath.Join(basePath, "aerollm-state.db")
	db, err := bbolt.Open(dbPath, 0o600, &bbolt.Options{Timeout: defaultOpenTimeout, NoFreelistSync: true})
	if err != nil {
		return nil, fmt.Errorf("open state db: %w", err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{bucketAgentState, bucketShortTermMemory} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return fmt.Errorf("create bucket %s: %w", name, err)
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &BboltStateStore{db: db, idx: newFlatIndex(DefaultMaxVectorsPerSession), basePath: basePath}
	if err := s.loadIndex(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *BboltStateStore) loadIndex() error {
	return s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketShortTermMemory))
		if b == nil {
			return fmt.Errorf("bucket %s missing", bucketShortTermMemory)
		}
		return b.ForEach(func(k, v []byte) error {
			var vs []Vector
			if err := json.Unmarshal(v, &vs); err != nil {
				return fmt.Errorf("decode short-term memory for session %q: %w", string(k), err)
			}
			s.idx.load(string(k), vs)
			return nil
		})
	})
}

// acquire takes the read lock and fails if the store is closed. Callers must
// call s.mu.RUnlock() when err == nil.
func (s *BboltStateStore) acquire() error {
	if s == nil {
		return ErrStoreClosed
	}
	s.mu.RLock()
	if s.closed || s.db == nil {
		s.mu.RUnlock()
		return ErrStoreClosed
	}
	return nil
}

// SaveAgentState stores agent session state bytes.
func (s *BboltStateStore) SaveAgentState(ctx context.Context, sessionID string, state []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sessionID == "" {
		return ErrEmptySessionID
	}
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.mu.RUnlock()
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketAgentState))
		if b == nil {
			return fmt.Errorf("bucket %s missing", bucketAgentState)
		}
		return b.Put([]byte(sessionID), state)
	})
}

// GetAgentState retrieves agent session state bytes. It returns (nil, nil)
// when no state exists for the session.
func (s *BboltStateStore) GetAgentState(ctx context.Context, sessionID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sessionID == "" {
		return nil, ErrEmptySessionID
	}
	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	var out []byte
	if err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketAgentState))
		if b == nil {
			return fmt.Errorf("bucket %s missing", bucketAgentState)
		}
		if v := b.Get([]byte(sessionID)); v != nil {
			out = append([]byte(nil), v...)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteAgentState removes agent session state.
func (s *BboltStateStore) DeleteAgentState(ctx context.Context, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sessionID == "" {
		return ErrEmptySessionID
	}
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.mu.RUnlock()
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketAgentState))
		if b == nil {
			return fmt.Errorf("bucket %s missing", bucketAgentState)
		}
		return b.Delete([]byte(sessionID))
	})
}

// StoreShortTermMemory upserts dense vectors into the session's flat index and
// persists the session's complete vector set in bbolt. Vectors must have a
// non-empty ID and finite values; the batch is rejected as a whole otherwise.
func (s *BboltStateStore) StoreShortTermMemory(ctx context.Context, sessionID string, vectors []Vector) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sessionID == "" {
		return ErrEmptySessionID
	}
	for _, v := range vectors {
		if err := validateVector(v); err != nil {
			return err
		}
	}
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.mu.RUnlock()
	if len(vectors) == 0 {
		return nil
	}
	snapshot := s.idx.upsertAll(sessionID, vectors)
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode short-term memory: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketShortTermMemory))
		if b == nil {
			return fmt.Errorf("bucket %s missing", bucketShortTermMemory)
		}
		return b.Put([]byte(sessionID), payload)
	})
}

// SearchShortTermMemory searches the flat index for top-k nearest vectors by
// cosine similarity. Vectors whose dimension differs from the query are
// skipped; an empty, zero or non-finite query returns no results. topK <= 0
// returns all comparable vectors.
func (s *BboltStateStore) SearchShortTermMemory(ctx context.Context, sessionID string, queryVector []float64, topK int) ([]ScoredVector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sessionID == "" {
		return nil, ErrEmptySessionID
	}
	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	return s.idx.search(sessionID, queryVector, topK), nil
}

// Close closes the underlying bbolt database. It is idempotent; subsequent
// operations return ErrStoreClosed.
func (s *BboltStateStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func cosineSimilarity(a, b []float64) float64 {
	if len(a) == 0 || len(b) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	sim := dot / (math.Sqrt(na) * math.Sqrt(nb))
	if math.IsNaN(sim) || math.IsInf(sim, 0) {
		return 0
	}
	// Clamp floating-point drift.
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return sim
}

// sortScoredVectors sorts scores descending by score, ties broken by ID.
func sortScoredVectors(scores []ScoredVector) {
	sort.SliceStable(scores, func(i, j int) bool {
		if scores[i].Score != scores[j].Score {
			return scores[i].Score > scores[j].Score
		}
		return scores[i].Vector.ID < scores[j].Vector.ID
	})
}
