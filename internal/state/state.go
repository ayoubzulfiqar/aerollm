package state

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

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
	ID    string
	Data  []float64
	Meta  map[string]string
}

// ScoredVector is a vector with similarity score.
type ScoredVector struct {
	Vector Vector
	Score  float64
}

// flatIndex is a lightweight flat index for short-term memory.
type flatIndex struct {
	mu       sync.RWMutex
	bySession map[string][]Vector
}

func newFlatIndex() *flatIndex {
	return &flatIndex{bySession: make(map[string][]Vector)}
}

func (f *flatIndex) upsert(sessionID string, v Vector) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.bySession[sessionID] {
		if f.bySession[sessionID][i].ID == v.ID {
			f.bySession[sessionID][i] = v
			return
		}
	}
	f.bySession[sessionID] = append(f.bySession[sessionID], v)
}

func (f *flatIndex) search(sessionID string, query []float64, topK int) []ScoredVector {
	f.mu.RLock()
	defer f.mu.RUnlock()
	vecs := f.bySession[sessionID]
	if len(vecs) == 0 {
		return nil
	}
	scores := make([]ScoredVector, 0, len(vecs))
	for _, v := range vecs {
		scores = append(scores, ScoredVector{Vector: v, Score: cosineSimilarity(query, v.Data)})
	}
	sortScoredVectors(scores)
	if topK > 0 && len(scores) > topK {
		scores = scores[:topK]
	}
	return scores
}

// BboltStateStore uses bbolt for the KV layer and a flat index for vector search.
type BboltStateStore struct {
	db       *bbolt.DB
	idx      *flatIndex
	basePath string
}

// OpenBboltStateStore opens or creates a bbolt-backed state store.
func OpenBboltStateStore(basePath string) (*BboltStateStore, error) {
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(basePath, "aerollm-state.db")
	db, err := bbolt.Open(dbPath, 0o644, &bbolt.Options{NoFreelistSync: true, NoSync: false})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("agent_state"))
		_, _ = tx.CreateBucketIfNotExists([]byte("short_term_memory"))
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &BboltStateStore{db: db, idx: newFlatIndex(), basePath: basePath}, nil
}

// SaveAgentState stores agent session state bytes.
func (s *BboltStateStore) SaveAgentState(ctx context.Context, sessionID string, state []byte) error {
	_ = ctx
	if s == nil || s.db == nil {
		return fmt.Errorf("state store not initialized")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("agent_state"))
		if b == nil {
			return fmt.Errorf("bucket agent_state missing")
		}
		return b.Put([]byte(sessionID), state)
	})
}

// GetAgentState retrieves agent session state bytes.
func (s *BboltStateStore) GetAgentState(ctx context.Context, sessionID string) ([]byte, error) {
	_ = ctx
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("state store not initialized")
	}
	var out []byte
	if err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("agent_state"))
		if b == nil {
			return fmt.Errorf("bucket agent_state missing")
		}
		v := b.Get([]byte(sessionID))
		if v != nil {
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
	_ = ctx
	if s == nil || s.db == nil {
		return fmt.Errorf("state store not initialized")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("agent_state"))
		if b == nil {
			return fmt.Errorf("bucket agent_state missing")
		}
		return b.Delete([]byte(sessionID))
	})
}

// StoreShortTermMemory stores dense vectors in the flat index and mirrors metadata in bbolt.
func (s *BboltStateStore) StoreShortTermMemory(ctx context.Context, sessionID string, vectors []Vector) error {
	_ = ctx
	if s == nil {
		return fmt.Errorf("state store not initialized")
	}
	for _, v := range vectors {
		s.idx.upsert(sessionID, v)
	}
	if s.db == nil {
		return nil
	}
	payload, _ := json.Marshal(vectors)
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("short_term_memory"))
		if b == nil {
			return fmt.Errorf("bucket short_term_memory missing")
		}
		return b.Put([]byte(sessionID), payload)
	})
}

// SearchShortTermMemory searches the flat index for top-k nearest vectors by cosine similarity.
func (s *BboltStateStore) SearchShortTermMemory(ctx context.Context, sessionID string, queryVector []float64, topK int) ([]ScoredVector, error) {
	_ = ctx
	if s == nil {
		return nil, fmt.Errorf("state store not initialized")
	}
	return s.idx.search(sessionID, queryVector, topK), nil
}

// Close closes the underlying bbolt database.
func (s *BboltStateStore) Close() error {
	if s == nil || s.db == nil {
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
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// sortScoredVectors sorts scores descending by score.
func sortScoredVectors(scores []ScoredVector) {
	sort.Slice(scores, func(i, j int) bool { return scores[i].Score > scores[j].Score })
}
