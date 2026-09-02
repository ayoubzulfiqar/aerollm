package swarm

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/state"
)

// KnowledgeFragment represents learned swarm knowledge.
type KnowledgeFragment struct {
	ID        string
	Topic     string
	Content   string
	Embedding []float64
	Source    string
	CreatedAt time.Time
}

// KnowledgeStore persists swarm knowledge fragments.
type KnowledgeStore struct {
	mu       sync.RWMutex
	fragments []KnowledgeFragment
}

// NewKnowledgeStore creates a new knowledge store.
func NewKnowledgeStore() *KnowledgeStore {
	return &KnowledgeStore{fragments: make([]KnowledgeFragment, 0)}
}

// Add appends a knowledge fragment.
func (k *KnowledgeStore) Add(f KnowledgeFragment) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.fragments = append(k.fragments, f)
}

// All returns all fragments.
func (k *KnowledgeStore) All() []KnowledgeFragment {
	k.mu.RLock()
	defer k.mu.RUnlock()
	out := make([]KnowledgeFragment, len(k.fragments))
	copy(out, k.fragments)
	return out
}

// FederatedLearning coordinates knowledge sharing across sub-agents.
type FederatedLearning struct {
	stateStore state.StateStore
	knowledge  *KnowledgeStore
}

// NewFederatedLearning creates a new federated learning coordinator.
func NewFederatedLearning(store state.StateStore) *FederatedLearning {
	return &FederatedLearning{
		stateStore: store,
		knowledge:  NewKnowledgeStore(),
	}
}

// ShareKnowledge writes a fragment to shared state and local store.
func (f *FederatedLearning) ShareKnowledge(ctx context.Context, fragment KnowledgeFragment) error {
	if f == nil {
		return nil
	}
	if fragment.ID == "" {
		fragment.ID = time.Now().UTC().Format("20060102T150405.000Z")
	}
	fragment.CreatedAt = time.Now().UTC()
	f.knowledge.Add(fragment)
	if f.stateStore != nil {
		_ = f.stateStore.StoreShortTermMemory(ctx, "swarm-knowledge", []state.Vector{
			{ID: fragment.ID, Data: fragment.Embedding, Meta: map[string]string{"topic": fragment.Topic, "source": fragment.Source}},
		})
	}
	return nil
}

// ExportCheckpoint writes current knowledge to a JSONL checkpoint file.
func (f *FederatedLearning) ExportCheckpoint(ctx context.Context, path string) error {
	_ = ctx
	if f == nil || path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	fragments := f.knowledge.All()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, frag := range fragments {
		b, _ := json.Marshal(frag)
		file.Write(b)
		file.Write([]byte("\n"))
	}
	return nil
}
