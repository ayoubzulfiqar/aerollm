package swarm

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
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
	mu        sync.RWMutex
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

// ShareKnowledge writes a fragment to the local store and, when a state store
// is configured and the fragment carries an embedding, to shared short-term
// memory. Errors from the state store are returned (the fragment is still kept
// locally). Fragments with non-finite embeddings are rejected.
func (f *FederatedLearning) ShareKnowledge(ctx context.Context, fragment KnowledgeFragment) error {
	if f == nil {
		return errors.New("federated learning not configured")
	}
	for _, x := range fragment.Embedding {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return errors.New("knowledge fragment embedding contains NaN or Inf")
		}
	}
	if fragment.ID == "" {
		fragment.ID = newID("frag")
	}
	fragment.CreatedAt = time.Now().UTC()
	if fragment.Embedding != nil {
		fragment.Embedding = append([]float64(nil), fragment.Embedding...)
	}
	f.knowledge.Add(fragment)
	if f.stateStore != nil && len(fragment.Embedding) > 0 {
		if err := f.stateStore.StoreShortTermMemory(ctx, "swarm-knowledge", []state.Vector{
			{ID: fragment.ID, Data: fragment.Embedding, Meta: map[string]string{"topic": fragment.Topic, "source": fragment.Source}},
		}); err != nil {
			return fmt.Errorf("share knowledge %s: %w", fragment.ID, err)
		}
	}
	return nil
}

// ExportCheckpoint writes current knowledge to a JSONL checkpoint file. The
// file is written atomically (temp file + rename) with 0600 permissions. The
// path must be chosen by the operator, never taken from untrusted input.
func (f *FederatedLearning) ExportCheckpoint(ctx context.Context, path string) (err error) {
	if f == nil {
		return errors.New("federated learning not configured")
	}
	if strings.TrimSpace(path) == "" {
		return errors.New("checkpoint path is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path = filepath.Clean(path)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(0o600); err != nil {
		return err
	}
	w := bufio.NewWriter(tmp)
	for _, frag := range f.knowledge.All() {
		b, mErr := json.Marshal(frag)
		if mErr != nil {
			err = fmt.Errorf("encode fragment %s: %w", frag.ID, mErr)
			return err
		}
		if _, err = w.Write(append(b, '\n')); err != nil {
			return err
		}
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
