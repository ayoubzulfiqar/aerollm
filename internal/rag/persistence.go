package rag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used by the RAG stores in a persist.Store. The vector store and the
// keyword index persist their documents independently so either can be used
// on its own; embeddings and term statistics are recomputed on load.
const (
	VectorStoreBucket  = "rag_vector_documents"
	KeywordIndexBucket = "rag_keyword_documents"
)

// ErrPartialLoad is wrapped by EnablePersistence errors when some persisted
// documents could not be decoded or embedded. Persistence is enabled and the
// remaining documents are loaded; the skipped documents stay in the
// persist.Store and are retried on the next load.
var ErrPartialLoad = errors.New("rag: some persisted documents could not be loaded")

// errPersistenceEnabled is returned when EnablePersistence is called twice.
var errPersistenceEnabled = errors.New("rag: persistence already enabled")

// persistedDocument is the durable form of a Document. Seq preserves the
// insertion order used to break score ties.
type persistedDocument struct {
	ID       string                 `json:"id,omitempty"`
	Content  string                 `json:"content"`
	Source   string                 `json:"source,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Seq      int                    `json:"seq"`
}

func newPersistedDocument(doc Document, seq int) persistedDocument {
	return persistedDocument{ID: doc.ID, Content: doc.Content, Source: doc.Source, Metadata: doc.Metadata, Seq: seq}
}

func (p persistedDocument) document() Document {
	return Document{ID: p.ID, Content: p.Content, Source: p.Source, Metadata: p.Metadata}
}

type loadedDocument struct {
	key string
	doc persistedDocument
}

// loadPersistedDocuments reads every document in bucket, ordered by Seq.
// Documents that fail to decode are skipped and reported in bad.
func loadPersistedDocuments(ps persist.Store, bucket string) (docs []loadedDocument, bad []string, err error) {
	err = ps.ForEach(bucket, func(key string, raw json.RawMessage) error {
		var d persistedDocument
		if jerr := json.Unmarshal(raw, &d); jerr != nil {
			bad = append(bad, key)
			return nil
		}
		docs = append(docs, loadedDocument{key: key, doc: d})
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("rag: load %s: %w", bucket, err)
	}
	sort.SliceStable(docs, func(i, j int) bool {
		if docs[i].doc.Seq != docs[j].doc.Seq {
			return docs[i].doc.Seq < docs[j].doc.Seq
		}
		return docs[i].key < docs[j].key
	})
	return docs, bad, nil
}

func partialLoadError(bucket string, undecodable, failed []string, cause error) error {
	if len(undecodable) == 0 && len(failed) == 0 {
		return nil
	}
	var parts []string
	if len(undecodable) > 0 {
		parts = append(parts, fmt.Sprintf("%d undecodable (%s)", len(undecodable), clipList(undecodable)))
	}
	if len(failed) > 0 {
		parts = append(parts, fmt.Sprintf("%d not indexed (%s)", len(failed), clipList(failed)))
	}
	err := fmt.Errorf("%w from %s: %s", ErrPartialLoad, bucket, strings.Join(parts, "; "))
	if cause != nil {
		err = fmt.Errorf("%w: %w", err, cause)
	}
	return err
}

func clipList(keys []string) string {
	const max = 10
	if len(keys) <= max {
		return strings.Join(keys, ",")
	}
	return strings.Join(keys[:max], ",") + fmt.Sprintf(",... %d more", len(keys)-max)
}

// NewInMemoryVectorStoreWithPersistence creates a vector store whose
// documents are persisted in ps (bucket VectorStoreBucket) and reloaded, with
// their embeddings recomputed by e (nil selects the HashingEmbedder). On a
// partial load the store is returned together with an error wrapping
// ErrPartialLoad; on any other error the store is nil.
func NewInMemoryVectorStoreWithPersistence(ps persist.Store, e Embedder) (*InMemoryVectorStore, error) {
	s := NewInMemoryVectorStoreWithEmbedder(e)
	if err := s.EnablePersistence(ps); err != nil {
		if errors.Is(err, ErrPartialLoad) {
			return s, err
		}
		return nil, err
	}
	return s, nil
}

// EnablePersistence is EnablePersistenceContext with a background context.
func (s *InMemoryVectorStore) EnablePersistence(ps persist.Store) error {
	return s.EnablePersistenceContext(context.Background(), ps)
}

// EnablePersistenceContext loads the documents persisted in ps (re-embedding
// them) and writes every later Add/Remove through to ps. Documents already
// in memory are written to ps and win over persisted copies with the same
// key. On an error that does not wrap ErrPartialLoad the index is unchanged
// and persistence stays disabled.
func (s *InMemoryVectorStore) EnablePersistenceContext(ctx context.Context, ps persist.Store) error {
	if ps == nil {
		return errors.New("rag: nil persist store")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.ps != nil {
		return errPersistenceEnabled
	}
	stored, undecodable, err := loadPersistedDocuments(ps, VectorStoreBucket)
	if err != nil {
		return err
	}

	// Write the documents currently in memory (writers are blocked by
	// writeMu, so this snapshot stays current).
	s.mu.RLock()
	current := make(map[string]persistedDocument, len(s.docs))
	for key, e := range s.docs {
		current[key] = newPersistedDocument(e.doc, e.seq)
	}
	s.mu.RUnlock()
	for key, d := range current {
		if err := ps.Put(VectorStoreBucket, key, d); err != nil {
			return fmt.Errorf("rag: persist document: %w", err)
		}
	}

	type prepared struct {
		key string
		doc Document
		vec []float64
	}
	var ready []prepared
	var failed []string
	var embedErr error
	for _, ld := range stored {
		if _, inMemory := current[ld.key]; inMemory {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		doc := ld.doc.document()
		vec, err := s.embedder.Embed(ctx, doc.Content)
		if err != nil {
			failed = append(failed, ld.key)
			if embedErr == nil {
				embedErr = err
			}
			continue
		}
		ready = append(ready, prepared{key: ld.key, doc: doc, vec: vec})
	}

	s.mu.Lock()
	for _, p := range ready {
		s.docs[p.key] = &vectorEntry{doc: p.doc, vector: p.vec, seq: s.nextSeq}
		s.nextSeq++
	}
	s.mu.Unlock()
	s.ps = ps
	return partialLoadError(VectorStoreBucket, undecodable, failed, embedErr)
}

// NewInMemoryKeywordIndexWithPersistence creates a keyword index whose
// documents are persisted in ps (bucket KeywordIndexBucket) and re-indexed on
// load. On a partial load the index is returned together with an error
// wrapping ErrPartialLoad; on any other error the index is nil.
func NewInMemoryKeywordIndexWithPersistence(ps persist.Store) (*InMemoryKeywordIndex, error) {
	i := NewInMemoryKeywordIndex()
	if err := i.EnablePersistence(ps); err != nil {
		if errors.Is(err, ErrPartialLoad) {
			return i, err
		}
		return nil, err
	}
	return i, nil
}

// EnablePersistence loads the documents persisted in ps (re-indexing them)
// and writes every later Add/Remove through to ps. Documents already in
// memory are written to ps and win over persisted copies with the same key.
// On an error that does not wrap ErrPartialLoad the index is unchanged and
// persistence stays disabled.
func (i *InMemoryKeywordIndex) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("rag: nil persist store")
	}
	i.writeMu.Lock()
	defer i.writeMu.Unlock()
	if i.ps != nil {
		return errPersistenceEnabled
	}
	stored, undecodable, err := loadPersistedDocuments(ps, KeywordIndexBucket)
	if err != nil {
		return err
	}
	i.mu.RLock()
	current := make(map[string]persistedDocument, len(i.docs))
	for key, e := range i.docs {
		current[key] = newPersistedDocument(e.doc, e.seq)
	}
	i.mu.RUnlock()
	for key, d := range current {
		if err := ps.Put(KeywordIndexBucket, key, d); err != nil {
			return fmt.Errorf("rag: persist document: %w", err)
		}
	}
	i.mu.Lock()
	for _, ld := range stored {
		if _, inMemory := current[ld.key]; inMemory {
			continue
		}
		i.insertLocked(ld.key, newKeywordEntry(ld.doc.document(), i.nextSeq))
		i.nextSeq++
	}
	i.mu.Unlock()
	i.ps = ps
	return partialLoadError(KeywordIndexBucket, undecodable, nil, nil)
}
