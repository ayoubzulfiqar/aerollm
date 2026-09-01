package agent

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"unicode"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// MemoryProvider is the interface for agent memory backends.
type MemoryProvider interface {
	Remember(ctx context.Context, conversationID string, message models.Message) error
	Recall(ctx context.Context, conversationID string, limit int) ([]models.Message, error)
	Summarize(ctx context.Context, conversationID string) (string, error)
}

// MessageMemory stores conversation history in memory.
type MessageMemory struct {
	mu      sync.RWMutex
	history map[string][]models.Message
}

// NewMessageMemory creates a new in-memory conversation history store.
func NewMessageMemory() *MessageMemory {
	return &MessageMemory{history: make(map[string][]models.Message)}
}

// Remember appends a message to conversation history.
func (m *MessageMemory) Remember(ctx context.Context, conversationID string, message models.Message) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	m.history[conversationID] = append(m.history[conversationID], message)
	return nil
}

// Recall returns recent messages for a conversation.
func (m *MessageMemory) Recall(ctx context.Context, conversationID string, limit int) ([]models.Message, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	history := m.history[conversationID]
	if limit <= 0 || limit > len(history) {
		return history, nil
	}
	return history[len(history)-limit:], nil
}

// Summarize returns a simple summary of the conversation.
func (m *MessageMemory) Summarize(ctx context.Context, conversationID string) (string, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	history := m.history[conversationID]
	if len(history) == 0 {
		return "", nil
	}
	summary := fmt.Sprintf("conversation %s has %d messages", conversationID, len(history))
	return summary, nil
}

// VectorMemory is the interface for long-term vector-backed memory.
type VectorMemory interface {
	Upsert(ctx context.Context, conversationID string, message models.Message) error
	Search(ctx context.Context, conversationID string, query string, limit int) ([]models.Message, error)
}

// EmbeddingProvider generates vector embeddings for text.
type EmbeddingProvider interface {
	Embed(ctx context.Context, text string) ([]float64, error)
}

// VectorStore stores and searches dense vectors with associated payloads.
type VectorStore interface {
	Upsert(ctx context.Context, id string, vector []float64, payload map[string]interface{}) error
	Search(ctx context.Context, vector []float64, limit int) ([]VectorHit, error)
}

// VectorHit represents a search result from a vector store.
type VectorHit struct {
	ID       string
	Score    float64
	Payload  map[string]interface{}
}

// simpleEmbeddingProvider implements EmbeddingProvider using a bag-of-words vector.
type simpleEmbeddingProvider struct {
	dimensions int
	vocabulary map[string]int
	mu         sync.RWMutex
}

// NewSimpleEmbeddingProvider creates a simple embedding provider.
func NewSimpleEmbeddingProvider() *simpleEmbeddingProvider {
	return &simpleEmbeddingProvider{
		dimensions: 256,
		vocabulary: make(map[string]int),
	}
}

func (p *simpleEmbeddingProvider) Embed(ctx context.Context, text string) ([]float64, error) {
	_ = ctx
	words := tokenize(text)
	vector := make([]float64, p.dimensions)
	p.mu.RLock()
	for _, word := range words {
		if idx, ok := p.vocabulary[word]; ok {
			vector[idx] += 1.0
		}
	}
	p.mu.RUnlock()
	return vector, nil
}

func (p *simpleEmbeddingProvider) ensureWord(word string) int {
	p.mu.RLock()
	idx, ok := p.vocabulary[word]
	p.mu.RUnlock()
	if ok {
		return idx
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if idx, ok := p.vocabulary[word]; ok {
		return idx
	}
	idx = len(p.vocabulary)
	if idx >= p.dimensions {
		idx = p.dimensions - 1
	}
	p.vocabulary[word] = idx
	return idx
}

func tokenize(text string) []string {
	text = strings.ToLower(text)
	var words []string
	var b strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else if b.Len() > 0 {
			words = append(words, b.String())
			b.Reset()
		}
	}
	if b.Len() > 0 {
		words = append(words, b.String())
	}
	return words
}

// inMemoryVectorStore implements VectorStore using in-memory cosine search.
type inMemoryVectorStore struct {
	mu      sync.RWMutex
	vectors map[string][]float64
	payload map[string]map[string]interface{}
}

// NewInMemoryVectorStore creates a new in-memory vector store.
func NewInMemoryVectorStore() *inMemoryVectorStore {
	return &inMemoryVectorStore{
		vectors: make(map[string][]float64),
		payload: make(map[string]map[string]interface{}),
	}
}

func (s *inMemoryVectorStore) Upsert(_ context.Context, id string, vector []float64, payload map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := make([]float64, len(vector))
	copy(clone, vector)
	s.vectors[id] = clone
	if payload == nil {
		s.payload[id] = make(map[string]interface{})
	} else {
		s.payload[id] = payload
	}
	return nil
}

func (s *inMemoryVectorStore) Search(_ context.Context, vector []float64, limit int) ([]VectorHit, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var hits []VectorHit
	for id, candidate := range s.vectors {
		score := cosineSimilarity(vector, candidate)
		hits = append(hits, VectorHit{ID: id, Score: score, Payload: s.payload[id]})
	}
	if limit <= 0 || limit > len(hits) {
		limit = len(hits)
	}
	selectionSortTopHits(hits, limit)
	return hits[:limit], nil
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

func selectionSortTopHits(hits []VectorHit, limit int) {
	for i := 0; i < len(hits) && i < limit; i++ {
		maxIdx := i
		for j := i + 1; j < len(hits); j++ {
			if hits[j].Score > hits[maxIdx].Score {
				maxIdx = j
			}
		}
		if maxIdx != i {
			hits[i], hits[maxIdx] = hits[maxIdx], hits[i]
		}
	}
}

// inMemoryVectorMemory implements VectorMemory with pluggable embedding + vector store.
type inMemoryVectorMemory struct {
	embedder EmbeddingProvider
	store    VectorStore
}

// NewInMemoryVectorMemory creates a VectorMemory using simple embeddings and an in-memory store.
func NewInMemoryVectorMemory() VectorMemory {
	return &inMemoryVectorMemory{
		embedder: NewSimpleEmbeddingProvider(),
		store:    NewInMemoryVectorStore(),
	}
}

// SetEmbeddingProvider replaces the embedding provider.
func (v *inMemoryVectorMemory) SetEmbeddingProvider(provider EmbeddingProvider) {
	v.embedder = provider
}

// SetVectorStore replaces the vector store.
func (v *inMemoryVectorMemory) SetVectorStore(store VectorStore) {
	v.store = store
}

// Upsert embeds the message content and stores it.
func (v *inMemoryVectorMemory) Upsert(ctx context.Context, conversationID string, message models.Message) error {
	if v.embedder == nil || v.store == nil {
		return fmt.Errorf("vector memory not initialized")
	}
	text := ""
	if message.Content != nil {
		text = *message.Content
	}
	vector, err := v.embedder.Embed(ctx, text)
	if err != nil {
		return err
	}
	payload := map[string]interface{}{
		"conversation_id": conversationID,
		"role":            message.Role,
		"content":         text,
	}
	return v.store.Upsert(ctx, fmt.Sprintf("%s-%s-%d", conversationID, message.Role, len(text)), vector, payload)
}

// Search embeds the query and returns the top matching messages.
func (v *inMemoryVectorMemory) Search(ctx context.Context, conversationID string, query string, limit int) ([]models.Message, error) {
	if v.embedder == nil || v.store == nil {
		return nil, fmt.Errorf("vector memory not initialized")
	}
	vector, err := v.embedder.Embed(ctx, query)
	if err != nil {
		return nil, err
	}
	hits, err := v.store.Search(ctx, vector, limit)
	if err != nil {
		return nil, err
	}
	var out []models.Message
	for _, hit := range hits {
		payload := hit.Payload
		if payload == nil {
			continue
		}
		cid, _ := payload["conversation_id"].(string)
		if cid != conversationID {
			continue
		}
		role, _ := payload["role"].(string)
		content, _ := payload["content"].(string)
		msg := models.Message{Role: models.MessageRole(role), Content: &content}
		out = append(out, msg)
		if len(out) >= limit && limit > 0 {
			break
		}
	}
	return out, nil
}
