package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// MemoryProvider is the interface for agent memory backends.
type MemoryProvider interface {
	Remember(ctx context.Context, conversationID string, message models.Message) error
	Recall(ctx context.Context, conversationID string, limit int) ([]models.Message, error)
	Summarize(ctx context.Context, conversationID string) (string, error)
}

const (
	// DefaultMaxMessagesPerConversation bounds a single conversation's history.
	DefaultMaxMessagesPerConversation = 1000
	// DefaultMaxConversations bounds the number of conversations kept.
	DefaultMaxConversations = 10000
)

// MessageMemory stores conversation history in memory. It is safe for
// concurrent use and bounded: each conversation keeps at most
// MaxMessagesPerConversation messages (oldest dropped first) and at most
// MaxConversations conversations are kept (least recently updated evicted).
type MessageMemory struct {
	mu      sync.RWMutex
	history map[string][]models.Message
	touched map[string]time.Time

	MaxMessagesPerConversation int
	MaxConversations           int
}

// NewMessageMemory creates a new in-memory conversation history store.
func NewMessageMemory() *MessageMemory {
	return &MessageMemory{
		history:                    make(map[string][]models.Message),
		touched:                    make(map[string]time.Time),
		MaxMessagesPerConversation: DefaultMaxMessagesPerConversation,
		MaxConversations:           DefaultMaxConversations,
	}
}

// Remember appends a message to conversation history.
func (m *MessageMemory) Remember(ctx context.Context, conversationID string, message models.Message) error {
	_ = ctx
	if conversationID == "" {
		return fmt.Errorf("conversation id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.history == nil {
		m.history = make(map[string][]models.Message)
	}
	if m.touched == nil {
		m.touched = make(map[string]time.Time)
	}
	if _, exists := m.history[conversationID]; !exists {
		m.evictLocked()
	}
	h := append(m.history[conversationID], cloneMessage(message))
	if max := m.MaxMessagesPerConversation; max > 0 && len(h) > max {
		h = append([]models.Message(nil), h[len(h)-max:]...)
	}
	m.history[conversationID] = h
	m.touched[conversationID] = time.Now()
	return nil
}

// evictLocked drops the least recently updated conversation when the store
// is full. Callers must hold m.mu.
func (m *MessageMemory) evictLocked() {
	max := m.MaxConversations
	if max <= 0 || len(m.history) < max {
		return
	}
	var oldestID string
	var oldest time.Time
	for id, ts := range m.touched {
		if oldestID == "" || ts.Before(oldest) {
			oldestID, oldest = id, ts
		}
	}
	delete(m.history, oldestID)
	delete(m.touched, oldestID)
}

// Recall returns (a copy of) the most recent messages for a conversation.
// limit <= 0 returns the whole history.
func (m *MessageMemory) Recall(ctx context.Context, conversationID string, limit int) ([]models.Message, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	history := m.history[conversationID]
	if limit > 0 && limit < len(history) {
		history = history[len(history)-limit:]
	}
	out := make([]models.Message, len(history))
	for i, msg := range history {
		out[i] = cloneMessage(msg)
	}
	return out, nil
}

// Summarize returns an extractive summary of the conversation: message counts
// per role, the first user request and the latest exchanges, each truncated.
func (m *MessageMemory) Summarize(ctx context.Context, conversationID string) (string, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	history := m.history[conversationID]
	if len(history) == 0 {
		return "", nil
	}
	counts := map[models.MessageRole]int{}
	for _, msg := range history {
		counts[msg.Role]++
	}
	roles := make([]string, 0, len(counts))
	for role, n := range counts {
		roles = append(roles, fmt.Sprintf("%d %s", n, role))
	}
	sort.Strings(roles)

	var sb strings.Builder
	fmt.Fprintf(&sb, "Conversation %s: %d messages (%s).", conversationID, len(history), strings.Join(roles, ", "))
	for _, msg := range history {
		if msg.Role == models.RoleUser && messageText(msg) != "" {
			fmt.Fprintf(&sb, " First request: %q.", clip(messageText(msg), 200))
			break
		}
	}
	start := len(history) - 4
	if start < 0 {
		start = 0
	}
	sb.WriteString(" Recent:")
	for _, msg := range history[start:] {
		text := messageText(msg)
		if text == "" && len(msg.ToolCalls) > 0 {
			names := make([]string, len(msg.ToolCalls))
			for i, tc := range msg.ToolCalls {
				names[i] = tc.Function.Name
			}
			text = "called tools: " + strings.Join(names, ", ")
		}
		fmt.Fprintf(&sb, " [%s] %s", msg.Role, clip(text, 160))
	}
	return sb.String(), nil
}

func messageText(msg models.Message) string {
	if msg.Content != nil {
		return strings.TrimSpace(*msg.Content)
	}
	if msg.ToolResult != nil {
		return strings.TrimSpace(*msg.ToolResult)
	}
	return ""
}

func clip(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "..."
}

// cloneMessage deep-copies the pointer and slice fields of a message so that
// stored history cannot be mutated through caller-held references.
func cloneMessage(m models.Message) models.Message {
	cp := m
	if m.Content != nil {
		v := *m.Content
		cp.Content = &v
	}
	if m.Name != nil {
		v := *m.Name
		cp.Name = &v
	}
	if m.ToolCallID != nil {
		v := *m.ToolCallID
		cp.ToolCallID = &v
	}
	if m.ToolResult != nil {
		v := *m.ToolResult
		cp.ToolResult = &v
	}
	if m.ToolCalls != nil {
		cp.ToolCalls = append([]models.ToolCall(nil), m.ToolCalls...)
	}
	if m.CacheControl != nil {
		v := *m.CacheControl
		cp.CacheControl = &v
	}
	return cp
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
	ID      string
	Score   float64
	Payload map[string]interface{}
}

// simpleEmbeddingProvider implements EmbeddingProvider with the hashing trick:
// every token is hashed into one of `dimensions` buckets (with a hash-derived
// sign to reduce collision bias) and the vector is L2-normalised. It needs no
// vocabulary, so it is stateless, deterministic and safe for concurrent use.
type simpleEmbeddingProvider struct {
	dimensions int
}

// NewSimpleEmbeddingProvider creates a simple embedding provider.
func NewSimpleEmbeddingProvider() *simpleEmbeddingProvider {
	return &simpleEmbeddingProvider{dimensions: 256}
}

func (p *simpleEmbeddingProvider) Embed(ctx context.Context, text string) ([]float64, error) {
	_ = ctx
	dims := p.dimensions
	if dims <= 0 {
		dims = 256
	}
	vector := make([]float64, dims)
	for _, word := range tokenize(text) {
		h := fnv.New64a()
		_, _ = h.Write([]byte(word))
		sum := h.Sum64()
		idx := int(sum % uint64(dims))
		sign := 1.0
		if (sum>>63)&1 == 1 {
			sign = -1.0
		}
		vector[idx] += sign
	}
	var norm float64
	for _, v := range vector {
		norm += v * v
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for i := range vector {
			vector[i] /= norm
		}
	}
	return vector, nil
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
	if id == "" {
		return fmt.Errorf("vector id is required")
	}
	for _, v := range vector {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("vector contains NaN or Inf")
		}
	}
	clone := make([]float64, len(vector))
	copy(clone, vector)
	p := make(map[string]interface{}, len(payload))
	for k, v := range payload {
		p[k] = v
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vectors[id] = clone
	s.payload[id] = p
	return nil
}

// Search returns the hits with a positive cosine similarity to vector, best
// first (ties broken by ID). limit <= 0 returns all positive hits.
func (s *inMemoryVectorStore) Search(_ context.Context, vector []float64, limit int) ([]VectorHit, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hits := make([]VectorHit, 0, len(s.vectors))
	for id, candidate := range s.vectors {
		score := cosineSimilarity(vector, candidate)
		if score <= 0 {
			continue
		}
		p := make(map[string]interface{}, len(s.payload[id]))
		for k, v := range s.payload[id] {
			p[k] = v
		}
		hits = append(hits, VectorHit{ID: id, Score: score, Payload: p})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
	if limit > 0 && limit < len(hits) {
		hits = hits[:limit]
	}
	return hits, nil
}

// cosineSimilarity returns the cosine of the angle between a and b, or 0 when
// the vectors are empty, of different dimensions, zero, or non-finite.
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
	return sim
}

// inMemoryVectorMemory implements VectorMemory with pluggable embedding + vector store.
type inMemoryVectorMemory struct {
	mu       sync.RWMutex
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
	v.mu.Lock()
	defer v.mu.Unlock()
	v.embedder = provider
}

// SetVectorStore replaces the vector store.
func (v *inMemoryVectorMemory) SetVectorStore(store VectorStore) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.store = store
}

func (v *inMemoryVectorMemory) deps() (EmbeddingProvider, VectorStore) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.embedder, v.store
}

// Upsert embeds the message content and stores it. Identical messages in the
// same conversation are de-duplicated; distinct messages never overwrite each
// other.
func (v *inMemoryVectorMemory) Upsert(ctx context.Context, conversationID string, message models.Message) error {
	embedder, store := v.deps()
	if embedder == nil || store == nil {
		return fmt.Errorf("vector memory not initialized")
	}
	text := messageText(message)
	if text == "" {
		return nil
	}
	vector, err := embedder.Embed(ctx, text)
	if err != nil {
		return err
	}
	payload := map[string]interface{}{
		"conversation_id": conversationID,
		"role":            string(message.Role),
		"content":         text,
	}
	sum := sha256.Sum256([]byte(conversationID + "\x00" + string(message.Role) + "\x00" + text))
	return store.Upsert(ctx, conversationID+"-"+hex.EncodeToString(sum[:12]), vector, payload)
}

// Search embeds the query and returns the best matching messages of the
// given conversation.
func (v *inMemoryVectorMemory) Search(ctx context.Context, conversationID string, query string, limit int) ([]models.Message, error) {
	embedder, store := v.deps()
	if embedder == nil || store == nil {
		return nil, fmt.Errorf("vector memory not initialized")
	}
	vector, err := embedder.Embed(ctx, query)
	if err != nil {
		return nil, err
	}
	// The store is shared across conversations and cannot filter, so fetch a
	// wider candidate set before filtering by conversation.
	candidates := 0
	if limit > 0 {
		candidates = limit * 8
		if candidates < 64 {
			candidates = 64
		}
	}
	hits, err := store.Search(ctx, vector, candidates)
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
		c := content
		out = append(out, models.Message{Role: models.MessageRole(role), Content: &c})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}
