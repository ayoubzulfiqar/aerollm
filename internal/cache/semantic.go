package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// EmbeddingProvider is the minimal interface needed to request embeddings
// from the Universal Provider chain. This avoids importing the universal
// package directly (avoids circular dependency).
type EmbeddingProvider interface {
	Embedding(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error)
}

// SimpleVector is a lightweight text-derived vector placeholder.
// Kept for backward compatibility with the original SemanticCache.
type SimpleVector struct {
	Key       string       `json:"key"`
	Tokens    []string     `json:"tokens"`
	Vector    []float64    `json:"vector"`
	Response  []byte       `json:"response"`
	CreatedAt time.Time    `json:"created_at"`
	TTL       time.Duration `json:"ttl"`
}

// SemanticCache provides simple semantic-like search over cached responses.
// This is the legacy bag-of-tokens-based cache, kept for backward compatibility.
type SemanticCache struct {
	entries []SimpleVector
	mu      sync.RWMutex
	prefix  string
}

// NewSemanticCache creates a new SemanticCache (legacy).
func NewSemanticCache(prefix string) *SemanticCache {
	return &SemanticCache{prefix: prefix}
}

// tokenize splits text into lowercase tokens.
func tokenize(text string) []string {
	text = lower(text)
	parts := splitFields(text)
	out := make([]string, 0, len(parts))
	seen := make(map[string]bool)
	for _, p := range parts {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

// lower converts a string to lowercase without importing strings.
func lower(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			out[i] = c + 32
		} else {
			out[i] = c
		}
	}
	return string(out)
}

// splitFields splits on non-alphanumeric characters.
func splitFields(s string) []string {
	var out []string
	var cur []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			cur = append(cur, c)
		} else if len(cur) > 0 {
			out = append(out, string(cur))
			cur = cur[:0]
		}
	}
	if len(cur) > 0 {
		out = append(out, string(cur))
	}
	return out
}

// cosineSimilarity returns cosine similarity between two equal-length vectors.
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

// buildVector builds a simple bag-of-tokens vector.
func buildVector(tokens []string) []float64 {
	v := make([]float64, len(tokens))
	for i := range v {
		v[i] = 1.0 / float64(i+1)
	}
	return v
}

// Search finds the closest cached entry above the threshold.
func (s *SemanticCache) Search(query string, threshold float64) (*CacheEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queryTokens := tokenize(query)
	queryVec := buildVector(queryTokens)

	best := -1.0
	bestIdx := -1
	for i, entry := range s.entries {
		if time.Since(entry.CreatedAt) > entry.TTL {
			continue
		}
		score := cosineSimilarity(queryVec, entry.Vector)
		if score > best {
			best = score
			bestIdx = i
		}
	}

	if bestIdx >= 0 && best >= threshold {
		return &CacheEntry{
			Key:      s.entries[bestIdx].Key,
			Response: s.entries[bestIdx].Response,
			Semantic: true,
		}, nil
	}
	return nil, nil
}

// Upsert adds or updates a cached response.
func (s *SemanticCache) Upsert(key, query string, resp []byte, ttl time.Duration) error {
	tokens := tokenize(query)
	vector := buildVector(tokens)

	s.mu.Lock()
	defer s.mu.Unlock()

	entry := SimpleVector{
		Key:       key,
		Tokens:    tokens,
		Vector:    vector,
		Response:  resp,
		CreatedAt: time.Now(),
		TTL:       ttl,
	}

	for i := range s.entries {
		if s.entries[i].Key == key {
			s.entries[i] = entry
			return nil
		}
	}
	s.entries = append(s.entries, entry)
	return nil
}

// Export exports all semantic entries for persistence.
func (s *SemanticCache) Export() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.entries)
}

// Import loads semantic entries from exported bytes.
func (s *SemanticCache) Import(data []byte) error {
	var entries []SimpleVector
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = entries
	return nil
}

// Stats returns semantic cache statistics.
func (s *SemanticCache) Stats() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	active := 0
	for _, e := range s.entries {
		if time.Since(e.CreatedAt) <= e.TTL {
			active++
		}
	}
	return map[string]int{
		"total_entries":  len(s.entries),
		"active_entries": active,
	}
}

// PurgeExpired removes expired entries.
func (s *SemanticCache) PurgeExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	before := len(s.entries)
	filtered := s.entries[:0]
	for _, e := range s.entries {
		if time.Since(e.CreatedAt) <= e.TTL {
			filtered = append(filtered, e)
		}
	}
	s.entries = filtered
	return before - len(s.entries)
}

// Clear removes all entries from the semantic cache.
func (s *SemanticCache) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make([]SimpleVector, 0)
	return nil
}

// Sort caches by similarity to the query for inspection/debugging.
func (s *SemanticCache) Sort(query string) []SimpleVector {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queryTokens := tokenize(query)
	queryVec := buildVector(queryTokens)

	out := append([]SimpleVector(nil), s.entries...)
	sortByScore(out, queryVec)
	return out
}

// sortByScore sorts entries by descending cosine similarity.
func sortByScore(entries []SimpleVector, queryVec []float64) {
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0; j-- {
			ci := cosineSimilarity(queryVec, entries[j].Vector)
			cj := cosineSimilarity(queryVec, entries[j-1].Vector)
			if ci > cj {
				entries[j], entries[j-1] = entries[j-1], entries[j]
			} else {
				break
			}
		}
	}
}

// FormatEntry returns a readable representation.
func FormatEntry(e SimpleVector) string {
	return fmt.Sprintf("key=%s tokens=%d created=%s", e.Key, len(e.Tokens), e.CreatedAt.Format(time.RFC3339))
}

// =========================================================================
// Production Vector-Semantic Cache
// =========================================================================

// VectorSemanticEntry stores an embedded prompt alongside its cached response.
type VectorSemanticEntry struct {
	Key        string                 `json:"key"`
	Prompt     string                 `json:"prompt"`
	Vector     []float64              `json:"vector"`
	Response   []byte                 `json:"response"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
	ExpiresAt  time.Time              `json:"expires_at"`
}

// VectorSemanticCache is a production-grade semantic cache that uses an
// embedding model to embed user prompts and performs cosine-similarity
// search to find semantically equivalent cached responses.
// If similarity >= threshold (default 0.95), the cached response is returned
// immediately, bypassing the LLM provider entirely.
type VectorSemanticCache struct {
	mu        sync.RWMutex
	entries   []VectorSemanticEntry
	prefix    string
	ttl       time.Duration
	threshold float64
	embedder  EmbeddingProvider
}

// NewVectorSemanticCache creates a new production semantic cache.
func NewVectorSemanticCache(prefix string, ttl time.Duration, threshold float64, embedder EmbeddingProvider) *VectorSemanticCache {
	if threshold <= 0 {
		threshold = 0.95
	}
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &VectorSemanticCache{
		prefix:    prefix,
		ttl:       ttl,
		threshold: threshold,
		embedder:  embedder,
		entries:   make([]VectorSemanticEntry, 0),
	}
}

// CosineSimilarity returns the cosine similarity between two float64 vectors.
func CosineSimilarity(a, b []float64) float64 {
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

// GetEmbedder returns the configured embedding provider.
func (s *VectorSemanticCache) GetEmbedder() EmbeddingProvider {
	return s.embedder
}

// SetEmbedder updates the embedding provider (useful for hot-reload).
func (s *VectorSemanticCache) SetEmbedder(ep EmbeddingProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.embedder = ep
}

// Search searches for a semantically similar cached response.
// Embeds the query, computes cosine similarity against all entries,
// and returns a hit if best score >= threshold.
func (s *VectorSemanticCache) Search(ctx context.Context, query string) (*VectorSemanticEntry, error) {
	s.mu.RLock()
	embedder := s.embedder
	threshold := s.threshold
	s.mu.RUnlock()

	if embedder == nil {
		return nil, nil
	}

	req := &models.EmbeddingRequest{Input: query}
	resp, err := embedder.Embedding(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(resp.Data) == 0 {
		return nil, nil
	}
	queryVec := resp.Data[0].Embedding

	s.mu.RLock()
	defer s.mu.RUnlock()
	bestScore := -1.0
	bestIdx := -1
	for i, entry := range s.entries {
		if time.Since(entry.CreatedAt) > s.ttl {
			continue
		}
		score := CosineSimilarity(queryVec, entry.Vector)
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}

	if bestIdx >= 0 && bestScore >= threshold {
		if time.Since(s.entries[bestIdx].CreatedAt) <= s.ttl {
			copied := s.entries[bestIdx]
			return &copied, nil
		}
	}
	return nil, nil
}

// Upsert stores a prompt embedding and its response in the semantic cache.
func (s *VectorSemanticCache) Upsert(ctx context.Context, key, query string, resp []byte, metadata map[string]interface{}) error {
	s.mu.RLock()
	embedder := s.embedder
	s.mu.RUnlock()

	if embedder != nil {
		req := &models.EmbeddingRequest{Input: query}
		embResp, err := embedder.Embedding(ctx, req)
		if err == nil && len(embResp.Data) > 0 && len(embResp.Data[0].Embedding) > 0 {
			s.mu.Lock()
			defer s.mu.Unlock()
			entry := VectorSemanticEntry{
				Key:       key,
				Prompt:    query,
				Vector:    embResp.Data[0].Embedding,
				Response:  resp,
				Metadata:  metadata,
				CreatedAt: time.Now().UTC(),
				ExpiresAt: time.Now().Add(s.ttl),
			}
			for i := range s.entries {
				if s.entries[i].Key == key {
					s.entries[i] = entry
					return nil
				}
			}
			s.entries = append(s.entries, entry)
			return nil
		}
	}

	// Fallback: store without embedding if embedder is unavailable.
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := VectorSemanticEntry{
		Key:       key,
		Prompt:    query,
		Response:  resp,
		Metadata:  metadata,
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().Add(s.ttl),
	}
	for i := range s.entries {
		if s.entries[i].Key == key {
			s.entries[i] = entry
			return nil
		}
	}
	s.entries = append(s.entries, entry)
	return nil
}

// Stats returns cache statistics.
func (s *VectorSemanticCache) Stats() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	active := 0
	for _, e := range s.entries {
		if time.Since(e.CreatedAt) <= s.ttl {
			active++
		}
	}
	return map[string]int{
		"total_entries":  len(s.entries),
		"active_entries": active,
	}
}

// PurgeExpired removes expired entries and returns count removed.
func (s *VectorSemanticCache) PurgeExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	before := len(s.entries)
	filtered := s.entries[:0]
	for _, e := range s.entries {
		if time.Since(e.CreatedAt) <= s.ttl {
			filtered = append(filtered, e)
		}
	}
	s.entries = filtered
	return before - len(s.entries)
}

// Clear removes all entries from the semantic cache.
func (s *VectorSemanticCache) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make([]VectorSemanticEntry, 0)
	return nil
}

// Inspect returns a paginated list of semantic cache entries (metadata only — no payloads).
func (s *VectorSemanticCache) Inspect(cursor uint64, pageSize int) ([]CacheInspectEntry, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	start := int(cursor)
	if start >= len(s.entries) {
		return nil, 0
	}
	end := start + pageSize
	if end > len(s.entries) {
		end = len(s.entries)
	}

	var entries []CacheInspectEntry
	for i := start; i < end; i++ {
		e := s.entries[i]
		entries = append(entries, CacheInspectEntry{
			Key:        e.Key,
			TokenCount: len(e.Response), // Approximate token count from response size
			Semantic:   true,
			CreatedAt:  e.CreatedAt,
		})
	}

	return entries, uint64(end)
}

// Export serializes all entries for persistence.
func (s *VectorSemanticCache) Export() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.entries)
}

// Import deserializes entries from exported bytes.
func (s *VectorSemanticCache) Import(data []byte) error {
	var entries []VectorSemanticEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = entries
	return nil
}
