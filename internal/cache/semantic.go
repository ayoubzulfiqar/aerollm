package cache

import (
	
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// SimpleVector is a lightweight text-derived vector placeholder.
type SimpleVector struct {
	Key       string
	Tokens    []string
	Vector    []float64
	Response  []byte
	CreatedAt time.Time
	TTL       time.Duration
}

// SemanticCache provides simple semantic-like search over cached responses.
type SemanticCache struct {
	entries []SimpleVector
	mu      sync.RWMutex
	prefix  string
}

// NewSemanticCache creates a new SemanticCache.
func NewSemanticCache(prefix string) *SemanticCache {
	return &SemanticCache{prefix: prefix}
}

// tokenize splits text into lowercase tokens.
func tokenize(text string) []string {
	text = strings.ToLower(text)
	parts := strings.FieldsFunc(text, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'))
	})
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

// Sort caches by similarity to the query for inspection/debugging.
func (s *SemanticCache) Sort(query string) []SimpleVector {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queryTokens := tokenize(query)
	queryVec := buildVector(queryTokens)

	out := append([]SimpleVector(nil), s.entries...)
	sort.SliceStable(out, func(i, j int) bool {
		si := cosineSimilarity(queryVec, out[i].Vector)
		sj := cosineSimilarity(queryVec, out[j].Vector)
		return si > sj
	})
	return out
}

// FormatEntry returns a readable representation.
func FormatEntry(e SimpleVector) string {
	return fmt.Sprintf("key=%s tokens=%d created=%s", e.Key, len(e.Tokens), e.CreatedAt.Format(time.RFC3339))
}
