package cache

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// EmbeddingProvider is the minimal interface needed to request embeddings
// from the Universal Provider chain. This avoids importing the universal
// package directly (avoids circular dependency).
type EmbeddingProvider interface {
	Embedding(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error)
}

// SimpleVector is a lightweight text-derived vector.
// Kept for backward compatibility with the original SemanticCache.
type SimpleVector struct {
	Key       string        `json:"key"`
	Tokens    []string      `json:"tokens"`
	Vector    []float64     `json:"vector"`
	Response  []byte        `json:"response"`
	CreatedAt time.Time     `json:"created_at"`
	TTL       time.Duration `json:"ttl"`
}

// legacyMaxEntries bounds the legacy SemanticCache.
const legacyMaxEntries = 10_000

// SemanticCache provides simple lexical-similarity search over cached
// responses using hashed bag-of-words vectors. This is the legacy cache used
// by RedisCache.GetSemantic/SetSemantic; VectorSemanticCache is preferred.
type SemanticCache struct {
	entries []SimpleVector
	mu      sync.RWMutex
	prefix  string
	embed   *HashingEmbedder
}

// NewSemanticCache creates a new SemanticCache (legacy).
func NewSemanticCache(prefix string) *SemanticCache {
	return &SemanticCache{prefix: prefix, embed: NewHashingEmbedder(0)}
}

// tokenize splits text into unique lowercase alphanumeric tokens.
func tokenize(text string) []string {
	parts := wordTokens(text, maxEmbedInputBytes)
	out := make([]string, 0, len(parts))
	seen := make(map[string]bool, len(parts))
	for _, p := range parts {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

var defaultHashingEmbedder = NewHashingEmbedder(0)

func (s *SemanticCache) vector(text string) []float64 {
	e := s.embed
	if e == nil {
		e = defaultHashingEmbedder
	}
	return e.Vector(text)
}

// Search finds the closest unexpired cached entry at or above threshold.
// Non-positive or NaN thresholds default to 0.95.
func (s *SemanticCache) Search(query string, threshold float64) (*CacheEntry, error) {
	threshold = validThreshold(threshold, 0.95)
	queryVec := s.vector(query)

	s.mu.RLock()
	defer s.mu.RUnlock()

	best := -1.0
	bestIdx := -1
	now := time.Now()
	for i, entry := range s.entries {
		if now.Sub(entry.CreatedAt) > entry.TTL {
			continue
		}
		score := CosineSimilarity(queryVec, entry.Vector)
		if score > best {
			best = score
			bestIdx = i
		}
	}

	if bestIdx >= 0 && best >= threshold {
		e := s.entries[bestIdx]
		return &CacheEntry{
			Key:       e.Key,
			Response:  append([]byte(nil), e.Response...),
			CreatedAt: e.CreatedAt,
			TTL:       e.TTL,
			Semantic:  true,
		}, nil
	}
	return nil, nil
}

// Upsert adds or updates a cached response. The cache holds at most
// 10,000 entries; expired and then oldest entries are evicted first.
func (s *SemanticCache) Upsert(key, query string, resp []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return nil // would expire immediately
	}
	entry := SimpleVector{
		Key:       key,
		Tokens:    tokenize(query),
		Vector:    s.vector(query),
		Response:  append([]byte(nil), resp...),
		CreatedAt: time.Now(),
		TTL:       ttl,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.entries {
		if s.entries[i].Key == key {
			s.entries[i] = entry
			return nil
		}
	}
	if len(s.entries) >= legacyMaxEntries {
		s.purgeExpiredLocked()
		if len(s.entries) >= legacyMaxEntries {
			// Evict the oldest entry.
			oldest := 0
			for i := range s.entries {
				if s.entries[i].CreatedAt.Before(s.entries[oldest].CreatedAt) {
					oldest = i
				}
			}
			s.entries = append(s.entries[:oldest], s.entries[oldest+1:]...)
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

// Import loads semantic entries from exported bytes. Vectors are recomputed
// from the stored tokens (exports from older versions carried vectors that
// did not depend on content); entries beyond the cap are dropped.
func (s *SemanticCache) Import(data []byte) error {
	var entries []SimpleVector
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	if len(entries) > legacyMaxEntries {
		entries = entries[len(entries)-legacyMaxEntries:]
	}
	for i := range entries {
		entries[i].Vector = s.vector(strings.Join(entries[i].Tokens, " "))
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
	now := time.Now()
	for _, e := range s.entries {
		if now.Sub(e.CreatedAt) <= e.TTL {
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
	return s.purgeExpiredLocked()
}

func (s *SemanticCache) purgeExpiredLocked() int {
	before := len(s.entries)
	filtered := s.entries[:0]
	now := time.Now()
	for _, e := range s.entries {
		if now.Sub(e.CreatedAt) <= e.TTL {
			filtered = append(filtered, e)
		}
	}
	// Clear the tail so evicted responses can be garbage collected.
	for i := len(filtered); i < before; i++ {
		s.entries[i] = SimpleVector{}
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

// Sort returns the entries ordered by similarity to the query (debugging).
func (s *SemanticCache) Sort(query string) []SimpleVector {
	queryVec := s.vector(query)
	s.mu.RLock()
	out := append([]SimpleVector(nil), s.entries...)
	s.mu.RUnlock()
	sortByScore(out, queryVec)
	return out
}

// sortByScore sorts entries by descending cosine similarity.
func sortByScore(entries []SimpleVector, queryVec []float64) {
	scores := make(map[int]float64, len(entries))
	idx := make([]int, len(entries))
	for i := range entries {
		idx[i] = i
		scores[i] = CosineSimilarity(queryVec, entries[i].Vector)
	}
	sort.SliceStable(idx, func(a, b int) bool { return scores[idx[a]] > scores[idx[b]] })
	sorted := make([]SimpleVector, len(entries))
	for i, j := range idx {
		sorted[i] = entries[j]
	}
	copy(entries, sorted)
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
	Key       string                 `json:"key"`
	Namespace string                 `json:"namespace,omitempty"`
	Prompt    string                 `json:"prompt"`
	Vector    []float64              `json:"vector"`
	Response  []byte                 `json:"response"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
	ExpiresAt time.Time              `json:"expires_at"`
}

// VectorCacheOptions configures a VectorSemanticCache.
type VectorCacheOptions struct {
	Prefix    string
	TTL       time.Duration // default 15m
	Threshold float64       // default 0.95, clamped to (0,1]
	// Embedder produces prompt vectors. When nil the cache is disabled:
	// Search always misses and Upsert stores nothing. Pass
	// NewHashingEmbedder(0) for a deterministic local fallback.
	Embedder EmbeddingProvider
	// EmbeddingModel is sent as EmbeddingRequest.Model.
	EmbeddingModel string
	MaxEntries     int   // default 10,000 (LRU eviction)
	MaxBytes       int64 // default 256 MiB of cached responses (LRU eviction)
	MaxEntryBytes  int   // default 1 MiB; larger responses are not cached
	// MaxPromptBytes skips semantic caching for longer prompts, which would
	// otherwise be embedded lossily (default 32 KiB).
	MaxPromptBytes int
}

// SemanticStats is a typed snapshot of VectorSemanticCache statistics.
type SemanticStats struct {
	Entries       int     `json:"total_entries"`
	ActiveEntries int     `json:"active_entries"`
	Namespaces    int     `json:"namespaces"`
	Bytes         int64   `json:"bytes"`
	Hits          int64   `json:"hits"`
	Misses        int64   `json:"misses"`
	Evictions     int64   `json:"evictions"`
	HitRate       float64 `json:"hit_rate"`
	Enabled       bool    `json:"enabled"`
	// EmbedErrors counts failed embedding calls (served as misses when
	// they wrap ErrEmbeddingUnavailable).
	EmbedErrors int64 `json:"embed_errors"`
}

type vsNode struct {
	entry VectorSemanticEntry
	norm  float64
	size  int64
}

// VectorSemanticCache is a semantic cache that embeds prompts and performs
// cosine-similarity search, scoped by namespace. If similarity >= threshold
// the cached response is returned, bypassing the LLM provider. Entries
// expire after the TTL and are evicted LRU when MaxEntries/MaxBytes is hit.
// All methods are safe for concurrent use.
type VectorSemanticCache struct {
	mu        sync.RWMutex
	prefix    string
	ttl       time.Duration
	threshold float64
	embedder  EmbeddingProvider
	model     string

	maxEntries     int
	maxBytes       int64
	maxEntryBytes  int
	maxPromptBytes int

	lru   *list.List                          // front = most recently used; values *vsNode
	index map[string]map[string]*list.Element // namespace -> key -> element
	bytes int64

	hits        atomic.Int64
	misses      atomic.Int64
	evictions   atomic.Int64
	embedErrors atomic.Int64

	now func() time.Time
}

// NewVectorSemanticCache creates a new semantic cache. A nil embedder
// disables the cache (see VectorCacheOptions.Embedder).
func NewVectorSemanticCache(prefix string, ttl time.Duration, threshold float64, embedder EmbeddingProvider) *VectorSemanticCache {
	return NewVectorSemanticCacheWithOptions(VectorCacheOptions{Prefix: prefix, TTL: ttl, Threshold: threshold, Embedder: embedder})
}

// NewVectorSemanticCacheWithOptions creates a semantic cache with explicit limits.
func NewVectorSemanticCacheWithOptions(opts VectorCacheOptions) *VectorSemanticCache {
	if opts.TTL <= 0 {
		opts.TTL = 15 * time.Minute
	}
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = 10_000
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 256 << 20
	}
	if opts.MaxEntryBytes <= 0 {
		opts.MaxEntryBytes = 1 << 20
	}
	if opts.MaxPromptBytes <= 0 {
		opts.MaxPromptBytes = maxEmbedInputBytes
	}
	return &VectorSemanticCache{
		prefix:         opts.Prefix,
		ttl:            opts.TTL,
		threshold:      validThreshold(opts.Threshold, 0.95),
		embedder:       opts.Embedder,
		model:          opts.EmbeddingModel,
		maxEntries:     opts.MaxEntries,
		maxBytes:       opts.MaxBytes,
		maxEntryBytes:  opts.MaxEntryBytes,
		maxPromptBytes: opts.MaxPromptBytes,
		lru:            list.New(),
		index:          make(map[string]map[string]*list.Element),
		now:            time.Now,
	}
}

// CosineSimilarity returns the cosine similarity between two float64 vectors.
// Mismatched lengths, empty or zero vectors and non-finite results yield 0.
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
	s := dot / (math.Sqrt(na) * math.Sqrt(nb))
	if math.IsNaN(s) || math.IsInf(s, 0) {
		return 0
	}
	return s
}

func vectorNorm(v []float64) float64 {
	var n float64
	for _, x := range v {
		n += x * x
	}
	n = math.Sqrt(n)
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return 0
	}
	return n
}

// GetEmbedder returns the configured embedding provider.
func (s *VectorSemanticCache) GetEmbedder() EmbeddingProvider {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.embedder
}

// SetEmbedder replaces the embedding provider (useful for hot-reload).
// Vectors produced by different embedders are not comparable, so existing
// entries are discarded.
func (s *VectorSemanticCache) SetEmbedder(ep EmbeddingProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.embedder = ep
	s.clearLocked()
}

// Enabled reports whether an embedder is configured.
func (s *VectorSemanticCache) Enabled() bool {
	return s.GetEmbedder() != nil
}

// Threshold returns the similarity threshold.
func (s *VectorSemanticCache) Threshold() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.threshold
}

// embed returns the embedding for text, or (nil, nil) when the cache is
// disabled or the prompt is too large to embed faithfully.
func (s *VectorSemanticCache) embed(ctx context.Context, text string) ([]float64, error) {
	s.mu.RLock()
	embedder, model, maxPrompt := s.embedder, s.model, s.maxPromptBytes
	s.mu.RUnlock()
	if embedder == nil || text == "" || len(text) > maxPrompt {
		return nil, nil
	}
	resp, err := embedder.Embedding(ctx, &models.EmbeddingRequest{Model: model, Input: text})
	if err != nil {
		s.embedErrors.Add(1)
		return nil, err
	}
	if resp == nil || len(resp.Data) == 0 || len(resp.Data[0].Embedding) == 0 {
		s.embedErrors.Add(1)
		return nil, fmt.Errorf("%w: embedder returned no vector", ErrEmbeddingUnavailable)
	}
	return resp.Data[0].Embedding, nil
}

// Search searches the default (empty) namespace. See SearchNS.
func (s *VectorSemanticCache) Search(ctx context.Context, query string) (*VectorSemanticEntry, error) {
	return s.SearchNS(ctx, "", query)
}

// SearchNS embeds the query and returns the most similar unexpired entry in
// namespace ns whose similarity is >= the threshold, or (nil, nil) on a
// miss. Entries from other namespaces are never returned. The returned
// entry is a copy that the caller may modify.
func (s *VectorSemanticCache) SearchNS(ctx context.Context, ns, query string) (*VectorSemanticEntry, error) {
	if s == nil {
		return nil, nil
	}
	queryVec, err := s.embed(ctx, query)
	if err != nil {
		s.misses.Add(1)
		if errors.Is(err, ErrEmbeddingUnavailable) {
			return nil, nil // degrade to a miss
		}
		return nil, err
	}
	if queryVec == nil {
		return nil, nil
	}
	qnorm := vectorNorm(queryVec)
	if qnorm == 0 {
		s.misses.Add(1)
		return nil, nil
	}

	now := s.now()
	s.mu.RLock()
	threshold := s.threshold
	var best *list.Element
	bestScore := -1.0
	for _, el := range s.index[ns] {
		n := el.Value.(*vsNode)
		if !now.Before(n.entry.ExpiresAt) || n.norm == 0 || len(n.entry.Vector) != len(queryVec) {
			continue
		}
		var dot float64
		for i, x := range queryVec {
			dot += x * n.entry.Vector[i]
		}
		score := dot / (qnorm * n.norm)
		if score > bestScore {
			bestScore = score
			best = el
		}
	}
	var hit *VectorSemanticEntry
	if best != nil && bestScore >= threshold {
		hit = copyEntry(best.Value.(*vsNode).entry)
	}
	s.mu.RUnlock()

	if hit == nil {
		s.misses.Add(1)
		return nil, nil
	}
	s.hits.Add(1)
	s.mu.Lock()
	if el, ok := s.index[ns][hit.Key]; ok && el == best {
		s.lru.MoveToFront(el)
	}
	s.mu.Unlock()
	return hit, nil
}

func copyEntry(e VectorSemanticEntry) *VectorSemanticEntry {
	cp := e
	cp.Response = append([]byte(nil), e.Response...)
	cp.Vector = nil // callers never need the raw vector; avoid sharing it
	if e.Metadata != nil {
		cp.Metadata = make(map[string]interface{}, len(e.Metadata))
		for k, v := range e.Metadata {
			cp.Metadata[k] = v
		}
	}
	return &cp
}

// Upsert stores into the default (empty) namespace. See UpsertNS.
func (s *VectorSemanticCache) Upsert(ctx context.Context, key, query string, resp []byte, metadata map[string]interface{}) error {
	return s.UpsertNS(ctx, "", key, query, resp, metadata)
}

// UpsertNS embeds the prompt and stores the response under (ns, key). It is
// a no-op when the cache is disabled, the prompt is too long, or the
// response exceeds MaxEntryBytes. Embedding failures are returned.
func (s *VectorSemanticCache) UpsertNS(ctx context.Context, ns, key, query string, resp []byte, metadata map[string]interface{}) error {
	if s == nil || key == "" || len(resp) == 0 {
		return nil
	}
	s.mu.RLock()
	tooBig := len(resp) > s.maxEntryBytes
	s.mu.RUnlock()
	if tooBig {
		return nil
	}
	vec, err := s.embed(ctx, query)
	if err != nil {
		if errors.Is(err, ErrEmbeddingUnavailable) {
			return nil // nothing cached; the response was served anyway
		}
		return err
	}
	if vec == nil {
		return nil
	}
	for _, x := range vec {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			s.embedErrors.Add(1)
			return errors.New("cache: embedder returned non-finite vector")
		}
	}
	if vectorNorm(vec) == 0 {
		return nil // a zero vector can never match; do not store it
	}

	var meta map[string]interface{}
	if metadata != nil {
		meta = make(map[string]interface{}, len(metadata))
		for k, v := range metadata {
			meta[k] = v
		}
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := VectorSemanticEntry{
		Key:       key,
		Namespace: ns,
		Prompt:    query,
		Vector:    append([]float64(nil), vec...),
		Response:  append([]byte(nil), resp...),
		Metadata:  meta,
		CreatedAt: now.UTC(),
		ExpiresAt: now.Add(s.ttl),
	}
	s.insertLocked(entry)
	return nil
}

func entrySize(e *VectorSemanticEntry) int64 {
	return int64(len(e.Response) + len(e.Prompt) + 8*len(e.Vector) + len(e.Key) + len(e.Namespace) + 128)
}

func (s *VectorSemanticCache) insertLocked(entry VectorSemanticEntry) {
	node := &vsNode{entry: entry, norm: vectorNorm(entry.Vector), size: entrySize(&entry)}
	byKey := s.index[entry.Namespace]
	if byKey == nil {
		byKey = make(map[string]*list.Element)
		s.index[entry.Namespace] = byKey
	}
	if el, ok := byKey[entry.Key]; ok {
		old := el.Value.(*vsNode)
		s.bytes -= old.size
		el.Value = node
		s.bytes += node.size
		s.lru.MoveToFront(el)
	} else {
		byKey[entry.Key] = s.lru.PushFront(node)
		s.bytes += node.size
	}
	s.enforceLimitsLocked()
}

func (s *VectorSemanticCache) removeLocked(el *list.Element) {
	n := el.Value.(*vsNode)
	s.lru.Remove(el)
	s.bytes -= n.size
	if byKey := s.index[n.entry.Namespace]; byKey != nil {
		delete(byKey, n.entry.Key)
		if len(byKey) == 0 {
			delete(s.index, n.entry.Namespace)
		}
	}
}

func (s *VectorSemanticCache) enforceLimitsLocked() {
	if s.lru.Len() <= s.maxEntries && s.bytes <= s.maxBytes {
		return
	}
	// Expired entries go first, then least recently used.
	now := s.now()
	for el := s.lru.Back(); el != nil && (s.lru.Len() > s.maxEntries || s.bytes > s.maxBytes); {
		prev := el.Prev()
		if !now.Before(el.Value.(*vsNode).entry.ExpiresAt) {
			s.removeLocked(el)
			s.evictions.Add(1)
		}
		el = prev
	}
	for s.lru.Len() > 0 && (s.lru.Len() > s.maxEntries || s.bytes > s.maxBytes) {
		s.removeLocked(s.lru.Back())
		s.evictions.Add(1)
	}
}

// Delete removes one entry and reports whether it existed.
func (s *VectorSemanticCache) Delete(ns, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.index[ns][key]; ok {
		s.removeLocked(el)
		return true
	}
	return false
}

// ClearNamespace removes every entry of a namespace and returns the count.
func (s *VectorSemanticCache) ClearNamespace(ns string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKey := s.index[ns]
	n := 0
	for _, el := range byKey {
		s.removeLocked(el)
		n++
	}
	return n
}

// Len returns the number of stored entries (including expired ones not yet purged).
func (s *VectorSemanticCache) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lru.Len()
}

// Stats returns cache statistics ("total_entries", "active_entries",
// "hits", "misses", "evictions").
func (s *VectorSemanticCache) Stats() map[string]int {
	st := s.Snapshot()
	return map[string]int{
		"total_entries":  st.Entries,
		"active_entries": st.ActiveEntries,
		"hits":           int(st.Hits),
		"misses":         int(st.Misses),
		"evictions":      int(st.Evictions),
	}
}

// Snapshot returns typed cache statistics.
func (s *VectorSemanticCache) Snapshot() SemanticStats {
	now := s.now()
	s.mu.RLock()
	st := SemanticStats{
		Entries:    s.lru.Len(),
		Namespaces: len(s.index),
		Bytes:      s.bytes,
		Enabled:    s.embedder != nil,
	}
	for el := s.lru.Front(); el != nil; el = el.Next() {
		if now.Before(el.Value.(*vsNode).entry.ExpiresAt) {
			st.ActiveEntries++
		}
	}
	s.mu.RUnlock()
	st.Hits, st.Misses, st.Evictions = s.hits.Load(), s.misses.Load(), s.evictions.Load()
	st.EmbedErrors = s.embedErrors.Load()
	if total := st.Hits + st.Misses; total > 0 {
		st.HitRate = float64(st.Hits) / float64(total)
	}
	return st
}

// PurgeExpired removes expired entries and returns count removed.
func (s *VectorSemanticCache) PurgeExpired() int {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for el := s.lru.Front(); el != nil; {
		next := el.Next()
		if !now.Before(el.Value.(*vsNode).entry.ExpiresAt) {
			s.removeLocked(el)
			removed++
		}
		el = next
	}
	return removed
}

// StartJanitor purges expired entries every interval until ctx is done.
func (s *VectorSemanticCache) StartJanitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.PurgeExpired()
			}
		}
	}()
}

// Clear removes all entries from the semantic cache.
func (s *VectorSemanticCache) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLocked()
	return nil
}

func (s *VectorSemanticCache) clearLocked() {
	s.lru.Init()
	s.index = make(map[string]map[string]*list.Element)
	s.bytes = 0
}

// Inspect returns a page of entry metadata (never payloads or prompts),
// ordered by creation time. cursor is the offset returned by the previous
// call; the returned cursor is 0 when there are no more entries.
func (s *VectorSemanticCache) Inspect(cursor uint64, pageSize int) ([]CacheInspectEntry, uint64) {
	if pageSize <= 0 {
		pageSize = 50
	}
	if pageSize > 500 {
		pageSize = 500
	}
	s.mu.RLock()
	all := make([]CacheInspectEntry, 0, s.lru.Len())
	for el := s.lru.Front(); el != nil; el = el.Next() {
		e := &el.Value.(*vsNode).entry
		all = append(all, CacheInspectEntry{
			Key:        e.Key,
			Namespace:  e.Namespace,
			Model:      metaString(e.Metadata, "model"),
			TokenCount: metaInt(e.Metadata, "total_tokens", "token_count"),
			Semantic:   true,
			CreatedAt:  e.CreatedAt,
			ExpiresAt:  e.ExpiresAt,
		})
	}
	s.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.Before(all[j].CreatedAt)
		}
		if all[i].Namespace != all[j].Namespace {
			return all[i].Namespace < all[j].Namespace
		}
		return all[i].Key < all[j].Key
	})
	if cursor >= uint64(len(all)) {
		return nil, 0
	}
	start := int(cursor)
	end := start + pageSize
	if end >= len(all) {
		return all[start:], 0
	}
	return all[start:end], uint64(end)
}

func metaString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func metaInt(m map[string]interface{}, keys ...string) int {
	for _, k := range keys {
		switch v := m[k].(type) {
		case int:
			return v
		case int64:
			return int(v)
		case float64:
			if v >= 0 && v < math.MaxInt32 {
				return int(v)
			}
		}
	}
	return 0
}

// Export serializes all entries for persistence.
func (s *VectorSemanticCache) Export() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]VectorSemanticEntry, 0, s.lru.Len())
	for el := s.lru.Back(); el != nil; el = el.Prev() {
		out = append(out, el.Value.(*vsNode).entry)
	}
	return json.Marshal(out)
}

// Import replaces the cache contents with previously exported entries.
// Expired, empty or malformed entries are skipped and limits are enforced.
func (s *VectorSemanticCache) Import(data []byte) error {
	var entries []VectorSemanticEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLocked()
	for _, e := range entries {
		if e.Key == "" || len(e.Vector) == 0 || len(e.Response) == 0 || len(e.Response) > s.maxEntryBytes {
			continue
		}
		if e.ExpiresAt.IsZero() {
			e.ExpiresAt = e.CreatedAt.Add(s.ttl)
		}
		if !now.Before(e.ExpiresAt) {
			continue
		}
		s.insertLocked(e)
	}
	return nil
}
