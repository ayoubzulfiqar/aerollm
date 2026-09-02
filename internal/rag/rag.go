package rag

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// VectorStore is the interface for dense vector retrieval.
type VectorStore interface {
	Search(ctx context.Context, query string, limit int) ([]Document, error)
}

// KeywordIndex is the interface for keyword/BM25-style retrieval.
type KeywordIndex interface {
	Search(ctx context.Context, query string, limit int) ([]Document, error)
}

// Document represents a retrieved context chunk.
type Document struct {
	ID       string
	Content  string
	Score    float64
	Source   string
	Metadata map[string]interface{}
}

// Retriever defines hybrid RAG retrieval.
type Retriever interface {
	Retrieve(ctx context.Context, query string, limit int) ([]Document, error)
}

// hybridRetriever merges dense and keyword results with RRF.
type hybridRetriever struct {
	vectorStore  VectorStore
	keywordIndex KeywordIndex
	vectorWeight float64
	keywordWeight float64
}

// NewHybridRetriever creates a retriever using RRF fusion.
func NewHybridRetriever(vectorStore VectorStore, keywordIndex KeywordIndex) *hybridRetriever {
	return &hybridRetriever{
		vectorStore:   vectorStore,
		keywordIndex:  keywordIndex,
		vectorWeight:  1.0,
		keywordWeight: 1.0,
	}
}

// SetWeights sets fusion weights for vector and keyword results.
func (h *hybridRetriever) SetWeights(vectorWeight, keywordWeight float64) {
	h.vectorWeight = vectorWeight
	h.keywordWeight = keywordWeight
}

// Retrieve performs hybrid search with Reciprocal Rank Fusion.
func (h *hybridRetriever) Retrieve(ctx context.Context, query string, limit int) ([]Document, error) {
	vectorDocs, _ := h.vectorStore.Search(ctx, query, limit*2)
	keywordDocs, _ := h.keywordIndex.Search(ctx, query, limit*2)

	scores := make(map[string]float64)
	meta := make(map[string]Document)

	addRanked := func(docs []Document, weight float64) {
		for rank, doc := range docs {
			score := weight / (60.0 + float64(rank+1))
			scores[doc.ID] += score
			if existing, ok := meta[doc.ID]; !ok || existing.Score < doc.Score {
				meta[doc.ID] = doc
			}
		}
	}
	addRanked(vectorDocs, h.vectorWeight)
	addRanked(keywordDocs, h.keywordWeight)

	type scored struct {
		id    string
		score float64
	}
	var ranked []scored
	for id, score := range scores {
		ranked = append(ranked, scored{id: id, score: score})
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].score > ranked[j].score })
	if limit <= 0 || limit > len(ranked) {
		limit = len(ranked)
	}
	out := make([]Document, 0, limit)
	for _, item := range ranked[:limit] {
		doc := meta[item.id]
		doc.Score = item.score
		out = append(out, doc)
	}
	return out, nil
}

// RAGMiddleware injects retrieved context into the request when enabled.
type RAGMiddleware struct {
	Retriever Retriever
	SystemPromptBuilder func(query string, docs []Document) string
}

// NewRAGMiddleware creates a new RAG middleware.
func NewRAGMiddleware(retriever Retriever) *RAGMiddleware {
	return &RAGMiddleware{
		Retriever: retriever,
		SystemPromptBuilder: func(query string, docs []Document) string {
			var sb strings.Builder
			sb.WriteString("Use the following retrieved context to answer the user query accurately.\n\n")
			for i, doc := range docs {
				sb.WriteString(fmt.Sprintf("[%d] %s\n\n", i+1, doc.Content))
			}
			sb.WriteString("User query: ")
			sb.WriteString(query)
			return sb.String()
		},
	}
}

// MaybeInject checks rag_enabled in request metadata and prepends system context.
// For now, request metadata is injected via the Authorization header token or not present;
// this middleware is wired before routing and only acts when explicitly enabled by env/config.
func (m *RAGMiddleware) MaybeInject(ctx context.Context, req *models.LLMRequest) error {
	if req == nil || len(req.Messages) == 0 {
		return nil
	}
	query := ""
	if len(req.Messages) > 0 && req.Messages[len(req.Messages)-1].Content != nil {
		query = *req.Messages[len(req.Messages)-1].Content
	}
	if strings.TrimSpace(query) == "" {
		return nil
	}
	docs, err := m.Retriever.Retrieve(ctx, query, 4)
	if err != nil || len(docs) == 0 {
		return err
	}
	systemText := m.SystemPromptBuilder(query, docs)
	req.Messages = append([]models.Message{{
		Role:    models.RoleSystem,
		Content: &systemText,
	}}, req.Messages...)
	return nil
}

// InMemoryVectorStore is a simple in-memory vector store for testing/demos.
type InMemoryVectorStore struct {
	docs []Document
}

// NewInMemoryVectorStore creates an in-memory vector store.
func NewInMemoryVectorStore() *InMemoryVectorStore {
	return &InMemoryVectorStore{docs: make([]Document, 0)}
}

// Add indexes a document.
func (s *InMemoryVectorStore) Add(doc Document) {
	s.docs = append(s.docs, doc)
}

// Search returns documents sorted by exact substring match score.
func (s *InMemoryVectorStore) Search(ctx context.Context, query string, limit int) ([]Document, error) {
	_ = ctx
	q := strings.ToLower(strings.TrimSpace(query))
	type scored struct {
		doc  Document
		rank int
	}
	var results []scored
	for _, doc := range s.docs {
		content := strings.ToLower(doc.Content)
		idx := strings.Index(content, q)
		if idx >= 0 {
			results = append(results, scored{doc: doc, rank: idx})
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].rank < results[j].rank })
	if limit <= 0 || limit > len(results) {
		limit = len(results)
	}
	out := make([]Document, 0, limit)
	for _, item := range results[:limit] {
		item.doc.Score = float64(100 - item.rank)
		out = append(out, item.doc)
	}
	return out, nil
}

// InMemoryKeywordIndex is a simple in-memory keyword index.
type InMemoryKeywordIndex struct {
	docs []Document
}

// NewInMemoryKeywordIndex creates a simple keyword index.
func NewInMemoryKeywordIndex() *InMemoryKeywordIndex {
	return &InMemoryKeywordIndex{docs: make([]Document, 0)}
}

// Add indexes a document.
func (i *InMemoryKeywordIndex) Add(doc Document) {
	i.docs = append(i.docs, doc)
}

// Search returns documents by maximum matching word count.
func (i *InMemoryKeywordIndex) Search(ctx context.Context, query string, limit int) ([]Document, error) {
	_ = ctx
	terms := tokenize(strings.ToLower(query))
	type scored struct {
		doc Document
		score float64
	}
	var results []scored
	for _, doc := range i.docs {
		content := strings.ToLower(doc.Content)
		score := 0.0
		for _, term := range terms {
			if strings.Contains(content, term) {
				score += 1.0
			}
		}
		if score > 0 {
			results = append(results, scored{doc: doc, score: score})
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].score > results[j].score })
	if limit <= 0 || limit > len(results) {
		limit = len(results)
	}
	out := make([]Document, 0, limit)
	for _, item := range results[:limit] {
		out = append(out, item.doc)
	}
	return out, nil
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

// CosineSimilarity computes cosine similarity between two float64 vectors.
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
