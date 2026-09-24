package cache

import (
	"context"
	"errors"
	"hash/fnv"
	"math"
	"strings"
	"unicode"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// maxEmbedInputBytes bounds the text processed by the hashing embedder and
// the default prompt limit of the semantic cache.
const maxEmbedInputBytes = 32 << 10

// HashingEmbedder is a deterministic, dependency-free EmbeddingProvider that
// maps text to a fixed-size vector using the hashing trick over word
// unigrams and bigrams (sub-linear TF weighting, signed buckets, L2
// normalised). It captures lexical — not deep semantic — similarity, so use
// a high threshold (>= 0.95) with it. It is safe for concurrent use.
type HashingEmbedder struct {
	Dims int
}

// NewHashingEmbedder returns a hashing embedder with dims buckets
// (default 1024 when dims <= 0).
func NewHashingEmbedder(dims int) *HashingEmbedder {
	if dims <= 0 {
		dims = 1024
	}
	return &HashingEmbedder{Dims: dims}
}

// Embedding implements EmbeddingProvider.
func (h *HashingEmbedder) Embedding(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errors.New("cache: nil embedding request")
	}
	return &models.EmbeddingResponse{
		Object: "list",
		Model:  "aerollm-hashing-embedder",
		Data:   []models.Embedding{{Object: "embedding", Embedding: h.Vector(req.Input), Index: 0}},
	}, nil
}

// Vector returns the embedding of text. Empty or symbol-only text yields a
// zero vector (which never matches anything).
func (h *HashingEmbedder) Vector(text string) []float64 {
	dims := h.Dims
	if dims <= 0 {
		dims = 1024
	}
	vec := make([]float64, dims)
	tokens := wordTokens(text, maxEmbedInputBytes)
	if len(tokens) == 0 {
		return vec
	}
	counts := make(map[string]int, len(tokens)*2)
	for i, t := range tokens {
		counts["u:"+t]++
		if i > 0 {
			counts["b:"+tokens[i-1]+" "+t]++
		}
	}
	for feat, n := range counts {
		hs := fnv.New64a()
		_, _ = hs.Write([]byte(feat))
		sum := hs.Sum64()
		idx := int(sum % uint64(dims))
		sign := 1.0
		if (sum>>63)&1 == 1 {
			sign = -1.0
		}
		vec[idx] += sign * (1 + math.Log(float64(n)))
	}
	var norm float64
	for _, x := range vec {
		norm += x * x
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for i := range vec {
			vec[i] /= norm
		}
	}
	return vec
}

// wordTokens lower-cases text and splits it into letter/digit runs,
// processing at most limit bytes. It is Unicode aware.
func wordTokens(text string, limit int) []string {
	if len(text) > limit {
		text = strings.ToValidUTF8(text[:limit], "")
	}
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}
