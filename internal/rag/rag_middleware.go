package rag

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// RAGHTTPMiddleware injects retrieved context into requests when enabled.
// It supports `rag_enabled=true` in the JSON request body.
func RAGHTTPMiddleware(retriever Retriever) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if retriever == nil || r.Method != http.MethodPost {
				next(w, r)
				return
			}
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil || len(bodyBytes) == 0 {
				next(w, r)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			var req models.LLMRequest
			if err := json.Unmarshal(bodyBytes, &req); err != nil {
				next(w, r)
				return
			}
			if !req.RagEnabled {
				next(w, r)
				return
			}
			_ = NewRAGMiddleware(retriever).MaybeInject(r.Context(), &req)
			out, _ := json.Marshal(req)
			r.Body = io.NopCloser(bytes.NewReader(out))
			r.ContentLength = int64(len(out))
			next(w, r)
		}
	}
}
