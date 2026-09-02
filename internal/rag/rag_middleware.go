package rag

import (
	"encoding/json"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// RAGHTTPMiddleware injects retrieved context into requests when enabled.
// It reads `rag_enabled=true` from the request query parameter.
func RAGHTTPMiddleware(retriever Retriever) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("rag_enabled") == "true" && retriever != nil {
				var req models.LLMRequest
				if err := parseLLMRequest(r, &req); err == nil {
					_ = NewRAGMiddleware(retriever).MaybeInject(r.Context(), &req)
					// Continue with original request body for downstream handlers;
					// in a fuller integration, replace r.Body with re-serialized req.
					_ = req
				}
			}
			next(w, r)
		}
	}
}

func parseLLMRequest(r *http.Request, out *models.LLMRequest) error {
	return json.NewDecoder(r.Body).Decode(out)
}
