package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// Embeddings handles the /v1/embeddings endpoint.
func (h *Handler) Embeddings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	ctx, span := h.Telemetry.StartSpan(ctx, "Embeddings")
	defer span.End()

	var req models.EmbeddingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.Logger.Error("invalid embeddings request", "error", err)
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	p, ok := h.resolveProvider(ctx, req.Model)
	if !ok {
		http.Error(w, `{"error":"provider not found for model"}`, http.StatusNotFound)
		return
	}
	mp, ok := p.(providers.MultiEndpointProvider)
	if !ok {
		http.Error(w, `{"error":"embedding provider not supported"}`, http.StatusNotImplemented)
		return
	}
	resp, err := mp.Embeddings(ctx, &req)
	if err != nil {
		h.Logger.Error("embeddings failed", "error", err)
		http.Error(w, `{"error":"embeddings failed"}`, http.StatusInternalServerError)
		return
	}
	_ = start
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ImageGenerations handles the /v1/images/generations endpoint.
func (h *Handler) ImageGenerations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req models.ImageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	p, ok := h.resolveProvider(ctx, req.Model)
	if !ok {
		http.Error(w, `{"error":"provider not found for model"}`, http.StatusNotFound)
		return
	}
	mp, ok := p.(providers.MultiEndpointProvider)
	if !ok {
		http.Error(w, `{"error":"image provider not supported"}`, http.StatusNotImplemented)
		return
	}
	resp, err := mp.ImageGenerations(ctx, &req)
	if err != nil {
		h.Logger.Error("image generation failed", "error", err)
		http.Error(w, `{"error":"image generation failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// AudioTranscriptions handles the /v1/audio/transcriptions endpoint.
func (h *Handler) AudioTranscriptions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req models.AudioRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	p, ok := h.resolveProvider(ctx, req.Model)
	if !ok {
		http.Error(w, `{"error":"provider not found for model"}`, http.StatusNotFound)
		return
	}
	mp, ok := p.(providers.MultiEndpointProvider)
	if !ok {
		http.Error(w, `{"error":"audio provider not supported"}`, http.StatusNotImplemented)
		return
	}
	resp, err := mp.AudioTranscriptions(ctx, &req)
	if err != nil {
		h.Logger.Error("audio transcription failed", "error", err)
		http.Error(w, `{"error":"audio transcription failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// Responses handles the /v1/responses endpoint.
func (h *Handler) Responses(w http.ResponseWriter, r *http.Request) {
	var req models.ResponsesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	p, ok := h.resolveProvider(r.Context(), req.Model)
	if !ok {
		http.Error(w, `{"error":"provider not found for model"}`, http.StatusNotFound)
		return
	}
	if mp, ok := p.(providers.MultiEndpointProvider); ok {
		resp, err := mp.Responses(r.Context(), &req)
		if err != nil {
			h.Logger.Error("responses failed", "error", err)
			http.Error(w, `{"error":"responses failed"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	// Fallback: alias to ChatCompletions via legacy Provider.
	chatReq := &models.LLMRequest{
		Model: req.Model,
	}
	if req.Input != "" {
		chatReq.Messages = append(chatReq.Messages, models.Message{
			Role:    models.RoleUser,
			Content: &req.Input,
		})
	}
	resp, err := p.ChatCompletions(r.Context(), chatReq)
	if err != nil {
		h.Logger.Error("responses fallback failed", "error", err)
		http.Error(w, `{"error":"responses failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// Messages handles the /v1/messages endpoint (Anthropic compatibility).
func (h *Handler) Messages(w http.ResponseWriter, r *http.Request) {
	h.ChatCompletions(w, r)
}

// resolveProvider looks up the provider for a model alias, falling back to Router.Route.
func (h *Handler) resolveProvider(ctx context.Context, model string) (providers.Provider, bool) {
	if h.ModelResolver != nil {
		if p, ok := h.ModelResolver(model); ok {
			return p, true
		}
	}
	p, err := h.Router.Route(ctx, &models.LLMRequest{Model: model})
	return p, err == nil
}
