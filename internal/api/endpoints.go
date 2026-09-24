package api

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// ModelEntry is one item of the OpenAI-compatible GET /v1/models list.
type ModelEntry struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// ModelList is the OpenAI-compatible GET /v1/models response.
type ModelList struct {
	Object string       `json:"object"`
	Data   []ModelEntry `json:"data"`
}

// listModels returns configured models, de-duplicated and sorted by ID.
func (h *Handler) listModels() []ModelEntry {
	seen := map[string]bool{}
	out := []ModelEntry{}
	if h.ModelLister != nil {
		for _, m := range h.ModelLister() {
			if m.ID == "" || seen[m.ID] {
				continue
			}
			seen[m.ID] = true
			if m.Object == "" {
				m.Object = "model"
			}
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Models handles GET /v1/models and GET /v1/models/{id}.
// @Summary List models
// @Description Lists the models the gateway can route to (OpenAI-compatible).
// @Tags models
// @Produce json
// @Success 200 {object} ModelList
// @Router /v1/models [get]
func (h *Handler) Models(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	all := h.listModels()
	// Virtual keys only see the models they may use.
	if p, ok := middleware.PrincipalFromContext(r.Context()); ok && p.Virtual != nil {
		visible := all[:0:0]
		for _, m := range all {
			if p.Virtual.AllowsModel(m.ID) {
				visible = append(visible, m)
			}
		}
		all = visible
	}
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/models"), "/")
	if id == "" {
		writeJSON(w, http.StatusOK, ModelList{Object: "list", Data: all})
		return
	}
	for _, m := range all {
		if m.ID == id {
			writeJSON(w, http.StatusOK, m)
			return
		}
	}
	if _, ok := h.ResolveModel(id); ok {
		writeJSON(w, http.StatusOK, ModelEntry{ID: id, Object: "model", Created: time.Now().Unix(), OwnedBy: "aerollm"})
		return
	}
	writeError(w, http.StatusNotFound, "model not found")
}

// multiEndpoint resolves a provider for model that supports the extra
// OpenAI endpoints, writing the error response itself on failure.
func (h *Handler) multiEndpoint(w http.ResponseWriter, ctx context.Context, model, what string) (providers.MultiEndpointProvider, bool) {
	if strings.TrimSpace(model) == "" {
		writeError(w, http.StatusBadRequest, "model is required")
		return nil, false
	}
	p, ok := h.resolveProvider(ctx, model)
	if !ok {
		writeError(w, http.StatusNotFound, "no provider configured for model")
		return nil, false
	}
	mp, ok := asMultiEndpoint(p)
	if !ok {
		writeError(w, http.StatusNotImplemented, what+" is not supported by the provider for this model")
		return nil, false
	}
	return mp, true
}

// asMultiEndpoint looks through wrappers such as the router's circuit
// breaker for a provider that implements the extra OpenAI endpoints.
func asMultiEndpoint(p providers.Provider) (providers.MultiEndpointProvider, bool) {
	for i := 0; p != nil && i < 4; i++ {
		if mp, ok := p.(providers.MultiEndpointProvider); ok {
			return mp, true
		}
		u, ok := p.(interface{ Unwrap() providers.Provider })
		if !ok {
			break
		}
		p = u.Unwrap()
	}
	return nil, false
}

// isNotSupported reports whether err means the provider lacks an endpoint.
func isNotSupported(err error) bool {
	if err == nil {
		return false
	}
	var ue *providers.UpstreamError
	if errors.As(err, &ue) {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "not supported")
}

// endpointError maps an upstream error on an auxiliary endpoint.
func (h *Handler) endpointError(w http.ResponseWriter, what string, err error) {
	if isNotSupported(err) {
		writeError(w, http.StatusNotImplemented, what+" is not supported by the provider for this model")
		return
	}
	h.Logger.Error(what+" failed", "error", err)
	writeUpstreamError(w, err)
}

// Embeddings handles the /v1/embeddings endpoint.
// @Summary Create embeddings
// @Description Generate embeddings for the given input.
// @Tags embeddings
// @Accept json
// @Produce json
// @Param req body models.EmbeddingRequest true "Embeddings request"
// @Success 200 {object} models.EmbeddingResponse
// @Failure 400 {object} map[string]string
// @Router /v1/embeddings [post]
func (h *Handler) Embeddings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	ctx, span := h.Telemetry.StartSpan(r.Context(), "Embeddings")
	defer span.End()

	var req models.EmbeddingRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	mp, ok := h.multiEndpoint(w, ctx, req.Model, "embeddings")
	if !ok {
		return
	}
	resp, err := mp.Embeddings(ctx, &req)
	if err != nil {
		h.endpointError(w, "embeddings", err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// ImageGenerations handles the /v1/images/generations endpoint.
// @Summary Generate image
// @Tags images
// @Accept json
// @Produce json
// @Param req body models.ImageRequest true "Image generation request"
// @Success 200 {object} models.ImageResponse
// @Router /v1/images/generations [post]
func (h *Handler) ImageGenerations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	ctx := r.Context()
	var req models.ImageRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Prompt) == "" {
		writeError(w, http.StatusBadRequest, "prompt is required")
		return
	}
	if req.N < 0 || req.N > 10 {
		writeError(w, http.StatusBadRequest, "n must be between 1 and 10")
		return
	}
	mp, ok := h.multiEndpoint(w, ctx, req.Model, "image generation")
	if !ok {
		return
	}
	resp, err := mp.ImageGenerations(ctx, &req)
	if err != nil {
		h.endpointError(w, "image generation", err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// AudioTranscriptions handles the /v1/audio/transcriptions endpoint.
// @Summary Transcribe audio
// @Tags audio
// @Accept json
// @Produce json
// @Param req body models.AudioRequest true "Audio transcription request"
// @Success 200 {object} models.AudioResponse
// @Router /v1/audio/transcriptions [post]
func (h *Handler) AudioTranscriptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	ctx := r.Context()
	var req models.AudioRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	mp, ok := h.multiEndpoint(w, ctx, req.Model, "audio transcription")
	if !ok {
		return
	}
	resp, err := mp.AudioTranscriptions(ctx, &req)
	if err != nil {
		h.endpointError(w, "audio transcription", err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// Responses handles the /v1/responses endpoint.
// @Summary Create response
// @Description Create a response using the OpenAI Responses API.
// @Tags responses
// @Accept json
// @Produce json
// @Param req body models.ResponsesRequest true "Responses request"
// @Success 200 {object} models.ResponsesResponse
// @Router /v1/responses [post]
func (h *Handler) Responses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var req models.ResponsesRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Model) == "" {
		writeError(w, http.StatusBadRequest, "model is required")
		return
	}
	p, ok := h.resolveProvider(r.Context(), req.Model)
	if !ok {
		writeError(w, http.StatusNotFound, "no provider configured for model")
		return
	}
	if mp, ok := asMultiEndpoint(p); ok {
		resp, err := mp.Responses(r.Context(), &req)
		if err == nil {
			writeJSON(w, http.StatusOK, resp)
			return
		}
		if !isNotSupported(err) {
			h.endpointError(w, "responses", err)
			return
		}
	}
	// Fallback: serve the Responses API through chat completions.
	chatReq := &models.LLMRequest{Model: req.Model, Tools: req.Tools}
	if req.Input != "" {
		input := req.Input
		chatReq.Messages = append(chatReq.Messages, models.Message{Role: models.RoleUser, Content: &input})
	}
	if len(chatReq.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "input is required")
		return
	}
	resp, err := p.ChatCompletions(r.Context(), chatReq)
	if err != nil {
		h.endpointError(w, "responses", err)
		return
	}
	out := &models.ResponsesResponse{ID: resp.ID, Object: "response", CreatedAt: resp.Created, Model: resp.Model, Usage: resp.Usage}
	if out.Model == "" {
		out.Model = req.Model
	}
	if out.CreatedAt == 0 {
		out.CreatedAt = time.Now().Unix()
	}
	for _, c := range resp.Choices {
		out.Output = append(out.Output, c.Message)
	}
	writeJSON(w, http.StatusOK, out)
}

// resolveProvider looks up the provider for a model alias, falling back to
// the router.
func (h *Handler) resolveProvider(ctx context.Context, model string) (providers.Provider, bool) {
	if p, ok := h.ResolveModel(model); ok {
		return p, true
	}
	if h.Router == nil {
		return nil, false
	}
	p, err := h.Router.Route(ctx, &models.LLMRequest{Model: model})
	if err != nil || p == nil {
		return nil, false
	}
	return p, true
}

// Embed returns the embedding of text using model through the gateway's
// provider resolution (used by the semantic cache).
func (h *Handler) Embed(ctx context.Context, model, text string) ([]float64, error) {
	p, ok := h.resolveProvider(ctx, model)
	if !ok {
		return nil, errors.New("no provider configured for embedding model")
	}
	mp, ok := asMultiEndpoint(p)
	if !ok {
		return nil, errors.New("embedding model provider does not support embeddings")
	}
	resp, err := mp.Embeddings(ctx, &models.EmbeddingRequest{Model: model, Input: text})
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.Data) == 0 || len(resp.Data[0].Embedding) == 0 {
		return nil, errors.New("empty embedding response")
	}
	return resp.Data[0].Embedding, nil
}
