package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/ayoubzulfiqar/aerollm/internal/cache"
)

// CacheHandler handles cache management and inspection endpoints.
// These endpoints require Master/Admin API key authentication — not standard virtual keys.
type CacheHandler struct {
	Cache        *cache.RedisCache
	SemanticCache *cache.VectorSemanticCache
}

// NewCacheHandler creates a new CacheHandler.
func NewCacheHandler(c *cache.RedisCache, sc *cache.VectorSemanticCache) *CacheHandler {
	return &CacheHandler{
		Cache:        c,
		SemanticCache: sc,
	}
}

// CacheStatsResponse is the response for GET /v1/cache/stats.
type CacheStatsResponse struct {
	Exact struct {
		TotalEntries int `json:"total_entries"`
	} `json:"exact"`
	Semantic struct {
		TotalEntries  int `json:"total_entries"`
		ActiveEntries int `json:"active_entries"`
	} `json:"semantic"`
}

// CacheInspectEntry is a lightweight view of a cached entry.
type CacheInspectEntry struct {
	Key        string `json:"key"`
	Model      string `json:"model,omitempty"`
	Semantic   bool   `json:"semantic"`
	TokenCount int    `json:"token_count,omitempty"`
	CreatedAt  string `json:"created_at"`
}

// CacheInspectResponse is the response for GET /v1/cache/inspect.
type CacheInspectResponse struct {
	Entries []CacheInspectEntry `json:"entries"`
	Cursor  string              `json:"cursor"`
}

// Stats handles GET /v1/cache/stats.
// @Summary Get cache statistics
// @Description Returns statistics about the exact-match and semantic caches (total entries, hit/miss rates).
// @Tags cache
// @Produce json
// @Success 200 {object} CacheStatsResponse
// @Router /v1/cache/stats [get]
func (h *CacheHandler) Stats(w http.ResponseWriter, r *http.Request) {
	resp := &CacheStatsResponse{}

	// Exact cache stats
	if h.Cache != nil {
		stats, err := h.Cache.Stats(r.Context())
		if err == nil {
			if v, ok := stats["exact_total_entries"]; ok {
				resp.Exact.TotalEntries = v.(int)
			}
		}
	}

	// Semantic cache stats
	if h.SemanticCache != nil {
		stats := h.SemanticCache.Stats()
		resp.Semantic.TotalEntries = stats["total_entries"]
		resp.Semantic.ActiveEntries = stats["active_entries"]
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Clear handles DELETE /v1/cache.
// @Summary Clear cache
// @Description Clears the entire cache. Optionally target a specific type via ?type=semantic or ?type=exact.
// @Tags cache
// @Param type query string false "Cache type to clear: 'exact', 'semantic', or 'all' (default: all)"
// @Success 200 {object} map[string]interface{}
// @Router /v1/cache [delete]
func (h *CacheHandler) Clear(w http.ResponseWriter, r *http.Request) {
	cacheType := r.URL.Query().Get("type")
	if cacheType == "" {
		cacheType = "all"
	}

	result := map[string]interface{}{}

	// Clear exact cache
	if cacheType == "all" || cacheType == "exact" {
		if h.Cache != nil {
			if err := h.Cache.ClearExact(r.Context()); err != nil {
				result["exact_error"] = err.Error()
			} else {
				result["exact_cleared"] = true
			}
		}
	}

	// Clear semantic cache
	if cacheType == "all" || cacheType == "semantic" {
		if h.Cache != nil {
			if err := h.Cache.ClearSemantic(r.Context()); err != nil {
				result["semantic_error"] = err.Error()
			} else {
				result["semantic_cleared"] = true
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}

// Inspect handles GET /v1/cache/inspect.
// @Summary Inspect cache entries
// @Description Returns a paginated list of cached keys/hashes (does not return full payloads for security).
// @Tags cache
// @Param type query string false "Cache type: 'exact' (default), 'semantic', or 'all'"
// @Param cursor query string false "Pagination cursor (default: \"0\" for first page)"
// @Param page_size query int false "Page size (default: 50, max: 500)"
// @Success 200 {object} CacheInspectResponse
// @Router /v1/cache/inspect [get]
func (h *CacheHandler) Inspect(w http.ResponseWriter, r *http.Request) {
	cacheType := r.URL.Query().Get("type")
	if cacheType == "" {
		cacheType = "exact"
	}

	cursor := r.URL.Query().Get("cursor")
	if cursor == "" {
		cursor = "0"
	}

	pageSize := 50
	if ps := r.URL.Query().Get("page_size"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 && n <= 500 {
			pageSize = n
		}
	}

	resp := &CacheInspectResponse{Entries: []CacheInspectEntry{}}

	// Inspect exact cache
	if cacheType == "exact" || cacheType == "all" {
		if h.Cache != nil {
			entries, nextCursor, err := h.Cache.Inspect(r.Context(), cursor, pageSize)
			if err != nil {
				http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
				return
			}
			for _, e := range entries {
				resp.Entries = append(resp.Entries, CacheInspectEntry{
					Key:        e.Key,
					TokenCount: e.TokenCount,
					Semantic:   e.Semantic,
					CreatedAt:  e.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
				})
			}
			resp.Cursor = nextCursor
		}
	}

	// Inspect semantic cache
	if cacheType == "semantic" || cacheType == "all" {
		if h.SemanticCache != nil {
			cur, _ := strconv.ParseUint(cursor, 10, 64)
			entries, nextCursor := h.SemanticCache.Inspect(cur, pageSize)
			for _, e := range entries {
				resp.Entries = append(resp.Entries, CacheInspectEntry{
					Key:        e.Key,
					TokenCount: e.TokenCount,
					Semantic:   e.Semantic,
					CreatedAt:  e.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
				})
			}
			resp.Cursor = strconv.FormatUint(nextCursor, 10)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
