package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/cache"
)

// CacheHandler handles cache management and inspection endpoints.
// These endpoints require admin authentication.
type CacheHandler struct {
	Cache         *cache.RedisCache
	SemanticCache *cache.VectorSemanticCache
}

// NewCacheHandler creates a new CacheHandler.
func NewCacheHandler(c *cache.RedisCache, sc *cache.VectorSemanticCache) *CacheHandler {
	return &CacheHandler{Cache: c, SemanticCache: sc}
}

// CacheStatsResponse is the response for GET /v1/cache/stats.
type CacheStatsResponse struct {
	Exact struct {
		TotalEntries int     `json:"total_entries"`
		Truncated    bool    `json:"truncated,omitempty"`
		Hits         int64   `json:"hits"`
		Misses       int64   `json:"misses"`
		HitRate      float64 `json:"hit_rate"`
		Error        string  `json:"error,omitempty"`
	} `json:"exact"`
	Semantic struct {
		Enabled       bool `json:"enabled"`
		TotalEntries  int  `json:"total_entries"`
		ActiveEntries int  `json:"active_entries"`
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

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// Stats handles GET /v1/cache/stats.
// @Summary Get cache statistics
// @Description Returns statistics about the exact-match and semantic caches (entries, hit/miss rates).
// @Tags cache
// @Produce json
// @Success 200 {object} CacheStatsResponse
// @Router /v1/cache/stats [get]
func (h *CacheHandler) Stats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	resp := &CacheStatsResponse{}
	if h.Cache != nil {
		st, err := h.Cache.ExactStats(r.Context())
		resp.Exact.TotalEntries = st.Entries
		resp.Exact.Truncated = st.Truncated
		resp.Exact.Hits = st.Hits
		resp.Exact.Misses = st.Misses
		resp.Exact.HitRate = st.HitRate
		if err != nil {
			resp.Exact.Error = "entry count unavailable"
		}
	}
	if h.SemanticCache != nil {
		stats := h.SemanticCache.Stats()
		resp.Semantic.Enabled = h.SemanticCache.Enabled()
		resp.Semantic.TotalEntries = stats["total_entries"]
		resp.Semantic.ActiveEntries = stats["active_entries"]
	}
	writeJSON(w, http.StatusOK, resp)
}

// Clear handles DELETE /v1/cache.
// @Summary Clear cache
// @Description Clears the cache. Target a type via ?type=semantic|exact|all (default all).
// @Tags cache
// @Param type query string false "Cache type to clear: 'exact', 'semantic', or 'all' (default: all)"
// @Success 200 {object} map[string]interface{}
// @Router /v1/cache [delete]
func (h *CacheHandler) Clear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		methodNotAllowed(w, http.MethodDelete)
		return
	}
	cacheType := r.URL.Query().Get("type")
	if cacheType == "" {
		cacheType = "all"
	}
	if cacheType != "all" && cacheType != "exact" && cacheType != "semantic" {
		writeError(w, http.StatusBadRequest, "type must be exact, semantic or all")
		return
	}

	result := map[string]interface{}{}
	status := http.StatusOK
	if cacheType == "all" || cacheType == "exact" {
		if h.Cache != nil {
			if err := h.Cache.ClearExact(r.Context()); err != nil {
				result["exact_error"] = "clear failed"
				status = http.StatusInternalServerError
			} else {
				result["exact_cleared"] = true
			}
		}
	}
	if cacheType == "all" || cacheType == "semantic" {
		if h.SemanticCache != nil {
			if err := h.SemanticCache.Clear(); err != nil {
				result["semantic_error"] = "clear failed"
				status = http.StatusInternalServerError
			} else {
				result["semantic_cleared"] = true
			}
		}
		if h.Cache != nil {
			_ = h.Cache.ClearSemantic(r.Context())
		}
	}
	writeJSON(w, status, result)
}

// Inspect handles GET /v1/cache/inspect.
// @Summary Inspect cache entries
// @Description Returns a paginated list of cached keys (never payloads).
// @Tags cache
// @Param type query string false "Cache type: 'exact' (default) or 'semantic'"
// @Param cursor query string false "Pagination cursor (default: \"0\" for first page)"
// @Param page_size query int false "Page size (default: 50, max: 500)"
// @Success 200 {object} CacheInspectResponse
// @Router /v1/cache/inspect [get]
func (h *CacheHandler) Inspect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	q := r.URL.Query()
	cacheType := q.Get("type")
	if cacheType == "" {
		cacheType = "exact"
	}
	if cacheType != "exact" && cacheType != "semantic" {
		writeError(w, http.StatusBadRequest, "type must be exact or semantic")
		return
	}
	cursor := q.Get("cursor")
	if cursor == "" {
		cursor = "0"
	}
	pageSize := 50
	if ps := q.Get("page_size"); ps != "" {
		n, err := strconv.Atoi(ps)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "page_size must be between 1 and 500")
			return
		}
		pageSize = n
	}

	resp := &CacheInspectResponse{Entries: []CacheInspectEntry{}, Cursor: "0"}
	switch cacheType {
	case "exact":
		if h.Cache != nil {
			entries, next, err := h.Cache.Inspect(r.Context(), cursor, pageSize)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "cache inspect failed")
				return
			}
			for _, e := range entries {
				resp.Entries = append(resp.Entries, CacheInspectEntry{Key: e.Key, Model: e.Model, TokenCount: e.TokenCount, CreatedAt: formatTime(e.CreatedAt)})
			}
			resp.Cursor = next
		}
	case "semantic":
		if h.SemanticCache != nil {
			cur, err := strconv.ParseUint(cursor, 10, 64)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid cursor")
				return
			}
			entries, next := h.SemanticCache.Inspect(cur, pageSize)
			for _, e := range entries {
				resp.Entries = append(resp.Entries, CacheInspectEntry{Key: e.Key, Model: e.Model, TokenCount: e.TokenCount, Semantic: true, CreatedAt: formatTime(e.CreatedAt)})
			}
			resp.Cursor = strconv.FormatUint(next, 10)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
