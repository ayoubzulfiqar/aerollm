package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/analytics"
)

// SpendHandler handles global spend analytics endpoints.
// All endpoints require Master/Admin API key authentication — they are
// NOT accessible via standard virtual keys.
type SpendHandler struct {
	Engine *analytics.AnalyticsEngine
	Logger func(msg string, kv ...interface{})
}

// NewSpendHandler creates a new spend handler.
func NewSpendHandler(engine *analytics.AnalyticsEngine, logger func(msg string, kv ...interface{})) *SpendHandler {
	if logger == nil {
		logger = func(string, ...interface{}) {}
	}
	return &SpendHandler{Engine: engine, Logger: logger}
}

// SpendReport handles GET /global/spend/report.
// Query parameters:
//   - start: RFC3339 timestamp (required)
//   - end: RFC3339 timestamp (required)
//   - group_by: "api_key", "customer", "team", "model" (default: "api_key")
//   - filter: optional filter value (api_key, customer_id, or team_id)
func (h *SpendHandler) SpendReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	q := r.URL.Query()

	startStr := q.Get("start")
	endStr := q.Get("end")
	groupBy := q.Get("group_by")
	if groupBy == "" {
		groupBy = "api_key"
	}

	var tr analytics.TimeRange
	now := time.Now().UTC()
	if startStr != "" {
		start, err := time.Parse(time.RFC3339, startStr)
		if err != nil {
			http.Error(w, `{"error":"invalid start timestamp"}`, http.StatusBadRequest)
			return
		}
		tr.Start = start
	} else {
		tr.Start = now.Add(-24 * time.Hour) // default: last 24h
	}

	if endStr != "" {
		end, err := time.Parse(time.RFC3339, endStr)
		if err != nil {
			http.Error(w, `{"error":"invalid end timestamp"}`, http.StatusBadRequest)
			return
		}
		tr.End = end
	} else {
		tr.End = now
	}

	if tr.Start.After(tr.End) {
		http.Error(w, `{"error":"start must be before end"}`, http.StatusBadRequest)
		return
	}

	report, err := h.Engine.GenerateReport(ctx, tr, groupBy)
	if err != nil {
		h.Logger("spend report generation failed", "error", err)
		http.Error(w, `{"error":"report generation failed"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(report)
}

// SpendLogs handles GET /global/spend/logs.
// Query parameters:
//   - filter: api_key, customer_id, or team_id to filter by
//   - page: page number (default 1)
//   - page_size: entries per page (default 50, max 500)
func (h *SpendHandler) SpendLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	q := r.URL.Query()

	filter := q.Get("filter")
	page, _ := strconv.Atoi(q.Get("page"))
	pageSize, _ := strconv.Atoi(q.Get("page_size"))

	logs, err := h.Engine.GetLogs(ctx, filter, page, pageSize)
	if err != nil {
		h.Logger("spend logs query failed", "error", err)
		http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(logs)
}

// IsAdminRequest checks if the request comes from an admin/master API key.
// This is a helper for the middleware to enforce admin-only access.
func IsAdminRequest(r *http.Request, validAdminKeys map[string]bool) bool {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return false
	}
	if len(auth) > 7 && auth[:7] == "Bearer " {
		auth = auth[7:]
	}
	return validAdminKeys[auth]
}

// AdminAuthMiddleware wraps handlers with admin-only API key validation.
// Unlike the standard auth middleware, this rejects virtual keys (sk-)
// and only accepts master/admin API keys.
func AdminAuthMiddleware(validAdminKeys map[string]bool, logger func(msg string, kv ...interface{})) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			if auth == "" {
				http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
				return
			}
			if len(auth) > 7 && auth[:7] == "Bearer " {
				auth = auth[7:]
			}
			if strings.HasPrefix(auth, "sk-") {
				http.Error(w, `{"error":"admin access required"}`, http.StatusForbidden)
				return
			}
			if !validAdminKeys[auth] {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
