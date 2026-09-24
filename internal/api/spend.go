package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/analytics"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
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
// @Summary Get spend report
// @Description Aggregates spend over a time range, grouped by api_key, customer, team, or model.
// @Tags analytics
// @Param start query string true "RFC3339 start timestamp"
// @Param end query string true "RFC3339 end timestamp"
// @Param group_by query string false "Group by: api_key, customer, team, model"
// @Param filter query string false "Optional filter value"
// @Success 200 {object} analytics.SpendReport
// @Router /global/spend/report [get]
func (h *SpendHandler) SpendReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
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
			writeError(w, http.StatusBadRequest, "invalid start timestamp (want RFC3339)")
			return
		}
		tr.Start = start
	} else {
		tr.Start = now.Add(-24 * time.Hour) // default: last 24h
	}

	if endStr != "" {
		end, err := time.Parse(time.RFC3339, endStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid end timestamp (want RFC3339)")
			return
		}
		tr.End = end
	} else {
		tr.End = now
	}

	if tr.Start.After(tr.End) {
		writeError(w, http.StatusBadRequest, "start must be before end")
		return
	}

	report, err := h.Engine.GenerateReport(ctx, tr, groupBy)
	if err != nil {
		h.Logger("spend report generation failed", "error", err)
		if errors.Is(err, analytics.ErrInvalidGroupBy) || errors.Is(err, analytics.ErrInvalidTimeRange) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "report generation failed")
		return
	}

	writeJSON(w, http.StatusOK, report)
}

// SpendLogs handles GET /global/spend/logs.
// @Summary Get spend logs
// @Description Returns paginated, detailed transaction logs for a specific key or customer.
// @Tags analytics
// @Param filter query string false "Filter by api_key, customer_id, or team_id"
// @Param page query int false "Page number (default 1)"
// @Param page_size query int false "Entries per page (default 50, max 500)"
// @Success 200 {object} analytics.SpendLogsResponse
// @Router /global/spend/logs [get]
func (h *SpendHandler) SpendLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}

	ctx := r.Context()
	q := r.URL.Query()

	filter := q.Get("filter")
	page, pageSize := 1, 50
	if v := q.Get("page"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "page must be a positive integer")
			return
		}
		page = n
	}
	if v := q.Get("page_size"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "page_size must be between 1 and 500")
			return
		}
		pageSize = n
	}

	logs, err := h.Engine.GetLogs(ctx, filter, page, pageSize)
	if err != nil {
		h.Logger("spend logs query failed", "error", err)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}

	writeJSON(w, http.StatusOK, logs)
}

// IsAdminRequest checks if the request carries one of the admin API keys.
// Keys are compared by SHA-256 digest in constant time.
func IsAdminRequest(r *http.Request, validAdminKeys map[string]bool) bool {
	key := middleware.APIKeyFromRequest(r)
	if key == "" {
		return false
	}
	want := sha256.Sum256([]byte(key))
	match := 0
	for k, ok := range validAdminKeys {
		if !ok || k == "" {
			continue
		}
		have := sha256.Sum256([]byte(k))
		match |= subtle.ConstantTimeCompare(want[:], have[:])
	}
	return match == 1
}

// AdminAuthMiddleware wraps handlers with admin-only API key validation.
// Only the listed admin keys are accepted; virtual and client keys get 403.
//
// Deprecated: use middleware.Authenticator.RequireAdmin.
func AdminAuthMiddleware(validAdminKeys map[string]bool, logger func(msg string, kv ...interface{})) func(http.Handler) http.Handler {
	if logger == nil {
		logger = func(string, ...interface{}) {}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if middleware.APIKeyFromRequest(r) == "" {
				writeError(w, http.StatusUnauthorized, "missing api key")
				return
			}
			if !IsAdminRequest(r, validAdminKeys) {
				logger("admin access denied", "path", r.URL.Path)
				writeError(w, http.StatusForbidden, "admin access required")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
