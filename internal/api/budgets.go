package api

import (
	"context"
	"math"
	"net/http"
	"regexp"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
)

// BudgetManager is the subset of finops.CostTracker used by BudgetHandler.
type BudgetManager interface {
	SetBudget(ctx context.Context, apiKey string, limitUSD float64) error
	GetBudget(ctx context.Context, apiKey string) (finops.BudgetStatus, error)
	RemoveBudget(ctx context.Context, apiKey string) error
}

// BudgetHandler manages per-key spend limits (admin only). Budgets are
// keyed by the gateway key ID ("key_<hex>", as shown in logs and spend
// reports); callers may pass the raw key instead, which is converted to its
// ID and never stored.
type BudgetHandler struct {
	Budgets BudgetManager
	// Period is the tracker-wide budget period ("", "daily", "monthly").
	Period finops.BudgetPeriod
}

// NewBudgetHandler creates a BudgetHandler.
func NewBudgetHandler(b BudgetManager, period finops.BudgetPeriod) *BudgetHandler {
	return &BudgetHandler{Budgets: b, Period: period}
}

var keyIDPattern = regexp.MustCompile(`^key_[0-9a-f]{16}$`)

// resolveKeyID accepts a key_id or a raw key.
func resolveKeyID(keyID, key string) (string, bool) {
	keyID, key = strings.TrimSpace(keyID), strings.TrimSpace(key)
	switch {
	case keyID != "" && key != "":
		return "", false
	case keyID != "":
		return keyID, keyIDPattern.MatchString(keyID)
	case key != "":
		return middleware.KeyID(key), true
	}
	return "", false
}

// ServeHTTP implements GET/PUT/POST/DELETE /v1/budgets.
//
//	GET    ?key_id=key_...|?key=sk-...          -> budget status
//	PUT    {"key_id"|"key", "max_usd", "period"} -> set limit, returns status
//	DELETE ?key_id=...|?key=...                  -> 204
func (h *BudgetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	switch r.Method {
	case http.MethodGet, http.MethodDelete:
		q := r.URL.Query()
		id, ok := resolveKeyID(q.Get("key_id"), q.Get("key"))
		if !ok {
			writeError(w, http.StatusBadRequest, "exactly one of key_id (key_<16 hex>) or key is required")
			return
		}
		if r.Method == http.MethodDelete {
			if err := h.Budgets.RemoveBudget(ctx, id); err != nil {
				writeError(w, http.StatusInternalServerError, "failed to remove budget")
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		st, err := h.Budgets.GetBudget(ctx, id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to read budget")
			return
		}
		writeJSON(w, http.StatusOK, budgetView(id, st))
	case http.MethodPut, http.MethodPost:
		var body struct {
			KeyID  string   `json:"key_id"`
			Key    string   `json:"key"`
			MaxUSD *float64 `json:"max_usd"`
			Period string   `json:"period"`
		}
		if !decodeJSON(w, r, &body) {
			return
		}
		id, ok := resolveKeyID(body.KeyID, body.Key)
		if !ok {
			writeError(w, http.StatusBadRequest, "exactly one of key_id (key_<16 hex>) or key is required")
			return
		}
		if body.MaxUSD == nil || math.IsNaN(*body.MaxUSD) || math.IsInf(*body.MaxUSD, 0) || *body.MaxUSD < 0 {
			writeError(w, http.StatusBadRequest, "max_usd must be a non-negative number")
			return
		}
		if p := strings.ToLower(strings.TrimSpace(body.Period)); p != "" && p != "none" && finops.BudgetPeriod(p) != h.Period {
			writeError(w, http.StatusBadRequest, "per-key periods are not supported; the gateway budget period is "+periodName(h.Period))
			return
		}
		if err := h.Budgets.SetBudget(ctx, id, *body.MaxUSD); err != nil {
			writeError(w, http.StatusBadRequest, "failed to set budget: "+err.Error())
			return
		}
		st, err := h.Budgets.GetBudget(ctx, id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "budget set but could not be read back")
			return
		}
		writeJSON(w, http.StatusOK, budgetView(id, st))
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete)
	}
}

func periodName(p finops.BudgetPeriod) string {
	if p == "" {
		return "lifetime"
	}
	return string(p)
}

func budgetView(id string, st finops.BudgetStatus) map[string]interface{} {
	return map[string]interface{}{
		"key_id":        id,
		"has_limit":     st.HasLimit,
		"limit_usd":     st.LimitUSD,
		"spend_usd":     st.SpendUSD,
		"remaining_usd": st.RemainingUSD,
		"exceeded":      st.Exceeded,
		"period":        periodName(st.Period),
		"period_key":    st.PeriodKey,
	}
}
