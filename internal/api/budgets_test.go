package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestBudgetHandlerLifecycle(t *testing.T) {
	tracker := finops.NewCostTracker(nil, finops.NewPricingMap(), nil)
	h := NewBudgetHandler(tracker, "")
	serve := func(method, target, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, target, strings.NewReader(body)))
		return w
	}

	if w := serve("PUT", "/v1/budgets", `{"key":"sk-secret","max_usd":1.5}`); w.Code != 200 {
		t.Fatalf("set: %d %s", w.Code, w.Body.String())
	} else if strings.Contains(w.Body.String(), "sk-secret") {
		t.Fatal("raw key must never be echoed")
	}
	id := middleware.KeyID("sk-secret")
	w := serve("GET", "/v1/budgets?key_id="+id, "")
	var view map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &view)
	if w.Code != 200 || view["limit_usd"] != 1.5 || view["has_limit"] != true {
		t.Fatalf("get: %d %s", w.Code, w.Body.String())
	}

	// Spend beyond the limit is enforced by the tracker.
	if _, err := tracker.Record(context.Background(), finops.CostRequest{APIKey: id, Model: "gpt-4o", CostUSD: 2, Usage: &models.Usage{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.CheckBudget(context.Background(), id, 0); err == nil {
		t.Fatal("budget must be exceeded after recording spend over the limit")
	}

	if w := serve("DELETE", "/v1/budgets?key_id="+id, ""); w.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", w.Code)
	}
	for name, c := range map[string]struct{ method, target, body string }{
		"no key":          {"GET", "/v1/budgets", ""},
		"bad key_id":      {"GET", "/v1/budgets?key_id=nope", ""},
		"both":            {"PUT", "/v1/budgets", `{"key":"a","key_id":"key_0123456789abcdef","max_usd":1}`},
		"negative":        {"PUT", "/v1/budgets", `{"key":"a","max_usd":-1}`},
		"missing max":     {"PUT", "/v1/budgets", `{"key":"a"}`},
		"per-key period":  {"PUT", "/v1/budgets", `{"key":"a","max_usd":1,"period":"daily"}`},
		"method":          {"PATCH", "/v1/budgets", ""},
		"trailing object": {"PUT", "/v1/budgets", `{"key":"a","max_usd":1}{}`},
	} {
		w := serve(c.method, c.target, c.body)
		if w.Code != http.StatusBadRequest && w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: expected 400/405, got %d", name, w.Code)
		}
	}
}
