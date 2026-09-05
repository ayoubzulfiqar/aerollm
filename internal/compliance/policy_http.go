package compliance

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
)

// HTTPPolicyRule describes a web-exposed compliance rule.
type HTTPPolicyRule struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Expression string `json:"expression"`
	Severity   string `json:"severity"`
}

// HTTPPolicyStore stores rules for HTTP evaluation.
type HTTPPolicyStore struct {
	mu    sync.RWMutex
	rules map[string]HTTPPolicyRule
}

// PolicyDecision describes evaluation result.
type PolicyDecision struct {
	RuleID   string `json:"rule_id"`
	Name     string `json:"name"`
	Severity string `json:"severity"`
	Allowed  bool   `json:"allowed"`
	Reason   string `json:"reason"`
}

// NewHTTPPolicyStore initializes the store.
func NewHTTPPolicyStore() *HTTPPolicyStore {
	return &HTTPPolicyStore{rules: make(map[string]HTTPPolicyRule)}
}

// UpsertRule adds or updates a rule.
func (s *HTTPPolicyStore) UpsertRule(rule HTTPPolicyRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[rule.ID] = rule
}

// GetRule returns a rule by id.
func (s *HTTPPolicyStore) GetRule(id string) (HTTPPolicyRule, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rules[id]
	return r, ok
}

// ListRules returns all rules.
func (s *HTTPPolicyStore) ListRules() []HTTPPolicyRule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]HTTPPolicyRule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	return out
}

// Evaluate evaluates an HTTP request.
func (s *HTTPPolicyStore) Evaluate(r *http.Request) []PolicyDecision {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var decisions []PolicyDecision
	for _, rule := range s.rules {
		allowed := evaluateExpression(rule.Expression, r)
		decisions = append(decisions, PolicyDecision{
			RuleID:   rule.ID,
			Name:     rule.Name,
			Severity: rule.Severity,
			Allowed:  allowed,
			Reason:   rule.Expression,
		})
	}
	return decisions
}

// HTTPBlockHandler returns middleware that blocks disallowed requests.
func HTTPBlockHandler(store *HTTPPolicyStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for _, d := range store.Evaluate(r) {
				if !d.Allowed && (d.Severity == "high" || d.Severity == "block") {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusUnavailableForLegalReasons)
					_ = json.NewEncoder(w).Encode(d)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// HTTPPolicyHandler exposes JSON CRUD for rules.
func HTTPPolicyHandler(store *HTTPPolicyStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var rule HTTPPolicyRule
			if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			store.UpsertRule(rule)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(rule)
		case http.MethodGet:
			id := r.URL.Query().Get("id")
			if id == "" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.ListRules())
				return
			}
			if rule, ok := store.GetRule(id); ok {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(rule)
				return
			}
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}

func evaluateExpression(expr string, r *http.Request) bool {
	expr = strings.ToLower(strings.TrimSpace(expr))
	switch expr {
	case "allow":
		return true
	case "deny":
		return false
	case "allow-post":
		return r.Method == http.MethodPost
	case "deny-post":
		return r.Method != http.MethodPost
	case "allow-get":
		return r.Method == http.MethodGet
	case "deny-get":
		return r.Method != http.MethodGet
	default:
		return true
	}
}
