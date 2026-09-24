package compliance

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
)

// HTTPPolicyRule describes a web-exposed compliance rule.
//
// Expression language (case-insensitive):
//
//	allow | deny
//	allow-<METHOD>          request allowed only if its method is METHOD
//	deny-<METHOD>           request denied if its method is METHOD
//	allow-path:<prefix>     request allowed only under <prefix>
//	deny-path:<prefix>      request denied under <prefix>
//	require-header:<Name>   request denied unless header Name is present
//	deny-header:<Name>      request denied if header Name is present
//
// Paths use the same segment-aware prefix matching as Rule.Path. Severity is
// one of low, medium, high, block (default low); only "high" and "block"
// rules are enforced by HTTPBlockHandler, the others are advisory.
type HTTPPolicyRule struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Expression string `json:"expression"`
	Severity   string `json:"severity"`
}

// HTTPPolicyStore stores rules for HTTP evaluation. It is safe for
// concurrent use.
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

var (
	ruleIDRe     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	methodRe     = regexp.MustCompile(`^[a-z]{1,16}$`)
	headerNameRe = regexp.MustCompile(`^[A-Za-z0-9!#$%&'*+.^_|~-]{1,128}$`)
	severities   = []string{"low", "medium", "high", "block"}
)

// ValidateExpression reports whether expr is a supported expression.
func ValidateExpression(expr string) error {
	op, arg := splitExpression(expr)
	switch op {
	case "allow", "deny":
		if arg == "" {
			return nil
		}
	case "allow-path", "deny-path":
		if !strings.HasPrefix(arg, "/") || len(arg) > 512 {
			return fmt.Errorf("path must start with '/'")
		}
		return nil
	case "require-header", "deny-header":
		if !headerNameRe.MatchString(arg) {
			return fmt.Errorf("invalid header name")
		}
		return nil
	default:
		if arg == "" {
			if m, ok := strings.CutPrefix(op, "allow-"); ok && methodRe.MatchString(m) {
				return nil
			}
			if m, ok := strings.CutPrefix(op, "deny-"); ok && methodRe.MatchString(m) {
				return nil
			}
		}
	}
	return fmt.Errorf("unsupported expression %q", expr)
}

func normalizeRule(rule *HTTPPolicyRule) error {
	rule.ID = strings.TrimSpace(rule.ID)
	if !ruleIDRe.MatchString(rule.ID) {
		return errors.New("invalid id: use 1-128 of [A-Za-z0-9._:-]")
	}
	if len(rule.Name) > 256 {
		return errors.New("name too long")
	}
	rule.Expression = strings.TrimSpace(rule.Expression)
	if err := ValidateExpression(rule.Expression); err != nil {
		return fmt.Errorf("invalid expression: %w", err)
	}
	rule.Severity = strings.ToLower(strings.TrimSpace(rule.Severity))
	if rule.Severity == "" {
		rule.Severity = "low"
	}
	if !slices.Contains(severities, rule.Severity) {
		return errors.New("invalid severity: use low, medium, high or block")
	}
	return nil
}

// UpsertRule validates and adds or updates a rule.
func (s *HTTPPolicyStore) UpsertRule(rule HTTPPolicyRule) error {
	if err := normalizeRule(&rule); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[rule.ID] = rule
	return nil
}

// DeleteRule removes a rule by id.
func (s *HTTPPolicyStore) DeleteRule(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[id]; !ok {
		return false
	}
	delete(s.rules, id)
	return true
}

// GetRule returns a rule by id.
func (s *HTTPPolicyStore) GetRule(id string) (HTTPPolicyRule, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rules[id]
	return r, ok
}

// ListRules returns all rules sorted by ID.
func (s *HTTPPolicyStore) ListRules() []HTTPPolicyRule {
	s.mu.RLock()
	out := make([]HTTPPolicyRule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	s.mu.RUnlock()
	slices.SortFunc(out, func(a, b HTTPPolicyRule) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// Evaluate evaluates an HTTP request against every rule, in rule-ID order.
func (s *HTTPPolicyStore) Evaluate(r *http.Request) []PolicyDecision {
	rules := s.ListRules()
	decisions := make([]PolicyDecision, 0, len(rules))
	for _, rule := range rules {
		decisions = append(decisions, PolicyDecision{
			RuleID:   rule.ID,
			Name:     rule.Name,
			Severity: rule.Severity,
			Allowed:  evaluateExpression(rule.Expression, r),
			Reason:   rule.Expression,
		})
	}
	return decisions
}

// HTTPBlockHandler returns middleware that blocks requests (451) denied by a
// rule of severity "high" or "block".
func HTTPBlockHandler(store *HTTPPolicyStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if store != nil {
				for _, d := range store.Evaluate(r) {
					if !d.Allowed && (d.Severity == "high" || d.Severity == "block") {
						writeJSON(w, http.StatusUnavailableForLegalReasons, d)
						return
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// maxPolicyRuleBody caps rule-management request bodies.
const maxPolicyRuleBody = 64 << 10

// HTTPPolicyHandler exposes JSON CRUD for rules. It performs no
// authentication itself: mount it behind admin authentication.
func HTTPPolicyHandler(store *HTTPPolicyStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if store == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "policy store unavailable"})
			return
		}
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			var rule HTTPPolicyRule
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxPolicyRuleBody))
			if err := dec.Decode(&rule); err != nil {
				var mbe *http.MaxBytesError
				if errors.As(err, &mbe) {
					writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
					return
				}
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
				return
			}
			if err := dec.Decode(&struct{}{}); err != io.EOF {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
				return
			}
			if err := store.UpsertRule(rule); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			saved, _ := store.GetRule(strings.TrimSpace(rule.ID))
			writeJSON(w, http.StatusOK, saved)
		case http.MethodGet:
			id := r.URL.Query().Get("id")
			if id == "" {
				writeJSON(w, http.StatusOK, store.ListRules())
				return
			}
			if rule, ok := store.GetRule(id); ok {
				writeJSON(w, http.StatusOK, rule)
				return
			}
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing id"})
				return
			}
			if !store.DeleteRule(id) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
		default:
			w.Header().Set("Allow", "GET, POST, PUT, DELETE")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		}
	}
}

// splitExpression returns the lowercased operator and the argument (case
// preserved) of "op:arg" expressions, or (lowercased expr, "") otherwise.
func splitExpression(expr string) (string, string) {
	expr = strings.TrimSpace(expr)
	if op, arg, ok := strings.Cut(expr, ":"); ok {
		return strings.ToLower(op), strings.TrimSpace(arg)
	}
	return strings.ToLower(expr), ""
}

// evaluateExpression returns whether r is allowed by expr. Unknown
// expressions fail closed (not allowed).
func evaluateExpression(expr string, r *http.Request) bool {
	op, arg := splitExpression(expr)
	switch op {
	case "allow":
		return true
	case "deny":
		return false
	case "allow-path":
		return arg != "" && pathMatches(arg, r.URL.Path)
	case "deny-path":
		return arg == "" || !pathMatches(arg, r.URL.Path)
	case "require-header":
		return arg != "" && r.Header.Get(arg) != ""
	case "deny-header":
		return arg != "" && r.Header.Get(arg) == ""
	}
	if arg == "" {
		if m, ok := strings.CutPrefix(op, "allow-"); ok && methodRe.MatchString(m) {
			return strings.EqualFold(r.Method, m)
		}
		if m, ok := strings.CutPrefix(op, "deny-"); ok && methodRe.MatchString(m) {
			return !strings.EqualFold(r.Method, m)
		}
	}
	return false
}
