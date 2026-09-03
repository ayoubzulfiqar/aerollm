package compliance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// PolicyResult is the outcome of evaluating a policy.
type PolicyResult struct {
	Allowed bool
	Policy  string
	Reason  string
}

// PolicyEngine evaluates requests against policies.
type PolicyEngine interface {
	Evaluate(ctx context.Context, input map[string]interface{}) (PolicyResult, error)
}

// Rule defines a single allow/deny rule.
type Rule struct {
	ID      string
	Allow   bool
	Path    string
	Methods []string
	Headers map[string]string
	Body    string
}

// SimplePolicyEngine evaluates a list of rules against request metadata.
type SimplePolicyEngine struct {
	modulePath string
	rules      []Rule
}

// NewSimplePolicyEngine creates a lightweight policy engine.
func NewSimplePolicyEngine(modulePath string) *SimplePolicyEngine {
	return &SimplePolicyEngine{modulePath: modulePath}
}

// AddRule appends a policy rule.
func (e *SimplePolicyEngine) AddRule(r Rule) {
	e.rules = append(e.rules, r)
}

// Evaluate applies rules in order; first match wins, default allow if none match.
func (e *SimplePolicyEngine) Evaluate(ctx context.Context, input map[string]interface{}) (PolicyResult, error) {
	_ = ctx
	method, _ := input["method"].(string)
	path, _ := input["path"].(string)
	headers, _ := input["header"].(map[string]interface{})

	headerStr := ""
	for k, v := range headers {
		headerStr += fmt.Sprintf("%s:%v ", k, v)
	}
	body, _ := input["body"].(string)
	if body == "" {
		body, _ = input["data"].(string)
	}

	for _, rule := range e.rules {
		if rule.Path != "" && !strings.HasPrefix(path, rule.Path) {
			continue
		}
		if len(rule.Methods) > 0 {
			found := false
			for _, m := range rule.Methods {
				if strings.EqualFold(m, method) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		if rule.Headers != nil {
			match := true
			for k, v := range rule.Headers {
				actual := ""
				if hv, ok := headers[k]; ok {
					switch t := hv.(type) {
					case string:
						actual = t
					case []string:
						actual = strings.Join(t, ",")
					case []interface{}:
						parts := make([]string, 0, len(t))
						for _, item := range t {
							if s, ok := item.(string); ok {
								parts = append(parts, s)
							}
						}
						actual = strings.Join(parts, ",")
					}
				}
				if !strings.Contains(actual, v) {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		if rule.Body != "" && !strings.Contains(body, rule.Body) {
			continue
		}
		if rule.Allow {
			return PolicyResult{Allowed: true, Policy: rule.ID, Reason: "allowed by rule"}, nil
		}
		return PolicyResult{Allowed: false, Policy: rule.ID, Reason: "denied by rule"}, nil
	}

	return PolicyResult{Allowed: true, Policy: e.modulePath, Reason: "default allow"}, nil
}

// ComplianceMiddleware returns HTTP 451 when a policy denies a request.
func ComplianceMiddleware(engine PolicyEngine) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			input := map[string]interface{}{
				"method": r.Method,
				"path":   r.URL.Path,
				"header": map[string]interface{}{},
			}
			for k, v := range r.Header {
				if len(v) == 1 {
					input["header"].(map[string]interface{})[k] = v[0]
				} else {
					input["header"].(map[string]interface{})[k] = v
				}
			}
			if r.Body != nil {
				bodyBytes := make([]byte, r.ContentLength)
				if len(bodyBytes) > 0 {
					_, _ = r.Body.Read(bodyBytes)
					input["body"] = string(bodyBytes)
				}
				r.Body = nil
			}
			result, err := engine.Evaluate(r.Context(), input)
			if err != nil {
				http.Error(w, `{"error":"compliance evaluation failed"}`, http.StatusInternalServerError)
				return
			}
			if !result.Allowed {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnavailableForLegalReasons)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"error":   "policy violation",
					"policy":  result.Policy,
					"reason":  result.Reason,
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
