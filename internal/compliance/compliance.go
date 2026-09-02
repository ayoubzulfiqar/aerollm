package compliance

import (
	"context"
	"encoding/json"
	"net/http"
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

// RegoPolicyEngine is a stub for OPA-backed evaluation.
type RegoPolicyEngine struct {
	modulePath string
}

// NewRegoPolicyEngine creates a new engine.
func NewRegoPolicyEngine(modulePath string) *RegoPolicyEngine {
	return &RegoPolicyEngine{modulePath: modulePath}
}

// Evaluate always allows in this stub implementation.
func (e *RegoPolicyEngine) Evaluate(ctx context.Context, input map[string]interface{}) (PolicyResult, error) {
	_ = ctx
	_ = input
	return PolicyResult{Allowed: true, Policy: e.modulePath, Reason: "stub: no policies loaded"}, nil
}

// ComplianceMiddleware returns HTTP 451 when a policy denies a request.
func ComplianceMiddleware(engine PolicyEngine) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			input := map[string]interface{}{
				"method": r.Method,
				"path":   r.URL.Path,
				"header": map[string][]string(r.Header),
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
