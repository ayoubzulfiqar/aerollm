package compliance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"path"
	"strings"
	"sync"
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

// Rule defines a single allow/deny rule. All non-empty conditions must match
// for the rule to apply:
//   - Path: segment-aware prefix of the cleaned request path ("/admin"
//     matches "/admin" and "/admin/x" but not "/administrator"; a trailing
//     "/" matches everything below it).
//   - Methods: case-insensitive method list.
//   - Headers: header name (case-insensitive) -> required substring of its
//     value; a missing header never matches.
//   - Body: required substring of the request body.
type Rule struct {
	ID      string
	Allow   bool
	Path    string
	Methods []string
	Headers map[string]string
	Body    string
}

func cloneRule(r Rule) Rule {
	r.Methods = append([]string(nil), r.Methods...)
	r.Headers = maps.Clone(r.Headers)
	return r
}

// SimplePolicyEngine evaluates a list of rules against request metadata. It
// is safe for concurrent use.
type SimplePolicyEngine struct {
	modulePath string
	mu         sync.RWMutex
	rules      []Rule
}

// NewSimplePolicyEngine creates a lightweight policy engine.
func NewSimplePolicyEngine(modulePath string) *SimplePolicyEngine {
	return &SimplePolicyEngine{modulePath: modulePath}
}

// AddRule appends a policy rule.
func (e *SimplePolicyEngine) AddRule(r Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, cloneRule(r))
}

// pathMatches implements the segment-aware prefix match described on Rule.
func pathMatches(rulePath, reqPath string) bool {
	if rulePath == "" {
		return true
	}
	if reqPath == "" {
		reqPath = "/"
	}
	clean := path.Clean("/" + reqPath)
	if strings.HasSuffix(rulePath, "/") {
		return strings.HasPrefix(clean+"/", rulePath) || clean+"/" == rulePath
	}
	return clean == rulePath || strings.HasPrefix(clean, rulePath+"/")
}

func headerValue(headers map[string]interface{}, name string) (string, bool) {
	for k, hv := range headers {
		if !strings.EqualFold(k, name) {
			continue
		}
		switch t := hv.(type) {
		case string:
			return t, true
		case []string:
			return strings.Join(t, ","), true
		case []interface{}:
			parts := make([]string, 0, len(t))
			for _, item := range t {
				if s, ok := item.(string); ok {
					parts = append(parts, s)
				}
			}
			return strings.Join(parts, ","), true
		default:
			return fmt.Sprint(t), true
		}
	}
	return "", false
}

// Evaluate applies rules in order; first match wins, default allow if none match.
func (e *SimplePolicyEngine) Evaluate(ctx context.Context, input map[string]interface{}) (PolicyResult, error) {
	method, _ := input["method"].(string)
	reqPath, _ := input["path"].(string)
	headers, _ := input["header"].(map[string]interface{})
	body, _ := input["body"].(string)
	if body == "" {
		body, _ = input["data"].(string)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, rule := range e.rules {
		if !pathMatches(rule.Path, reqPath) {
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
		if len(rule.Headers) > 0 {
			match := true
			for k, v := range rule.Headers {
				actual, ok := headerValue(headers, k)
				if !ok || !strings.Contains(actual, v) {
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

// MaxPolicyBodyBytes caps the request body ComplianceMiddleware buffers for
// policy evaluation. Larger bodies are rejected with 413.
var MaxPolicyBodyBytes int64 = 10 << 20

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

var errBodyTooLarge = errors.New("request body too large")

// readAndRestoreBody buffers the body (bounded) and restores it for next.
func readAndRestoreBody(r *http.Request) (string, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return "", nil
	}
	limit := MaxPolicyBodyBytes
	if limit <= 0 {
		limit = 10 << 20
	}
	if r.ContentLength > limit {
		return "", errBodyTooLarge
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	_ = r.Body.Close()
	if err != nil {
		return "", err
	}
	if int64(len(b)) > limit {
		return "", errBodyTooLarge
	}
	r.Body = io.NopCloser(bytes.NewReader(b))
	r.ContentLength = int64(len(b))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
	return string(b), nil
}

// RequestInput builds the policy input map for r: method, path, query,
// header (canonical names), and body (read up to MaxPolicyBodyBytes and
// restored for the next handler).
func RequestInput(r *http.Request) (map[string]interface{}, error) {
	headers := make(map[string]interface{}, len(r.Header))
	for k, v := range r.Header {
		if len(v) == 1 {
			headers[k] = v[0]
		} else {
			headers[k] = append([]string(nil), v...)
		}
	}
	input := map[string]interface{}{
		"method": r.Method,
		"path":   r.URL.Path,
		"query":  r.URL.RawQuery,
		"header": headers,
	}
	body, err := readAndRestoreBody(r)
	if err != nil {
		return nil, err
	}
	if body != "" {
		input["body"] = body
	}
	return input, nil
}

// ComplianceMiddleware returns HTTP 451 when a policy denies a request.
// Evaluation errors fail closed (500). The request body is restored for the
// next handler.
func ComplianceMiddleware(engine PolicyEngine) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if engine == nil {
				next.ServeHTTP(w, r)
				return
			}
			input, err := RequestInput(r)
			if err != nil {
				if errors.Is(err, errBodyTooLarge) {
					writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
				} else {
					writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read request body"})
				}
				return
			}
			result, err := engine.Evaluate(r.Context(), input)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "compliance evaluation failed"})
				return
			}
			if !result.Allowed {
				writeJSON(w, http.StatusUnavailableForLegalReasons, map[string]interface{}{
					"error":  "policy violation",
					"policy": result.Policy,
					"reason": result.Reason,
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
