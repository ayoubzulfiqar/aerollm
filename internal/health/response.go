package health

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ServeHTTP satisfies http.Handler for /readyz: 200 when ready, 503 when not.
// Only GET and HEAD are allowed.
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req == nil {
		return
	}
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"error":"method not allowed"}`))
		return
	}
	checks := r.Checks(req.Context())
	out, code := ReadinessResponse(checks)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	if req.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(out)
}

// PrintChecks prints readiness checks in text form.
func PrintChecks(checks []Check) {
	for _, c := range checks {
		fmt.Printf("check=%s healthy=%v latency=%s error=%s\n", c.Name, c.Healthy, c.Latency, c.Error)
	}
}

// MustMarshalJSON returns JSON for a value or an error.
func MustMarshalJSON(v interface{}) ([]byte, error) { return json.Marshal(v) }
