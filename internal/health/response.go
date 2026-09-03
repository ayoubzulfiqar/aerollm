package health

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ServeHTTP satisfies http.Handler for /readyz.
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req == nil {
		return
	}
	checks := r.Checks(req.Context())
	out, _ := ReadinessResponse(checks)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
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
