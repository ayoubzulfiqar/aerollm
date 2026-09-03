package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Check represents the result of a dependency/health check.
type Check struct {
	Name      string        `json:"name"`
	Healthy   bool          `json:"healthy"`
	Latency   time.Duration `json:"latency_ms"`
	Error     string        `json:"error"`
	CheckedAt time.Time     `json:"checked_at"`
}

// Checker is a dependency health checker.
type Checker interface {
	Name() string
	Check(ctx context.Context) Check
}

// Registry tracks named dependency checkers.
type Registry struct {
	mu       sync.RWMutex
	checkers map[string]Checker
}

// NewRegistry creates a new health registry.
func NewRegistry() *Registry {
	return &Registry{checkers: make(map[string]Checker)}
}

// Register adds a health checker.
func (r *Registry) Register(c Checker) {
	if r == nil || c == nil {
		return
	}
	r.mu.Lock()
	r.checkers[c.Name()] = c
	r.mu.Unlock()
}

// Checks evaluates all registered checkers.
func (r *Registry) Checks(ctx context.Context) []Check {
	r.mu.RLock()
	checkers := make([]Checker, 0, len(r.checkers))
	for _, c := range r.checkers {
		checkers = append(checkers, c)
	}
	r.mu.RUnlock()

	out := make([]Check, len(checkers))
	for i, c := range checkers {
		out[i] = c.Check(ctx)
	}
	return out
}

// LivenessResponse returns a JSON liveness payload.
func LivenessResponse() ([]byte, int) {
	return []byte(`{"status":"ok"}`), http.StatusOK
}

// ReadinessResponse returns a JSON readiness payload from checks.
func ReadinessResponse(checks []Check) ([]byte, int) {
	ready := true
	for _, c := range checks {
		if !c.Healthy {
			ready = false
			break
		}
	}
	status := "ready"
	if !ready {
		status = "not_ready"
	}
	out, _ := json.Marshal(map[string]interface{}{
		"status": status,
		"checks": checks,
	})
	if out == nil {
		out = []byte(`{"status":"error"}`)
	}
	return out, http.StatusOK
}
