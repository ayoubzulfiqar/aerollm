package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// DefaultCheckTimeout bounds each individual checker run by Registry.Checks.
const DefaultCheckTimeout = 5 * time.Second

// Check represents the result of a dependency/health check.
//
// Latency is serialized as milliseconds (float) under "latency_ms".
type Check struct {
	Name      string        `json:"name"`
	Healthy   bool          `json:"healthy"`
	Latency   time.Duration `json:"latency_ms"`
	Error     string        `json:"error,omitempty"`
	CheckedAt time.Time     `json:"checked_at"`
}

// checkJSON is the wire form of Check.
type checkJSON struct {
	Name      string    `json:"name"`
	Healthy   bool      `json:"healthy"`
	LatencyMs float64   `json:"latency_ms"`
	Error     string    `json:"error,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
}

// MarshalJSON emits Latency in milliseconds.
func (c Check) MarshalJSON() ([]byte, error) {
	return json.Marshal(checkJSON{
		Name:      c.Name,
		Healthy:   c.Healthy,
		LatencyMs: float64(c.Latency) / float64(time.Millisecond),
		Error:     c.Error,
		CheckedAt: c.CheckedAt,
	})
}

// UnmarshalJSON parses latency_ms back into a Duration.
func (c *Check) UnmarshalJSON(data []byte) error {
	var w checkJSON
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*c = Check{
		Name:      w.Name,
		Healthy:   w.Healthy,
		Latency:   time.Duration(w.LatencyMs * float64(time.Millisecond)),
		Error:     w.Error,
		CheckedAt: w.CheckedAt,
	}
	return nil
}

// Checker is a dependency health checker.
type Checker interface {
	Name() string
	Check(ctx context.Context) Check
}

// Registry tracks named dependency checkers. It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	checkers map[string]Checker
	timeout  time.Duration
}

// NewRegistry creates a new health registry.
func NewRegistry() *Registry {
	return &Registry{checkers: make(map[string]Checker), timeout: DefaultCheckTimeout}
}

// Register adds (or replaces, by name) a health checker.
func (r *Registry) Register(c Checker) {
	if r == nil || c == nil {
		return
	}
	r.mu.Lock()
	if r.checkers == nil {
		r.checkers = make(map[string]Checker)
	}
	r.checkers[c.Name()] = c
	r.mu.Unlock()
}

// Unregister removes a checker by name.
func (r *Registry) Unregister(name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.checkers, name)
	r.mu.Unlock()
}

// SetTimeout sets the per-check timeout (non-positive restores the default).
func (r *Registry) SetTimeout(d time.Duration) {
	if r == nil {
		return
	}
	if d <= 0 {
		d = DefaultCheckTimeout
	}
	r.mu.Lock()
	r.timeout = d
	r.mu.Unlock()
}

// Checks evaluates all registered checkers concurrently, each bounded by the
// per-check timeout. A checker that panics or does not return in time is
// reported unhealthy. Results are sorted by name.
func (r *Registry) Checks(ctx context.Context) []Check {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.RLock()
	names := make([]string, 0, len(r.checkers))
	checkers := make(map[string]Checker, len(r.checkers))
	for name, c := range r.checkers {
		names = append(names, name)
		checkers[name] = c
	}
	timeout := r.timeout
	r.mu.RUnlock()
	if timeout <= 0 {
		timeout = DefaultCheckTimeout
	}
	sort.Strings(names)

	out := make([]Check, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func(i int, name string, c Checker) {
			defer wg.Done()
			out[i] = runCheck(ctx, name, c, timeout)
		}(i, name, checkers[name])
	}
	wg.Wait()
	return out
}

// runCheck runs one checker with a timeout and panic recovery.
func runCheck(parent context.Context, name string, c Checker, timeout time.Duration) Check {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	start := time.Now()
	result := make(chan Check, 1) // buffered: a late checker never blocks
	go func() {
		defer func() {
			if p := recover(); p != nil {
				result <- Check{Name: name, Healthy: false, Error: "health check panicked"}
			}
		}()
		result <- c.Check(ctx)
	}()

	var chk Check
	select {
	case chk = <-result:
	case <-ctx.Done():
		chk = Check{Name: name, Healthy: false, Error: fmt.Sprintf("health check timed out: %v", ctx.Err())}
	}
	if chk.Name == "" {
		chk.Name = name
	}
	if chk.Latency == 0 {
		chk.Latency = time.Since(start)
	}
	if chk.CheckedAt.IsZero() {
		chk.CheckedAt = time.Now()
	}
	return chk
}

// LivenessResponse returns a JSON liveness payload.
func LivenessResponse() ([]byte, int) {
	return []byte(`{"status":"ok"}`), http.StatusOK
}

// ReadinessResponse returns a JSON readiness payload from checks and the
// HTTP status to serve: 200 when every check is healthy (or there are none),
// 503 otherwise (Kubernetes readiness semantics).
func ReadinessResponse(checks []Check) ([]byte, int) {
	ready := true
	for _, c := range checks {
		if !c.Healthy {
			ready = false
			break
		}
	}
	status, code := "ready", http.StatusOK
	if !ready {
		status, code = "not_ready", http.StatusServiceUnavailable
	}
	if checks == nil {
		checks = []Check{}
	}
	out, err := json.Marshal(map[string]interface{}{
		"status": status,
		"checks": checks,
	})
	if err != nil {
		return []byte(`{"status":"error"}`), http.StatusInternalServerError
	}
	return out, code
}
