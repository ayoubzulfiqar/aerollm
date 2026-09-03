package health

import (
	"context"
	"net/http"
	"testing"
	"time"
)

type fakeChecker struct {
	name    string
	healthy bool
}

func (f *fakeChecker) Name() string        { return f.name }
func (f *fakeChecker) Check(ctx context.Context) Check {
	return Check{Name: f.name, Healthy: f.healthy, Latency: 0, CheckedAt: time.Now()}
}

func TestRegistryEvaluatesCheckers(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&fakeChecker{name: "redis", healthy: true})
	reg.Register(&fakeChecker{name: "database", healthy: false})

	checks := reg.Checks(context.Background())
	if len(checks) != 2 {
		t.Fatalf("expected 2 checks, got %d", len(checks))
	}
	healthy := map[string]bool{}
	for _, c := range checks {
		healthy[c.Name] = c.Healthy
	}
	if !healthy["redis"] {
		t.Fatalf("expected redis healthy=true")
	}
	if healthy["database"] {
		t.Fatalf("expected database healthy=false")
	}
}

func TestReadinessResponseMarksNotReady(t *testing.T) {
	checks := []Check{
		{Name: "a", Healthy: true},
		{Name: "b", Healthy: false},
	}
	out, code := ReadinessResponse(checks)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if string(out) == `{"status":"ready"}` {
		t.Fatalf("expected not_ready when any check is unhealthy")
	}
}
