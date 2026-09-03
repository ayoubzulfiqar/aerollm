package main

import (
	"context"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/health"
)

var readiness = health.NewRegistry()

type timeHealthChecker struct{}

func (t *timeHealthChecker) Name() string    { return "time" }
func (t *timeHealthChecker) Check(_ context.Context) health.Check {
	return health.Check{Name: "time", Healthy: true, CheckedAt: time.Now()}
}

func init() {
	readiness.Register(&timeHealthChecker{})
}
