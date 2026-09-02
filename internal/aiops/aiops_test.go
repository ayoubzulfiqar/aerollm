package aiops

import (
	"context"
	"testing"
	"time"
)

func TestMetaAgentTunerEvaluatesAndApplies(t *testing.T) {
	applied := false
	source := &DefaultMetricsSource{
		requestsFn: func() int64 { return 100 },
		errorsFn:   func() int64 { return 50 },
		latencyFn:  func() float64 { return 500 },
	}
	tuner := NewMetaAgentTuner(source, 10*time.Millisecond, 0)
	tuner.RegisterAction(TunerAction{
		Name: "test-action",
		Apply: func(ctx context.Context) error {
			applied = true
			return nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	tuner.Run(ctx)
	if !applied {
		t.Fatalf("expected tuner to apply action under degraded metrics")
	}
}
