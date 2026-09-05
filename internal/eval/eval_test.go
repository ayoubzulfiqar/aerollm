package eval

import (
	"context"
	"strings"
	"testing"
)

type fakeJudge struct{}

func (f *fakeJudge) ChatCompletion(ctx context.Context, prompt string) (string, error) {
	return "85", nil
}

func TestJudgePipelineScores(t *testing.T) {
	store := NewInMemoryScoreStore()
	pipeline := NewJudgePipeline(&fakeJudge{}, store, "general")
	score, err := pipeline.ScoreRequest(context.Background(), "hi", "hello", "m1", "p1", "v1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if score != 85 {
		t.Fatalf("expected score 85, got %f", score)
	}
}

func TestRegressionDetector(t *testing.T) {
	store := NewInMemoryScoreStore()
	_ = store.AppendScore(context.Background(), ScoreRecord{PromptVersion: "v1", Score: 90, RecordedAt: 1})
	_ = store.AppendScore(context.Background(), ScoreRecord{PromptVersion: "v2", Score: 70, RecordedAt: 2})
	detector := NewRegressionDetector(store)
	regressions, err := detector.Detect(context.Background(), ScoreFilter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(regressions) != 1 {
		t.Fatalf("expected 1 regression, got %d", len(regressions))
	}
}

func TestBenchmarkRunner(t *testing.T) {
	store := NewInMemoryScoreStore()
	runner := NewBenchmarkRunner(&fakeJudge{}, store)
	dataset := strings.NewReader(`{"prompt":"hello"}`)
	result, err := runner.Run(context.Background(), dataset, "m1", "p1", "general")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Total != 1 {
		t.Fatalf("expected total 1, got %d", result.Total)
	}
}
