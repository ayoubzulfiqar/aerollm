package main

import (
	"strings"
	"testing"
)

func TestEvalJudgeOutput(t *testing.T) {
	output := captureOutput(t, []string{"eval", "--kind", "judge", "--prompt", "hi", "--response", "hello", "--model", "m1", "--provider", "p1", "--prompt-version", "v1"})
	if !strings.Contains(output, "85") {
		t.Fatalf("expected eval output, got: %s", output)
	}
}
