package main

import (
	"strings"
	"testing"
)

func TestPolicyCreateOutput(t *testing.T) {
	output := captureOutput(t, []string{"policy", "--id", "allow", "--expr", "allow", "--severity", "low"})
	if !strings.Contains(output, `"severity":"low"`) {
		t.Fatalf("expected policy output, got: %s", output)
	}
}
