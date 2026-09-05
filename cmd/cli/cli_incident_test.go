package main

import (
	"strings"
	"testing"
)

func TestIncidentCreateOutput(t *testing.T) {
	output := captureOutput(t, []string{"incident", "--title", "outage", "--severity", "high"})
	if !strings.Contains(output, `"severity":"high"`) {
		t.Fatalf("expected incident output, got: %s", output)
	}
}
