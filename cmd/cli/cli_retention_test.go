package main

import (
	"strings"
	"testing"
)

func TestRetentionCreateOutput(t *testing.T) {
	output := captureOutput(t, []string{"retention", "--id", "logs", "--resource", "logs", "--ttl", "24", "--max-items", "1000"})
	if !strings.Contains(output, `"resource":"logs"`) {
		t.Fatalf("expected retention output, got: %s", output)
	}
}
