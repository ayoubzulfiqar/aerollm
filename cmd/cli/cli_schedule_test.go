package main

import (
	"strings"
	"testing"
)

func TestScheduleCreateOutput(t *testing.T) {
	output := captureOutput(t, []string{"schedule", "--name", "backup", "--schedule", "0 0 * * *"})
	if !strings.Contains(output, `"name":"backup"`) {
		t.Fatalf("expected schedule output, got: %s", output)
	}
}
