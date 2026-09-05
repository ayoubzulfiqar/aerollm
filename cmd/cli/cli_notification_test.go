package main

import (
	"strings"
	"testing"
)

func TestNotificationChannelOutput(t *testing.T) {
	output := captureOutput(t, []string{"notification", "--resource", "channel"})
	if !strings.Contains(output, "[") {
		t.Fatalf("expected notification output, got: %s", output)
	}
}
