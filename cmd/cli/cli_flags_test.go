package main

import (
	"strings"
	"testing"
)

func TestFlagsSetOutput(t *testing.T) {
	output := captureOutput(t, []string{"flags", "--set", `{"key":"darkmode","enabled":true,"strategy":"global"}`, "--key", "darkmode"})
	if !strings.Contains(output, `"enabled":true`) {
		t.Fatalf("expected flags output, got: %s", output)
	}
}
