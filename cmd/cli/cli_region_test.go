package main

import (
	"strings"
	"testing"
)

func TestRegionCreateOutput(t *testing.T) {
	output := captureOutput(t, []string{"region", "--resource", "region", "--name", "us-east-1", "--endpoint", "https://us.example.com", "--primary"})
	if !strings.Contains(output, `"name":"us-east-1"`) {
		t.Fatalf("expected region output, got: %s", output)
	}
}
