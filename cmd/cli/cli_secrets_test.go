package main

import (
	"strings"
	"testing"
)

func TestSecretsCreateOutput(t *testing.T) {
	output := captureOutput(t, []string{"secrets", "--name", "api-key", "--value", "secret123", "--type", "token"})
	if !strings.Contains(output, `"type":"token"`) {
		t.Fatalf("expected secrets output, got: %s", output)
	}
}
