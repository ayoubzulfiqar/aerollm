package k8s

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestFileConfigSourceRunEmitsOnChange(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/config.yaml"
	_ = os.WriteFile(path, []byte("v1"), 0o644)

	src := &FileConfigSource{Path: path}
	updates := make(chan []byte, 64)
	go func() { _ = src.Run(context.Background(), updates) }()

	var got []byte
	select {
	case got = <-updates:
	case <-time.After(3 * time.Second):
		t.Fatal("expected initial update")
	}
	if string(got) != "v1" { t.Fatalf("expected v1, got %s", string(got)) }

	_ = os.WriteFile(path, []byte("v2"), 0o644)
	select {
	case got = <-updates:
		if string(got) != "v2" { t.Fatalf("expected v2, got %s", string(got)) }
	case <-time.After(4 * time.Second):
		t.Fatal("expected updated config")
	}
}

func TestInMemoryConfigSourceRunEmitsPayloads(t *testing.T) {
	src := NewInMemoryConfigSource([]byte("a"), []byte("b"))
	updates := make(chan []byte, 64)
	go func() { _ = src.Run(context.Background(), updates) }()

	var vals []string
	for i := 0; i < 2; i++ {
		select {
		case b := <-updates:
			vals = append(vals, string(b))
		case <-time.After(time.Second):
			t.Fatal("missing payload")
		}
	}
	if len(vals) != 2 || vals[0] != "a" || vals[1] != "b" {
		t.Fatalf("unexpected payloads: %v", vals)
	}
}

func TestDefaultOperatorConfig(t *testing.T) {
	_ = os.Setenv("AEROLLM_OPERATOR_CONFIG", "/custom/path")
	defer os.Unsetenv("AEROLLM_OPERATOR_CONFIG")
	if DefaultOperatorConfig() != "/custom/path" { t.Fatalf("unexpected default config path") }
}
