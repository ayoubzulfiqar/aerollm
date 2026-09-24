package k8s

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestFileConfigSourceRunEmitsOnChange(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/config.yaml"
	_ = os.WriteFile(path, []byte("v1"), 0o644)

	src := &FileConfigSource{Path: path, Interval: 20 * time.Millisecond}
	updates := make(chan []byte, 64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = src.Run(ctx, updates) }()

	var got []byte
	select {
	case got = <-updates:
	case <-time.After(3 * time.Second):
		t.Fatal("expected initial update")
	}
	if string(got) != "v1" {
		t.Fatalf("expected v1, got %s", string(got))
	}

	_ = os.WriteFile(path, []byte("v2"), 0o644)
	select {
	case got = <-updates:
		if string(got) != "v2" {
			t.Fatalf("expected v2, got %s", string(got))
		}
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
	if DefaultOperatorConfig() != "/custom/path" {
		t.Fatalf("unexpected default config path")
	}
}

func TestFileConfigSourceMissingFile(t *testing.T) {
	src := &FileConfigSource{Path: t.TempDir() + "/missing.json"}
	if err := src.Run(context.Background(), make(chan []byte, 1)); err == nil {
		t.Fatal("expected error for missing file")
	}
	if err := (&FileConfigSource{}).Run(context.Background(), make(chan []byte, 1)); err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestFileConfigSourceTooLarge(t *testing.T) {
	path := t.TempDir() + "/big.json"
	if err := os.WriteFile(path, make([]byte, MaxConfigBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	err := (&FileConfigSource{Path: path}).Run(context.Background(), make(chan []byte, 1))
	if !errors.Is(err, ErrConfigTooLarge) {
		t.Fatalf("expected ErrConfigTooLarge, got %v", err)
	}
}

func TestHTTPConfigSourceSkipsErrorsAndDedupes(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		switch {
		case n == 1:
			http.Error(w, "boom", http.StatusInternalServerError)
		case n <= 3:
			_, _ = w.Write([]byte("v1"))
		case n == 4:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("not found page"))
		default:
			_, _ = w.Write([]byte("v2"))
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := make(chan []byte, 16)
	done := make(chan error, 1)
	src := &HTTPConfigSource{URL: srv.URL, Interval: 10 * time.Millisecond}
	go func() { done <- src.Run(ctx, updates) }()

	var got []string
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case b := <-updates:
			got = append(got, string(b))
		case <-deadline:
			t.Fatalf("timed out, got %v", got)
		}
	}
	if got[0] != "v1" || got[1] != "v2" {
		t.Fatalf("expected [v1 v2] (error bodies ignored, duplicates suppressed), got %v", got)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil on cancel, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestHTTPConfigSourceSizeCap(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = w.Write(make([]byte, MaxConfigBytes+1))
			return
		}
		_, _ = w.Write([]byte("small"))
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := make(chan []byte, 4)
	go func() { _ = (&HTTPConfigSource{URL: srv.URL, Interval: 10 * time.Millisecond}).Run(ctx, updates) }()
	select {
	case b := <-updates:
		if string(b) != "small" {
			t.Fatalf("oversized body must be dropped, got %d bytes", len(b))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
}

func TestHTTPConfigSourceInvalidURL(t *testing.T) {
	if err := (&HTTPConfigSource{}).Run(context.Background(), make(chan []byte)); err == nil {
		t.Fatal("expected error for empty url")
	}
	if err := (&HTTPConfigSource{URL: "http://bad host/\x7f"}).Run(context.Background(), make(chan []byte)); err == nil {
		t.Fatal("expected error for malformed url")
	}
}
