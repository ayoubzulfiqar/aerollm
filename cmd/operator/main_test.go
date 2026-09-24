package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a goroutine-safe bytes.Buffer.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "operator-config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const manifests = `{"items":[
 {"kind":"AeroRoute","metadata":{"name":"r1"},"spec":{"strategy":"cost","providers":["openai"]}},
 {"kind":"AeroBudget","metadata":{"name":"b1"},"spec":{"max_usd":10,"api_key":"sk-supersecret"}},
 {"kind":"AeroAgentPipeline","metadata":{"name":"p1"},"spec":{"nodes":["a","b"],"edges":["a->b"]}}
]}`

func TestRunOnceValidManifests(t *testing.T) {
	var out syncBuffer
	err := run(context.Background(), []string{"--once", "--config", writeConfig(t, manifests)}, &out, &out)
	if err != nil {
		t.Fatalf("unexpected error: %v\n%s", err, out.String())
	}
	logs := out.String()
	if strings.Count(logs, `"msg":"reconciled"`) != 3 {
		t.Fatalf("expected 3 reconciled resources:\n%s", logs)
	}
	if !strings.Contains(logs, notAppliedM) {
		t.Fatalf("expected honest not-applied status:\n%s", logs)
	}
	if strings.Contains(logs, "sk-supersecret") {
		t.Fatalf("api key leaked into logs:\n%s", logs)
	}
}

func TestRunOnceReportsInvalidManifests(t *testing.T) {
	body := `[{"kind":"AeroBudget","metadata":{"name":"b1"},"spec":{"max_usd":-1}},
	          {"kind":"AeroRoute","metadata":{"name":"ok"}}]`
	var out syncBuffer
	err := run(context.Background(), []string{"--once", "--config", writeConfig(t, body)}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "1 resource(s) failed") {
		t.Fatalf("expected 1 failure, got %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "max_usd") {
		t.Fatalf("expected validation error in logs:\n%s", out.String())
	}
}

func TestRunMissingConfigFails(t *testing.T) {
	var out syncBuffer
	err := run(context.Background(), []string{"--config", filepath.Join(t.TempDir(), "nope.json")}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "config file") {
		t.Fatalf("expected config file error, got %v", err)
	}
}

func TestRunFlagErrors(t *testing.T) {
	cases := [][]string{
		{"--interval", "0s", "--config", "x"},
		{"--bogus"},
		{"--config", "", "--config-url", ""},
		{"--once", "--config-url", "https://example.com/cfg"},
		{"--config-url", "ftp://example.com/cfg"},
		{"--config", "x", "extra"},
	}
	for _, args := range cases {
		var out syncBuffer
		if err := run(context.Background(), args, &out, &out); err == nil {
			t.Errorf("run(%v) expected error", args)
		}
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	var out syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"--config", writeConfig(t, manifests), "--interval", "20ms"}, &out, &out)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(out.String(), `"msg":"reconciled"`) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean shutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("operator did not stop after cancel")
	}
	if !strings.Contains(out.String(), "aero-operator stopped") {
		t.Fatalf("expected shutdown log:\n%s", out.String())
	}
}
