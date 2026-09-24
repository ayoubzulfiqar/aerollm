package evolution

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func newTestEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "patches")
	return NewEngine(Config{Interval: time.Hour, PatchDir: dir, MaxPending: 3}), dir
}

func listFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestNewEngineDefaultsMaxPending(t *testing.T) {
	e := NewEngine(Config{})
	// Previously panicked with "slice bounds out of range" when MaxPending was 0.
	if err := e.Submit(ImprovementProposal{ID: "a", Score: 1}); err != nil {
		t.Fatal(err)
	}
	if len(e.Pending()) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(e.Pending()))
	}
}

func TestSubmitValidation(t *testing.T) {
	e, _ := newTestEngine(t)
	if err := e.Submit(ImprovementProposal{Score: math.NaN()}); !errors.Is(err, ErrInvalidProposal) {
		t.Fatalf("NaN score must be rejected, got %v", err)
	}
	if err := e.Submit(ImprovementProposal{Score: math.Inf(1)}); !errors.Is(err, ErrInvalidProposal) {
		t.Fatalf("Inf score must be rejected, got %v", err)
	}
	if err := e.Submit(ImprovementProposal{Score: 1, Payload: make([]byte, MaxPayloadBytes+1)}); !errors.Is(err, ErrInvalidProposal) {
		t.Fatalf("oversize payload must be rejected, got %v", err)
	}
	var nilEngine *Engine
	if err := nilEngine.Submit(ImprovementProposal{}); err == nil {
		t.Fatal("nil engine must error")
	}
	if nilEngine.Pending() != nil || nilEngine.Applied() != nil {
		t.Fatal("nil engine accessors must be safe")
	}
	nilEngine.Start(context.Background()) // must not panic
}

func TestSubmitBoundsAndDedup(t *testing.T) {
	e, _ := newTestEngine(t)
	for i, id := range []string{"a", "b", "c", "d"} {
		if err := e.Submit(ImprovementProposal{ID: id, Score: float64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	p := e.Pending()
	if len(p) != 3 || p[0].ID != "b" {
		t.Fatalf("expected oldest dropped at MaxPending=3, got %+v", p)
	}
	// Same ID replaces rather than duplicates.
	if err := e.Submit(ImprovementProposal{ID: "c", Score: 42}); err != nil {
		t.Fatal(err)
	}
	p = e.Pending()
	if len(p) != 3 || p[1].Score != 42 {
		t.Fatalf("expected in-place replacement, got %+v", p)
	}
	// Generated IDs are unique even for identical descriptions.
	e2, _ := newTestEngine(t)
	_ = e2.Submit(ImprovementProposal{Description: "same"})
	_ = e2.Submit(ImprovementProposal{Description: "same"})
	p = e2.Pending()
	if len(p) != 2 || p[0].ID == p[1].ID {
		t.Fatalf("expected two distinct generated IDs, got %+v", p)
	}
}

func TestPendingReturnsDeepCopy(t *testing.T) {
	e, _ := newTestEngine(t)
	payload := []byte("abc")
	_ = e.Submit(ImprovementProposal{ID: "a", Payload: payload})
	payload[0] = 'X' // caller mutation after submit must not leak in
	p := e.Pending()
	p[0].Payload[1] = 'Y'
	if got := string(e.Pending()[0].Payload); got != "abc" {
		t.Fatalf("internal payload mutated: %q", got)
	}
}

func TestEvaluateWritesBestAndKeepsOthers(t *testing.T) {
	e, dir := newTestEngine(t)
	_ = e.Submit(ImprovementProposal{ID: "low", Type: "prompt", Score: 1, Payload: []byte("low")})
	_ = e.Submit(ImprovementProposal{ID: "high", Type: "prompt", Score: 5, Payload: []byte("high")})
	_ = e.Submit(ImprovementProposal{ID: "mid", Type: "prompt", Score: 3, Payload: []byte("mid")})

	if err := e.evaluate(context.Background()); err != nil {
		t.Fatal(err)
	}
	files := listFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %v", files)
	}
	data, _ := os.ReadFile(filepath.Join(dir, files[0]))
	if string(data) != "high" {
		t.Fatalf("expected best payload, got %q", data)
	}
	// Windows does not implement Unix permission bits.
	if runtime.GOOS != "windows" {
		info, _ := os.Stat(filepath.Join(dir, files[0]))
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("expected 0600, got %v", info.Mode().Perm())
		}
	}
	pending := e.Pending()
	if len(pending) != 2 {
		t.Fatalf("other proposals must remain pending, got %+v", pending)
	}

	// Next evaluations drain the rest in score order, then do nothing.
	_ = e.evaluate(context.Background())
	_ = e.evaluate(context.Background())
	_ = e.evaluate(context.Background())
	if got := e.Applied(); strings.Join(got, ",") != "high,mid,low" {
		t.Fatalf("unexpected applied order: %v", got)
	}
	if len(listFiles(t, dir)) != 3 || len(e.Pending()) != 0 {
		t.Fatalf("expected 3 files and nothing pending")
	}
	if err := e.Submit(ImprovementProposal{ID: "high", Score: 9}); !errors.Is(err, ErrAlreadyApplied) {
		t.Fatalf("resubmitting an applied ID must fail, got %v", err)
	}
	if s := e.Stats(); s.Applied != 3 || s.Pending != 0 || s.LastPatchPath == "" {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestEvaluateTypeCannotEscapePatchDir(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "a", "patches")
	e := NewEngine(Config{PatchDir: dir})
	_ = e.Submit(ImprovementProposal{ID: "../../x", Type: "/../../../evil", Score: 1, Payload: []byte("p")})
	if err := e.evaluate(context.Background()); err != nil {
		t.Fatal(err)
	}
	files := listFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected the proposal inside PatchDir, got %v", files)
	}
	if !strings.HasPrefix(files[0], "evolution-evil-") || strings.ContainsAny(files[0], "/\\") {
		t.Fatalf("unexpected file name %q", files[0])
	}
	// Nothing written outside PatchDir.
	for _, p := range []string{filepath.Join(base, "a"), base} {
		for _, n := range listFiles(t, p) {
			if strings.HasPrefix(n, "evolution-") {
				t.Fatalf("file escaped PatchDir: %s/%s", p, n)
			}
		}
	}
}

func TestSanitizeType(t *testing.T) {
	cases := map[string]string{
		"":                        "proposal",
		"../../..":                "proposal",
		"Prompt":                  "prompt",
		"a/b\\c":                  "abc",
		strings.Repeat("x", 100):  strings.Repeat("x", maxTypeLen),
		"routing-policy_v2":       "routing-policy_v2",
		"\x00\u202e../etc/passwd": "etcpasswd",
	}
	for in, want := range cases {
		if got := sanitizeType(in); got != want {
			t.Errorf("sanitizeType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEvaluateWriteFailureKeepsProposal(t *testing.T) {
	base := t.TempDir()
	blocker := filepath.Join(base, "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// PatchDir below a regular file cannot be created.
	e := NewEngine(Config{PatchDir: filepath.Join(blocker, "patches")})
	_ = e.Submit(ImprovementProposal{ID: "a", Score: 1, Payload: []byte("p")})
	if err := e.evaluate(context.Background()); err == nil {
		t.Fatal("expected write error")
	}
	if len(e.Pending()) != 1 || len(e.Applied()) != 0 || e.Stats().LastError == "" {
		t.Fatal("failed proposal must stay pending and error must be recorded")
	}
}

func TestStartStopsOnCancelAndCanRestart(t *testing.T) {
	e, dir := newTestEngine(t)
	e.cfg.Interval = 5 * time.Millisecond
	_ = e.Submit(ImprovementProposal{ID: "a", Score: 1, Payload: []byte("p")})

	for round := 0; round < 2; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			e.Start(ctx)
			close(done)
		}()
		time.Sleep(30 * time.Millisecond)
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Start did not return after cancel")
		}
	}
	if len(listFiles(t, dir)) != 1 {
		t.Fatalf("expected the proposal to be written once, got %v", listFiles(t, dir))
	}
}
