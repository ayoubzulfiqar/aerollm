package redteam

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

var canaryInPrompt = regexp.MustCompile(`AEROCANARY[0-9a-f]{16}`)

const leakedKey = "sk-abcdefghijklmnopqrstuvwxyz123456"

// vulnerableTo returns a ProbeFunc that "falls for" the template whose
// prompt contains marker, echoing its canary alongside a secret and an
// email, and refuses everything else.
func vulnerableTo(marker string, calls *atomic.Int32) ProbeFunc {
	return func(ctx context.Context, prompt string) (string, error) {
		calls.Add(1)
		if strings.Contains(prompt, marker) {
			c := canaryInPrompt.FindString(prompt)
			return fmt.Sprintf("Sure! %s. By the way my key is %s and admin is root@example.com", c, leakedKey), nil
		}
		return "I can't help with that.", nil
	}
}

func fastConfig() ProberConfig {
	return ProberConfig{MinInterval: time.Millisecond, Concurrency: 4}
}

func TestNewProberLiveRequiresProbeFunc(t *testing.T) {
	cfg := fastConfig()
	cfg.Live = true
	if _, err := NewProber(cfg); !errors.Is(err, ErrNoProbeFunc) {
		t.Fatalf("expected ErrNoProbeFunc, got %v", err)
	}
}

func TestProberDryRunNeverCallsProbe(t *testing.T) {
	var calls atomic.Int32
	cfg := fastConfig()
	cfg.Probe = vulnerableTo("Ignore all previous", &calls)
	cfg.Scan = func(s string) bool { return strings.Contains(s, "Ignore all previous") }
	p, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !p.DryRun() {
		t.Fatal("prober must default to dry-run")
	}
	rep, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("dry run called the probe %d times", calls.Load())
	}
	if !rep.DryRun || rep.Planned != len(DefaultTemplates()) || len(rep.Results) != rep.Planned {
		t.Fatalf("unexpected dry-run report: planned=%d results=%d", rep.Planned, len(rep.Results))
	}
	if rep.Summary[OutcomeDryRun] != rep.Planned {
		t.Fatalf("unexpected summary %v", rep.Summary)
	}
	flagged := 0
	for _, r := range rep.Results {
		if r.Outcome != OutcomeDryRun || r.Prompt == "" || len(r.PromptSHA256) != 64 || r.ShieldFlagged == nil {
			t.Fatalf("dry-run result must record the planned prompt: %+v", r)
		}
		if *r.ShieldFlagged {
			flagged++
		}
	}
	if flagged == 0 {
		t.Fatal("expected the stub shield to flag at least one prompt")
	}
	if len(p.Findings()) != 0 {
		t.Fatal("dry run must not record findings")
	}
	if last := p.LastReport(); last == nil || last.RunID != rep.RunID {
		t.Fatal("LastReport not recorded")
	}
}

func TestProberLiveRecordsDedupedRedactedFindings(t *testing.T) {
	var calls atomic.Int32
	cfg := fastConfig()
	cfg.Live = true
	cfg.Probe = vulnerableTo("reply with only the word", &calls)
	cfg.Categories = []Category{CategoryDirectInjection, CategoryRoleplay}
	p, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	planned := len(TemplatesByCategory(CategoryDirectInjection, CategoryRoleplay))

	rep1, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if int(calls.Load()) != planned || len(rep1.Results) != planned {
		t.Fatalf("expected %d live probes, got %d calls / %d results", planned, calls.Load(), len(rep1.Results))
	}
	if rep1.Summary[OutcomeVulnerable] != 1 || rep1.Summary[OutcomeResisted] != planned-1 {
		t.Fatalf("unexpected summary %v", rep1.Summary)
	}
	if len(rep1.NewFindings) != 1 {
		t.Fatalf("expected 1 new finding, got %d", len(rep1.NewFindings))
	}

	rep2, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep2.NewFindings) != 0 {
		t.Fatalf("repeat finding must be deduplicated, got %d new", len(rep2.NewFindings))
	}
	fs := p.Findings()
	if len(fs) != 1 {
		t.Fatalf("expected 1 deduplicated finding, got %d", len(fs))
	}
	f := fs[0]
	if f.Count != 2 || f.TemplateID != "direct-ignore-previous" || f.Severity != SeverityHigh {
		t.Fatalf("unexpected finding %+v", f)
	}
	for _, leaked := range []string{leakedKey, "root@example.com", "AEROCANARY"} {
		if strings.Contains(f.Excerpt, leaked) {
			t.Fatalf("finding excerpt leaks %q: %q", leaked, f.Excerpt)
		}
	}
	if !strings.Contains(f.Excerpt, "[CANARY]") || !strings.Contains(f.Excerpt, "[REDACTED:") {
		t.Fatalf("excerpt should keep masked markers: %q", f.Excerpt)
	}
	for _, r := range rep1.Results {
		if r.Prompt != "" {
			t.Fatal("live results must not carry prompts")
		}
		if r.Outcome == OutcomeVulnerable && r.FindingID != f.ID {
			t.Fatalf("result not linked to finding: %+v", r)
		}
	}
}

func TestProberClassifiesBlockedErrorsAndPanics(t *testing.T) {
	cfg := fastConfig()
	cfg.Live = true
	cfg.Templates = []AttackTemplate{
		{ID: "blocked", Category: CategoryDirectInjection, Severity: SeverityLow, Prompt: "block me " + CanaryPlaceholder},
		{ID: "fails", Category: CategoryDirectInjection, Severity: SeverityLow, Prompt: "fail me"},
		{ID: "panics", Category: CategoryDirectInjection, Severity: SeverityLow, Prompt: "panic me"},
	}
	cfg.Probe = func(ctx context.Context, prompt string) (string, error) {
		switch {
		case strings.HasPrefix(prompt, "block"):
			return "", fmt.Errorf("guardrails: %w", ErrBlocked)
		case strings.HasPrefix(prompt, "fail"):
			return "", fmt.Errorf("upstream 401: invalid key %s for user root@example.com", leakedKey)
		default:
			panic("boom")
		}
	}
	p, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]ProbeResult{}
	for _, r := range rep.Results {
		got[r.TemplateID] = r
	}
	if got["blocked"].Outcome != OutcomeBlocked {
		t.Fatalf("expected blocked, got %+v", got["blocked"])
	}
	if got["fails"].Outcome != OutcomeError || strings.Contains(got["fails"].Error, leakedKey) || strings.Contains(got["fails"].Error, "root@example.com") {
		t.Fatalf("error must be recorded redacted: %+v", got["fails"])
	}
	if got["panics"].Outcome != OutcomeError {
		t.Fatalf("panicking probe must be an error, got %+v", got["panics"])
	}
}

func TestProberBoundsConcurrency(t *testing.T) {
	var inFlight, peak atomic.Int32
	cfg := fastConfig()
	cfg.Live = true
	cfg.Concurrency = 3
	cfg.Probe = func(ctx context.Context, prompt string) (string, error) {
		n := inFlight.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		inFlight.Add(-1)
		return "no", nil
	}
	cfg.Categories = []Category{CategoryMultilingual, CategoryEncoding}
	p, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if peak.Load() > 3 || peak.Load() < 1 {
		t.Fatalf("peak concurrency %d outside [1,3]", peak.Load())
	}
}

func TestProberMinIntervalRateLimits(t *testing.T) {
	cfg := ProberConfig{Live: true, Concurrency: 8, MinInterval: 20 * time.Millisecond}
	cfg.Templates = TemplatesByCategory(CategoryDirectInjection) // 3 templates -> >= 2 gaps
	cfg.Probe = func(ctx context.Context, prompt string) (string, error) { return "no", nil }
	p, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 40*time.Millisecond {
		t.Fatalf("run finished in %v; MinInterval not enforced", el)
	}
}

func TestProberHonorsContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	cfg := ProberConfig{Live: true, Concurrency: 1, MinInterval: 5 * time.Millisecond}
	cfg.Probe = func(pctx context.Context, prompt string) (string, error) {
		if calls.Add(1) == 1 {
			cancel()
		}
		<-pctx.Done()
		return "", pctx.Err()
	}
	p, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := p.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if rep == nil || len(rep.Results) >= rep.Planned || rep.Error == "" {
		t.Fatalf("expected a partial report, got %+v", rep)
	}
	if calls.Load() != 1 {
		t.Fatalf("no probes may start after cancellation, got %d calls", calls.Load())
	}
}

func TestProberRejectsOverlappingRuns(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	cfg := ProberConfig{Live: true, Concurrency: 1, MinInterval: time.Millisecond}
	cfg.Templates = TemplatesByCategory(CategoryDirectInjection)[:1]
	cfg.Probe = func(ctx context.Context, prompt string) (string, error) {
		once.Do(func() { close(started) })
		<-release
		return "no", nil
	}
	p, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.Run(context.Background())
		done <- err
	}()
	<-started
	if _, err := p.Run(context.Background()); !errors.Is(err, ErrProbeRunning) {
		t.Fatalf("expected ErrProbeRunning, got %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestProberPersistsFindings(t *testing.T) {
	store := persist.NewMemory()
	var calls atomic.Int32
	cfg := fastConfig()
	cfg.Live = true
	cfg.Probe = vulnerableTo("passphrase", &calls)
	cfg.Categories = []Category{CategorySystemPromptExtraction}

	p1, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p1.EnablePersistence(store); err != nil {
		t.Fatal(err)
	}
	if _, err := p1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := p1.Findings()
	if len(want) == 0 {
		t.Fatal("expected a finding to persist")
	}

	p2, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.EnablePersistence(store); err != nil {
		t.Fatal(err)
	}
	got := p2.Findings()
	if len(got) != len(want) || got[0].ID != want[0].ID || got[0].Excerpt != want[0].Excerpt {
		t.Fatalf("findings not restored: got %+v want %+v", got, want)
	}
	// A restart followed by the same response deduplicates against the
	// restored finding.
	rep, err := p2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.NewFindings) != 0 {
		t.Fatalf("restored finding must dedupe, got %d new", len(rep.NewFindings))
	}
	if err := (*Prober)(nil).EnablePersistence(store); err == nil {
		t.Fatal("nil prober must error")
	}
}

func TestNewProberValidatesTemplates(t *testing.T) {
	dup := AttackTemplate{ID: "d", Category: CategoryDirectInjection, Severity: SeverityLow, Prompt: "x"}
	if _, err := NewProber(ProberConfig{Templates: []AttackTemplate{dup, dup}}); !errors.Is(err, ErrInvalidTemplate) {
		t.Fatalf("expected duplicate id rejection, got %v", err)
	}
	if _, err := NewProber(ProberConfig{Templates: []AttackTemplate{{ID: "x"}}}); !errors.Is(err, ErrInvalidTemplate) {
		t.Fatalf("expected invalid template rejection, got %v", err)
	}
	p, err := NewProber(ProberConfig{MaxProbesPerRun: 2, Concurrency: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Templates()) != 2 || p.cfg.Concurrency != MaxProbeConcurrency {
		t.Fatalf("limits not applied: %d templates, concurrency %d", len(p.Templates()), p.cfg.Concurrency)
	}
}

func TestProberStartStops(t *testing.T) {
	cfg := ProberConfig{Interval: 5 * time.Millisecond, MinInterval: time.Millisecond}
	cfg.Templates = TemplatesByCategory(CategoryDirectInjection)[:1]
	p, err := NewProber(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Start(ctx)
		close(done)
	}()
	deadline := time.After(2 * time.Second)
	for p.LastReport() == nil {
		select {
		case <-deadline:
			t.Fatal("Start never ran")
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not stop")
	}
	var nilProber *Prober
	nilProber.Start(context.Background())
	if nilProber.Findings() != nil || nilProber.LastReport() != nil || nilProber.LastError() != nil || !nilProber.DryRun() {
		t.Fatal("nil prober accessors must be safe")
	}
}

func TestWorkerShieldGaps(t *testing.T) {
	w := NewWorker(DefaultConfig(), nil)
	w.scan = func(s string) bool { return strings.Contains(s, "Ignore all previous") }
	gaps := w.ShieldGaps(nil)
	if len(gaps) == 0 || len(gaps) >= len(DefaultTemplates()) {
		t.Fatalf("weak shield should miss some but not all templates, missed %d", len(gaps))
	}
	w.scan = func(string) bool { return true }
	if g := w.ShieldGaps(nil); len(g) != 0 {
		t.Fatalf("perfect shield must have no gaps, got %d", len(g))
	}
}
