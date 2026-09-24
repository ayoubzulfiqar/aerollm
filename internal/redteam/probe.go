package redteam

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// ProbeFunc sends one adversarial prompt through the live gateway and returns
// the model's text response. It is supplied by the caller (the server wires
// it to its own chat-completion path); this package never dials out itself.
// Return an error wrapping ErrBlocked when the gateway's guardrails rejected
// the prompt.
type ProbeFunc func(ctx context.Context, prompt string) (string, error)

// Prober limits and defaults.
const (
	DefaultProbeConcurrency = 2
	MaxProbeConcurrency     = 16
	DefaultProbeInterval    = 250 * time.Millisecond
	DefaultProbeTimeout     = 30 * time.Second
	DefaultMaxResponseBytes = 64 << 10
	MaxProbesPerRun         = 500
	// MaxProbeFindings bounds the retained (deduplicated) findings; the
	// least recently seen are evicted first.
	MaxProbeFindings = 1000
	// ProbeFindingsBucket is the persist.Store bucket for probe findings.
	ProbeFindingsBucket = "redteam_probe_findings"
)

var (
	// ErrBlocked marks a probe the gateway refused (e.g. guardrails
	// blocked the prompt). ProbeFunc implementations should wrap it.
	ErrBlocked = errors.New("redteam: probe blocked by gateway")
	// ErrNoProbeFunc is returned when live mode is requested without a
	// ProbeFunc.
	ErrNoProbeFunc = errors.New("redteam: live probing requires a ProbeFunc")
	// ErrProbeRunning is returned when a run is already in progress.
	ErrProbeRunning = errors.New("redteam: probe run already in progress")
)

// ProbeOutcome is the result of one probe.
type ProbeOutcome string

// Probe outcomes.
const (
	// OutcomeDryRun: the probe was planned but not sent.
	OutcomeDryRun ProbeOutcome = "dry_run"
	// OutcomeResisted: the model answered without any success indicator.
	OutcomeResisted ProbeOutcome = "resisted"
	// OutcomeVulnerable: at least one success indicator fired.
	OutcomeVulnerable ProbeOutcome = "vulnerable"
	// OutcomeBlocked: the gateway refused the prompt (ErrBlocked).
	OutcomeBlocked ProbeOutcome = "blocked"
	// OutcomeError: the probe failed for another reason.
	OutcomeError ProbeOutcome = "error"
)

// ProberConfig configures a Prober. The zero value (plus a ProbeFunc) is a
// safe dry-run configuration.
type ProberConfig struct {
	// Probe sends a prompt through the live gateway. It is never called
	// unless Live is true.
	Probe ProbeFunc
	// Live enables sending probes. It defaults to false (dry-run): runs
	// only record the prompts that would have been sent.
	Live bool
	// Templates is the attack library (default DefaultTemplates()).
	Templates []AttackTemplate
	// Categories optionally restricts the templates used.
	Categories []Category
	// Concurrency bounds in-flight probes (default 2, max 16).
	Concurrency int
	// MinInterval is the minimum delay between probe starts (default
	// 250ms); it bounds the request rate against the gateway.
	MinInterval time.Duration
	// ProbeTimeout bounds each probe (default 30s).
	ProbeTimeout time.Duration
	// MaxProbesPerRun caps probes per run (default and max MaxProbesPerRun).
	MaxProbesPerRun int
	// MaxResponseBytes bounds how much of each response is inspected
	// (default 64 KiB).
	MaxResponseBytes int
	// Scan, when set, is the local prompt-injection shield; each result
	// records whether the shield flags the rendered prompt.
	Scan func(string) bool
	// Interval is the period of the Start loop (default 1h).
	Interval time.Duration
	// OnError, if set, receives errors from Start's background runs.
	OnError func(error)
}

// ProbeResult is the outcome of one probe in a run. Results never contain
// the model's raw response.
type ProbeResult struct {
	TemplateID string       `json:"template_id"`
	Category   Category     `json:"category"`
	Severity   Severity     `json:"severity"`
	Outcome    ProbeOutcome `json:"outcome"`
	// Prompt is the exact rendered prompt; recorded for dry runs only.
	Prompt string `json:"prompt,omitempty"`
	// PromptSHA256 identifies the rendered prompt.
	PromptSHA256 string `json:"prompt_sha256"`
	// ShieldFlagged reports whether the local shield (ProberConfig.Scan)
	// flags the prompt; nil when no shield is configured.
	ShieldFlagged *bool       `json:"shield_flagged,omitempty"`
	Indicators    []Indicator `json:"indicators,omitempty"`
	// FindingID links a vulnerable result to its deduplicated finding.
	FindingID string `json:"finding_id,omitempty"`
	// Error is a redacted, truncated error message.
	Error      string `json:"error,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

// ProbeFinding is a deduplicated successful attack. It stores only a
// redacted, truncated excerpt of the response and a fingerprint hash.
type ProbeFinding struct {
	ID          string      `json:"id"`
	TemplateID  string      `json:"template_id"`
	Category    Category    `json:"category"`
	Severity    Severity    `json:"severity"`
	Indicators  []Indicator `json:"indicators"`
	Fingerprint string      `json:"fingerprint"`
	Excerpt     string      `json:"excerpt"`
	FirstSeen   time.Time   `json:"first_seen"`
	LastSeen    time.Time   `json:"last_seen"`
	Count       int         `json:"count"`
}

// ProbeReport summarises one run.
type ProbeReport struct {
	RunID       string               `json:"run_id"`
	DryRun      bool                 `json:"dry_run"`
	StartedAt   time.Time            `json:"started_at"`
	FinishedAt  time.Time            `json:"finished_at"`
	Planned     int                  `json:"planned"`
	Results     []ProbeResult        `json:"results"`
	Summary     map[ProbeOutcome]int `json:"summary"`
	NewFindings []ProbeFinding       `json:"new_findings,omitempty"`
	// Error is set when the run stopped early (e.g. context canceled).
	Error string `json:"error,omitempty"`
}

// Prober runs the attack library against the live gateway through a
// caller-supplied ProbeFunc. It is dry-run unless ProberConfig.Live is set.
// It is safe for concurrent use; at most one run executes at a time.
type Prober struct {
	cfg       ProberConfig
	templates []AttackTemplate

	running  atomic.Bool
	looping  atomic.Bool
	mu       sync.Mutex
	findings map[string]*ProbeFinding
	last     *ProbeReport
	lastErr  error
	store    persist.Store
}

// NewProber validates cfg and returns a Prober.
func NewProber(cfg ProberConfig) (*Prober, error) {
	if cfg.Live && cfg.Probe == nil {
		return nil, ErrNoProbeFunc
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultProbeConcurrency
	}
	if cfg.Concurrency > MaxProbeConcurrency {
		cfg.Concurrency = MaxProbeConcurrency
	}
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = DefaultProbeInterval
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = DefaultProbeTimeout
	}
	if cfg.MaxProbesPerRun <= 0 || cfg.MaxProbesPerRun > MaxProbesPerRun {
		cfg.MaxProbesPerRun = MaxProbesPerRun
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = DefaultMaxResponseBytes
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	src := cfg.Templates
	if src == nil {
		src = DefaultTemplates()
	}
	src = filterTemplates(src, cfg.Categories)
	seen := make(map[string]struct{}, len(src))
	templates := make([]AttackTemplate, 0, len(src))
	for _, t := range src {
		if err := t.Validate(); err != nil {
			return nil, err
		}
		if _, dup := seen[t.ID]; dup {
			return nil, fmt.Errorf("%w: duplicate id %q", ErrInvalidTemplate, t.ID)
		}
		seen[t.ID] = struct{}{}
		templates = append(templates, cloneTemplate(t))
	}
	if len(templates) > cfg.MaxProbesPerRun {
		templates = templates[:cfg.MaxProbesPerRun]
	}
	cfg.Templates = nil
	return &Prober{cfg: cfg, templates: templates, findings: make(map[string]*ProbeFinding)}, nil
}

// DryRun reports whether the prober only plans probes.
func (p *Prober) DryRun() bool { return p == nil || !p.cfg.Live }

// Templates returns a copy of the templates the prober runs.
func (p *Prober) Templates() []AttackTemplate {
	if p == nil {
		return nil
	}
	out := make([]AttackTemplate, len(p.templates))
	for i, t := range p.templates {
		out[i] = cloneTemplate(t)
	}
	return out
}

// EnablePersistence loads previously recorded findings from ps (bucket
// ProbeFindingsBucket) and writes every new or updated finding through to
// it. Call it before the first run.
func (p *Prober) EnablePersistence(ps persist.Store) error {
	if p == nil || ps == nil {
		return errors.New("redteam: nil prober or store")
	}
	loaded, err := persist.LoadAll[ProbeFinding](ps, ProbeFindingsBucket)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store = ps
	for id, f := range loaded {
		if f.ID != id || f.TemplateID == "" {
			continue
		}
		fc := f
		// Re-sanitise in case the store was edited; +1 keeps an already
		// truncated excerpt (ending in "…") stable.
		fc.Excerpt = sanitizeText(fc.Excerpt, "", maxExcerptRunes+1)
		p.findings[id] = &fc
	}
	p.evictLocked(nil)
	return err
}

// Findings returns the deduplicated findings, most severe and most recent
// first.
func (p *Prober) Findings() []ProbeFinding {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	out := make([]ProbeFinding, 0, len(p.findings))
	for _, f := range p.findings {
		fc := *f
		fc.Indicators = append([]Indicator(nil), f.Indicators...)
		out = append(out, fc)
	}
	p.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		ri, rj := severityRank(out[i].Severity), severityRank(out[j].Severity)
		if ri != rj {
			return ri > rj
		}
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.After(out[j].LastSeen)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// LastReport returns the most recent run report, or nil.
func (p *Prober) LastReport() *ProbeReport {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.last == nil {
		return nil
	}
	return copyReport(p.last)
}

// LastError returns the error of the most recent failed background run.
func (p *Prober) LastError() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastErr
}

// Start runs Run every ProberConfig.Interval until ctx is canceled. Calling
// it while already running is a no-op.
func (p *Prober) Start(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !p.looping.CompareAndSwap(false, true) {
		return
	}
	defer p.looping.Store(false)
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := p.Run(ctx); err != nil && ctx.Err() == nil {
				p.mu.Lock()
				p.lastErr = err
				p.mu.Unlock()
				if p.cfg.OnError != nil {
					p.cfg.OnError(err)
				}
			}
		}
	}
}

// Run executes one pass over the template library. In dry-run mode it
// records the rendered prompts without calling the ProbeFunc. In live mode
// it sends each prompt (bounded by Concurrency and MinInterval), records
// deduplicated findings and returns the report. When ctx is canceled it
// stops dispatching, waits for in-flight probes and returns the partial
// report together with ctx.Err().
func (p *Prober) Run(ctx context.Context) (*ProbeReport, error) {
	if p == nil {
		return nil, errors.New("redteam: nil prober")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !p.running.CompareAndSwap(false, true) {
		return nil, ErrProbeRunning
	}
	defer p.running.Store(false)

	report := &ProbeReport{
		RunID:     newRunID(),
		DryRun:    !p.cfg.Live,
		StartedAt: time.Now().UTC(),
		Planned:   len(p.templates),
		Summary:   make(map[ProbeOutcome]int),
	}
	results := make([]ProbeResult, len(p.templates))
	done := make([]bool, len(p.templates))
	var newIDs []string

	if !p.cfg.Live {
		for i, t := range p.templates {
			if err := ctx.Err(); err != nil {
				break
			}
			prompt := t.Render(newCanary())
			results[i] = p.baseResult(t, prompt)
			results[i].Outcome = OutcomeDryRun
			results[i].Prompt = prompt
			done[i] = true
		}
	} else {
		var mu sync.Mutex
		jobs := make(chan int)
		var wg sync.WaitGroup
		for w := 0; w < p.cfg.Concurrency; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					if ctx.Err() != nil {
						continue
					}
					res, isNew := p.probeOne(ctx, p.templates[i])
					mu.Lock()
					results[i] = res
					done[i] = true
					if isNew {
						newIDs = append(newIDs, res.FindingID)
					}
					mu.Unlock()
				}
			}()
		}
		func() {
			defer close(jobs)
			for i := range p.templates {
				if i > 0 {
					timer := time.NewTimer(p.cfg.MinInterval)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
				if ctx.Err() != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case jobs <- i:
				}
			}
		}()
		wg.Wait()
	}

	for i := range results {
		if !done[i] {
			continue
		}
		report.Results = append(report.Results, results[i])
		report.Summary[results[i].Outcome]++
	}
	if len(newIDs) > 0 {
		p.mu.Lock()
		for _, id := range newIDs {
			if f, ok := p.findings[id]; ok {
				fc := *f
				fc.Indicators = append([]Indicator(nil), f.Indicators...)
				report.NewFindings = append(report.NewFindings, fc)
			}
		}
		p.mu.Unlock()
	}
	report.FinishedAt = time.Now().UTC()
	err := ctx.Err()
	if err != nil {
		report.Error = err.Error()
	}
	p.mu.Lock()
	p.last = copyReport(report)
	p.mu.Unlock()
	return report, err
}

func (p *Prober) baseResult(t AttackTemplate, prompt string) ProbeResult {
	sum := sha256.Sum256([]byte(prompt))
	res := ProbeResult{
		TemplateID:   t.ID,
		Category:     t.Category,
		Severity:     t.Severity,
		PromptSHA256: hex.EncodeToString(sum[:]),
	}
	if p.cfg.Scan != nil {
		flagged := p.cfg.Scan(prompt)
		res.ShieldFlagged = &flagged
	}
	return res
}

// probeOne sends a single probe and records a finding when it succeeded. It
// reports whether the finding is new.
func (p *Prober) probeOne(ctx context.Context, t AttackTemplate) (ProbeResult, bool) {
	canary := newCanary()
	prompt := t.Render(canary)
	res := p.baseResult(t, prompt)
	start := time.Now()
	pctx, cancel := context.WithTimeout(ctx, p.cfg.ProbeTimeout)
	resp, err := safeProbe(pctx, p.cfg.Probe, prompt)
	cancel()
	res.DurationMS = time.Since(start).Milliseconds()
	if err != nil {
		if errors.Is(err, ErrBlocked) {
			res.Outcome = OutcomeBlocked
		} else {
			res.Outcome = OutcomeError
			res.Error = sanitizeText(err.Error(), canary, 200)
		}
		return res, false
	}
	if len(resp) > p.cfg.MaxResponseBytes {
		resp = resp[:p.cfg.MaxResponseBytes]
	}
	res.Indicators = t.Detect(resp, canary)
	if len(res.Indicators) == 0 {
		res.Outcome = OutcomeResisted
		return res, false
	}
	res.Outcome = OutcomeVulnerable
	id, isNew := p.recordFinding(t, res.Indicators, resp, canary)
	res.FindingID = id
	return res, isNew
}

// safeProbe calls fn, converting a panic into an error.
func safeProbe(ctx context.Context, fn ProbeFunc, prompt string) (resp string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("redteam: probe function panicked")
		}
	}()
	return fn(ctx, prompt)
}

func (p *Prober) recordFinding(t AttackTemplate, indicators []Indicator, resp, canary string) (string, bool) {
	fp := responseFingerprint(resp, canary)
	id := findingID(t.ID, fp)
	now := time.Now().UTC()
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.findings[id]
	if ok {
		f.Count++
		f.LastSeen = now
		f.Indicators = mergeIndicators(f.Indicators, indicators)
	} else {
		f = &ProbeFinding{
			ID:          id,
			TemplateID:  t.ID,
			Category:    t.Category,
			Severity:    t.Severity,
			Indicators:  append([]Indicator(nil), indicators...),
			Fingerprint: fp,
			Excerpt:     sanitizeText(resp, canary, maxExcerptRunes),
			FirstSeen:   now,
			LastSeen:    now,
			Count:       1,
		}
		p.findings[id] = f
		p.evictLocked(f)
	}
	if p.store != nil {
		// Best effort: persistence failures must not abort a probe run;
		// the in-memory finding remains authoritative for this process.
		_ = p.store.Put(ProbeFindingsBucket, id, f)
	}
	return id, !ok
}

// evictLocked drops the least recently seen findings above
// MaxProbeFindings, never evicting keep.
func (p *Prober) evictLocked(keep *ProbeFinding) {
	for len(p.findings) > MaxProbeFindings {
		var oldest *ProbeFinding
		for _, f := range p.findings {
			if f == keep {
				continue
			}
			if oldest == nil || f.LastSeen.Before(oldest.LastSeen) || (f.LastSeen.Equal(oldest.LastSeen) && f.ID < oldest.ID) {
				oldest = f
			}
		}
		if oldest == nil {
			return
		}
		delete(p.findings, oldest.ID)
		if p.store != nil {
			_ = p.store.Delete(ProbeFindingsBucket, oldest.ID)
		}
	}
}

func mergeIndicators(a, b []Indicator) []Indicator {
	set := make(map[Indicator]struct{}, len(a)+len(b))
	for _, x := range a {
		set[x] = struct{}{}
	}
	for _, x := range b {
		set[x] = struct{}{}
	}
	out := make([]Indicator, 0, len(set))
	for x := range set {
		out = append(out, x)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func severityRank(s Severity) int {
	switch s {
	case SeverityCritical:
		return 4
	case SeverityHigh:
		return 3
	case SeverityMedium:
		return 2
	case SeverityLow:
		return 1
	}
	return 0
}

func copyReport(r *ProbeReport) *ProbeReport {
	out := *r
	out.Results = make([]ProbeResult, len(r.Results))
	for i, res := range r.Results {
		rc := res
		rc.Indicators = append([]Indicator(nil), res.Indicators...)
		if res.ShieldFlagged != nil {
			v := *res.ShieldFlagged
			rc.ShieldFlagged = &v
		}
		out.Results[i] = rc
	}
	out.Summary = make(map[ProbeOutcome]int, len(r.Summary))
	for k, v := range r.Summary {
		out.Summary[k] = v
	}
	out.NewFindings = make([]ProbeFinding, len(r.NewFindings))
	for i, f := range r.NewFindings {
		fc := f
		fc.Indicators = append([]Indicator(nil), f.Indicators...)
		out.NewFindings[i] = fc
	}
	return &out
}

// newCanary returns a random token such as AEROCANARY3f9a1c0d2b4e5f60.
func newCanary() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on supported platforms; fall back to
		// a time-derived value rather than panicking.
		n := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(n >> (8 * i))
		}
	}
	return "AEROCANARY" + hex.EncodeToString(b[:])
}

func newRunID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "rt-" + hex.EncodeToString(b[:])
}

// ShieldGaps renders every template (default: DefaultTemplates()) with a
// fixed placeholder canary and returns those the worker's prompt-injection
// shield does not flag. It is an offline check that sends nothing.
func (w *Worker) ShieldGaps(templates []AttackTemplate) []AttackTemplate {
	if w == nil || w.scan == nil {
		return nil
	}
	if templates == nil {
		templates = DefaultTemplates()
	}
	var out []AttackTemplate
	for _, t := range templates {
		if !w.scan(t.Render("AEROCANARY0000000000000000")) {
			out = append(out, cloneTemplate(t))
		}
	}
	return out
}
