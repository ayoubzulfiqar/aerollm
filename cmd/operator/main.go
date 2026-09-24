// Command aero-operator watches AeroLLM resource manifests (AeroRoute,
// AeroBudget, AeroAgentPipeline) from a JSON file and/or an HTTP endpoint,
// validates them against api/v1alpha1 and records the desired state.
//
// There is no Kubernetes or control-plane client wired in yet: resources are
// validated and logged as JSON lines, and every reconcile honestly reports
// applied=false ("validated; no control-plane apply target configured").
// Use --once to validate a manifest file in CI; the exit status is non-zero
// when any resource is invalid.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/k8s"
)

const (
	version     = "v0.2.0"
	notAppliedM = "validated; no control-plane apply target configured"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "aero-operator:", err)
		os.Exit(1)
	}
}

type options struct {
	configPath string
	configURL  string
	interval   time.Duration
	once       bool
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("aero-operator", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.configPath, "config", k8s.DefaultOperatorConfig(), "path to a JSON manifest file (object, list {\"items\":[...]} or array)")
	fs.StringVar(&o.configURL, "config-url", "", "optional http(s) URL polled for JSON manifests")
	fs.DurationVar(&o.interval, "interval", 2*time.Second, "poll interval for config sources")
	fs.BoolVar(&o.once, "once", false, "validate the --config file once and exit (non-zero if any resource is invalid)")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if o.interval <= 0 {
		return o, fmt.Errorf("--interval must be positive, got %s", o.interval)
	}
	explicitConfig := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			explicitConfig = true
		}
	})
	// With only --config-url given, don't also require the default file.
	if o.configURL != "" && !explicitConfig {
		o.configPath = ""
	}
	if o.configPath == "" && o.configURL == "" {
		return o, errors.New("one of --config or --config-url is required")
	}
	if o.once && o.configPath == "" {
		return o, errors.New("--once requires --config")
	}
	if o.configURL != "" && !strings.HasPrefix(o.configURL, "https://") && !strings.HasPrefix(o.configURL, "http://") {
		return o, fmt.Errorf("--config-url must be an http(s) URL")
	}
	return o, nil
}

// run is the testable body of main. It returns when ctx is cancelled or
// every config source has finished.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	opts, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(stdout, nil))

	var sources []k8s.ConfigSource
	if opts.configPath != "" {
		// Fail fast on a missing/unreadable file instead of logging and idling.
		f, err := os.Open(opts.configPath)
		if err != nil {
			return fmt.Errorf("config file: %w", err)
		}
		data, err := io.ReadAll(io.LimitReader(f, k8s.MaxConfigBytes+1))
		f.Close()
		if err != nil {
			return fmt.Errorf("config file: %w", err)
		}
		if len(data) > k8s.MaxConfigBytes {
			return fmt.Errorf("config file: %w", k8s.ErrConfigTooLarge)
		}
		if opts.once {
			sources = append(sources, &namedSource{name: "file:" + opts.configPath, inner: k8s.NewInMemoryConfigSource(data)})
		} else {
			sources = append(sources, &k8s.FileConfigSource{Path: opts.configPath, Interval: opts.interval})
		}
	}
	if opts.configURL != "" {
		sources = append(sources, &k8s.HTTPConfigSource{URL: opts.configURL, Interval: opts.interval})
	}

	desired := newDesiredState()
	apply := func(ctx context.Context, kind k8s.ResourceKind, name string, spec map[string]interface{}) (k8s.ApplyResult, error) {
		desired.record(kind, name, spec)
		logger.Info("desired state recorded", "kind", string(kind), "name", name, "spec", redactSpec(spec))
		return k8s.ApplyResult{Kind: kind, Name: name, Applied: false, Message: notAppliedM}, nil
	}
	reconciler := &k8s.ManifestReconciler{RouterApply: apply, BudgetApply: apply, PipelineApply: apply}

	logger.Info("starting aero-operator", "version", version, "config", opts.configPath, "config_url", opts.configURL, "once", opts.once)
	failures := 0
	for res := range k8s.RunReconcileLoop(ctx, reconciler, nil, sources...) {
		kind, name, status := describe(res.Object)
		if res.Error != nil {
			failures++
			logger.Error("reconcile failed", "source", res.Source, "kind", kind, "name", name, "error", res.Error.Error())
			continue
		}
		logger.Info("reconciled", "source", res.Source, "kind", kind, "name", name, "status", status)
	}
	logger.Info("aero-operator stopped", "resources", desired.len(), "failures", failures)
	if opts.once && failures > 0 {
		return fmt.Errorf("%d resource(s) failed validation", failures)
	}
	return nil
}

// namedSource gives the one-shot in-memory source a meaningful name.
type namedSource struct {
	name  string
	inner k8s.ConfigSource
}

func (n *namedSource) Name() string { return n.name }
func (n *namedSource) Run(ctx context.Context, updates chan<- []byte) error {
	return n.inner.Run(ctx, updates)
}

// desiredState remembers the last validated spec per kind/name.
type desiredState struct {
	mu    sync.Mutex
	specs map[string]map[string]interface{}
}

func newDesiredState() *desiredState {
	return &desiredState{specs: make(map[string]map[string]interface{})}
}

func (d *desiredState) record(kind k8s.ResourceKind, name string, spec map[string]interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.specs[string(kind)+"/"+name] = spec
}

func (d *desiredState) len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.specs)
}

// sensitiveSpecKeys are never written to logs.
var sensitiveSpecKeys = map[string]bool{"api_key": true, "alert_webhook": true}

func redactSpec(spec map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(spec))
	for k, v := range spec {
		if sensitiveSpecKeys[k] {
			if s, ok := v.(string); ok && s != "" {
				v = "[REDACTED]"
			}
		}
		out[k] = v
	}
	return out
}

func describe(obj interface{}) (kind, name, status string) {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return "", "", ""
	}
	kind, _ = m["kind"].(string)
	if md, ok := m["metadata"].(map[string]interface{}); ok {
		name, _ = md["name"].(string)
	}
	if st, ok := m["status"].(map[string]interface{}); ok {
		status, _ = st["message"].(string)
	}
	return kind, name, status
}
