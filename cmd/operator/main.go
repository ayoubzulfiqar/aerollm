// Command aero-operator reconciles AeroLLM resources (AeroRoute, AeroBudget,
// AeroAgentPipeline) into the AeroLLM gateway's admin API.
//
// Kubernetes mode (--kube, the default inside a pod when no manifest source
// is configured) lists and watches the aerollm.io/v1alpha1 custom resources
// with a stdlib-only client (in-cluster service account or kubeconfig),
// validates them, applies them to the gateway (POST /config/update, PUT
// /v1/budgets) with the admin key from $AEROLLM_ADMIN_KEY (configurable),
// and records the outcome in each resource's status subresource
// (observedGeneration plus Valid/Ready conditions).
//
// Manifest mode (--config file and/or --config-url) validates JSON manifests
// and, when --gateway-url is set, applies them to the gateway as well;
// otherwise it honestly reports applied=false. Use --once to validate a
// manifest file in CI; the exit status is non-zero when any resource is
// invalid.
//
// A YAML file given with --operator-config supplies defaults for every
// flag (see deploy/operator-config.yaml).
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

	"github.com/ayoubzulfiqar/aerollm/internal/k8s"
)

const (
	version     = "v0.3.0"
	notAppliedM = "validated; no control-plane apply target configured"
)

// serviceAccountDir is replaceable in tests.
var serviceAccountDir = k8s.DefaultServiceAccountDir

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

// run is the testable body of main. It returns when ctx is cancelled or
// every config source has finished.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	opts, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(stdout, nil))
	if opts.kube {
		return runKube(ctx, opts, logger)
	}
	return runManifests(ctx, opts, logger)
}

// gatewayFromOptions builds the gateway client, or returns nil when no
// gateway URL is configured.
func gatewayFromOptions(opts options) (*gatewayClient, error) {
	if opts.gatewayURL == "" {
		return nil, nil
	}
	key := strings.TrimSpace(getenv(opts.adminKeyEnv))
	if key == "" {
		return nil, fmt.Errorf("gateway admin key: environment variable %s is empty", opts.adminKeyEnv)
	}
	return newGatewayClient(opts.gatewayURL, key, opts.gatewayAllowHTTP, opts.gatewayCAFile, opts.requestTimeout)
}

// loadRESTConfig picks --kubeconfig, else the in-cluster service account,
// else $KUBECONFIG / ~/.kube/config.
func loadRESTConfig(opts options) (*k8s.RESTConfig, error) {
	kopts := k8s.KubeconfigOptions{Context: opts.kubeContext, AllowInsecure: opts.allowInsecureTLS}
	var cfg *k8s.RESTConfig
	var err error
	switch {
	case opts.kubeconfig != "":
		cfg, err = k8s.LoadKubeconfig(opts.kubeconfig, kopts)
	default:
		cfg, err = k8s.InClusterConfigFrom(k8s.InClusterOptions{Dir: serviceAccountDir, Getenv: getenv})
		if errors.Is(err, k8s.ErrNotInCluster) {
			path := k8s.DefaultKubeconfigPath()
			if path == "" {
				return nil, errors.New("not in a cluster and no kubeconfig found (set --kubeconfig)")
			}
			cfg, err = k8s.LoadKubeconfig(path, kopts)
		}
	}
	if err != nil {
		return nil, err
	}
	cfg.Timeout = opts.requestTimeout
	return cfg, nil
}

// runKube watches the custom resources and reconciles them until ctx is
// cancelled.
func runKube(ctx context.Context, opts options, logger *slog.Logger) error {
	restCfg, err := loadRESTConfig(opts)
	if err != nil {
		return fmt.Errorf("kubernetes config: %w", err)
	}
	client, err := k8s.NewClient(restCfg)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	gw, err := gatewayFromOptions(opts)
	if err != nil {
		return err
	}
	ctrl := newController(client, gw, logger)

	kinds := make([]string, 0, len(opts.kinds))
	for _, k := range opts.kinds {
		kinds = append(kinds, string(k))
	}
	ns := opts.namespace
	if ns == "" {
		ns = "*"
	}
	logger.Info("starting aero-operator", "version", version, "mode", "kube", "api_server", client.Host(),
		"namespace", ns, "kinds", kinds, "gateway", gw.base, "resync", opts.resync.String())

	var wg sync.WaitGroup
	for _, kind := range opts.kinds {
		gvr, _ := k8s.ResourceForKind(kind)
		kind := kind
		inf := &k8s.Informer{
			Client:       client,
			Resource:     gvr,
			Namespace:    opts.namespace,
			WatchTimeout: opts.watchTimeout,
			Logger:       logger,
			Handle:       func(ev k8s.WatchEvent) { ctrl.observe(kind, gvr, ev) },
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = inf.Run(ctx)
		}()
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		ctrl.runWorker(ctx)
	}()
	go func() {
		defer wg.Done()
		ctrl.runResync(ctx, opts.resync)
	}()
	wg.Wait()
	ctrl.stop()
	logger.Info("aero-operator stopped")
	return nil
}

// runManifests is the file/URL manifest mode.
func runManifests(ctx context.Context, opts options, logger *slog.Logger) error {
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

	// --once only validates; it never touches the gateway.
	var gw *gatewayClient
	if !opts.once {
		var err error
		if gw, err = gatewayFromOptions(opts); err != nil {
			return err
		}
	}

	desired := newDesiredState()
	apply := func(ctx context.Context, kind k8s.ResourceKind, name string, spec map[string]interface{}) (k8s.ApplyResult, error) {
		desired.record(kind, name, spec)
		logger.Info("desired state recorded", "kind", string(kind), "name", name, "spec", redactSpec(spec))
		if gw == nil {
			return k8s.ApplyResult{Kind: kind, Name: name, Applied: false, Message: notAppliedM}, nil
		}
		p, err := planManifest(kind, spec)
		if err != nil {
			return k8s.ApplyResult{Kind: kind, Name: name, Message: err.Error()}, err
		}
		if p.unsupported != "" {
			return k8s.ApplyResult{Kind: kind, Name: name, Message: p.unsupported}, nil
		}
		if err := gw.apply(ctx, p); err != nil {
			return k8s.ApplyResult{Kind: kind, Name: name, Message: err.Error()}, err
		}
		return k8s.ApplyResult{Kind: kind, Name: name, Applied: true, Message: "applied " + p.summary + p.note()}, nil
	}
	reconciler := &k8s.ManifestReconciler{RouterApply: apply, BudgetApply: apply, PipelineApply: apply}

	logger.Info("starting aero-operator", "version", version, "mode", "manifest", "config", opts.configPath, "config_url", opts.configURL, "once", opts.once, "gateway", gatewayBase(gw))
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

func gatewayBase(gw *gatewayClient) string {
	if gw == nil {
		return ""
	}
	return gw.base
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
