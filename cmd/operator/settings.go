package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/ayoubzulfiqar/aerollm/internal/k8s"
)

// Defaults.
const (
	defaultAdminKeyEnv    = "AEROLLM_ADMIN_KEY"
	gatewayURLEnv         = "AEROLLM_GATEWAY_URL"
	defaultRequestTimeout = 15 * time.Second
	defaultResync         = 10 * time.Minute
	defaultWatchTimeout   = 5 * time.Minute
	maxSettingsBytes      = 1 << 20
)

// getenv is replaceable in tests.
var getenv = os.Getenv

type options struct {
	// Manifest (file / URL) mode.
	configPath string
	configURL  string
	interval   time.Duration
	once       bool

	// Kubernetes mode.
	kube             bool
	kubeconfig       string
	kubeContext      string
	namespace        string
	allowInsecureTLS bool
	kinds            []k8s.ResourceKind
	resync           time.Duration
	watchTimeout     time.Duration

	// Gateway admin API.
	gatewayURL       string
	gatewayAllowHTTP bool
	gatewayCAFile    string
	adminKeyEnv      string
	requestTimeout   time.Duration

	operatorConfig string
}

// duration decodes "30s"-style strings from YAML.
type duration time.Duration

func (d *duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("line %d: duration must be a string like \"30s\"", n.Line)
	}
	v, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*d = duration(v)
	return nil
}

// fileSettings is the YAML operator config (--operator-config). Unknown
// keys are rejected. Command-line flags override file values.
type fileSettings struct {
	Manifest         *string   `yaml:"manifest"`
	ManifestURL      *string   `yaml:"manifest_url"`
	Interval         *duration `yaml:"interval"`
	Kube             *bool     `yaml:"kube"`
	Kubeconfig       *string   `yaml:"kubeconfig"`
	Context          *string   `yaml:"context"`
	Namespace        *string   `yaml:"namespace"`
	AllowInsecureTLS *bool     `yaml:"allow_insecure_tls"`
	Kinds            []string  `yaml:"kinds"`
	ResyncInterval   *duration `yaml:"resync_interval"`
	WatchTimeout     *duration `yaml:"watch_timeout"`
	GatewayURL       *string   `yaml:"gateway_url"`
	GatewayAllowHTTP *bool     `yaml:"gateway_allow_http"`
	GatewayCAFile    *string   `yaml:"gateway_ca_file"`
	AdminKeyEnv      *string   `yaml:"admin_key_env"`
	RequestTimeout   *duration `yaml:"request_timeout"`
}

func loadFileSettings(path string) (*fileSettings, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("operator config: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxSettingsBytes+1))
	if err != nil {
		return nil, fmt.Errorf("operator config: %w", err)
	}
	if len(data) > maxSettingsBytes {
		return nil, fmt.Errorf("operator config: larger than %d bytes", maxSettingsBytes)
	}
	var fs fileSettings
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&fs); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("operator config %s: %w", path, err)
	}
	return &fs, nil
}

func parseKinds(list []string) ([]k8s.ResourceKind, error) {
	var out []k8s.ResourceKind
	seen := map[k8s.ResourceKind]bool{}
	for _, raw := range list {
		for _, s := range strings.Split(raw, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			k := k8s.ResourceKind(s)
			if _, ok := k8s.ResourceForKind(k); !ok {
				return nil, fmt.Errorf("unknown kind %q (want AeroRoute, AeroBudget, AeroAgentPipeline)", s)
			}
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	return out, nil
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var o options
	var kinds string
	fs := flag.NewFlagSet("aero-operator", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.configPath, "config", "", "path to a JSON manifest file (object, list {\"items\":[...]} or array); default "+k8s.DefaultOperatorConfig())
	fs.StringVar(&o.configURL, "config-url", "", "optional http(s) URL polled for JSON manifests")
	fs.DurationVar(&o.interval, "interval", 2*time.Second, "poll interval for manifest sources")
	fs.BoolVar(&o.once, "once", false, "validate the --config file once and exit (non-zero if any resource is invalid)")
	fs.StringVar(&o.operatorConfig, "operator-config", "", "YAML operator config file (flags override its values)")
	fs.BoolVar(&o.kube, "kube", false, "watch AeroRoute/AeroBudget/AeroAgentPipeline custom resources in Kubernetes (default when running in a pod without --config/--config-url)")
	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "kubeconfig path (default: in-cluster service account, then $KUBECONFIG or ~/.kube/config)")
	fs.StringVar(&o.kubeContext, "context", "", "kubeconfig context (default: current-context)")
	fs.StringVar(&o.namespace, "namespace", "", "namespace to watch (default: all namespaces)")
	fs.BoolVar(&o.allowInsecureTLS, "allow-insecure-tls", false, "honour insecure-skip-tls-verify in the kubeconfig (unsafe)")
	fs.StringVar(&kinds, "kinds", "", "comma-separated kinds to watch (default: all three)")
	fs.DurationVar(&o.resync, "resync", defaultResync, "re-apply every resource to the gateway this often (0 disables)")
	fs.DurationVar(&o.watchTimeout, "watch-timeout", defaultWatchTimeout, "server-side watch timeout before reconnecting")
	fs.StringVar(&o.gatewayURL, "gateway-url", "", "AeroLLM gateway base URL for admin calls (env "+gatewayURLEnv+")")
	fs.BoolVar(&o.gatewayAllowHTTP, "gateway-allow-http", false, "allow a plain-http gateway URL (the admin key is sent in clear text)")
	fs.StringVar(&o.gatewayCAFile, "gateway-ca-file", "", "PEM CA bundle to verify the gateway's TLS certificate")
	fs.StringVar(&o.adminKeyEnv, "admin-key-env", defaultAdminKeyEnv, "name of the environment variable holding the gateway admin key")
	fs.DurationVar(&o.requestTimeout, "request-timeout", defaultRequestTimeout, "timeout for gateway and non-watch Kubernetes requests")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	visited := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { visited[f.Name] = true })

	// YAML settings fill in anything not given on the command line.
	configSet, urlSet := visited["config"], visited["config-url"]
	if o.operatorConfig != "" {
		s, err := loadFileSettings(o.operatorConfig)
		if err != nil {
			return o, err
		}
		setStr := func(flagName string, dst *string, v *string) {
			if v != nil && !visited[flagName] {
				*dst = *v
			}
		}
		setBool := func(flagName string, dst *bool, v *bool) {
			if v != nil && !visited[flagName] {
				*dst = *v
			}
		}
		setDur := func(flagName string, dst *time.Duration, v *duration) {
			if v != nil && !visited[flagName] {
				*dst = time.Duration(*v)
			}
		}
		setStr("config", &o.configPath, s.Manifest)
		setStr("config-url", &o.configURL, s.ManifestURL)
		configSet = configSet || s.Manifest != nil
		urlSet = urlSet || s.ManifestURL != nil
		setDur("interval", &o.interval, s.Interval)
		setBool("kube", &o.kube, s.Kube)
		setStr("kubeconfig", &o.kubeconfig, s.Kubeconfig)
		setStr("context", &o.kubeContext, s.Context)
		setStr("namespace", &o.namespace, s.Namespace)
		setBool("allow-insecure-tls", &o.allowInsecureTLS, s.AllowInsecureTLS)
		if len(s.Kinds) > 0 && !visited["kinds"] {
			kinds = strings.Join(s.Kinds, ",")
		}
		setDur("resync", &o.resync, s.ResyncInterval)
		setDur("watch-timeout", &o.watchTimeout, s.WatchTimeout)
		setStr("gateway-url", &o.gatewayURL, s.GatewayURL)
		setBool("gateway-allow-http", &o.gatewayAllowHTTP, s.GatewayAllowHTTP)
		setStr("gateway-ca-file", &o.gatewayCAFile, s.GatewayCAFile)
		setStr("admin-key-env", &o.adminKeyEnv, s.AdminKeyEnv)
		setDur("request-timeout", &o.requestTimeout, s.RequestTimeout)
	}
	if o.gatewayURL == "" {
		o.gatewayURL = strings.TrimSpace(getenv(gatewayURLEnv))
	}
	var err error
	if o.kinds, err = parseKinds([]string{kinds}); err != nil {
		return o, err
	}
	if len(o.kinds) == 0 {
		o.kinds = []k8s.ResourceKind{k8s.KindAeroRoute, k8s.KindAeroBudget, k8s.KindAeroAgentPipeline}
	}
	if o.interval <= 0 {
		return o, fmt.Errorf("--interval must be positive, got %s", o.interval)
	}
	if o.requestTimeout <= 0 {
		return o, fmt.Errorf("--request-timeout must be positive, got %s", o.requestTimeout)
	}
	if o.resync < 0 {
		return o, fmt.Errorf("--resync must not be negative, got %s", o.resync)
	}
	if o.watchTimeout < time.Second {
		return o, fmt.Errorf("--watch-timeout must be at least 1s, got %s", o.watchTimeout)
	}
	if strings.TrimSpace(o.adminKeyEnv) == "" {
		return o, errors.New("--admin-key-env must name an environment variable")
	}

	// Inside a pod with no manifest source configured, default to watching
	// the cluster.
	if !o.kube && !configSet && !urlSet && getenv("KUBERNETES_SERVICE_HOST") != "" {
		o.kube = true
	}
	if o.gatewayURL != "" {
		if _, err := validateGatewayURL(o.gatewayURL, o.gatewayAllowHTTP); err != nil {
			return o, err
		}
	}
	if o.kube {
		if o.configPath != "" || o.configURL != "" {
			return o, errors.New("--kube cannot be combined with --config or --config-url")
		}
		if o.once {
			return o, errors.New("--once requires --config")
		}
		if o.gatewayURL == "" {
			return o, fmt.Errorf("--kube requires --gateway-url (or %s / gateway_url)", gatewayURLEnv)
		}
		return o, nil
	}

	// With only --config-url given, don't also require the default file.
	if !configSet && !(urlSet && o.configURL != "") {
		o.configPath = k8s.DefaultOperatorConfig()
	}
	if o.configPath == "" && o.configURL == "" {
		return o, errors.New("one of --config, --config-url or --kube is required")
	}
	if o.once && o.configPath == "" {
		return o, errors.New("--once requires --config")
	}
	if o.configURL != "" && !strings.HasPrefix(o.configURL, "https://") && !strings.HasPrefix(o.configURL, "http://") {
		return o, fmt.Errorf("--config-url must be an http(s) URL")
	}
	return o, nil
}
