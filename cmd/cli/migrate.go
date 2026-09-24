package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v3"
)

// ---- LiteLLM input schema ---------------------------------------------------

// litellmModelEntry is one entry of LiteLLM's model_list.
type litellmModelEntry struct {
	ModelName     string         `yaml:"model_name"` // public alias clients request
	LitellmParams map[string]any `yaml:"litellm_params"`
	ModelInfo     map[string]any `yaml:"model_info"`
	// Extra captures entry-level keys (older configs put rpm/tpm here).
	Extra map[string]any `yaml:",inline"`
}

// litellmConfig is the subset of LiteLLM's proxy config.yaml we migrate.
type litellmConfig struct {
	ModelList            []litellmModelEntry `yaml:"model_list"`
	LitellmSettings      map[string]any      `yaml:"litellm_settings"`
	GeneralSettings      map[string]any      `yaml:"general_settings"`
	RouterSettings       map[string]any      `yaml:"router_settings"`
	EnvironmentVariables map[string]any      `yaml:"environment_variables"`
	Extra                map[string]any      `yaml:",inline"`
}

// ---- AeroLLM output schema (mirrors internal/config.Config) -----------------

// aerollmConfig mirrors internal/config/config.go for YAML output. Durations
// are emitted as Go duration strings ("30s") because the loader decodes
// time.Duration fields with a string hook; a bare integer would be read as
// nanoseconds.
type aerollmConfig struct {
	App        aerollmAppConfig        `yaml:"app"`
	Server     aerollmServerConfig     `yaml:"server"`
	Redis      aerollmRedisConfig      `yaml:"redis"`
	Providers  []aerollmProviderConfig `yaml:"providers"`
	Router     aerollmRouterConfig     `yaml:"router"`
	RateLimit  aerollmRateLimitConfig  `yaml:"rate_limit"`
	Cache      aerollmCacheConfig      `yaml:"cache"`
	Telemetry  aerollmTelemetryConfig  `yaml:"telemetry"`
	Agent      aerollmAgentConfig      `yaml:"agent"`
	Logging    aerollmLoggingConfig    `yaml:"logging"`
	Guardrails aerollmGuardrailsConfig `yaml:"guardrails"`
	Finops     aerollmFinopsConfig     `yaml:"finops"`
	Webhooks   aerollmWebhooksConfig   `yaml:"webhooks"`
	Callbacks  aerollmCallbacksConfig  `yaml:"callbacks"`
}

// aerollmProviderConfig mirrors config.ProviderConfig. Models entries are
// upstream model ids, or "alias=upstream" to expose an upstream model under
// another name (the provider registry rewrites the request model).
type aerollmProviderConfig struct {
	Name         string   `yaml:"name"`
	Type         string   `yaml:"type"`
	APIKey       string   `yaml:"api_key,omitempty"`
	BaseURL      string   `yaml:"base_url,omitempty"`
	Models       []string `yaml:"models"`
	Weight       int      `yaml:"weight,omitempty"`
	Timeout      string   `yaml:"timeout,omitempty"`
	RateLimitRPS float64  `yaml:"rate_limit_rps,omitempty"`
}

type aerollmRouterConfig struct {
	Strategy string `yaml:"strategy"`
}

type aerollmAppConfig struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	Env     string `yaml:"env"`
}

type aerollmServerConfig struct {
	Port         int    `yaml:"port"`
	ReadTimeout  string `yaml:"read_timeout"`
	WriteTimeout string `yaml:"write_timeout"`
	IdleTimeout  string `yaml:"idle_timeout"`
}

type aerollmRedisConfig struct {
	Addr         string `yaml:"addr"`
	Password     string `yaml:"password,omitempty"`
	DB           int    `yaml:"db"`
	PoolSize     int    `yaml:"pool_size"`
	MinIdleConns int    `yaml:"min_idle_conns"`
}

type aerollmRateLimitConfig struct {
	DefaultRPS      float64 `yaml:"default_rps"`
	BurstMultiplier int     `yaml:"burst_multiplier"`
	Strategy        string  `yaml:"strategy"`
}

type aerollmCacheConfig struct {
	Enabled           bool    `yaml:"enabled"`
	TTL               string  `yaml:"ttl"`
	ExactPrefix       string  `yaml:"exact_prefix"`
	SemanticPrefix    string  `yaml:"semantic_prefix"`
	SemanticThreshold float64 `yaml:"semantic_threshold"`
}

type aerollmTelemetryConfig struct {
	Enabled     bool    `yaml:"enabled"`
	Exporter    string  `yaml:"exporter"`
	ServiceName string  `yaml:"service_name"`
	SampleRate  float64 `yaml:"sample_rate"`
}

type aerollmAgentConfig struct {
	Enabled          bool   `yaml:"enabled"`
	MaxIterations    int    `yaml:"max_iterations"`
	ToolTimeout      string `yaml:"tool_timeout"`
	MaxConcurrent    int    `yaml:"max_concurrent"`
	CacheToolResults bool   `yaml:"cache_tool_results"`
}

type aerollmLoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type aerollmGuardrailsConfig struct {
	Enabled bool `yaml:"enabled"`
}

type aerollmFinopsConfig struct {
	Enabled       bool    `yaml:"enabled"`
	DefaultMaxUSD float64 `yaml:"default_max_usd"`
}

type aerollmWebhooksConfig struct {
	Enabled bool `yaml:"enabled"`
}

type aerollmCallbackLangfuse struct {
	Enabled bool   `yaml:"enabled"`
	APIKey  string `yaml:"api_key"`
	BaseURL string `yaml:"base_url"`
}

type aerollmCallbackDatadog struct {
	Enabled bool   `yaml:"enabled"`
	APIKey  string `yaml:"api_key"`
	Site    string `yaml:"site"`
}

type aerollmCallbacksConfig struct {
	Langfuse *aerollmCallbackLangfuse `yaml:"langfuse,omitempty"`
	Datadog  *aerollmCallbackDatadog  `yaml:"datadog,omitempty"`
}

// ---- command ----------------------------------------------------------------

type migrateOptions struct {
	// InlineSecrets copies literal (non-env) secrets into the output instead
	// of replacing them with ${ENV} placeholders.
	InlineSecrets bool
	Force         bool
	Strict        bool
}

func newMigrateCmd() *cobra.Command {
	var input, output string
	var opts migrateOptions

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate configurations from other gateways (LiteLLM) to AeroLLM",
	}

	llmCmd := &cobra.Command{
		Use:   "litellm",
		Short: "Convert a LiteLLM proxy config.yaml to an AeroLLM config.yaml",
		Long: `Convert a LiteLLM proxy config into an AeroLLM config.yaml.

Deployments in model_list are grouped into AeroLLM providers (same provider,
endpoint and credentials), router_settings.routing_strategy is mapped to an
AeroLLM strategy, and supported litellm_settings/general_settings are carried
over. Anything that cannot be represented is reported as a warning.

Secrets are never written in clear: os.environ/VAR references become ${VAR}
placeholders, and literal keys are replaced with ${AEROLLM_<PROVIDER>_API_KEY}
placeholders (use --inline-secrets to copy them). The output file is created
with mode 0600 and is not overwritten without --force. Use --output - to
print the config to stdout.`,
		Example: `  aerollm migrate litellm -i litellm_config.yaml -o config.yaml
  aerollm migrate litellm -i litellm_config.yaml -o - --strict`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLitellmMigrate(input, output, opts, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	llmCmd.Flags().StringVarP(&input, "input", "i", "", "path to LiteLLM config.yaml (required)")
	llmCmd.Flags().StringVarP(&output, "output", "o", "config.yaml", "path to output AeroLLM config.yaml (\"-\" for stdout)")
	llmCmd.Flags().BoolVar(&opts.Force, "force", false, "overwrite the output file if it exists")
	llmCmd.Flags().BoolVar(&opts.InlineSecrets, "inline-secrets", false, "copy literal secrets into the output instead of ${ENV} placeholders")
	llmCmd.Flags().BoolVar(&opts.Strict, "strict", false, "fail if the migration produced any warnings")
	_ = llmCmd.MarkFlagRequired("input")

	cmd.AddCommand(llmCmd)
	return cmd
}

const maxConfigBytes = 16 << 20

func runLitellmMigrate(inputPath, outputPath string, opts migrateOptions, stdout, stderr io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("reading input file: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	f.Close()
	if err != nil {
		return fmt.Errorf("reading input file: %w", err)
	}
	if len(data) > maxConfigBytes {
		return fmt.Errorf("input file larger than %d bytes", maxConfigBytes)
	}

	var llmCfg litellmConfig
	if err := yaml.Unmarshal(data, &llmCfg); err != nil {
		return fmt.Errorf("parsing LiteLLM config: %w", err)
	}

	aeroCfg, report, err := convertLitellmToAero(&llmCfg, opts)
	if err != nil {
		return fmt.Errorf("converting config: %w", err)
	}

	out, err := yaml.Marshal(aeroCfg)
	if err != nil {
		return fmt.Errorf("marshaling output config: %w", err)
	}
	header := fmt.Sprintf("# AeroLLM configuration generated by `aerollm migrate litellm` from %s.\n"+
		"# Secrets are referenced as ${ENV_VAR} placeholders: set them in the gateway's\n"+
		"# environment. Review the migration warnings before deploying.\n", filepath.Base(inputPath))
	out = append([]byte(header), out...)

	for _, w := range report.Warnings {
		fmt.Fprintln(stderr, "warning:", w)
	}
	if opts.Strict && len(report.Warnings) > 0 {
		return fmt.Errorf("--strict: migration produced %d warning(s); nothing was written", len(report.Warnings))
	}

	summary := stdout
	if outputPath == "-" {
		if _, err := stdout.Write(out); err != nil {
			return err
		}
		summary = stderr
	} else {
		if sameFile(inputPath, outputPath) {
			return errors.New("output path is the input file; refusing to overwrite the LiteLLM config")
		}
		if err := writeFileSafely(outputPath, out, 0o600, opts.Force); err != nil {
			return fmt.Errorf("writing output file: %w", err)
		}
		fmt.Fprintf(summary, "Successfully migrated %s -> %s\n", inputPath, outputPath)
	}
	fmt.Fprintf(summary, "  Providers: %d\n", len(aeroCfg.Providers))
	fmt.Fprintf(summary, "  Router strategy: %s\n", aeroCfg.Router.Strategy)
	if len(report.EnvVars) > 0 {
		fmt.Fprintln(summary, "  Environment variables to set before starting AeroLLM:")
		for _, name := range sortedKeys(report.EnvVars) {
			fmt.Fprintf(summary, "    %s  (%s)\n", name, report.EnvVars[name])
		}
	}
	if len(report.Warnings) > 0 {
		fmt.Fprintf(summary, "  Warnings: %d (see above)\n", len(report.Warnings))
	}
	return nil
}

func sameFile(a, b string) bool {
	fa, err := os.Stat(a)
	if err != nil {
		return false
	}
	fb, err := os.Stat(b)
	if err != nil {
		return false
	}
	return os.SameFile(fa, fb)
}

// ---- conversion -------------------------------------------------------------

// migrationReport collects user-facing findings. It never contains secret
// values: only field names, model names and environment variable names.
type migrationReport struct {
	Warnings []string
	EnvVars  map[string]string // env var name -> what it is used for
	seen     map[string]bool
}

func (r *migrationReport) warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if r.seen == nil {
		r.seen = map[string]bool{}
	}
	if r.seen[msg] {
		return
	}
	r.seen[msg] = true
	r.Warnings = append(r.Warnings, msg)
}

func (r *migrationReport) needEnv(name, purpose string) {
	if r.EnvVars == nil {
		r.EnvVars = map[string]string{}
	}
	if _, ok := r.EnvVars[name]; !ok {
		r.EnvVars[name] = purpose
	}
}

// providerSpec describes how a LiteLLM provider prefix maps onto AeroLLM.
type providerSpec struct {
	Type    string // AeroLLM provider type
	Name    string // default AeroLLM provider name
	BaseURL string // base URL to emit when api_base is not set ("" = server default)
	KeyEnv  string // env var LiteLLM reads the key from when api_key is omitted
	Keyless bool   // local servers that need no API key
	Note    string // caveat reported once when the provider is used
}

// litellmProviders maps LiteLLM provider prefixes ("openai/gpt-4o") to
// AeroLLM provider types. Types are limited to what the gateway registers:
// openai, openai-compatible, anthropic, bedrock, gemini, groq, cohere, deepseek.
var litellmProviders = map[string]providerSpec{
	"openai":                 {Type: "openai", Name: "openai", KeyEnv: "OPENAI_API_KEY"},
	"text-completion-openai": {Type: "openai", Name: "openai", KeyEnv: "OPENAI_API_KEY", Note: "text-completion-openai models are served through the chat completions API in AeroLLM"},
	"azure": {Type: "openai-compatible", Name: "azure", KeyEnv: "AZURE_API_KEY",
		Note: "Azure OpenAI deployments were mapped to type openai-compatible against the Azure v1 API (<api_base>/openai/v1, deployment name as model): the gateway config does not accept type azure yet and Azure key auth uses the api-key header, so verify these providers before relying on them"},
	"azure_ai":  {Type: "openai-compatible", Name: "azure-ai", KeyEnv: "AZURE_AI_API_KEY"},
	"anthropic": {Type: "anthropic", Name: "anthropic", KeyEnv: "ANTHROPIC_API_KEY"},
	"bedrock": {Type: "bedrock", Name: "bedrock",
		Note: "bedrock: AWS credentials (aws_access_key_id/aws_secret_access_key/aws_session_token) are not part of the AeroLLM provider config; provide credentials to the gateway via the standard AWS environment"},
	"gemini": {Type: "gemini", Name: "gemini", KeyEnv: "GEMINI_API_KEY"},
	"vertex_ai": {Type: "gemini", Name: "gemini", KeyEnv: "GEMINI_API_KEY",
		Note: "vertex_ai models were mapped to the Gemini API provider: Vertex AI service-account credentials (vertex_project/vertex_location/vertex_credentials) are not supported, a Gemini API key is required"},
	"groq":         {Type: "groq", Name: "groq", KeyEnv: "GROQ_API_KEY"},
	"deepseek":     {Type: "deepseek", Name: "deepseek", KeyEnv: "DEEPSEEK_API_KEY"},
	"cohere":       {Type: "cohere", Name: "cohere", KeyEnv: "COHERE_API_KEY"},
	"cohere_chat":  {Type: "cohere", Name: "cohere", KeyEnv: "COHERE_API_KEY"},
	"mistral":      {Type: "openai-compatible", Name: "mistral", BaseURL: "https://api.mistral.ai/v1", KeyEnv: "MISTRAL_API_KEY"},
	"together_ai":  {Type: "openai-compatible", Name: "together", BaseURL: "https://api.together.xyz/v1", KeyEnv: "TOGETHERAI_API_KEY"},
	"fireworks_ai": {Type: "openai-compatible", Name: "fireworks", BaseURL: "https://api.fireworks.ai/inference/v1", KeyEnv: "FIREWORKS_AI_API_KEY"},
	"openrouter":   {Type: "openai-compatible", Name: "openrouter", BaseURL: "https://openrouter.ai/api/v1", KeyEnv: "OPENROUTER_API_KEY"},
	"perplexity":   {Type: "openai-compatible", Name: "perplexity", BaseURL: "https://api.perplexity.ai", KeyEnv: "PERPLEXITYAI_API_KEY"},
	"xai":          {Type: "openai-compatible", Name: "xai", BaseURL: "https://api.x.ai/v1", KeyEnv: "XAI_API_KEY"},
	"deepinfra":    {Type: "openai-compatible", Name: "deepinfra", BaseURL: "https://api.deepinfra.com/v1/openai", KeyEnv: "DEEPINFRA_API_KEY"},
	"ollama":       {Type: "openai-compatible", Name: "ollama", BaseURL: "http://localhost:11434/v1", Keyless: true},
	"ollama_chat":  {Type: "openai-compatible", Name: "ollama", BaseURL: "http://localhost:11434/v1", Keyless: true},
	"hosted_vllm":  {Type: "openai-compatible", Name: "vllm", Keyless: true},
	"vllm":         {Type: "openai-compatible", Name: "vllm", Keyless: true},
	"lm_studio":    {Type: "openai-compatible", Name: "lm-studio", BaseURL: "http://localhost:1234/v1", Keyless: true},
}

// fixedNameTypes are registered by the gateway under a fixed adapter name,
// so two providers of the same type collide.
var fixedNameTypes = map[string]bool{
	"anthropic": true, "bedrock": true, "gemini": true, "groq": true, "cohere": true, "deepseek": true,
}

// keylessPlaceholder is emitted as api_key for local servers that need no
// key: the gateway skips providers with an empty api_key.
const keylessPlaceholder = "not-needed"

var envVarPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// classifyLitellmModel resolves a litellm_params.model string (plus
// custom_llm_provider and api_base) to a provider prefix and the upstream
// model id. ok is false when the provider cannot be served by AeroLLM.
func classifyLitellmModel(llmModel, customProvider, apiBase string) (prefix, upstream string, ok bool) {
	llmModel = strings.TrimSpace(llmModel)
	if customProvider != "" {
		prefix = strings.ToLower(customProvider)
		upstream = strings.TrimPrefix(llmModel, prefix+"/")
	} else if p, rest, found := strings.Cut(llmModel, "/"); found {
		_, known := litellmProviders[strings.ToLower(p)]
		// An unknown first segment with an api_base is most likely part of an
		// org/model id served by an OpenAI-compatible server: keep it whole.
		if known || apiBase == "" {
			prefix, upstream = strings.ToLower(p), rest
		}
	}
	if prefix == "" {
		// No provider prefix: infer it the way LiteLLM does for common families.
		upstream = llmModel
		lower := strings.ToLower(llmModel)
		switch {
		case apiBase != "":
			prefix = "openai"
		case strings.HasPrefix(lower, "claude"):
			prefix = "anthropic"
		case strings.HasPrefix(lower, "gemini"):
			prefix = "gemini"
		case strings.HasPrefix(lower, "command"):
			prefix = "cohere"
		case strings.HasPrefix(lower, "deepseek"):
			prefix = "deepseek"
		default:
			prefix = "openai"
		}
	}
	if _, known := litellmProviders[prefix]; known {
		return prefix, upstream, upstream != ""
	}
	// Unknown provider: usable only if it exposes an OpenAI-compatible api_base.
	return prefix, upstream, apiBase != "" && upstream != ""
}

// mapLitellmStrategy converts router_settings.routing_strategy to an AeroLLM
// router strategy. ok is false for unknown strategies.
func mapLitellmStrategy(s string) (strategy string, ok bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "simple-shuffle", "":
		return "round_robin", true
	case "least-busy":
		return "least_busy", true
	case "usage-based-routing", "usage-based-routing-v2":
		return "usage_based", true
	case "latency-based-routing":
		return "latency", true
	case "cost-based-routing":
		return "cost", true
	default:
		return "round_robin", false
	}
}

// secretRef is the migrated form of a LiteLLM secret value.
type secretRef struct {
	Value    string // what to write to the output (placeholder or literal)
	Identity string // grouping identity that never exposes the secret
	Literal  bool   // true if the input held the secret in clear
	EnvVar   string // env var referenced by Value ("" if none)
}

// translateSecret converts a LiteLLM secret value: "os.environ/VAR" and
// "${VAR}"/"$VAR" become "${VAR}"; anything else is a literal secret.
func translateSecret(raw any) (secretRef, error) {
	if raw == nil {
		return secretRef{}, nil
	}
	s := strings.TrimSpace(fmt.Sprint(raw))
	if s == "" {
		return secretRef{}, nil
	}
	var name string
	switch {
	case strings.HasPrefix(s, "os.environ/"):
		name = strings.TrimPrefix(s, "os.environ/")
	case strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}"):
		name = s[2 : len(s)-1]
	case strings.HasPrefix(s, "$") && envVarPattern.MatchString(s[1:]):
		name = s[1:]
	default:
		sum := sha256.Sum256([]byte(s))
		return secretRef{Value: s, Identity: "literal:" + hex.EncodeToString(sum[:8]), Literal: true}, nil
	}
	if !envVarPattern.MatchString(name) {
		return secretRef{}, fmt.Errorf("invalid environment variable reference %q", name)
	}
	return secretRef{Value: "${" + name + "}", Identity: "env:" + name, EnvVar: name}, nil
}

// translateApiKeyRef converts a LiteLLM api_key reference to an AeroLLM
// ${VAR} placeholder. Literal keys are never echoed: "" is returned.
func translateApiKeyRef(keyRef string) string {
	ref, err := translateSecret(keyRef)
	if err != nil || ref.Literal {
		return ""
	}
	return ref.Value
}

// providerGroup accumulates deployments that become one AeroLLM provider.
type providerGroup struct {
	cfg       aerollmProviderConfig
	spec      providerSpec
	prefix    string
	key       secretRef
	keyless   bool // no key needed/known: emit keylessPlaceholder
	timeout   time.Duration
	rps       float64
	weight    int
	baseName  string
	modelSeen map[string]bool
	// served maps every model name clients may request from this provider
	// (upstream ids and aliases) to the upstream model id.
	served map[string]string
}

// addModel registers upstream (and alias=upstream when they differ).
func (g *providerGroup) addModel(alias, upstream string) {
	add := func(entry string) {
		if !g.modelSeen[entry] {
			g.modelSeen[entry] = true
			g.cfg.Models = append(g.cfg.Models, entry)
		}
	}
	add(upstream)
	g.served[upstream] = upstream
	if alias != "" && alias != upstream {
		if prev, ok := g.served[alias]; ok && prev != upstream {
			return // alias already bound to another upstream of this provider
		}
		add(alias + "=" + upstream)
		g.served[alias] = upstream
	}
}

// Known litellm_params keys that are consumed (or deliberately reported).
var handledParams = map[string]bool{
	"model": true, "api_key": true, "api_base": true, "base_url": true, "rpm": true, "tpm": true,
	"timeout": true, "stream_timeout": true, "weight": true, "custom_llm_provider": true,
}

// convertLitellmToAero transforms a parsed LiteLLM config into the AeroLLM
// schema and reports everything that could not be carried over.
func convertLitellmToAero(cfg *litellmConfig, opts migrateOptions) (*aerollmConfig, *migrationReport, error) {
	result := defaultAerollmConfig()
	report := &migrationReport{}

	for _, k := range sortedKeys(cfg.Extra) {
		report.warnf("top-level section %q is not supported by AeroLLM and was not migrated", k)
	}
	if len(cfg.ModelList) == 0 {
		return nil, nil, errors.New("model_list is empty: nothing to migrate")
	}

	// ---- router_settings
	aliasOverrides := map[string]string{}
	for _, k := range sortedKeys(cfg.RouterSettings) {
		v := cfg.RouterSettings[k]
		switch k {
		case "routing_strategy":
			s := fmt.Sprint(v)
			strategy, ok := mapLitellmStrategy(s)
			if !ok {
				report.warnf("router_settings.routing_strategy %q is not supported; using round_robin", s)
			}
			result.Router.Strategy = strategy
		case "redis_host", "redis_port", "redis_password":
			// handled below together
		case "model_group_alias":
			m, ok := v.(map[string]any)
			if !ok {
				report.warnf("router_settings.model_group_alias is not a mapping; ignored")
				continue
			}
			for alias, target := range m {
				switch t := target.(type) {
				case string:
					aliasOverrides[alias] = t
				case map[string]any:
					if model, ok := t["model"].(string); ok {
						aliasOverrides[alias] = model
					}
				}
			}
		default:
			report.warnf("router_settings.%s is not supported by AeroLLM and was not migrated", k)
		}
	}
	if err := applyRedisSettings(result, report, opts, "router_settings", cfg.RouterSettings["redis_host"], cfg.RouterSettings["redis_port"], cfg.RouterSettings["redis_password"]); err != nil {
		return nil, nil, err
	}

	// ---- litellm_settings
	var defaultTimeout time.Duration
	for _, k := range sortedKeys(cfg.LitellmSettings) {
		v := cfg.LitellmSettings[k]
		switch k {
		case "cache":
			if b, ok := v.(bool); ok {
				result.Cache.Enabled = b
			}
		case "cache_params":
			m, _ := v.(map[string]any)
			typ := strings.ToLower(fmt.Sprint(m["type"]))
			switch {
			case typ == "redis":
				if result.Redis.Addr == "localhost:6379" {
					if err := applyRedisSettings(result, report, opts, "litellm_settings.cache_params", m["host"], m["port"], m["password"]); err != nil {
						return nil, nil, err
					}
				}
			case typ == "local" || typ == "<nil>":
			default:
				report.warnf("litellm_settings.cache_params.type %q is not supported; AeroLLM uses its Redis exact/semantic cache", typ)
			}
			if ttl, ok := parseSeconds(m["ttl"]); ok && ttl > 0 {
				result.Cache.TTL = formatDuration(ttl)
			}
		case "request_timeout":
			if d, ok := parseSeconds(v); ok && d > 0 {
				defaultTimeout = d
			} else {
				report.warnf("litellm_settings.request_timeout %v is not a valid number of seconds; ignored", v)
			}
		case "success_callback", "failure_callback", "callbacks":
			for _, cb := range toStringList(v) {
				switch strings.ToLower(cb) {
				case "langfuse":
					if result.Callbacks.Langfuse == nil {
						result.Callbacks.Langfuse = &aerollmCallbackLangfuse{Enabled: true, APIKey: "${LANGFUSE_SECRET_KEY}", BaseURL: "${LANGFUSE_HOST:-https://cloud.langfuse.com}"}
						report.needEnv("LANGFUSE_SECRET_KEY", "Langfuse callback")
					}
				case "datadog":
					if result.Callbacks.Datadog == nil {
						result.Callbacks.Datadog = &aerollmCallbackDatadog{Enabled: true, APIKey: "${DD_API_KEY}", Site: "${DD_SITE:-datadoghq.com}"}
						report.needEnv("DD_API_KEY", "Datadog callback")
					}
				case "prometheus":
					result.Telemetry.Enabled = true
					result.Telemetry.Exporter = "prometheus"
				default:
					report.warnf("litellm_settings.%s: callback %q is not supported by AeroLLM", k, cb)
				}
			}
		default:
			report.warnf("litellm_settings.%s is not supported by AeroLLM and was not migrated", k)
		}
	}

	// ---- general_settings
	for _, k := range sortedKeys(cfg.GeneralSettings) {
		v := cfg.GeneralSettings[k]
		switch k {
		case "master_key":
			ref, err := translateSecret(v)
			switch {
			case err != nil:
				report.warnf("general_settings.master_key: %v", err)
			case ref.EnvVar != "":
				report.needEnv("AEROLLM_API_KEY", "gateway admin/master key; LiteLLM read it from $"+ref.EnvVar)
			case ref.Literal:
				report.needEnv("AEROLLM_API_KEY", "gateway admin/master key (the LiteLLM master_key value was not copied)")
			}
		case "database_url":
			report.warnf("general_settings.database_url was not migrated: AeroLLM stores keys and spend in Redis (the connection string was not copied)")
		default:
			report.warnf("general_settings.%s is not supported by AeroLLM and was not migrated", k)
		}
	}

	if len(cfg.EnvironmentVariables) > 0 {
		report.warnf("environment_variables were not migrated (values are never copied); set them in the AeroLLM environment: %s",
			strings.Join(sortedKeys(cfg.EnvironmentVariables), ", "))
	}

	// ---- model_list
	type groupKey struct{ typ, name, base, key, extra string }
	groups := map[groupKey]*providerGroup{}
	var order []*providerGroup
	aliasGroups := map[string][]*providerGroup{}
	droppedParams := map[string]int{}
	var tpmDropped int

	for i, entry := range cfg.ModelList {
		params := entry.LitellmParams
		where := fmt.Sprintf("model_list[%d]", i)
		llmModel, _ := params["model"].(string)
		if strings.TrimSpace(llmModel) == "" {
			report.warnf("%s (%q): litellm_params.model is missing; entry skipped", where, entry.ModelName)
			continue
		}
		alias := strings.TrimSpace(entry.ModelName)
		if alias == "" {
			alias = llmModel
		}
		where = fmt.Sprintf("%s (%s)", where, alias)

		apiBaseRaw := firstNonNil(params["api_base"], params["base_url"])
		apiBaseRef, err := translateSecret(apiBaseRaw)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: api_base: %w", where, err)
		}
		apiBase := apiBaseRef.Value
		if apiBaseRef.EnvVar != "" {
			report.needEnv(apiBaseRef.EnvVar, "api_base of "+alias)
		}
		if apiBaseRef.Literal {
			apiBase = stripURLCredentials(apiBase, report, where, opts.InlineSecrets)
		}

		customProvider, _ := params["custom_llm_provider"].(string)
		prefix, upstream, ok := classifyLitellmModel(llmModel, customProvider, apiBase)
		if !ok {
			report.warnf("%s: provider %q (model %q) is not supported by AeroLLM and has no OpenAI-compatible api_base; entry skipped", where, prefix, llmModel)
			continue
		}
		spec, known := litellmProviders[prefix]
		if !known {
			spec = providerSpec{Type: "openai-compatible", Name: sanitizeName(prefix)}
			report.warnf("%s: unknown provider %q treated as OpenAI-compatible at its api_base", where, prefix)
		}
		if spec.Note != "" {
			report.warnf("%s", spec.Note)
		}

		baseURL := spec.BaseURL
		if apiBase != "" {
			baseURL = apiBase
			switch {
			case strings.HasPrefix(prefix, "ollama") && !strings.HasSuffix(strings.TrimRight(baseURL, "/"), "/v1"):
				baseURL = strings.TrimRight(baseURL, "/") + "/v1"
			case prefix == "azure" || prefix == "azure_ai":
				baseURL = azureV1BaseURL(baseURL)
			}
		}
		if (prefix == "azure" || prefix == "azure_ai") && baseURL == "" {
			report.warnf("%s: %s requires api_base (https://<resource>.openai.azure.com); entry skipped", where, prefix)
			continue
		}
		provType, provName := spec.Type, spec.Name
		customHost := prefix == "openai" && apiBase != "" && !isOpenAIHost(apiBase)
		if customHost {
			provType, provName = "openai-compatible", hostName(apiBase)
		}
		if (prefix == "hosted_vllm" || prefix == "vllm") && baseURL == "" {
			report.warnf("%s: %s requires api_base; entry skipped", where, prefix)
			continue
		}

		key, err := translateSecret(params["api_key"])
		if err != nil {
			return nil, nil, fmt.Errorf("%s: api_key: %w", where, err)
		}
		keyless := spec.Keyless
		switch {
		case key.Value != "":
		case customHost:
			// Never default a third-party endpoint to the OpenAI key: that
			// would send the user's OpenAI credentials to another host.
			keyless = true
			report.warnf("%s: no api_key for OpenAI-compatible endpoint %s; set one in the output if the server requires authentication", where, apiBase)
		case spec.KeyEnv != "":
			key = secretRef{Value: "${" + spec.KeyEnv + "}", Identity: "env:" + spec.KeyEnv, EnvVar: spec.KeyEnv}
		}

		extra := ""
		if v, ok := params["api_version"]; ok {
			extra = fmt.Sprint(v)
		}
		gk := groupKey{typ: provType, name: provName, base: baseURL, key: key.Identity, extra: extra}
		g := groups[gk]
		if g == nil {
			g = &providerGroup{
				cfg:       aerollmProviderConfig{Type: provType, BaseURL: baseURL, Models: []string{}},
				spec:      spec,
				keyless:   keyless,
				prefix:    prefix,
				key:       key,
				baseName:  provName,
				modelSeen: map[string]bool{},
				served:    map[string]string{},
			}
			groups[gk] = g
			order = append(order, g)
		}
		g.addModel(alias, upstream)
		if !containsGroup(aliasGroups[alias], g) {
			aliasGroups[alias] = append(aliasGroups[alias], g)
		}

		// Limits and timeouts (litellm_params, or entry level in older configs).
		if rpm, ok := parseNumber(firstNonNil(params["rpm"], entry.Extra["rpm"])); ok && rpm > 0 {
			g.rps += rpm / 60
		}
		if _, ok := parseNumber(firstNonNil(params["tpm"], entry.Extra["tpm"])); ok {
			tpmDropped++
		}
		if d, ok := parseSeconds(firstNonNil(params["timeout"], params["stream_timeout"])); ok && d > g.timeout {
			g.timeout = d
		}
		if w, ok := parseNumber(params["weight"]); ok && w > 0 {
			g.weight += int(math.Round(w))
		}
		for k := range params {
			if !handledParams[k] {
				droppedParams[k]++
			}
		}
		for k := range entry.Extra {
			if k != "rpm" && k != "tpm" {
				droppedParams[k]++
			}
		}
	}
	if len(order) == 0 {
		return nil, nil, errors.New("no model_list entry could be migrated")
	}
	if tpmDropped > 0 {
		report.warnf("tpm limits are not supported per provider in AeroLLM and were dropped (%d deployment(s)); use per-key --tpm limits instead", tpmDropped)
	}
	for _, k := range sortedKeys(droppedParams) {
		report.warnf("litellm_params.%s is not supported by AeroLLM and was dropped (%d deployment(s))", k, droppedParams[k])
	}

	// ---- assign unique provider names, keys and limits
	usedNames := map[string]bool{}
	usedEnv := map[string]bool{}
	typeCount := map[string]int{}
	for _, g := range order {
		if g.key.EnvVar != "" {
			usedEnv[g.key.EnvVar] = true
		}
	}
	for _, g := range order {
		name := g.baseName
		for n := 2; usedNames[name]; n++ {
			name = fmt.Sprintf("%s-%d", g.baseName, n)
		}
		usedNames[name] = true
		g.cfg.Name = name
		typeCount[g.cfg.Type]++

		switch {
		case g.key.Literal && opts.InlineSecrets:
			g.cfg.APIKey = g.key.Value
			report.warnf("provider %s: literal api_key copied into the output because of --inline-secrets; keep the file private", name)
		case g.key.Literal:
			env := "AEROLLM_" + envName(name) + "_API_KEY"
			for n := 2; usedEnv[env]; n++ {
				env = fmt.Sprintf("AEROLLM_%s_%d_API_KEY", envName(name), n)
			}
			usedEnv[env] = true
			g.cfg.APIKey = "${" + env + "}"
			report.needEnv(env, fmt.Sprintf("API key of provider %s (the literal key in the LiteLLM config was not copied)", name))
		case g.key.EnvVar != "":
			g.cfg.APIKey = g.key.Value
			report.needEnv(g.key.EnvVar, "API key of provider "+name)
		case g.keyless:
			g.cfg.APIKey = keylessPlaceholder
		default:
			if g.cfg.Type != "bedrock" {
				report.warnf("provider %s has no api_key; the gateway skips providers without one", name)
			}
		}
		timeout := g.timeout
		if timeout == 0 {
			timeout = defaultTimeout
		}
		if timeout > 0 {
			g.cfg.Timeout = formatDuration(timeout)
		}
		if g.rps > 0 {
			g.cfg.RateLimitRPS = math.Round(g.rps*1000) / 1000
		}
		g.cfg.Weight = g.weight
		result.Providers = append(result.Providers, g.cfg)
	}
	for _, t := range sortedKeys(typeCount) {
		if fixedNameTypes[t] && typeCount[t] > 1 {
			report.warnf("%d %s providers were generated (different credentials or endpoints), but the gateway registers a single %s adapter, so only one will be used; consolidate them", typeCount[t], t, t)
		}
	}

	// ---- model groups and router_settings.model_group_alias
	for _, alias := range sortedKeys(aliasGroups) {
		if gs := aliasGroups[alias]; len(gs) > 1 {
			names := make([]string, len(gs))
			for i, g := range gs {
				names[i] = g.cfg.Name
			}
			report.warnf("model %q is served by %d providers (%s); AeroLLM resolves it to the first matching provider, so LiteLLM's load balancing across these deployments is not reproduced",
				alias, len(gs), strings.Join(names, ", "))
		}
	}
	for _, alias := range sortedKeys(aliasOverrides) {
		target := aliasOverrides[alias]
		found := false
		for i := range order {
			g := order[i]
			if up, ok := g.served[target]; ok {
				g.addModel(alias, up)
				found = true
			}
		}
		if !found {
			report.warnf("router_settings.model_group_alias %q -> %q: target model not found; alias dropped", alias, target)
		}
	}
	for i, g := range order {
		result.Providers[i].Models = g.cfg.Models
	}
	return result, report, nil
}

// applyRedisSettings maps LiteLLM redis host/port/password onto redis.*.
func applyRedisSettings(result *aerollmConfig, report *migrationReport, opts migrateOptions, section string, host, port, password any) error {
	if host != nil {
		h := strings.TrimSpace(fmt.Sprint(host))
		if strings.HasPrefix(h, "os.environ/") {
			ref, err := translateSecret(h)
			if err != nil {
				return fmt.Errorf("%s.redis_host: %w", section, err)
			}
			report.needEnv(ref.EnvVar, "Redis host")
			report.warnf("%s: the Redis host comes from $%s; set redis.addr (or AEROLLM_REDIS_ADDR) accordingly", section, ref.EnvVar)
		} else if h != "" {
			p := "6379"
			if port != nil {
				p = strings.TrimSpace(fmt.Sprint(port))
			}
			result.Redis.Addr = net.JoinHostPort(h, p)
		}
	}
	if password != nil {
		ref, err := translateSecret(password)
		if err != nil {
			return fmt.Errorf("%s.redis_password: %w", section, err)
		}
		switch {
		case ref.Value == "":
		case ref.Literal && !opts.InlineSecrets:
			result.Redis.Password = "${AEROLLM_REDIS_PASSWORD}"
			report.needEnv("AEROLLM_REDIS_PASSWORD", "Redis password (the literal value was not copied)")
		default:
			result.Redis.Password = ref.Value
			if ref.EnvVar != "" {
				report.needEnv(ref.EnvVar, "Redis password")
			}
		}
	}
	return nil
}

// stripURLCredentials removes user:password@ from a literal api_base unless
// secrets may be inlined.
func stripURLCredentials(raw string, report *migrationReport, where string, inline bool) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil || inline {
		return raw
	}
	u.User = nil
	report.warnf("%s: credentials embedded in api_base were removed; configure them separately", where)
	return u.String()
}

func isOpenAIHost(apiBase string) bool {
	u, err := url.Parse(apiBase)
	return err == nil && strings.EqualFold(u.Hostname(), "api.openai.com")
}

// hostName derives a provider name from an api_base host.
func hostName(apiBase string) string {
	u, err := url.Parse(apiBase)
	if err != nil || u.Hostname() == "" {
		return "custom"
	}
	return sanitizeName(u.Hostname())
}

var nonNameChars = regexp.MustCompile(`[^a-z0-9]+`)

func sanitizeName(s string) string {
	s = strings.Trim(nonNameChars.ReplaceAllString(strings.ToLower(s), "-"), "-")
	if len(s) > 48 {
		s = strings.TrimRight(s[:48], "-")
	}
	if s == "" {
		return "custom"
	}
	return s
}

func envName(providerName string) string {
	return strings.ToUpper(strings.ReplaceAll(sanitizeName(providerName), "-", "_"))
}

// parseNumber accepts YAML ints/floats and numeric strings.
func parseNumber(v any) (float64, bool) {
	switch t := v.(type) {
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case uint64:
		return float64(t), true
	case float64:
		return t, !math.IsNaN(t) && !math.IsInf(t, 0)
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
	}
	return 0, false
}

// parseSeconds accepts a number of seconds or a Go duration string.
func parseSeconds(v any) (time.Duration, bool) {
	if f, ok := parseNumber(v); ok {
		if f < 0 {
			return 0, false
		}
		return time.Duration(f * float64(time.Second)), true
	}
	if s, ok := v.(string); ok {
		d, err := time.ParseDuration(strings.TrimSpace(s))
		return d, err == nil && d >= 0
	}
	return 0, false
}

// formatDuration renders d compactly ("30s", "15m", "1h30m").
func formatDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

func toStringList(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func firstNonNil(vals ...any) any {
	for _, v := range vals {
		if v != nil {
			return v
		}
	}
	return nil
}

func containsGroup(list []*providerGroup, g *providerGroup) bool {
	for _, e := range list {
		if e == g {
			return true
		}
	}
	return false
}

// azureV1BaseURL turns an Azure resource endpoint into its OpenAI v1 API base
// (https://<resource>.openai.azure.com/openai/v1); explicit paths are kept.
func azureV1BaseURL(apiBase string) string {
	u, err := url.Parse(apiBase)
	if err != nil || u.Host == "" {
		return apiBase
	}
	if p := strings.TrimRight(u.Path, "/"); p == "" {
		u.Path = "/openai/v1"
	} else if p == "/openai" {
		u.Path = "/openai/v1"
	}
	return u.String()
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// defaultAerollmConfig returns a minimal AeroLLM config with the same
// defaults as internal/config.LoadConfig.
func defaultAerollmConfig() *aerollmConfig {
	return &aerollmConfig{
		App: aerollmAppConfig{
			Name:    "AeroLLM",
			Version: "dev",
			Env:     "production",
		},
		Server: aerollmServerConfig{
			Port:         8080,
			ReadTimeout:  "30s",
			WriteTimeout: "2m",
			IdleTimeout:  "2m",
		},
		Redis: aerollmRedisConfig{
			Addr:         "localhost:6379",
			DB:           0,
			PoolSize:     25,
			MinIdleConns: 5,
		},
		Router: aerollmRouterConfig{
			Strategy: "round_robin",
		},
		RateLimit: aerollmRateLimitConfig{
			DefaultRPS:      10.0,
			BurstMultiplier: 2,
			Strategy:        "token_bucket",
		},
		Cache: aerollmCacheConfig{
			Enabled:           true,
			TTL:               "15m",
			ExactPrefix:       "cache:exact:",
			SemanticPrefix:    "cache:semantic:",
			SemanticThreshold: 0.85,
		},
		Telemetry: aerollmTelemetryConfig{
			Enabled:     false,
			Exporter:    "prometheus",
			ServiceName: "aerollm",
			SampleRate:  1.0,
		},
		Agent: aerollmAgentConfig{
			Enabled:          true,
			MaxIterations:    10,
			ToolTimeout:      "30s",
			MaxConcurrent:    10,
			CacheToolResults: true,
		},
		Logging: aerollmLoggingConfig{
			Level:  "info",
			Format: "json",
		},
		Guardrails: aerollmGuardrailsConfig{
			Enabled: true,
		},
		Finops: aerollmFinopsConfig{
			Enabled: true,
		},
		Webhooks: aerollmWebhooksConfig{
			Enabled: true,
		},
	}
}
