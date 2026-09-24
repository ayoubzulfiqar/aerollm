package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// buildVersion is set via linker flags at build time.
var buildVersion = "dev"

// Config holds the entire application configuration.
type Config struct {
	App        AppConfig        `mapstructure:"app" json:"app"`
	Server     ServerConfig     `mapstructure:"server" json:"server"`
	Redis      RedisConfig      `mapstructure:"redis" json:"redis"`
	Auth       AuthConfig       `mapstructure:"auth" json:"auth"`
	Security   SecurityConfig   `mapstructure:"security" json:"security"`
	Providers  []ProviderConfig `mapstructure:"providers" json:"providers"`
	Callbacks  CallbacksConfig  `mapstructure:"callbacks" json:"callbacks"`
	Router     RouterConfig     `mapstructure:"router" json:"router"`
	RateLimit  RateLimitConfig  `mapstructure:"rate_limit" json:"rate_limit"`
	Cache      CacheConfig      `mapstructure:"cache" json:"cache"`
	Telemetry  TelemetryConfig  `mapstructure:"telemetry" json:"telemetry"`
	Agent      AgentConfig      `mapstructure:"agent" json:"agent"`
	Logging    LoggingConfig    `mapstructure:"logging" json:"logging"`
	Guardrails GuardrailsConfig `mapstructure:"guardrails" json:"guardrails"`
	Finops     FinopsConfig     `mapstructure:"finops" json:"finops"`
	Webhooks   WebhooksConfig   `mapstructure:"webhooks" json:"webhooks"`
}

// AppConfig holds application-level settings.
type AppConfig struct {
	Name    string `mapstructure:"name" json:"name"`
	Version string `mapstructure:"version" json:"version"`
	Env     string `mapstructure:"env" json:"env"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Port              int           `mapstructure:"port" json:"port"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout" json:"read_timeout"`
	ReadHeaderTimeout time.Duration `mapstructure:"read_header_timeout" json:"read_header_timeout"`
	// WriteTimeout bounds non-streaming responses. Streaming (SSE) responses
	// lift the deadline per request so long generations are not cut off.
	WriteTimeout    time.Duration `mapstructure:"write_timeout" json:"write_timeout"`
	IdleTimeout     time.Duration `mapstructure:"idle_timeout" json:"idle_timeout"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout" json:"shutdown_timeout"`
	// MaxBodyBytes caps request bodies on JSON endpoints.
	MaxBodyBytes int64 `mapstructure:"max_body_bytes" json:"max_body_bytes"`
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Addr         string `mapstructure:"addr" json:"addr"`
	Password     string `mapstructure:"password" json:"password"`
	DB           int    `mapstructure:"db" json:"db"`
	PoolSize     int    `mapstructure:"pool_size" json:"pool_size"`
	MinIdleConns int    `mapstructure:"min_idle_conns" json:"min_idle_conns"`
	// Required makes startup fail when Redis is unreachable. When false the
	// gateway starts in degraded mode (in-memory limiter, no exact cache).
	Required bool `mapstructure:"required" json:"required"`
}

// AuthConfig holds gateway authentication settings.
type AuthConfig struct {
	// MasterKey grants admin access (key management, config, control plane).
	// When empty a random key is generated at startup and printed once.
	MasterKey string `mapstructure:"master_key" json:"master_key"`
	// AdminKeys are additional keys with admin access.
	AdminKeys []string `mapstructure:"admin_keys" json:"admin_keys"`
	// APIKeys are static client keys allowed to call the inference endpoints.
	APIKeys []string `mapstructure:"api_keys" json:"api_keys"`
}

// SecurityConfig holds HTTP hardening settings.
type SecurityConfig struct {
	// CORSAllowedOrigins lists origins allowed to call the API from browsers.
	// Empty disables CORS; "*" allows any origin (credentials never allowed).
	CORSAllowedOrigins []string `mapstructure:"cors_allowed_origins" json:"cors_allowed_origins"`
	// EnablePprof exposes net/http/pprof on localhost:6060.
	EnablePprof bool `mapstructure:"enable_pprof" json:"enable_pprof"`
	// PublicMetrics serves /metrics without authentication.
	PublicMetrics bool `mapstructure:"public_metrics" json:"public_metrics"`
}

// ProviderConfig holds per-provider settings.
type ProviderConfig struct {
	Name         string        `mapstructure:"name" json:"name"`
	Type         string        `mapstructure:"type" json:"type"`
	BaseURL      string        `mapstructure:"base_url" json:"base_url"`
	APIKey       string        `mapstructure:"api_key" json:"api_key"`
	Weight       int           `mapstructure:"weight" json:"weight"`
	Timeout      time.Duration `mapstructure:"timeout" json:"timeout"`
	RateLimitRPS float64       `mapstructure:"rate_limit_rps" json:"rate_limit_rps"`
	Models       []string      `mapstructure:"models" json:"models"`
}

// ResolvedName returns the effective provider name.
func (p ProviderConfig) ResolvedName() string {
	if p.Name != "" {
		return p.Name
	}
	return p.Type
}

// Endpoint returns the API base URL, ensuring a sane default.
func (p ProviderConfig) Endpoint() string {
	if p.BaseURL != "" {
		return p.BaseURL
	}
	switch p.Type {
	case "anthropic":
		return "https://api.anthropic.com"
	case "bedrock":
		return "https://bedrock-runtime.us-east-1.amazonaws.com"
	case "gemini":
		return "https://generativelanguage.googleapis.com"
	case "groq":
		return "https://api.groq.com/openai/v1"
	case "deepseek":
		return "https://api.deepseek.com/v1"
	case "cohere":
		return "https://api.cohere.com"
	default:
		return "https://api.openai.com/v1"
	}
}

// KnownProviderTypes lists the provider types understood by the gateway.
var KnownProviderTypes = []string{"openai", "openai-compatible", "anthropic", "groq", "cohere", "deepseek", "bedrock", "gemini", "azure", "local", "vllm", "ollama"}

// Usable reports whether the provider has what it needs to serve traffic:
// an API key, except for Bedrock (which can use AWS environment credentials)
// and self-hosted types (local/vllm/ollama, or any provider with an explicit
// base_url and no key requirement such as an openai-compatible server).
func (p ProviderConfig) Usable() bool {
	if strings.TrimSpace(p.APIKey) != "" {
		return true
	}
	switch p.Type {
	case "bedrock", "local", "vllm", "ollama":
		return true
	case "openai-compatible":
		return strings.TrimSpace(p.BaseURL) != "" && !strings.Contains(p.BaseURL, "api.openai.com")
	}
	return false
}

// CircuitBreakConfig holds circuit breaker settings.
type CircuitBreakConfig struct {
	MaxFailures      int           `mapstructure:"max_failures" json:"max_failures"`
	ResetTimeout     time.Duration `mapstructure:"reset_timeout" json:"reset_timeout"`
	HalfOpenMaxCalls int           `mapstructure:"half_open_max_calls" json:"half_open_max_calls"`
}

// RouterConfig holds routing settings.
type RouterConfig struct {
	Strategy     string             `mapstructure:"strategy" json:"strategy"`
	CircuitBreak CircuitBreakConfig `mapstructure:"circuit_break" json:"circuit_break"`
	// MaxAttempts bounds how many providers are tried for one request when
	// earlier attempts fail with retryable errors.
	MaxAttempts int `mapstructure:"max_attempts" json:"max_attempts"`
}

// KnownRouterStrategies lists supported routing strategies.
var KnownRouterStrategies = []string{"round_robin", "least_busy", "usage_based", "latency", "cost", "fallback"}

// RateLimitConfig holds rate limiting settings.
type RateLimitConfig struct {
	DefaultRPS      float64 `mapstructure:"default_rps" json:"default_rps"`
	BurstMultiplier int     `mapstructure:"burst_multiplier" json:"burst_multiplier"`
	Strategy        string  `mapstructure:"strategy" json:"strategy"`
	// DefaultTPM is the advertised tokens-per-minute limit.
	DefaultTPM int `mapstructure:"default_tpm" json:"default_tpm"`
}

// CacheConfig holds caching settings.
type CacheConfig struct {
	Enabled           bool          `mapstructure:"enabled" json:"enabled"`
	TTL               time.Duration `mapstructure:"ttl" json:"ttl"`
	ExactPrefix       string        `mapstructure:"exact_prefix" json:"exact_prefix"`
	SemanticPrefix    string        `mapstructure:"semantic_prefix" json:"semantic_prefix"`
	SemanticThreshold float64       `mapstructure:"semantic_threshold" json:"semantic_threshold"`
	// SemanticEnabled turns on similarity-based caching.
	SemanticEnabled bool `mapstructure:"semantic_enabled" json:"semantic_enabled"`
	// SharedAcrossKeys lets different API keys share cache entries. Off by
	// default so one tenant can never be served another tenant's response.
	SharedAcrossKeys bool `mapstructure:"shared_across_keys" json:"shared_across_keys"`
}

// TelemetryConfig holds OpenTelemetry and Prometheus settings.
type TelemetryConfig struct {
	Enabled     bool    `mapstructure:"enabled" json:"enabled"`
	Exporter    string  `mapstructure:"exporter" json:"exporter"`
	OTLPAddr    string  `mapstructure:"otlp_addr" json:"otlp_addr"`
	ServiceName string  `mapstructure:"service_name" json:"service_name"`
	SampleRate  float64 `mapstructure:"sample_rate" json:"sample_rate"`
}

// AgentConfig holds the agentic tool executor settings.
type AgentConfig struct {
	Enabled          bool          `mapstructure:"enabled" json:"enabled"`
	MaxIterations    int           `mapstructure:"max_iterations" json:"max_iterations"`
	ToolTimeout      time.Duration `mapstructure:"tool_timeout" json:"tool_timeout"`
	MaxConcurrent    int           `mapstructure:"max_concurrent" json:"max_concurrent"`
	CacheToolResults bool          `mapstructure:"cache_tool_results" json:"cache_tool_results"`
}

// LoggingConfig holds structured logging settings.
type LoggingConfig struct {
	Level  string `mapstructure:"level" json:"level"`
	Format string `mapstructure:"format" json:"format"`
}

// GuardrailsConfig holds guardrail settings.
type GuardrailsConfig struct {
	Enabled bool `mapstructure:"enabled" json:"enabled"`
}

// FinopsConfig holds FinOps settings.
type FinopsConfig struct {
	Enabled       bool    `mapstructure:"enabled" json:"enabled"`
	DefaultMaxUSD float64 `mapstructure:"default_max_usd" json:"default_max_usd"`
}

// WebhooksConfig holds webhook settings.
type WebhooksConfig struct {
	Enabled bool `mapstructure:"enabled" json:"enabled"`
}

// CallbackWebhookConfig holds callback webhook settings.
type CallbackWebhookConfig struct {
	Enabled    bool          `mapstructure:"enabled" json:"enabled"`
	URL        string        `mapstructure:"url" json:"url"`
	Secret     string        `mapstructure:"secret" json:"secret"`
	Timeout    time.Duration `mapstructure:"timeout" json:"timeout"`
	Retries    int           `mapstructure:"retries" json:"retries"`
	RetryDelay time.Duration `mapstructure:"retry_delay" json:"retry_delay"`
}

// CallbackLangfuseConfig holds Langfuse callback settings.
type CallbackLangfuseConfig struct {
	Enabled bool `mapstructure:"enabled" json:"enabled"`
	// PublicKey/SecretKey are the Langfuse project key pair (preferred).
	PublicKey string `mapstructure:"public_key" json:"public_key"`
	SecretKey string `mapstructure:"secret_key" json:"secret_key"`
	// APIKey is the legacy single-key form ("pk:sk" or the secret key).
	APIKey    string `mapstructure:"api_key" json:"api_key"`
	BaseURL   string `mapstructure:"base_url" json:"base_url"`
	ProjectID string `mapstructure:"project_id" json:"project_id"`
}

// CallbackDatadogConfig holds Datadog callback settings.
type CallbackDatadogConfig struct {
	Enabled bool   `mapstructure:"enabled" json:"enabled"`
	APIKey  string `mapstructure:"api_key" json:"api_key"`
	BaseURL string `mapstructure:"base_url" json:"base_url"`
	Site    string `mapstructure:"site" json:"site"`
}

// CallbacksConfig holds observability callback configuration.
type CallbacksConfig struct {
	Webhook  CallbackWebhookConfig  `mapstructure:"webhook" json:"webhook"`
	Langfuse CallbackLangfuseConfig `mapstructure:"langfuse" json:"langfuse"`
	Datadog  CallbackDatadogConfig  `mapstructure:"datadog" json:"datadog"`
}

// setDefaults registers every default. Viper only applies environment
// overrides (AEROLLM_SECTION_KEY) to keys it knows about, so every key that
// should be overridable from the environment needs a default here.
func setDefaults(v *viper.Viper) {
	v.SetDefault("app.name", "AeroLLM")
	v.SetDefault("app.version", buildVersion)
	v.SetDefault("app.env", "development")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", 30*time.Second)
	v.SetDefault("server.read_header_timeout", 10*time.Second)
	v.SetDefault("server.write_timeout", 120*time.Second)
	v.SetDefault("server.idle_timeout", 120*time.Second)
	v.SetDefault("server.shutdown_timeout", 20*time.Second)
	v.SetDefault("server.max_body_bytes", int64(10<<20))
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 25)
	v.SetDefault("redis.min_idle_conns", 5)
	v.SetDefault("redis.required", false)
	v.SetDefault("auth.master_key", "")
	v.SetDefault("auth.admin_keys", []string{})
	v.SetDefault("auth.api_keys", []string{})
	v.SetDefault("security.cors_allowed_origins", []string{})
	v.SetDefault("security.enable_pprof", false)
	v.SetDefault("security.public_metrics", true)
	v.SetDefault("router.strategy", "round_robin")
	v.SetDefault("router.max_attempts", 3)
	v.SetDefault("router.circuit_break.max_failures", 5)
	v.SetDefault("router.circuit_break.reset_timeout", 60*time.Second)
	v.SetDefault("router.circuit_break.half_open_max_calls", 3)
	v.SetDefault("rate_limit.default_rps", 10.0)
	v.SetDefault("rate_limit.burst_multiplier", 2)
	v.SetDefault("rate_limit.strategy", "token_bucket")
	v.SetDefault("rate_limit.default_tpm", 60000)
	v.SetDefault("cache.enabled", true)
	v.SetDefault("cache.ttl", 15*time.Minute)
	v.SetDefault("cache.exact_prefix", "cache:exact:")
	v.SetDefault("cache.semantic_prefix", "cache:semantic:")
	v.SetDefault("cache.semantic_threshold", 0.95)
	v.SetDefault("cache.semantic_enabled", false)
	v.SetDefault("cache.shared_across_keys", false)
	v.SetDefault("telemetry.enabled", false)
	v.SetDefault("telemetry.exporter", "prometheus")
	v.SetDefault("telemetry.otlp_addr", "")
	v.SetDefault("telemetry.service_name", "aerollm")
	v.SetDefault("telemetry.sample_rate", 1.0)
	v.SetDefault("agent.enabled", true)
	v.SetDefault("agent.max_iterations", 10)
	v.SetDefault("agent.tool_timeout", 30*time.Second)
	v.SetDefault("agent.max_concurrent", 10)
	v.SetDefault("agent.cache_tool_results", true)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("guardrails.enabled", true)
	v.SetDefault("finops.enabled", true)
	v.SetDefault("finops.default_max_usd", 0)
	v.SetDefault("webhooks.enabled", true)
	v.SetDefault("callbacks.webhook.enabled", false)
	v.SetDefault("callbacks.webhook.url", "")
	v.SetDefault("callbacks.webhook.secret", "")
	v.SetDefault("callbacks.webhook.timeout", 5*time.Second)
	v.SetDefault("callbacks.webhook.retries", 3)
	v.SetDefault("callbacks.webhook.retry_delay", 200*time.Millisecond)
	v.SetDefault("callbacks.langfuse.enabled", false)
	v.SetDefault("callbacks.langfuse.api_key", "")
	v.SetDefault("callbacks.langfuse.public_key", "")
	v.SetDefault("callbacks.langfuse.secret_key", "")
	v.SetDefault("callbacks.langfuse.base_url", "https://cloud.langfuse.com")
	v.SetDefault("callbacks.langfuse.project_id", "default")
	v.SetDefault("callbacks.datadog.enabled", false)
	v.SetDefault("callbacks.datadog.api_key", "")
	v.SetDefault("callbacks.datadog.base_url", "https://api.datadoghq.com")
	v.SetDefault("callbacks.datadog.site", "datadoghq.com")
}

// LoadConfig reads configuration from config.yaml and AEROLLM_* environment
// variables (e.g. AEROLLM_REDIS_ADDR overrides redis.addr).
//
// configPath may be a directory containing config.yaml or a path to a YAML
// file. When empty, $AEROLLM_CONFIG is consulted, then ./config.yaml and
// /etc/aerollm/config.yaml. A missing file is not an error; a malformed one
// is. String values may reference environment variables as ${VAR} or
// ${VAR:-default}.
func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	if configPath == "" {
		configPath = os.Getenv("AEROLLM_CONFIG")
	}
	explicitFile := false
	if configPath != "" {
		if st, err := os.Stat(configPath); err == nil && !st.IsDir() {
			v.SetConfigFile(configPath)
			explicitFile = true
		} else if err == nil {
			v.SetConfigName("config")
			v.AddConfigPath(configPath)
		} else if filepath.Ext(configPath) == ".yaml" || filepath.Ext(configPath) == ".yml" {
			return nil, fmt.Errorf("config file %s: %w", configPath, err)
		} else {
			v.SetConfigName("config")
			v.AddConfigPath(configPath)
		}
	} else {
		v.SetConfigName("config")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/aerollm")
	}

	setDefaults(v)
	v.SetEnvPrefix("AEROLLM")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if explicitFile || !errors.As(err, &notFound) {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	ExpandEnv(&cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

// expandString replaces ${VAR} and ${VAR:-default} references. Bare $VAR is
// left untouched so secrets that legitimately contain '$' survive.
func expandString(s string) string {
	if !strings.Contains(s, "${") {
		return s
	}
	return envRef.ReplaceAllStringFunc(s, func(m string) string {
		sub := envRef.FindStringSubmatch(m)
		if val, ok := os.LookupEnv(sub[1]); ok && val != "" {
			return val
		}
		return sub[3]
	})
}

// ExpandEnv expands ${VAR} references in every string field of cfg.
func ExpandEnv(cfg *Config) {
	if cfg == nil {
		return
	}
	expandValue(reflect.ValueOf(cfg).Elem())
}

func expandValue(v reflect.Value) {
	switch v.Kind() {
	case reflect.String:
		if v.CanSet() {
			v.SetString(expandString(v.String()))
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			expandValue(v.Field(i))
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			expandValue(v.Index(i))
		}
	case reflect.Pointer:
		if !v.IsNil() {
			expandValue(v.Elem())
		}
	}
}

// Validate reports configuration values that would make the gateway
// misbehave. It is applied on load and on every hot reload.
func (c *Config) Validate() error {
	var errs []error
	if c.Server.Port < 0 || c.Server.Port > 65535 {
		errs = append(errs, fmt.Errorf("server.port %d out of range", c.Server.Port))
	}
	for name, d := range map[string]time.Duration{
		"server.read_timeout":  c.Server.ReadTimeout,
		"server.write_timeout": c.Server.WriteTimeout,
		"server.idle_timeout":  c.Server.IdleTimeout,
	} {
		if d < 0 {
			errs = append(errs, fmt.Errorf("%s must not be negative", name))
		}
	}
	if c.Server.MaxBodyBytes < 0 {
		errs = append(errs, errors.New("server.max_body_bytes must not be negative"))
	}
	if c.RateLimit.DefaultRPS < 0 {
		errs = append(errs, errors.New("rate_limit.default_rps must not be negative"))
	}
	if c.RateLimit.BurstMultiplier < 0 {
		errs = append(errs, errors.New("rate_limit.burst_multiplier must not be negative"))
	}
	if t := c.Cache.SemanticThreshold; t < 0 || t > 1 {
		errs = append(errs, fmt.Errorf("cache.semantic_threshold %v must be within [0,1]", t))
	}
	if c.Cache.TTL < 0 {
		errs = append(errs, errors.New("cache.ttl must not be negative"))
	}
	if r := c.Telemetry.SampleRate; r < 0 || r > 1 {
		errs = append(errs, fmt.Errorf("telemetry.sample_rate %v must be within [0,1]", r))
	}
	if c.Router.Strategy != "" && !contains(KnownRouterStrategies, c.Router.Strategy) {
		errs = append(errs, fmt.Errorf("router.strategy %q unknown (want one of %s)", c.Router.Strategy, strings.Join(KnownRouterStrategies, ", ")))
	}
	if c.Agent.MaxIterations < 0 || c.Agent.MaxConcurrent < 0 {
		errs = append(errs, errors.New("agent.max_iterations and agent.max_concurrent must not be negative"))
	}
	seen := map[string]bool{}
	for i, p := range c.Providers {
		if p.Type == "" {
			errs = append(errs, fmt.Errorf("providers[%d]: type is required", i))
		} else if !contains(KnownProviderTypes, p.Type) {
			errs = append(errs, fmt.Errorf("providers[%d]: unknown type %q", i, p.Type))
		}
		name := p.ResolvedName()
		if seen[name] {
			errs = append(errs, fmt.Errorf("providers[%d]: duplicate provider name %q", i, name))
		}
		seen[name] = true
		if p.Weight < 0 || p.RateLimitRPS < 0 || p.Timeout < 0 {
			errs = append(errs, fmt.Errorf("providers[%d]: weight, rate_limit_rps and timeout must not be negative", i))
		}
	}
	return errors.Join(errs...)
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
