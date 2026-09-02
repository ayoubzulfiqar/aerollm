package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// buildVersion is set via linker flags at build time.
var buildVersion = "dev"

// Config holds the entire application configuration.
type Config struct {
	App        AppConfig        `mapstructure:"app"`
	Server     ServerConfig     `mapstructure:"server"`
	Redis      RedisConfig      `mapstructure:"redis"`
	Providers  []ProviderConfig `mapstructure:"providers"`
	Router     RouterConfig     `mapstructure:"router"`
	RateLimit  RateLimitConfig  `mapstructure:"rate_limit"`
	Cache      CacheConfig      `mapstructure:"cache"`
	Telemetry  TelemetryConfig  `mapstructure:"telemetry"`
	Agent      AgentConfig      `mapstructure:"agent"`
	Logging    LoggingConfig    `mapstructure:"logging"`
	Guardrails GuardrailsConfig `mapstructure:"guardrails"`
	Finops     FinopsConfig     `mapstructure:"finops"`
	Webhooks   WebhooksConfig   `mapstructure:"webhooks"`
}

// AppConfig holds application-level settings.
type AppConfig struct {
	Name    string `mapstructure:"name"`
	Version string `mapstructure:"version"`
	Env     string `mapstructure:"env"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout"`
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Addr         string `mapstructure:"addr"`
	Password     string `mapstructure:"password"`
	DB           int    `mapstructure:"db"`
	PoolSize     int    `mapstructure:"pool_size"`
	MinIdleConns int    `mapstructure:"min_idle_conns"`
}

// ProviderConfig holds per-provider settings.
type ProviderConfig struct {
	Name         string        `mapstructure:"name"`
	Type         string        `mapstructure:"type"`
	BaseURL      string        `mapstructure:"base_url"`
	APIKey       string        `mapstructure:"api_key"`
	Weight       int           `mapstructure:"weight"`
	Timeout      time.Duration `mapstructure:"timeout"`
	RateLimitRPS float64       `mapstructure:"rate_limit_rps"`
}

// RouterConfig holds routing settings.
type RouterConfig struct {
	Strategy string `mapstructure:"strategy"`
	CircuitBreak struct {
		MaxFailures     int           `mapstructure:"max_failures"`
		ResetTimeout    time.Duration `mapstructure:"reset_timeout"`
		HalfOpenMaxCalls int          `mapstructure:"half_open_max_calls"`
	} `mapstructure:"circuit_break"`
}

// RateLimitConfig holds rate limiting settings.
type RateLimitConfig struct {
	DefaultRPS      float64 `mapstructure:"default_rps"`
	BurstMultiplier int     `mapstructure:"burst_multiplier"`
	Strategy        string  `mapstructure:"strategy"`
}

// CacheConfig holds caching settings.
type CacheConfig struct {
	Enabled           bool        `mapstructure:"enabled"`
	TTL               time.Duration `mapstructure:"ttl"`
	ExactPrefix       string      `mapstructure:"exact_prefix"`
	SemanticPrefix    string      `mapstructure:"semantic_prefix"`
	SemanticThreshold float64     `mapstructure:"semantic_threshold"`
}

// TelemetryConfig holds OpenTelemetry and Prometheus settings.
type TelemetryConfig struct {
	Enabled     bool    `mapstructure:"enabled"`
	Exporter    string  `mapstructure:"exporter"`
	OTLPAddr    string  `mapstructure:"otlp_addr"`
	ServiceName string  `mapstructure:"service_name"`
	SampleRate  float64 `mapstructure:"sample_rate"`
}

// AgentConfig holds the agentic tool executor settings.
type AgentConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	MaxIterations   int           `mapstructure:"max_iterations"`
	ToolTimeout     time.Duration `mapstructure:"tool_timeout"`
	MaxConcurrent   int           `mapstructure:"max_concurrent"`
	CacheToolResults bool          `mapstructure:"cache_tool_results"`
}

// LoggingConfig holds structured logging settings.
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// GuardrailsConfig holds guardrail settings.
type GuardrailsConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// FinopsConfig holds FinOps settings.
type FinopsConfig struct {
	Enabled        bool    `mapstructure:"enabled"`
	DefaultMaxUSD  float64 `mapstructure:"default_max_usd"`
}

// WebhooksConfig holds webhook settings.
type WebhooksConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// LoadConfig reads configuration from the given file path and environment variables.
func LoadConfig(configPath string) (*Config, error) {
	v := viper.NewWithOptions(
		viper.KeyDelimiter("::"),
	)

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	if configPath != "" {
		v.AddConfigPath(configPath)
	} else {
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/aerollm")
	}

	if err := v.ReadInConfig(); err != nil {
		// config file is optional; we'll use defaults if missing
	}

	v.SetDefault("app.name", "Aerollm")
	v.SetDefault("app.version", buildVersion)
	v.SetDefault("app.env", "development")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", 30*time.Second)
	v.SetDefault("server.write_timeout", 120*time.Second)
	v.SetDefault("server.idle_timeout", 120*time.Second)
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 25)
	v.SetDefault("redis.min_idle_conns", 5)
	v.SetDefault("router.strategy", "round_robin")
	v.SetDefault("router.circuit_break.max_failures", 5)
	v.SetDefault("router.circuit_break.reset_timeout", 60*time.Second)
	v.SetDefault("router.circuit_break.half_open_max_calls", 3)
	v.SetDefault("rate_limit.default_rps", 10.0)
	v.SetDefault("rate_limit.burst_multiplier", 2)
	v.SetDefault("rate_limit.strategy", "token_bucket")
	v.SetDefault("cache.enabled", true)
	v.SetDefault("cache.ttl", 15*time.Minute)
	v.SetDefault("cache.exact_prefix", "cache:exact:")
	v.SetDefault("cache.semantic_prefix", "cache:semantic:")
	v.SetDefault("cache.semantic_threshold", 0.85)
	v.SetDefault("telemetry.enabled", false)
	v.SetDefault("telemetry.exporter", "prometheus")
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

	v.SetEnvPrefix("AEROLLM")
	v.AutomaticEnv()
	v.AllowEmptyEnv(true)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}