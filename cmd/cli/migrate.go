package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v3"
)

// litellmModelEntry corresponds to entry in LiteLLM's model_list.
type litellmModelEntry struct {
	ModelName     string            `yaml:"model_name"`      // e.g. "gpt-4o"
	LitellmParams map[string]any    `yaml:"litellm_params"`  // model, api_key, api_base, etc.
	RoutingStrategy string          `yaml:"routing_strategy,omitempty"` // per-model route override
	// ... other fields ignored for migration
}

// litellmConfig is a subset of LiteLLM's config.yaml schema.
type litellmConfig struct {
	ModelList      []litellmModelEntry `yaml:"model_list"`
	GeneralSettings map[string]any      `yaml:"general_settings"`
	RouterSettings  map[string]any      `yaml:"router_settings"`
}

// aerollmConfig mirrors internal/config/config.go for YAML output.
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

// ... sub-configs ...

type aerollmProviderConfig struct {
	Name     string   `yaml:"name"`
	Type     string   `yaml:"type"`
	APIKey   string   `yaml:"api_key,omitempty"`
	BaseURL  string   `yaml:"base_url,omitempty"`
	Models   []string `yaml:"models"`
	Weight   int      `yaml:"weight,omitempty"`
}

type aerollmRouterConfig struct {
	Strategy string `yaml:"strategy"`
}

func newMigrateCmd() *cobra.Command {
	var input, output string

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate configurations between formats",
	}

	llmCmd := &cobra.Command{
		Use:   "litellm",
		Short: "Convert a LiteLLM config.yaml to an AeroLLM config.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLitellmMigrate(input, output)
		},
	}

	llmCmd.Flags().StringVarP(&input, "input", "i", "", "path to LiteLLM config.yaml (required)")
	llmCmd.Flags().StringVarP(&output, "output", "o", "config.yaml", "path to output AeroLLM config.yaml")
	_ = llmCmd.MarkFlagRequired("input")

	cmd.AddCommand(llmCmd)
	return cmd
}

func runLitellmMigrate(inputPath, outputPath string) error {
	// Read the LiteLLM config file
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("reading input file: %w", err)
	}

	var llmCfg litellmConfig
	if err := yaml.Unmarshal(data, &llmCfg); err != nil {
		return fmt.Errorf("parsing LiteLLM config: %w", err)
	}

	// Build AeroLLM config from LiteLLM config
	aeroCfg, err := convertLitellmToAero(&llmCfg)
	if err != nil {
		return fmt.Errorf("converting config: %w", err)
	}

	// Marshal to YAML
	out, err := yaml.Marshal(aeroCfg)
	if err != nil {
		return fmt.Errorf("marshaling output config: %w", err)
	}

	// Write output
	if err := os.WriteFile(outputPath, out, 0644); err != nil {
		return fmt.Errorf("writing output file: %w", err)
	}

	fmt.Printf("Successfully migrated %s -> %s\n", inputPath, outputPath)
	fmt.Printf("  Providers: %d\n", len(aeroCfg.Providers))
	fmt.Printf("  Router strategy: %s\n", aeroCfg.Router.Strategy)
	return nil
}

// convertLitellmToAero transforms a parsed LiteLLM config into the AeroLLM schema.
func convertLitellmToAero(cfg *litellmConfig) (*aerollmConfig, error) {
	result := defaultAerollmConfig()

	// Map general_settings
	if gs, ok := cfg.GeneralSettings["master_key"]; ok && gs != "" {
		// AeroLLM uses env var AEROLLM_MASTER_KEY, we just note it in comments
		result.App.Name = "AeroLLM"
	}

	// Map router_settings
	strategy := "round_robin"
	if rs, ok := cfg.RouterSettings["routing_strategy"]; ok {
		if s, ok := rs.(string); ok {
			strategy = mapLitellmStrategy(s)
		}
	}
	result.Router.Strategy = strategy

	// Map model_list entries to providers
	// Group by provider base URL (each unique API base = one provider)
	providerMap := make(map[string]*aerollmProviderConfig)
	order := []string{}

	for _, entry := range cfg.ModelList {
		params := entry.LitellmParams

		// Extract the litellm model name (e.g. "gpt-4o", "claude-3-opus@20240229", "bedrock/anthropic.claude-3-sonnet-20240229-bde75ac8")
		llmModel, _ := params["model"].(string)

		// Determine provider type from the model name prefix or api_base
		apiBase, _ := params["api_base"].(string)
		apiKey := ""
		if k, ok := params["api_key"]; ok {
			apiKey = fmt.Sprintf("%v", k)
		}

		providerType, providerPrefix, modelName := classifyLitellmModel(llmModel, apiBase)

		// Use api_base as the deduplication key; fall back to providerPrefix
		key := apiBase
		if key == "" {
			key = providerPrefix
		}
		if key == "" {
			key = "default"
		}

		pc, exists := providerMap[key]
		if !exists {
			pc = &aerollmProviderConfig{
				Name:    providerPrefix,
				Type:    providerType,
				Models:  []string{},
			}
			if apiKey != "" && apiKey != "os.environ/"+"LITELLM_API_KEY" {
				pc.APIKey = translateApiKeyRef(apiKey)
			}
			if apiBase != "" {
				pc.BaseURL = apiBase
			}
			providerMap[key] = pc
			order = append(order, key)
		}

		// Add the model if not already present
		modelAlreadyListed := false
		for _, m := range pc.Models {
			if m == modelName {
				modelAlreadyListed = true
				break
			}
		}
		if !modelAlreadyListed && modelName != "" {
			pc.Models = append(pc.Models, modelName)
		}
	}

	// Append providers in insertion order
	for _, k := range order {
		result.Providers = append(result.Providers, *providerMap[k])
	}

	return result, nil
}

// classifyLitellmModel inspects a LiteLLM model string and api_base to determine
// the AeroLLM provider type, a provider prefix (name), and the cleaned model name.
func classifyLitellmModel(llmModel, apiBase string) (providerType, providerName, modelName string) {
	// Strip the provider prefix from model names like "bedrock/anthropic.claude-..."
	parts := strings.SplitN(llmModel, "/", 2)

	// If there's a slash, the prefix indicates the provider family
	if len(parts) == 2 {
		prefix := parts[0]
		modelName = parts[1]
		switch prefix {
		case "bedrock":
			return "bedrock", "bedrock", modelName
		case "vertex_ai":
			return "gemini", "gemini", modelName
		case "together":
			return "openai-compatible", "together", modelName
		case "deepseek":
			return "openai-compatible", "deepseek", modelName
		case "openai":
			// Could be custom OpenAI-compatible; use api_base if provided
			if apiBase != "" {
				return "openai-compatible", "custom", modelName
			}
			return "openai-compatible", "openai", modelName
		default:
			// Unknown provider prefix — treat as OpenAI-compatible with custom base
			return "openai-compatible", prefix, modelName
		}
	}

	// No slash — infer from model name keywords
	lower := strings.ToLower(llmModel)
	switch {
	case strings.HasPrefix(lower, "claude"):
		return "anthropic", "anthropic", llmModel
	case strings.HasPrefix(lower, "gpt-"):
		if apiBase != "" {
			return "openai-compatible", "custom", llmModel
		}
		return "openai-compatible", "openai", llmModel
	case strings.HasPrefix(lower, "gemini"):
		return "gemini", "gemini", llmModel
	case strings.HasPrefix(lower, "llama"):
		if apiBase != "" {
			return "openai-compatible", "custom", llmModel
		}
		return "openai-compatible", "groq", llmModel
	case strings.Contains(lower, "command"):
		return "openai-compatible", "cohere", llmModel
	default:
		if apiBase != "" {
			return "openai-compatible", "custom", llmModel
		}
		return "openai-compatible", "openai", llmModel
	}
}

// translateApiKeyRef converts LiteLLM-style API key references to AeroLLM env var format.
// LiteLLM uses os.environ/LITELLM_API_KEY; AeroLLM uses ${ENV_VAR}.
func translateApiKeyRef(keyRef string) string {
	if keyRef == "" {
		return ""
	}
	// If it looks like a bare "os.environ/X" reference, convert to ${X}
	if strings.HasPrefix(keyRef, "os.environ/") {
		envName := strings.TrimPrefix(keyRef, "os.environ/")
		return "${" + envName + "}"
	}
	// If it's already ${...} style, keep as-is
	if strings.HasPrefix(keyRef, "${") {
		return keyRef
	}
	// Otherwise it's a literal key — keep as env var reference
	return "${" + strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(keyRef, "-", "_"), ".", "_")) + "}"
}

// mapLitellmStrategy converts LiteLLM router_settings routing_strategy to AeroLLM strategy.
func mapLitellmStrategy(s string) string {
	switch s {
	case "simple-rotation", "round robin":
		return "round_robin"
	case "lowest-latency", "latency-based-routing":
		return "latency"
	case "cost", "cost-optimized":
		return "cost"
	case "least-busy", "inflight":
		return "least-busy"
	case "usage-based", "tpm-rpm":
		return "usage-based"
	case "sequential", "fallback":
		return "fallback"
	default:
		return "round_robin"
	}
}

// defaultAerollmConfig returns a minimal AeroLLM config with sensible defaults.
func defaultAerollmConfig() *aerollmConfig {
	return &aerollmConfig{
		App: aerollmAppConfig{
			Name:    "AeroLLM",
			Version: "dev",
			Env:     "production",
		},
		Server: aerollmServerConfig{
			Port:         8080,
			ReadTimeout:  30,
			WriteTimeout: 120,
			IdleTimeout:  120,
		},
		Redis: aerollmRedisConfig{
			Addr:         "localhost:6379",
			Password:     "",
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
			TTL:               15,
			ExactPrefix:       "cache:exact:",
			SemanticPrefix:    "cache:semantic:",
			SemanticThreshold: 0.85,
		},
		Telemetry: aerollmTelemetryConfig{
			Enabled:   false,
			Exporter:  "prometheus",
			ServiceName: "aerollm",
			SampleRate:  1.0,
		},
		Agent: aerollmAgentConfig{
			Enabled:       true,
			MaxIterations: 10,
			ToolTimeout:   30,
			MaxConcurrent: 10,
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
			Enabled:       true,
			DefaultMaxUSD: 0,
		},
		Webhooks: aerollmWebhooksConfig{
			Enabled: true,
		},
		Callbacks: aerollmCallbacksConfig{},
	}
}

// --- Sub-config type definitions (matching config.go but YAML-friendly) ---

type aerollmAppConfig struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	Env     string `yaml:"env"`
}

type aerollmServerConfig struct {
	Port         int `yaml:"port"`
	ReadTimeout  int `yaml:"read_timeout"`
	WriteTimeout int `yaml:"write_timeout"`
	IdleTimeout  int `yaml:"idle_timeout"`
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
	TTL               int     `yaml:"ttl"`
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
	Enabled          bool `yaml:"enabled"`
	MaxIterations    int  `yaml:"max_iterations"`
	ToolTimeout      int  `yaml:"tool_timeout"`
	MaxConcurrent    int  `yaml:"max_concurrent"`
	CacheToolResults bool `yaml:"cache_tool_results"`
}

type aerollmLoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type aerollmGuardrailsConfig struct {
	Enabled bool `yaml:"enabled"`
}

type aerollmFinopsConfig struct {
	Enabled       bool  `yaml:"enabled"`
	DefaultMaxUSD int   `yaml:"default_max_usd"`
}

type aerollmWebhooksConfig struct {
	Enabled bool `yaml:"enabled"`
}

type aerollmCallbacksConfig struct{}
