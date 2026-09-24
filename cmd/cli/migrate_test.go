package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"go.yaml.in/yaml/v3"
)

// A realistic LiteLLM proxy config exercising the documented format.
const litellmFixture = `
model_list:
  - model_name: gpt-4o
    litellm_params:
      model: openai/gpt-4o
      api_key: os.environ/OPENAI_API_KEY
      rpm: 600
      tpm: 100000
  - model_name: gpt-4o-mini
    litellm_params:
      model: openai/gpt-4o-mini
      api_key: os.environ/OPENAI_API_KEY
      rpm: 120
  - model_name: gpt-4o                 # second deployment of the same model group
    litellm_params:
      model: azure/gpt4o-prod
      api_base: https://acme-eu.openai.azure.com/
      api_key: os.environ/AZURE_API_KEY
      api_version: "2024-06-01"
  - model_name: claude-sonnet
    litellm_params:
      model: anthropic/claude-3-5-sonnet-20241022
      api_key: sk-ant-LITERAL-SECRET-DO-NOT-COPY
      timeout: 45
  - model_name: llama-70b
    litellm_params:
      model: groq/llama3-70b-8192
      api_key: os.environ/GROQ_API_KEY
  - model_name: bedrock-claude
    litellm_params:
      model: bedrock/anthropic.claude-3-sonnet-20240229-v1:0
      aws_region_name: us-east-1
      aws_access_key_id: os.environ/AWS_ACCESS_KEY_ID
  - model_name: gemini-pro
    litellm_params:
      model: gemini/gemini-1.5-pro
      api_key: os.environ/GEMINI_API_KEY
  - model_name: deepseek-chat
    litellm_params:
      model: deepseek/deepseek-chat
      api_key: os.environ/DEEPSEEK_API_KEY
  - model_name: command-r
    litellm_params:
      model: cohere/command-r-plus
  - model_name: local-llama
    litellm_params:
      model: ollama/llama3
      api_base: http://localhost:11434
  - model_name: internal
    litellm_params:
      model: openai/my-finetune
      api_base: https://llm.internal.example.com/v1
      api_key: os.environ/INTERNAL_KEY
  - model_name: hf-model
    litellm_params:
      model: huggingface/meta-llama/Llama-2-7b

litellm_settings:
  drop_params: true
  request_timeout: 600
  success_callback: ["langfuse"]
  cache: true
  cache_params:
    type: redis
    host: redis.internal
    port: 6380
    password: literal-redis-password
    ttl: 3600

router_settings:
  routing_strategy: usage-based-routing-v2
  num_retries: 3
  model_group_alias: {"gpt-4": "gpt-4o"}

general_settings:
  master_key: sk-litellm-master-LITERAL
  database_url: postgresql://user:pw@db:5432/litellm

environment_variables:
  SOME_TOKEN: very-secret
`

func writeFixture(t *testing.T, content string) (dir, input string) {
	t.Helper()
	dir = t.TempDir()
	input = filepath.Join(dir, "litellm_config.yaml")
	if err := os.WriteFile(input, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, input
}

func findProvider(cfg *aerollmConfig, name string) *aerollmProviderConfig {
	for i := range cfg.Providers {
		if cfg.Providers[i].Name == name {
			return &cfg.Providers[i]
		}
	}
	return nil
}

func TestConvertLitellmFixture(t *testing.T) {
	var in litellmConfig
	if err := yaml.Unmarshal([]byte(litellmFixture), &in); err != nil {
		t.Fatal(err)
	}
	cfg, report, err := convertLitellmToAero(&in, migrateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Router.Strategy != "usage_based" {
		t.Errorf("strategy = %q", cfg.Router.Strategy)
	}
	names := make([]string, len(cfg.Providers))
	for i, p := range cfg.Providers {
		names[i] = p.Name
	}
	want := []string{"openai", "azure", "anthropic", "groq", "bedrock", "gemini", "deepseek", "cohere", "ollama", "llm-internal-example-com"}
	if !slices.Equal(names, want) {
		t.Fatalf("providers = %v, want %v", names, want)
	}

	openai := findProvider(cfg, "openai")
	if openai.Type != "openai" || openai.APIKey != "${OPENAI_API_KEY}" || openai.BaseURL != "" {
		t.Errorf("openai = %+v", openai)
	}
	// Both openai deployments are grouped; model_group_alias gpt-4 -> gpt-4o.
	if !slices.Equal(openai.Models, []string{"gpt-4o", "gpt-4o-mini", "gpt-4=gpt-4o"}) {
		t.Errorf("openai models = %v", openai.Models)
	}
	if openai.RateLimitRPS != 12 { // (600 + 120) rpm / 60
		t.Errorf("openai rps = %v", openai.RateLimitRPS)
	}
	if openai.Timeout != "10m" { // litellm_settings.request_timeout 600s
		t.Errorf("openai timeout = %q", openai.Timeout)
	}

	azure := findProvider(cfg, "azure")
	if azure.Type != "openai-compatible" || azure.BaseURL != "https://acme-eu.openai.azure.com/openai/v1" || azure.APIKey != "${AZURE_API_KEY}" {
		t.Errorf("azure = %+v", azure)
	}
	// Alias keeps the public model name working for the Azure deployment.
	if !slices.Equal(azure.Models, []string{"gpt4o-prod", "gpt-4o=gpt4o-prod", "gpt-4=gpt4o-prod"}) {
		t.Errorf("azure models = %v", azure.Models)
	}

	anthropic := findProvider(cfg, "anthropic")
	if anthropic.APIKey != "${AEROLLM_ANTHROPIC_API_KEY}" || anthropic.Timeout != "45s" {
		t.Errorf("anthropic = %+v", anthropic)
	}
	if !slices.Equal(anthropic.Models, []string{"claude-3-5-sonnet-20241022", "claude-sonnet=claude-3-5-sonnet-20241022"}) {
		t.Errorf("anthropic models = %v", anthropic.Models)
	}
	if p := findProvider(cfg, "bedrock"); p.Models[0] != "anthropic.claude-3-sonnet-20240229-v1:0" || p.APIKey != "" {
		t.Errorf("bedrock = %+v", p)
	}
	if p := findProvider(cfg, "cohere"); p.APIKey != "${COHERE_API_KEY}" || p.Models[0] != "command-r-plus" {
		t.Errorf("cohere (implicit env key) = %+v", p)
	}
	if p := findProvider(cfg, "ollama"); p.BaseURL != "http://localhost:11434/v1" || p.APIKey != keylessPlaceholder || p.Type != "openai-compatible" {
		t.Errorf("ollama = %+v", p)
	}
	if p := findProvider(cfg, "llm-internal-example-com"); p.Type != "openai-compatible" || p.APIKey != "${INTERNAL_KEY}" {
		t.Errorf("custom openai-compatible = %+v", p)
	}

	// Settings.
	if cfg.Redis.Addr != "redis.internal:6380" || cfg.Redis.Password != "${AEROLLM_REDIS_PASSWORD}" {
		t.Errorf("redis = %+v", cfg.Redis)
	}
	if !cfg.Cache.Enabled || cfg.Cache.TTL != "1h" {
		t.Errorf("cache = %+v", cfg.Cache)
	}
	if cfg.Callbacks.Langfuse == nil || cfg.Callbacks.Langfuse.APIKey != "${LANGFUSE_SECRET_KEY}" {
		t.Errorf("langfuse = %+v", cfg.Callbacks.Langfuse)
	}

	// Env vars the user must set (names only).
	for _, env := range []string{"OPENAI_API_KEY", "AZURE_API_KEY", "AEROLLM_ANTHROPIC_API_KEY", "AEROLLM_API_KEY", "AEROLLM_REDIS_PASSWORD", "COHERE_API_KEY"} {
		if _, ok := report.EnvVars[env]; !ok {
			t.Errorf("missing env var %s in report: %v", env, report.EnvVars)
		}
	}

	all := strings.Join(report.Warnings, "\n")
	for _, want := range []string{
		"huggingface",                // unsupported provider skipped
		"tpm limits",                 // tpm dropped
		"litellm_params.api_version", // unsupported param
		"litellm_params.aws_access_key_id",
		"litellm_settings.drop_params",
		"router_settings.num_retries",
		"general_settings.database_url",
		"environment_variables",
		"SOME_TOKEN", // names are fine...
		`model "gpt-4o" is served by 2 providers`,
		"Azure",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("warnings missing %q:\n%s", want, all)
		}
	}
	for _, secret := range []string{"very-secret", "LITERAL", "literal-redis-password", "user:pw"} {
		if strings.Contains(all, secret) {
			t.Errorf("warnings leak secret %q", secret)
		}
	}
}

func TestRunLitellmMigrateWritesSafeFile(t *testing.T) {
	dir, input := writeFixture(t, litellmFixture)
	output := filepath.Join(dir, "config.yaml")
	var stdout, stderr bytes.Buffer
	if err := runLitellmMigrate(input, output, migrateOptions{}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("output mode = %v, want 0600", fi.Mode().Perm())
	}
	data, _ := os.ReadFile(output)
	everything := string(data) + stdout.String() + stderr.String()
	for _, secret := range []string{"sk-ant-LITERAL-SECRET-DO-NOT-COPY", "sk-litellm-master-LITERAL", "literal-redis-password", "very-secret", "postgresql://"} {
		if strings.Contains(everything, secret) {
			t.Fatalf("secret %q leaked into output/logs", secret)
		}
	}
	if !strings.Contains(string(data), "${OPENAI_API_KEY}") || !strings.HasPrefix(string(data), "# AeroLLM configuration generated") {
		t.Fatalf("unexpected output:\n%s", data)
	}
	if !strings.Contains(stdout.String(), "Successfully migrated") || !strings.Contains(stdout.String(), "AEROLLM_ANTHROPIC_API_KEY") {
		t.Fatalf("summary = %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "warning:") {
		t.Fatalf("warnings not printed: %q", stderr.String())
	}

	// Refuse to overwrite without --force.
	if err := runLitellmMigrate(input, output, migrateOptions{}, nil, nil); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected overwrite refusal, got %v", err)
	}
	if err := runLitellmMigrate(input, output, migrateOptions{Force: true}, nil, nil); err != nil {
		t.Fatalf("--force: %v", err)
	}
	// Never overwrite the input itself.
	if err := runLitellmMigrate(input, input, migrateOptions{Force: true}, nil, nil); err == nil {
		t.Fatal("expected refusal to overwrite the input file")
	}
	if b, _ := os.ReadFile(input); string(b) != litellmFixture {
		t.Fatal("input file was modified")
	}
}

// The generated file must load and validate with the gateway's own loader.
func TestMigratedConfigLoadsWithGatewayLoader(t *testing.T) {
	dir, input := writeFixture(t, litellmFixture)
	output := filepath.Join(dir, "config.yaml")
	if err := runLitellmMigrate(input, output, migrateOptions{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENAI_API_KEY", "sk-openai-from-env")
	cfg, err := config.LoadConfig(output)
	if err != nil {
		t.Fatalf("gateway rejected migrated config: %v", err)
	}
	if cfg.Router.Strategy != "usage_based" || len(cfg.Providers) != 10 {
		t.Fatalf("loaded strategy=%q providers=%d", cfg.Router.Strategy, len(cfg.Providers))
	}
	var openai config.ProviderConfig
	for _, p := range cfg.Providers {
		if p.Name == "openai" {
			openai = p
		}
	}
	if openai.APIKey != "sk-openai-from-env" {
		t.Errorf("${OPENAI_API_KEY} not expanded by loader: %q", openai.APIKey)
	}
	if openai.Timeout.Minutes() != 10 {
		t.Errorf("timeout = %v, want 10m (not nanoseconds)", openai.Timeout)
	}
	if cfg.Server.ReadTimeout.Seconds() != 30 || cfg.Cache.TTL.Hours() != 1 {
		t.Errorf("durations mis-decoded: read_timeout=%v cache.ttl=%v", cfg.Server.ReadTimeout, cfg.Cache.TTL)
	}
}

func TestMigrateCommandStdoutAndStrict(t *testing.T) {
	isolateEnv(t)
	_, input := writeFixture(t, `
model_list:
  - model_name: gpt-4o
    litellm_params:
      model: gpt-4o
      api_key: os.environ/OPENAI_API_KEY
router_settings:
  routing_strategy: latency-based-routing
`)
	out, stderr, err := runCLI(t, "", "migrate", "litellm", "-i", input, "-o", "-")
	if err != nil {
		t.Fatal(err)
	}
	var cfg aerollmConfig
	if err := yaml.Unmarshal([]byte(out), &cfg); err != nil {
		t.Fatalf("stdout is not the YAML config: %v\n%s", err, out)
	}
	if cfg.Router.Strategy != "latency" || cfg.Providers[0].APIKey != "${OPENAI_API_KEY}" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if !strings.Contains(stderr, "Providers: 1") {
		t.Fatalf("summary should go to stderr with -o -: %q", stderr)
	}

	// --strict fails when there are warnings and writes nothing.
	dir, input2 := writeFixture(t, litellmFixture)
	_, _, err = runCLI(t, "", "migrate", "litellm", "-i", input2, "-o", filepath.Join(dir, "out.yaml"), "--strict")
	if err == nil || !strings.Contains(err.Error(), "--strict") {
		t.Fatalf("expected strict failure, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "out.yaml")); err == nil {
		t.Fatal("--strict must not write the output")
	}
}

func TestInlineSecretsOption(t *testing.T) {
	var in litellmConfig
	if err := yaml.Unmarshal([]byte(`
model_list:
  - model_name: a
    litellm_params: {model: openai/gpt-4o, api_key: sk-literal-one}
  - model_name: b
    litellm_params: {model: openai/gpt-4o-mini, api_key: sk-literal-two}
`), &in); err != nil {
		t.Fatal(err)
	}
	cfg, report, err := convertLitellmToAero(&in, migrateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Different literal keys -> separate providers with distinct placeholders.
	if len(cfg.Providers) != 2 || cfg.Providers[0].APIKey != "${AEROLLM_OPENAI_API_KEY}" || cfg.Providers[1].APIKey != "${AEROLLM_OPENAI_2_API_KEY}" {
		t.Fatalf("providers = %+v", cfg.Providers)
	}
	if len(report.EnvVars) != 2 {
		t.Fatalf("env vars = %v", report.EnvVars)
	}
	cfg, _, err = convertLitellmToAero(&in, migrateOptions{InlineSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Providers[0].APIKey != "sk-literal-one" {
		t.Fatalf("--inline-secrets should copy the key, got %q", cfg.Providers[0].APIKey)
	}
}

func TestCustomEndpointNeverGetsOpenAIKey(t *testing.T) {
	var in litellmConfig
	if err := yaml.Unmarshal([]byte(`
model_list:
  - model_name: local
    litellm_params: {model: openai/mistral-7b, api_base: "http://vllm.internal:8000/v1"}
  - model_name: gpt
    litellm_params: {model: openai/gpt-4o}
`), &in); err != nil {
		t.Fatal(err)
	}
	cfg, _, err := convertLitellmToAero(&in, migrateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	custom := findProvider(cfg, "vllm-internal")
	if custom == nil || custom.APIKey == "${OPENAI_API_KEY}" || custom.APIKey != keylessPlaceholder {
		t.Fatalf("custom endpoint = %+v", custom)
	}
	if p := findProvider(cfg, "openai"); p.APIKey != "${OPENAI_API_KEY}" {
		t.Fatalf("openai default key = %q", p.APIKey)
	}
}

func TestMapLitellmStrategy(t *testing.T) {
	tests := map[string]string{
		"simple-shuffle":         "round_robin",
		"least-busy":             "least_busy",
		"usage-based-routing":    "usage_based",
		"usage-based-routing-v2": "usage_based",
		"latency-based-routing":  "latency",
		"cost-based-routing":     "cost",
	}
	for in, want := range tests {
		got, ok := mapLitellmStrategy(in)
		if !ok || got != want {
			t.Errorf("mapLitellmStrategy(%q) = %q, %v; want %q", in, got, ok, want)
		}
		if !slices.Contains(config.KnownRouterStrategies, got) {
			t.Errorf("%q is not a strategy the gateway accepts", got)
		}
	}
	if got, ok := mapLitellmStrategy("magic-routing"); ok || got != "round_robin" {
		t.Errorf("unknown strategy = %q, %v", got, ok)
	}
}

func TestTranslateSecret(t *testing.T) {
	cases := []struct {
		in, want, env string
		literal       bool
	}{
		{"os.environ/OPENAI_API_KEY", "${OPENAI_API_KEY}", "OPENAI_API_KEY", false},
		{"${ANTHROPIC_API_KEY}", "${ANTHROPIC_API_KEY}", "ANTHROPIC_API_KEY", false},
		{"$GROQ_KEY", "${GROQ_KEY}", "GROQ_KEY", false},
		{"sk-literal-key", "sk-literal-key", "", true},
	}
	for _, c := range cases {
		got, err := translateSecret(c.in)
		if err != nil || got.Value != c.want || got.EnvVar != c.env || got.Literal != c.literal {
			t.Errorf("translateSecret(%q) = %+v, %v", c.in, got, err)
		}
		if got.Literal && strings.Contains(got.Identity, c.in) {
			t.Errorf("identity leaks the secret: %q", got.Identity)
		}
	}
	if _, err := translateSecret("os.environ/BAD-NAME;rm"); err == nil {
		t.Error("expected invalid env var name error")
	}
	// The old implementation turned a literal key into an env var *named
	// after the key*, leaking it into the output. Literal keys now map to "".
	if got := translateApiKeyRef("sk-literal-key"); got != "" {
		t.Errorf("translateApiKeyRef(literal) = %q, want empty", got)
	}
}

func TestClassifyLitellmModel(t *testing.T) {
	tests := []struct {
		model, custom, apiBase string
		prefix, upstream       string
		ok                     bool
	}{
		{"bedrock/anthropic.claude-3-sonnet", "", "", "bedrock", "anthropic.claude-3-sonnet", true},
		{"claude-3-opus-20240229", "", "", "anthropic", "claude-3-opus-20240229", true},
		{"gpt-4o", "", "", "openai", "gpt-4o", true},
		{"gpt-4o", "", "https://proxy.example.com/v1", "openai", "gpt-4o", true},
		{"gemini-1.5-pro", "", "", "gemini", "gemini-1.5-pro", true},
		{"command-r", "", "", "cohere", "command-r", true},
		{"openrouter/anthropic/claude-3", "", "", "openrouter", "anthropic/claude-3", true},
		{"llama3", "ollama", "", "ollama", "llama3", true},
		{"huggingface/meta-llama/Llama-2", "", "", "huggingface", "meta-llama/Llama-2", false},
		{"myprov/some-model", "", "https://x.example.com", "openai", "myprov/some-model", true},
		{"myprov/some-model", "", "", "myprov", "some-model", false},
		{"some-model", "myprov", "https://x.example.com", "myprov", "some-model", true},
		// org/model style ids behind an OpenAI-compatible api_base keep their slash.
		{"meta-llama/Llama-3-8b-Instruct", "", "http://vllm:8000/v1", "openai", "meta-llama/Llama-3-8b-Instruct", true},
	}
	for _, tt := range tests {
		prefix, upstream, ok := classifyLitellmModel(tt.model, tt.custom, tt.apiBase)
		if prefix != tt.prefix || upstream != tt.upstream || ok != tt.ok {
			t.Errorf("classifyLitellmModel(%q, %q, %q) = (%q, %q, %v), want (%q, %q, %v)",
				tt.model, tt.custom, tt.apiBase, prefix, upstream, ok, tt.prefix, tt.upstream, tt.ok)
		}
	}
}

func TestMigrateErrors(t *testing.T) {
	if err := runLitellmMigrate("/nonexistent/path.yaml", filepath.Join(t.TempDir(), "o.yaml"), migrateOptions{}, nil, nil); err == nil {
		t.Error("expected error for nonexistent input file")
	}
	_, empty := writeFixture(t, "general_settings: {master_key: x}\n")
	if err := runLitellmMigrate(empty, filepath.Join(t.TempDir(), "o.yaml"), migrateOptions{}, nil, nil); err == nil {
		t.Error("expected error for empty model_list")
	}
	_, bad := writeFixture(t, "model_list: [\n")
	if err := runLitellmMigrate(bad, filepath.Join(t.TempDir(), "o.yaml"), migrateOptions{}, nil, nil); err == nil {
		t.Error("expected YAML parse error")
	}
	_, badEnv := writeFixture(t, "model_list:\n  - model_name: a\n    litellm_params: {model: gpt-4o, api_key: \"os.environ/$(whoami)\"}\n")
	if err := runLitellmMigrate(badEnv, filepath.Join(t.TempDir(), "o.yaml"), migrateOptions{}, nil, nil); err == nil {
		t.Error("expected invalid env reference error")
	}
}
