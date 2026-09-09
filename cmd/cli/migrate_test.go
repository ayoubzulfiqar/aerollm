package main

import (
	"os"
	"path/filepath"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestRunLitellmMigrate_Success(t *testing.T) {
	// Create a temporary LiteLLM config
	tmpDir := t.TempDir()
	inputPath := filepath.Join(tmpDir, "litellm_config.yaml")
	outputPath := filepath.Join(tmpDir, "config.yaml")

	litellmYaml := `
model_list:
  - model_name: gpt-4o
    litellm_params:
      model: gpt-4o
      api_key: os.environ/OPENAI_API_KEY
      api_base: https://api.openai.com/v1
  - model_name: claude-3-opus@20240229
    litellm_params:
      model: claude-3-opus@20240229
      api_key: os.environ/ANTHROPIC_API_KEY
  - model_name: bedrock/anthropic.claude-3-sonnet-20240229-bde75ac8
    litellm_params:
      model: bedrock/anthropic.claude-3-sonnet-20240229-bde75ac8

general_settings:
  master_key: sk-1234

router_settings:
  routing_strategy: cost-optimized
`

	if err := os.WriteFile(inputPath, []byte(litellmYaml), 0644); err != nil {
		t.Fatalf("writing test config: %v", err)
	}

	if err := runLitellmMigrate(inputPath, outputPath); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}

	// Read and parse the output
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}

	var cfg aerollmConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("parsing output config: %v", err)
	}

	// Verify app metadata
	if cfg.App.Name != "AeroLLM" {
		t.Errorf("expected app name AeroLLM, got %s", cfg.App.Name)
	}

	// Verify router strategy was translated
	if cfg.Router.Strategy != "cost" {
		t.Errorf("expected router strategy 'cost', got %s", cfg.Router.Strategy)
	}

	// Verify providers: openai, anthropic, bedrock (gpt-4o merges with openai)
	if len(cfg.Providers) != 3 {
		t.Fatalf("expected 3 providers, got %d", len(cfg.Providers))
	}

	// Find openai provider (classified as custom because api_base is set)
	var openai, anthropic, bedrock *aerollmProviderConfig
	for i := range cfg.Providers {
		switch cfg.Providers[i].Type {
		case "openai-compatible":
			if cfg.Providers[i].Name == "custom" {
				openai = &cfg.Providers[i]
			}
		case "anthropic":
			anthropic = &cfg.Providers[i]
		case "bedrock":
			bedrock = &cfg.Providers[i]
		}
	}

	if openai == nil {
		t.Error("expected openai-compatible provider")
	} else {
		if openai.BaseURL != "https://api.openai.com/v1" {
			t.Errorf("expected base_url https://api.openai.com/v1, got %s", openai.BaseURL)
		}
		if openai.APIKey != "${OPENAI_API_KEY}" {
			t.Errorf("expected api_key ${OPENAI_API_KEY}, got %s", openai.APIKey)
		}
		found := false
		for _, m := range openai.Models {
			if m == "gpt-4o" {
				found = true
			}
		}
		if !found {
			t.Error("expected model gpt-4o in openai provider")
		}
	}

	if anthropic == nil {
		t.Error("expected anthropic provider")
	} else {
		found := false
		for _, m := range anthropic.Models {
			if m == "claude-3-opus@20240229" {
				found = true
			}
		}
		if !found {
			t.Error("expected model claude-3-opus@20240229 in anthropic provider")
		}
	}

	if bedrock == nil {
		t.Error("expected bedrock provider")
	} else {
		found := false
		for _, m := range bedrock.Models {
			if m == "anthropic.claude-3-sonnet-20240229-bde75ac8" {
				found = true
			}
		}
		if !found {
			t.Error("expected model in bedrock provider")
		}
	}
}

func TestRunLitellmMigrate_FileNotFound(t *testing.T) {
	err := runLitellmMigrate("/nonexistent/path.yaml", "/tmp/output.yaml")
	if err == nil {
		t.Error("expected error for nonexistent input file")
	}
}

func TestMapLitellmStrategy(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple-rotation", "round_robin"},
		{"cost", "cost"},
		{"lowest-latency", "latency"},
		{"least-busy", "least-busy"},
		{"usage-based", "usage-based"},
		{"sequential", "fallback"},
		{"unknown-strategy", "round_robin"},
	}

	for _, tt := range tests {
		got := mapLitellmStrategy(tt.input)
		if got != tt.expected {
			t.Errorf("mapLitellmStrategy(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestTranslateApiKeyRef(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"os.environ/OPENAI_API_KEY", "${OPENAI_API_KEY}"},
		{"${ANTHROPIC_API_KEY}", "${ANTHROPIC_API_KEY}"},
		{"sk-literal-key", "${SK_LITERAL_KEY}"},
		{"", ""},
	}

	for _, tt := range tests {
		got := translateApiKeyRef(tt.input)
		if got != tt.expected {
			t.Errorf("translateApiKeyRef(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestClassifyLitellmModel(t *testing.T) {
	tests := []struct {
		llmModel string
		apiBase  string
		ptype  string
		pname  string
		model  string
	}{
		{"bedrock/anthropic.claude-3-sonnet", "", "bedrock", "bedrock", "anthropic.claude-3-sonnet"},
		{"claude-3-opus@20240229", "", "anthropic", "anthropic", "claude-3-opus@20240229"},
		{"gpt-4o", "", "openai-compatible", "openai", "gpt-4o"},
		{"gpt-4o", "https://custom.openai.com/v1", "openai-compatible", "custom", "gpt-4o"},
		{"gemini-1.5-pro", "", "gemini", "gemini", "gemini-1.5-pro"},
		{"llama-3-70b", "", "openai-compatible", "groq", "llama-3-70b"},
		{"command-r", "", "openai-compatible", "cohere", "command-r"},
		{"some-unknown-model", "", "openai-compatible", "openai", "some-unknown-model"},
	}

	for _, tt := range tests {
		ptype, pname, model := classifyLitellmModel(tt.llmModel, tt.apiBase)
		if ptype != tt.ptype || pname != tt.pname || model != tt.model {
			t.Errorf("classifyLitellmModel(%q, %q) = (%q, %q, %q), want (%q, %q, %q)",
				tt.llmModel, tt.apiBase, ptype, pname, model, tt.ptype, tt.pname, tt.model)
		}
	}
}
