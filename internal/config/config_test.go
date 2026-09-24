package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfigDefaultsWithoutFile(t *testing.T) {
	cfg, err := LoadConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 8080 || cfg.Redis.Addr != "localhost:6379" || cfg.Router.Strategy != "round_robin" {
		t.Fatalf("defaults not applied: port=%d redis=%q strategy=%q", cfg.Server.Port, cfg.Redis.Addr, cfg.Router.Strategy)
	}
	if cfg.Server.WriteTimeout != 120*time.Second || cfg.Router.CircuitBreak.MaxFailures != 5 {
		t.Fatalf("nested defaults not applied: %+v", cfg.Server)
	}
	if cfg.Server.MaxBodyBytes != 10<<20 {
		t.Fatalf("max body default: %d", cfg.Server.MaxBodyBytes)
	}
}

func TestLoadConfigEnvOverridesNestedKeys(t *testing.T) {
	t.Setenv("AEROLLM_REDIS_ADDR", "redis:6379")
	t.Setenv("AEROLLM_SERVER_PORT", "9090")
	t.Setenv("AEROLLM_AUTH_MASTER_KEY", "mk-123")
	dir := t.TempDir()
	writeFile(t, dir, "config.yaml", "redis:\n  addr: file:6379\n")
	cfg, err := LoadConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "redis:6379" || cfg.Server.Port != 9090 || cfg.Auth.MasterKey != "mk-123" {
		t.Fatalf("env overrides not applied: redis=%q port=%d master=%q", cfg.Redis.Addr, cfg.Server.Port, cfg.Auth.MasterKey)
	}
}

func TestLoadConfigExpandsEnvReferences(t *testing.T) {
	t.Setenv("TEST_OPENAI_KEY", "sk-live")
	dir := t.TempDir()
	writeFile(t, dir, "config.yaml", `
providers:
  - name: openai
    type: openai
    api_key: "${TEST_OPENAI_KEY}"
    models: ["gpt-4o"]
  - name: groq
    type: groq
    api_key: "${TEST_MISSING_KEY}"
  - name: other
    type: openai-compatible
    api_key: "${TEST_MISSING_KEY:-fallback}"
    base_url: "https://x/$notexpanded"
`)
	cfg, err := LoadConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Providers[0].APIKey; got != "sk-live" {
		t.Fatalf("expected expanded key, got %q", got)
	}
	if got := cfg.Providers[1].APIKey; got != "" {
		t.Fatalf("missing env should expand to empty, got %q", got)
	}
	if got := cfg.Providers[2].APIKey; got != "fallback" {
		t.Fatalf("default should apply, got %q", got)
	}
	if got := cfg.Providers[2].BaseURL; got != "https://x/$notexpanded" {
		t.Fatalf("bare $ must be preserved, got %q", got)
	}
}

func TestLoadConfigExplicitFileAndErrors(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "custom.yaml", "server:\n  port: 7000\n")
	cfg, err := LoadConfig(p)
	if err != nil || cfg.Server.Port != 7000 {
		t.Fatalf("explicit file: cfg=%v err=%v", cfg, err)
	}
	if _, err := LoadConfig(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("explicitly named missing file must error")
	}
	bad := writeFile(t, t.TempDir(), "config.yaml", "server: [unclosed\n")
	if _, err := LoadConfig(filepath.Dir(bad)); err == nil {
		t.Fatal("malformed YAML must error")
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	cases := map[string]func(*Config){
		"port":      func(c *Config) { c.Server.Port = 70000 },
		"threshold": func(c *Config) { c.Cache.SemanticThreshold = 1.5 },
		"strategy":  func(c *Config) { c.Router.Strategy = "random" },
		"type":      func(c *Config) { c.Providers = []ProviderConfig{{Type: "nope"}} },
		"dupe":      func(c *Config) { c.Providers = []ProviderConfig{{Type: "openai"}, {Type: "openai"}} },
		"rps":       func(c *Config) { c.RateLimit.DefaultRPS = -1 },
	}
	for name, mutate := range cases {
		cfg, _ := LoadConfig(t.TempDir())
		mutate(cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestMaskSecretsDoesNotMutateLiveConfig(t *testing.T) {
	cfg := &Config{
		Providers: []ProviderConfig{{Name: "openai", Type: "openai", APIKey: "sk-secret"}},
		Redis:     RedisConfig{Password: "pw"},
		Auth:      AuthConfig{MasterKey: "mk", AdminKeys: []string{"ak"}},
	}
	r := NewConfigReloader(cfg)
	masked := r.MaskSecrets()
	if masked.Providers[0].APIKey != RedactedValue || masked.Redis.Password != RedactedValue || masked.Auth.MasterKey != RedactedValue || masked.Auth.AdminKeys[0] != RedactedValue {
		t.Fatalf("secrets not masked: %+v", masked)
	}
	if r.Current().Config.Providers[0].APIKey != "sk-secret" || cfg.Redis.Password != "pw" {
		t.Fatal("masking must not modify the live config")
	}
}

func TestApplyUpdateMergesAndRestoresSecrets(t *testing.T) {
	cfg, _ := LoadConfig(t.TempDir())
	cfg.Providers = []ProviderConfig{
		{Name: "openai", Type: "openai", APIKey: "sk-a", Models: []string{"gpt-4o"}},
		{Name: "groq", Type: "groq", APIKey: "gsk-b"},
	}
	r := NewConfigReloader(cfg)
	var reloaded *Config
	r.SetReloadCallback(func(_ context.Context, c *Config) error { reloaded = c; return nil })

	patch := `{"router":{"strategy":"latency"},"providers":[{"name":"openai","type":"openai","api_key":"***REDACTED***","models":["gpt-4o","gpt-4o-mini"]}]}`
	next, err := r.ApplyUpdate(context.Background(), []byte(patch))
	if err != nil {
		t.Fatal(err)
	}
	if reloaded != next {
		t.Fatal("reload callback should receive the merged config")
	}
	if next.Router.Strategy != "latency" || next.Server.Port != 8080 {
		t.Fatalf("merge lost fields: strategy=%q port=%d", next.Router.Strategy, next.Server.Port)
	}
	if len(next.Providers) != 1 || next.Providers[0].APIKey != "sk-a" || len(next.Providers[0].Models) != 2 {
		t.Fatalf("providers not replaced / secret not restored: %+v", next.Providers)
	}
	if len(r.GetModels()) != 2 {
		t.Fatalf("model info not rebuilt: %+v", r.GetModels())
	}
}

func TestApplyUpdateRejectsInvalidAndKeepsCurrent(t *testing.T) {
	cfg, _ := LoadConfig(t.TempDir())
	r := NewConfigReloader(cfg)
	if _, err := r.ApplyUpdate(context.Background(), []byte(`{"router":{"strategy":"bogus"}}`)); err == nil {
		t.Fatal("invalid strategy must be rejected")
	}
	if _, err := r.ApplyUpdate(context.Background(), []byte(`{"unknown_section":1}`)); err == nil {
		t.Fatal("unknown fields must be rejected")
	}
	r.SetReloadCallback(func(context.Context, *Config) error { return errors.New("boom") })
	if _, err := r.ApplyUpdate(context.Background(), []byte(`{"router":{"strategy":"cost"}}`)); err == nil {
		t.Fatal("callback error must propagate")
	}
	if r.Current().Config.Router.Strategy != "round_robin" {
		t.Fatal("failed reload must keep the current config")
	}
}

func TestDefaultContextWindow(t *testing.T) {
	cases := map[string]int{
		"gpt-4o-mini":                128000,
		"gpt-4.1":                    1047576,
		"gpt-4":                      8192,
		"claude-3-5-sonnet-20241022": 200000,
		"gemini-1.5-pro":             2097152,
		"unknown":                    4096,
	}
	for model, want := range cases {
		if got := DefaultContextWindow(model); got != want {
			t.Errorf("%s: got %d want %d", model, got, want)
		}
	}
}

func TestRepoConfigLoads(t *testing.T) {
	cfg, err := LoadConfig("../../config.yaml")
	if err != nil {
		t.Fatalf("repository config.yaml must load: %v", err)
	}
	if strings.Contains(cfg.Callbacks.Langfuse.SecretKey, "${") || strings.Contains(cfg.Callbacks.Datadog.APIKey, "${") {
		t.Fatalf("placeholders were not expanded: %+v", cfg.Callbacks)
	}
}

func TestProviderUsable(t *testing.T) {
	cases := []struct {
		p    ProviderConfig
		want bool
	}{
		{ProviderConfig{Type: "openai", APIKey: "sk"}, true},
		{ProviderConfig{Type: "openai"}, false},
		{ProviderConfig{Type: "bedrock"}, true},
		{ProviderConfig{Type: "ollama", BaseURL: "http://localhost:11434/v1"}, true},
		{ProviderConfig{Type: "openai-compatible", BaseURL: "http://vllm:8000/v1"}, true},
		{ProviderConfig{Type: "openai-compatible", BaseURL: "https://api.openai.com/v1"}, false},
	}
	for _, c := range cases {
		if got := c.p.Usable(); got != c.want {
			t.Errorf("%+v: got %v want %v", c.p, got, c.want)
		}
	}
	r := NewConfigReloader(&Config{Providers: []ProviderConfig{
		{Name: "a", Type: "openai", APIKey: "k", Models: []string{"m1"}},
		{Name: "b", Type: "groq", Models: []string{"m2"}},
	}})
	if got := r.GetModels(); len(got) != 1 || got[0].Model != "m1" {
		t.Fatalf("models from keyless providers must not be listed: %+v", got)
	}
}
