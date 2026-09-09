package universal

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestRegistryRegisterAndGet(t *testing.T) {
	reg := NewProviderRegistry()
	a := NewOpenAICompatibleAdapter("test", "openai-compatible", "key", "https://api.test.com/v1")
	if err := reg.Register(a, "gpt-4o"); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	got, ok := reg.Get("test")
	if !ok || got.Name() != "test" {
		t.Fatalf("expected adapter 'test', got %v %v", got, ok)
	}
	if len(reg.All()) != 1 {
		t.Fatalf("expected 1 adapter, got %d", len(reg.All()))
	}
}

func TestRegistryResolveProviderByModel(t *testing.T) {
	reg := NewProviderRegistry()
	a := NewOpenAICompatibleAdapter("openai", "openai-compatible", "key", "https://api.openai.com/v1")
	if err := reg.Register(a, "gpt-4o", "gpt-4o-mini"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	p, err := reg.ResolveProviderByModel("gpt-4o")
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if p.Name() != "openai" {
		t.Fatalf("expected provider 'openai', got %q", p.Name())
	}

	if _, err := reg.ResolveProviderByModel("nonexistent-model"); err == nil {
		t.Fatal("expected error for unknown model")
	}
}

func TestRegistryRegisterFromConfig(t *testing.T) {
	reg := NewProviderRegistry()
	cfgs := []config.ProviderConfig{
		{Name: "openai", Type: "openai-compatible", APIKey: "key1", BaseURL: "https://api.openai.com/v1", Models: []string{"gpt-4o"}},
		{Name: "groq", Type: "groq", APIKey: "key2", BaseURL: "https://api.groq.com/openai/v1", Models: []string{"llama-3-70b"}},
		{Name: "anthropic", Type: "anthropic", APIKey: "key3", BaseURL: "https://api.anthropic.com", Models: []string{"claude-3"}},
		{Name: "gemini", Type: "gemini", APIKey: "key4", BaseURL: "https://generativelanguage.googleapis.com", Models: []string{"gemini-1.5-pro"}},
		{Name: "bedrock", Type: "bedrock", BaseURL: "https://bedrock.us-east-1.amazonaws.com", Models: []string{"claude-bedrock"}},
	}

	if err := reg.RegisterFromConfig(cfgs); err != nil {
		t.Fatalf("RegisterFromConfig failed: %v", err)
	}

	if len(reg.All()) != 5 {
		t.Fatalf("expected 5 providers, got %d", len(reg.All()))
	}

	p, err := reg.ResolveProviderByModel("gpt-4o")
	if err != nil {
		t.Fatalf("resolve gpt-4o failed: %v", err)
	}
	if p.Name() != "openai" {
		t.Fatalf("expected 'openai', got %q", p.Name())
	}

	p, err = reg.ResolveProviderByModel("llama-3-70b")
	if err != nil {
		t.Fatalf("resolve llama-3-70b failed: %v", err)
	}
	if p.Name() != "groq" {
		t.Fatalf("expected 'groq', got %q", p.Name())
	}
}

func TestRegistryRegisterFromConfigUnsupported(t *testing.T) {
	reg := NewProviderRegistry()
	cfgs := []config.ProviderConfig{
		{Name: "unknown", Type: "unsupported-type", APIKey: "key"},
	}
	if err := reg.RegisterFromConfig(cfgs); err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestOpenAICompatibleAdapterMethods(t *testing.T) {
	a := NewOpenAICompatibleAdapter("test", "openai-compatible", "key", "https://api.test.com/v1")
	if a.Name() != "test" {
		t.Fatalf("expected name 'test', got %q", a.Name())
	}
	if a.Type() != "openai-compatible" {
		t.Fatalf("expected type 'openai-compatible', got %q", a.Type())
	}
	if a.ProviderType() != "openai-compatible" {
		t.Fatalf("expected ProviderType 'openai-compatible', got %q", a.ProviderType())
	}
	h := a.Health()
	if !h["healthy"].(bool) {
		t.Fatal("expected healthy=true")
	}
}

func TestNewAdapters(t *testing.T) {
	adapters := []struct {
		name string
		fn   func() *OpenAICompatibleAdapter
	}{
		{"gemini", func() *OpenAICompatibleAdapter { return NewGeminiAdapter("key", "https://generativelanguage.googleapis.com") }},
		{"bedrock", func() *OpenAICompatibleAdapter { return NewBedrockAdapter("key", "https://bedrock.us-east-1.amazonaws.com") }},
		{"anthropic", func() *OpenAICompatibleAdapter { return NewAnthropicAdapter("key", "https://api.anthropic.com") }},
		{"groq", func() *OpenAICompatibleAdapter { return NewGroqAdapter("key", "https://api.groq.com/openai/v1") }},
		{"cohere", func() *OpenAICompatibleAdapter { return NewCohereAdapter("key", "https://api.cohere.com") }},
		{"deepseek", func() *OpenAICompatibleAdapter { return NewDeepSeekAdapter("key", "https://api.deepseek.com") }},
		{"azure", func() *OpenAICompatibleAdapter { return NewAzureOpenAIAdapter("key", "https://azure.openai.com", "myresource") }},
	}
	for _, tc := range adapters {
		a := tc.fn()
		if a == nil {
			t.Fatalf("adapter %q is nil", tc.name)
		}
		if a.Name() == "" {
			t.Fatalf("adapter %q has empty name", tc.name)
		}
	}
}

func TestEmbeddingRequestDefaults(t *testing.T) {
	req := &models.EmbeddingRequest{Model: "text-embedding-ada-002", Input: "hello"}
	if req.Model != "text-embedding-ada-002" {
		t.Fatalf("unexpected model: %s", req.Model)
	}
}

func TestImageRequestDefaults(t *testing.T) {
	req := &models.ImageRequest{Model: "dall-e-3", Prompt: "a cat", N: 1, Size: "1024x1024"}
	if req.Prompt != "a cat" {
		t.Fatalf("unexpected prompt: %s", req.Prompt)
	}
}

func TestAudioRequestDefaults(t *testing.T) {
	req := &models.AudioRequest{Model: "whisper-1", File: "audio.wav", Prompt: "transcribe this"}
	if req.File != "audio.wav" {
		t.Fatalf("unexpected file: %s", req.File)
	}
}

func TestResponsesRequestDefaults(t *testing.T) {
	prev := "prev-123"
	req := &models.ResponsesRequest{Model: "gpt-4o", Input: "hello", Previous: &prev}
	if req.Model != "gpt-4o" {
		t.Fatalf("unexpected model: %s", req.Model)
	}
	if req.Previous == nil || *req.Previous != "prev-123" {
		t.Fatal("expected previous response id")
	}
}

func TestStreamNormalizer(t *testing.T) {
	n := NewStreamNormalizer()
	chunk, err := n.Normalize("test", []byte(`{"choices":[{"delta":{"content":"hello"}}]}`))
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	if chunk.Delta != "hello" {
		t.Fatalf("expected delta 'hello', got %q", chunk.Delta)
	}
	if chunk.Provider != "test" {
		t.Fatalf("expected provider 'test', got %q", chunk.Provider)
	}
}

func TestChatCompletionRouting(t *testing.T) {
	reg := NewProviderRegistry()
	a := NewOpenAICompatibleAdapter("test", "openai-compatible", "key", "https://api.test.com/v1")
	if err := reg.Register(a, "gpt-4o"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	_, err := reg.ChatCompletion(context.Background(), "nonexistent", &models.LLMRequest{
		Model:    "gpt-4o",
		Messages: []models.Message{{Role: models.RoleUser, Content: strPtr("hi")}},
	})
	if err == nil {
		t.Fatal("expected error for non-registered adapter")
	}
}