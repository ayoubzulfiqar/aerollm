package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt/wasmrttest"
)

// toolCallingProvider asks for the "shout" tool once, then answers with the
// tool's output.
type toolCallingProvider struct{}

func (toolCallingProvider) Name() string                 { return "tool-caller" }
func (toolCallingProvider) Type() providers.ProviderType { return providers.ProviderOpenAI }
func (toolCallingProvider) Health() providers.ProviderHealth {
	return providers.ProviderHealth{Name: "tool-caller", Healthy: true}
}
func (toolCallingProvider) Close() error { return nil }
func (toolCallingProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	last := req.Messages[len(req.Messages)-1]
	if last.Role == models.RoleTool {
		text := "tool said: " + last.TextContent()
		return &models.LLMResponse{ID: "r2", Model: req.Model, Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: &text}, FinishReason: "stop"}}}, nil
	}
	return &models.LLMResponse{ID: "r1", Model: req.Model, Choices: []models.Choice{{
		Message: models.Message{Role: models.RoleAssistant, ToolCalls: []models.ToolCall{{
			ID: "call_1", Type: "function",
			Function: models.ToolFunction{Name: "shout", Arguments: `{"mode":"upper","text":"hello wasm"}`},
		}}},
		FinishReason: "tool_calls",
	}}}, nil
}

func TestWasmToolRunsInAgentLoop(t *testing.T) {
	wasm := wasmrttest.Guest(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "shout.wasm"), wasm, 0o600); err != nil {
		t.Fatal(err)
	}
	a, _ := newTestApp(t, func(c *config.Config) {
		c.Plugins.Dir = dir
		c.Plugins.Tools = []config.PluginToolConfig{{Name: "shout", Description: "upper-cases text", File: "shout.wasm", Parameters: map[string]interface{}{"type": "object"}}}
	})
	a.router.RegisterProvider(toolCallingProvider{})
	w := do(a, "POST", "/v1/chat/completions", testAdminKey,
		`{"model":"gpt-4o","messages":[{"role":"user","content":"shout it"}],"tools":[{"type":"function","function":{"name":"shout"}}]}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "HELLO WASM") {
		t.Fatalf("wasm tool did not run through the agent loop: %d %s", w.Code, w.Body.String())
	}
}

func TestWasmToolConfigRejectsEscapingPaths(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.LoadConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Auth.MasterKey = testAdminKey
	cfg.Plugins.Dir = dir
	cfg.Plugins.Tools = []config.PluginToolConfig{{Name: "evil", File: "../outside.wasm"}}
	t.Setenv("AEROLLM_STATE_DIR", t.TempDir())
	if _, err := newApp(context.Background(), cfg, newLogger(os.Stderr, "error", "json"), appOptions{skipRedis: true}); err == nil {
		t.Fatal("a plugin file outside plugins.dir must be rejected")
	}
}

func TestWasmRequestHooks(t *testing.T) {
	wasm := wasmrttest.Guest(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "guard.wasm"), wasm, 0o600); err != nil {
		t.Fatal(err)
	}
	a, _ := newTestApp(t, func(c *config.Config) {
		c.Plugins.Dir = dir
		c.Plugins.Hooks = []config.PluginHookConfig{{ID: "guard", File: "guard.wasm"}}
	})
	a.router.RegisterProvider(echoProvider{})
	// The test guest leaves the payload unchanged unless told otherwise, so a
	// normal request passes through the hook to the provider.
	w := do(a, "POST", "/v1/chat/completions", testAdminKey, `{"model":"gpt-4o","messages":[{"role":"user","content":"through the hook"}]}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "echo: through the hook") {
		t.Fatalf("hooked request: %d %s", w.Code, w.Body.String())
	}
}
