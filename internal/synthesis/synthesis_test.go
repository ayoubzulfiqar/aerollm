package synthesis

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
)

func TestLLMCodeGeneratorTemplate(t *testing.T) {
	g := NewLLMCodeGenerator("local-small", "http://localhost:11434")
	code, err := g.Generate(context.Background(), "Fetch weather for a city")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	if !strings.Contains(code, "Execute(ctx context.Context") {
		t.Fatalf("generated code missing Execute method: %s", code)
	}
	if !strings.Contains(code, "Fetch weather for a city") {
		t.Fatalf("generated code missing description: %s", code)
	}
}

func TestLLMCodeGeneratorRejectsAndSanitizes(t *testing.T) {
	g := NewLLMCodeGenerator("m", "u")
	if _, err := g.Generate(context.Background(), "   "); err == nil {
		t.Fatal("expected error for blank description")
	}
	if _, err := g.Generate(context.Background(), strings.Repeat("a", MaxDescriptionLen+1)); err == nil {
		t.Fatal("expected error for oversized description")
	}
	// Injection attempt: quotes/backticks must stay inside the %q literal and
	// never reach the identifier.
	code, err := g.Generate(context.Background(), "123 \"}; func init(){ os.Exit(1) } //")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if strings.Contains(code, "func init()") && !strings.Contains(code, `"123 \"}; func init(){ os.Exit(1) } //"`) {
		t.Fatalf("description escaped its string literal: %s", code)
	}
	if !strings.Contains(code, "type funcinitosExit1Tool struct") {
		t.Fatalf("identifier not sanitized to start with a letter: %s", code)
	}
	if got := sanitizeIdentifier(strings.Repeat("x", 200)); len(got) != maxIdentifierLen {
		t.Fatalf("identifier not length-capped: %d", len(got))
	}
	if got := sanitizeIdentifier("!!!"); got != "tool" {
		t.Fatalf("expected fallback identifier, got %q", got)
	}
}

func TestWasmCompilerFailsExplicitly(t *testing.T) {
	c := NewWasmCompiler()
	out, err := c.Compile(context.Background(), "package main", "weather")
	if !errors.Is(err, ErrCompilerUnavailable) || out != nil {
		t.Fatalf("expected ErrCompilerUnavailable and no bytes, got %v %q", err, out)
	}
}

type fakeRegistry struct {
	registered []plugins.Metadata
}

func (f *fakeRegistry) Register(meta plugins.Metadata) error {
	f.registered = append(f.registered, meta)
	return nil
}
func (f *fakeRegistry) Unregister(string) error                  { return nil }
func (f *fakeRegistry) Get(string) (plugins.Metadata, bool)      { return plugins.Metadata{}, false }
func (f *fakeRegistry) List() []plugins.Metadata                 { return f.registered }
func (f *fakeRegistry) SetEnabled(id string, enabled bool) error { return nil }

func TestToolPromoter(t *testing.T) {
	if err := NewToolPromoter(nil).Promote(context.Background(), plugins.Metadata{ID: "1", Name: "weather"}); !errors.Is(err, ErrNoRegistry) {
		t.Fatalf("expected ErrNoRegistry, got %v", err)
	}
	reg := &fakeRegistry{}
	p := NewToolPromoter(reg)
	if err := p.Promote(context.Background(), plugins.Metadata{ID: "1", Name: "weather", Filename: "weather.wasm", Enabled: true}); err != nil {
		t.Fatalf("promote failed: %v", err)
	}
	if len(reg.registered) != 1 || reg.registered[0].Enabled {
		t.Fatalf("expected tool registered disabled, got %+v", reg.registered)
	}
	bad := []plugins.Metadata{
		{Name: "no-id"},
		{ID: "1"},
		{ID: "../x", Name: "n"},
		{ID: "1", Name: "bad name"},
		{ID: "1", Name: "n", Filename: "../../etc/passwd"},
		{ID: "1", Name: "n", Filename: "/etc/passwd"},
		{ID: "1", Name: "n", Filename: `..\evil.dll`},
		{ID: "1", Name: "n", Filename: ".."},
		{ID: "1", Name: "n", SizeBytes: -1},
	}
	for _, m := range bad {
		if err := p.Promote(context.Background(), m); err == nil {
			t.Errorf("expected validation error for %+v", m)
		}
	}
	if len(reg.registered) != 1 {
		t.Fatalf("invalid manifests must not be registered: %+v", reg.registered)
	}
}

func TestDeficitDetector(t *testing.T) {
	d := NewDeficitDetector()
	sig, ok := d.Analyze(context.Background(), "r", "The Weather tool missing for this", nil)
	if !ok || sig.MissingTool != "weather" {
		t.Fatalf("unexpected signal: %+v %v", sig, ok)
	}
	if _, ok := d.Analyze(context.Background(), "r", strings.Repeat("x", MaxAnalyzeBytes)+" Weather tool missing", nil); ok {
		t.Fatal("text beyond MaxAnalyzeBytes must not be analyzed")
	}
	if _, ok := d.Analyze(context.Background(), "r", "all good", nil); ok {
		t.Fatal("unexpected deficit")
	}
}
