package synthesis

import (
	"context"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
)

func TestLLMCodeGeneratorStub(t *testing.T) {
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

func TestWasmCompilerStub(t *testing.T) {
	c := NewWasmCompiler()
	out, err := c.Compile(context.Background(), "package main", "weather")
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if !strings.Contains(string(out), "placeholder:weather") {
		t.Fatalf("unexpected wasm output: %s", out)
	}
}

func TestToolPromoterStub(t *testing.T) {
	p := NewToolPromoter(nil)
	if err := p.Promote(context.Background(), plugins.Metadata{ID: "1", Name: "weather"}); err != nil {
		t.Fatalf("promote failed: %v", err)
	}
	if err := p.Promote(context.Background(), plugins.Metadata{Name: "no-id"}); err == nil {
		t.Fatalf("expected validation error")
	}
}
