package synthesis

import (
	"context"
	"strings"
	"testing"
)

func TestDeficitDetectorHeuristic(t *testing.T) {
	d := NewDeficitDetector()
	signal, ok := d.Analyze(context.Background(), "req-1", "I cannot do that without a search", nil)
	if !ok || signal.MissingTool == "" {
		t.Fatalf("expected deficit signal, got %v %v", ok, signal)
	}
	if !strings.Contains(signal.MissingTool, "search") {
		t.Fatalf("expected search tool, got %s", signal.MissingTool)
	}
}

func TestLLMCodeGeneratorStub(t *testing.T) {
	g := NewLLMCodeGenerator("local-small", "http://localhost:11434")
	code, err := g.Generate(context.Background(), "Fetch weather for a city")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	if !strings.Contains(code, "Execute(ctx context.Context") {
		t.Fatalf("generated code missing Execute method: %s", code)
	}
}

func TestManifestStoreLifecycle(t *testing.T) {
	store := NewInMemoryManifestStore()
	if err := store.SaveManifest(context.Background(), ToolManifest{Name: "weather"}); err != nil {
		t.Fatalf("save manifest failed: %v", err)
	}
	all, err := store.ListManifests(context.Background())
	if err != nil || len(all) != 1 {
		t.Fatalf("list manifests failed: %v %d", err, len(all))
	}
	m, ok := store.GetManifest(context.Background(), all[0].ID)
	if !ok || m.Name != "weather" {
		t.Fatalf("get manifest failed: %v %v", ok, m)
	}
}
