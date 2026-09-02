package synthesis

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
)

// CodeGenerator generates tool implementations from natural language descriptions.
type CodeGenerator interface {
	Generate(ctx context.Context, description string) (string, error)
}

// LLMCodeGenerator uses a local SLM to generate Go tool code.
type LLMCodeGenerator struct {
	model   string
	baseURL string
	client  interface{}
}

// NewLLMCodeGenerator creates a new code generator targeting a local SLM.
func NewLLMCodeGenerator(model, baseURL string) *LLMCodeGenerator {
	return &LLMCodeGenerator{model: model, baseURL: baseURL}
}

// Generate emits a Go tool implementation stub from a description.
func (g *LLMCodeGenerator) Generate(ctx context.Context, description string) (string, error) {
	_ = ctx
	if description == "" {
		return "", fmt.Errorf("description is empty")
	}
	sanitized := sanitizeIdentifier(description)
	code := "package main\n\nimport \"context\"\n\ntype Params struct {\n\tInput string\n}\n\ntype " + sanitized + "Tool struct{}\n\nfunc (t *" + sanitized + "Tool) Name() string { return \"" + sanitized + "\" }\nfunc (t *" + sanitized + "Tool) Description() string { return " + fmt.Sprintf("%q", description) + " }\nfunc (t *" + sanitized + "Tool) Parameters() map[string]interface{} {\n\treturn map[string]interface{}{\n\t\t\"type\": \"object\",\n\t\t\"properties\": map[string]interface{}{\n\t\t\t\"input\": map[string]interface{}{\n\t\t\t\t\"type\": \"string\",\n\t\t\t\t\"description\": \"input for " + sanitized + "\",\n\t\t\t},\n\t\t},\n\t}\n}\n\nfunc (t *" + sanitized + "Tool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {\n\t_ = ctx\n\tinput, _ := args[\"input\"].(string)\n\treturn map[string]interface{}{\"ok\": true, \"input\": input}, nil\n}\n"
	return code, nil
}

// WasmCompiler compiles generated Go code to WASM binary in memory.
type WasmCompiler struct{}

// NewWasmCompiler creates a new compiler.
func NewWasmCompiler() *WasmCompiler {
	return &WasmCompiler{}
}

// Compile converts Go source text into a WASM byte slice.
func (c *WasmCompiler) Compile(ctx context.Context, source, moduleName string) ([]byte, error) {
	_ = ctx
	_ = source
	_ = moduleName
	return []byte("wasm-stub:" + moduleName), nil
}

// ToolPromoter persists generated tools into the plugin registry.
type ToolPromoter struct {
	registry plugins.Registry
}

// NewToolPromoter creates a promoter.
func NewToolPromoter(registry plugins.Registry) *ToolPromoter {
	return &ToolPromoter{registry: registry}
}

// Promote validates the generated tool and registers its manifest.
func (p *ToolPromoter) Promote(ctx context.Context, manifest plugins.Metadata) error {
	_ = ctx
	if manifest.ID == "" || manifest.Name == "" {
		return fmt.Errorf("manifest id and name are required")
	}
	if p.registry == nil {
		return nil
	}
	return p.registry.Register(manifest)
}

// ManifestStore persists generated tool manifests.
type ManifestStore interface {
	SaveManifest(ctx context.Context, manifest ToolManifest) error
	GetManifest(ctx context.Context, id string) (ToolManifest, bool)
	ListManifests(ctx context.Context) ([]ToolManifest, error)
}

// ToolManifest is the metadata persisted for generated tools.
type ToolManifest struct {
	ID          string
	Name        string
	Description string
	Parameters  map[string]interface{}
	WasmPath    string
	CreatedAt   int64
	SuccessCount int64
	FailureCount int64
}

// InMemoryManifestStore implements ManifestStore in memory.
type InMemoryManifestStore struct {
	mu     sync.RWMutex
	items  map[string]ToolManifest
	nextID int64
}

// NewInMemoryManifestStore creates a store.
func NewInMemoryManifestStore() *InMemoryManifestStore {
	return &InMemoryManifestStore{items: make(map[string]ToolManifest)}
}

// SaveManifest stores a manifest and assigns an ID if missing.
func (s *InMemoryManifestStore) SaveManifest(ctx context.Context, manifest ToolManifest) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if manifest.ID == "" {
		s.nextID++
		manifest.ID = fmt.Sprintf("tool_%d", s.nextID)
	}
	if manifest.CreatedAt == 0 {
		manifest.CreatedAt = time.Now().UnixNano()
	}
	s.items[manifest.ID] = manifest
	return nil
}

// GetManifest retrieves a manifest.
func (s *InMemoryManifestStore) GetManifest(ctx context.Context, id string) (ToolManifest, bool) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.items[id]
	return m, ok
}

// ListManifests returns all manifests.
func (s *InMemoryManifestStore) ListManifests(ctx context.Context) ([]ToolManifest, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ToolManifest, 0, len(s.items))
	for _, m := range s.items {
		out = append(out, m)
	}
	return out, nil
}

func sanitizeIdentifier(s string) string {
	s = regexp.MustCompile(`[^a-zA-Z0-9_]+`).ReplaceAllString(s, "")
	if s == "" {
		return "tool"
	}
	return s
}
