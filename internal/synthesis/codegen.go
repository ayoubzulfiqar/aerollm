package synthesis

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
)

// CodeGenerator generates tool implementations from natural language descriptions.
type CodeGenerator interface {
	Generate(ctx context.Context, description string) (string, error)
}

// LLMCodeGenerator generates Go tool scaffolding from a description.
//
// Despite its name it does NOT call a model: it renders a fixed, safe
// template (the model/baseURL fields are reserved for a future SLM backend).
// The generated source is returned as text only; nothing in this package
// compiles, executes or writes it to disk.
type LLMCodeGenerator struct {
	model   string
	baseURL string
	client  interface{}
}

// NewLLMCodeGenerator creates a new code generator targeting a local SLM.
func NewLLMCodeGenerator(model, baseURL string) *LLMCodeGenerator {
	return &LLMCodeGenerator{model: model, baseURL: baseURL}
}

// MaxDescriptionLen caps tool descriptions accepted by Generate.
const MaxDescriptionLen = 2048

// maxIdentifierLen caps the generated Go identifier / tool name length.
const maxIdentifierLen = 48

// Generate emits a Go tool implementation from a description. The description
// is embedded only as a quoted Go string literal (%q) and the identifier is
// restricted to [A-Za-z0-9_] starting with a letter, so the description
// cannot inject code into the template.
func (g *LLMCodeGenerator) Generate(ctx context.Context, description string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if strings.TrimSpace(description) == "" {
		return "", fmt.Errorf("description is empty")
	}
	if len(description) > MaxDescriptionLen {
		return "", fmt.Errorf("description exceeds %d bytes", MaxDescriptionLen)
	}
	if !utf8.ValidString(description) {
		return "", fmt.Errorf("description is not valid UTF-8")
	}
	sanitized := sanitizeIdentifier(description)
	code := "package main\n\nimport \"context\"\n\ntype Params struct {\n\tInput string\n}\n\ntype " + sanitized + "Tool struct{}\n\nfunc (t *" + sanitized + "Tool) Name() string { return \"" + sanitized + "\" }\nfunc (t *" + sanitized + "Tool) Description() string { return " + fmt.Sprintf("%q", description) + " }\nfunc (t *" + sanitized + "Tool) Parameters() map[string]interface{} {\n\treturn map[string]interface{}{\n\t\t\"type\": \"object\",\n\t\t\"properties\": map[string]interface{}{\n\t\t\t\"input\": map[string]interface{}{\n\t\t\t\t\"type\": \"string\",\n\t\t\t\t\"description\": \"input for " + sanitized + "\",\n\t\t\t},\n\t\t},\n\t}\n}\n\nfunc (t *" + sanitized + "Tool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {\n\t_ = ctx\n\tinput, _ := args[\"input\"].(string)\n\treturn map[string]interface{}{\"ok\": true, \"input\": input}, nil\n}\n"
	return code, nil
}

// ErrCompilerUnavailable is returned by WasmCompiler.Compile: this build has
// no sandboxed Go->WASM toolchain, so generated code is never compiled.
var ErrCompilerUnavailable = errors.New("synthesis: wasm compilation is not available in this build")

// WasmCompiler would compile generated Go code to a WASM module. No compiler
// backend is wired in, so Compile always fails explicitly rather than
// returning fake bytes that could be mistaken for a real module.
type WasmCompiler struct{}

// NewWasmCompiler creates a new compiler.
func NewWasmCompiler() *WasmCompiler {
	return &WasmCompiler{}
}

// Compile always returns ErrCompilerUnavailable.
func (c *WasmCompiler) Compile(ctx context.Context, source, moduleName string) ([]byte, error) {
	_ = source
	_ = moduleName
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrCompilerUnavailable
}

// ErrNoRegistry is returned by ToolPromoter.Promote when no plugin registry
// is configured.
var ErrNoRegistry = errors.New("synthesis: no plugin registry configured")

var manifestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
var manifestNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,127}$`)

// ToolPromoter persists generated tools into the plugin registry.
type ToolPromoter struct {
	registry plugins.Registry
}

// NewToolPromoter creates a promoter.
func NewToolPromoter(registry plugins.Registry) *ToolPromoter {
	return &ToolPromoter{registry: registry}
}

// ValidateManifest checks a generated tool manifest: ID and Name must use a
// restricted charset, and Filename (if set) must be a bare file name with no
// path components, so a manifest can never point outside the plugin dir.
func ValidateManifest(manifest plugins.Metadata) error {
	if manifest.ID == "" || manifest.Name == "" {
		return fmt.Errorf("manifest id and name are required")
	}
	if !manifestIDPattern.MatchString(manifest.ID) {
		return fmt.Errorf("manifest id must match %s", manifestIDPattern.String())
	}
	if !manifestNamePattern.MatchString(manifest.Name) {
		return fmt.Errorf("manifest name must match %s", manifestNamePattern.String())
	}
	if f := manifest.Filename; f != "" {
		if f == "." || f == ".." || strings.ContainsAny(f, `/\`) || strings.Contains(f, "..") ||
			filepath.IsAbs(f) || filepath.Base(f) != f || strings.ContainsRune(f, 0) || len(f) > 255 {
			return fmt.Errorf("manifest filename must be a bare file name")
		}
	}
	if manifest.SizeBytes < 0 {
		return fmt.Errorf("manifest size must not be negative")
	}
	return nil
}

// Promote validates the generated tool and registers its manifest. Generated
// tools are always registered disabled (Enabled=false): enabling one requires
// an explicit, separate operator action.
func (p *ToolPromoter) Promote(ctx context.Context, manifest plugins.Metadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateManifest(manifest); err != nil {
		return err
	}
	if p == nil || p.registry == nil {
		return ErrNoRegistry
	}
	manifest.Enabled = false
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
	ID           string
	Name         string
	Description  string
	Parameters   map[string]interface{}
	WasmPath     string
	CreatedAt    int64
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

var nonIdentChars = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

// sanitizeIdentifier converts free text into a Go identifier: only
// [A-Za-z0-9_], starting with a letter, at most maxIdentifierLen bytes.
func sanitizeIdentifier(s string) string {
	s = nonIdentChars.ReplaceAllString(s, "")
	s = strings.TrimLeft(s, "0123456789_")
	if len(s) > maxIdentifierLen {
		s = s[:maxIdentifierLen]
	}
	if s == "" {
		return "tool"
	}
	return s
}
