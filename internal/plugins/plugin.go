package plugins

import "context"

// Hook points for plugin lifecycle.
type Hook string

const (
	HookOnRequest    Hook = "OnRequest"
	HookOnResponse   Hook = "OnResponse"
	HookOnToolCall   Hook = "OnToolCall"
	HookOnToolResult Hook = "OnToolResult"
)

// Plugin is the contract for a WASM-compatible plugin.
type Plugin interface {
	ID() string
	Name() string
	Enabled() bool
	Invoke(ctx context.Context, hook Hook, payload map[string]interface{}) (map[string]interface{}, error)
}

// Metadata stores plugin metadata.
type Metadata struct {
	ID          string
	Name        string
	Version     string
	Enabled     bool
	Filename    string
	SizeBytes   int64
	CreatedAt   int64
	UpdatedAt   int64
}

// Registry manages plugin metadata and enabled state.
type Registry interface {
	Register(meta Metadata) error
	Unregister(id string) error
	Get(id string) (Metadata, bool)
	List() []Metadata
	SetEnabled(id string, enabled bool) error
}

// Host executes plugin hooks for a request lifecycle.
type Host interface {
	RunHook(ctx context.Context, hook Hook, payload map[string]interface{}) (map[string]interface{}, error)
}
