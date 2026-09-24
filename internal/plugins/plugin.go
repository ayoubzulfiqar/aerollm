package plugins

import (
	"context"
	"errors"
	"fmt"
	"regexp"
)

// Hook points for plugin lifecycle.
type Hook string

const (
	HookOnRequest    Hook = "OnRequest"
	HookOnResponse   Hook = "OnResponse"
	HookOnToolCall   Hook = "OnToolCall"
	HookOnToolResult Hook = "OnToolResult"
)

// Valid reports whether h is one of the defined hook points.
func (h Hook) Valid() bool {
	switch h {
	case HookOnRequest, HookOnResponse, HookOnToolCall, HookOnToolResult:
		return true
	}
	return false
}

var (
	// ErrPluginNotFound reports an unknown plugin ID.
	ErrPluginNotFound = errors.New("plugins: plugin not found")
	// ErrPluginExists reports a duplicate registration.
	ErrPluginExists = errors.New("plugins: plugin already exists")
	// ErrPluginDisabled reports a hook call on a disabled plugin.
	ErrPluginDisabled = errors.New("plugins: plugin disabled")
	// ErrInvalidPluginID reports a malformed plugin ID.
	ErrInvalidPluginID = errors.New("plugins: invalid plugin id")
	// ErrInvalidHook reports an unknown hook point.
	ErrInvalidHook = errors.New("plugins: invalid hook")
	// ErrHookTimeout reports a plugin hook that did not finish in time.
	ErrHookTimeout = errors.New("plugins: hook timed out")
	// ErrPluginPanic reports a plugin hook that panicked.
	ErrPluginPanic = errors.New("plugins: plugin panicked")
	// ErrHostClosed reports use of a closed host.
	ErrHostClosed = errors.New("plugins: host closed")
)

// idPattern restricts plugin IDs to a path-, URL- and key-safe charset that
// cannot start with a dot, so IDs can never be "..", contain "/" or name a
// hidden file.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ValidateID reports whether id is an acceptable plugin ID.
func ValidateID(id string) error {
	if !idPattern.MatchString(id) {
		return fmt.Errorf("%w: %q", ErrInvalidPluginID, id)
	}
	return nil
}

// Plugin is the contract for a WASM-compatible plugin.
type Plugin interface {
	ID() string
	Name() string
	Enabled() bool
	Invoke(ctx context.Context, hook Hook, payload map[string]interface{}) (map[string]interface{}, error)
}

// Metadata stores plugin metadata.
type Metadata struct {
	ID        string
	Name      string
	Version   string
	Enabled   bool
	Filename  string
	SizeBytes int64
	CreatedAt int64
	UpdatedAt int64
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
