package plugins

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

// DefaultHookTimeout bounds a single plugin hook invocation.
const DefaultHookTimeout = 2 * time.Second

// HookError identifies the plugin and hook that failed.
type HookError struct {
	PluginID string
	Hook     Hook
	Err      error
}

func (e *HookError) Error() string {
	return fmt.Sprintf("plugin %q hook %s: %v", e.PluginID, e.Hook, e.Err)
}

func (e *HookError) Unwrap() error { return e.Err }

type invokeResult struct {
	out map[string]interface{}
	err error
}

// SafeInvoke runs p.Invoke with panic isolation and a deadline. The plugin
// receives a deep copy of payload and a context cancelled on timeout, so a
// plugin that overruns can neither block the caller nor mutate the caller's
// data afterwards. A plugin that ignores its context keeps running in the
// background until it returns (Go cannot kill a goroutine); its late result
// is discarded without blocking.
func SafeInvoke(ctx context.Context, p Plugin, hook Hook, payload map[string]interface{}, timeout time.Duration) (map[string]interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("%w: nil plugin", ErrPluginNotFound)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = DefaultHookTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	in := CopyPayload(payload)
	done := make(chan invokeResult, 1) // buffered: a late plugin never blocks
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- invokeResult{err: fmt.Errorf("%w: %v\n%s", ErrPluginPanic, r, debug.Stack())}
			}
		}()
		out, err := p.Invoke(ctx, hook, in)
		done <- invokeResult{out: out, err: err}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			return nil, res.err
		}
		// Copy again so a plugin goroutine that retained its result map
		// cannot race with the caller.
		return CopyPayload(res.out), nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w after %s", ErrHookTimeout, timeout)
		}
		return nil, ctx.Err()
	}
}

// CopyPayload deep-copies maps and slices of the JSON-like value types used in
// hook payloads. Other values are copied by assignment.
func CopyPayload(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = copyValue(v)
	}
	return out
}

func copyValue(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		return CopyPayload(t)
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = copyValue(e)
		}
		return out
	case []string:
		return append([]string(nil), t...)
	case []byte:
		return append([]byte(nil), t...)
	case map[string]string:
		out := make(map[string]string, len(t))
		for k, s := range t {
			out[k] = s
		}
		return out
	default:
		return v
	}
}

// NativeHost runs in-process Plugin implementations for lifecycle hooks with
// per-invocation timeouts and panic isolation. It implements Host and is safe
// for concurrent use.
type NativeHost struct {
	mu      sync.RWMutex
	plugins map[string]Plugin
	timeout time.Duration
}

// NewNativeHost creates a host; timeout <= 0 uses DefaultHookTimeout.
func NewNativeHost(timeout time.Duration) *NativeHost {
	if timeout <= 0 {
		timeout = DefaultHookTimeout
	}
	return &NativeHost{plugins: make(map[string]Plugin), timeout: timeout}
}

// Add registers a plugin.
func (h *NativeHost) Add(p Plugin) error {
	if p == nil {
		return fmt.Errorf("%w: nil plugin", ErrInvalidPluginID)
	}
	id, err := safeCall(p.ID)
	if err != nil {
		return err
	}
	if err := ValidateID(id); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.plugins[id]; ok {
		return fmt.Errorf("%w: %q", ErrPluginExists, id)
	}
	h.plugins[id] = p
	return nil
}

// Remove unregisters a plugin.
func (h *NativeHost) Remove(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.plugins[id]; !ok {
		return fmt.Errorf("%w: %q", ErrPluginNotFound, id)
	}
	delete(h.plugins, id)
	return nil
}

func safeCall[T any](fn func() T) (v T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrPluginPanic, r)
		}
	}()
	return fn(), nil
}

// Invoke runs hook on a single plugin.
func (h *NativeHost) Invoke(ctx context.Context, id string, hook Hook, payload map[string]interface{}) (map[string]interface{}, error) {
	if !hook.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidHook, hook)
	}
	h.mu.RLock()
	p, ok := h.plugins[id]
	h.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrPluginNotFound, id)
	}
	enabled, err := safeCall(p.Enabled)
	if err != nil {
		return nil, &HookError{PluginID: id, Hook: hook, Err: err}
	}
	if !enabled {
		return nil, fmt.Errorf("%w: %q", ErrPluginDisabled, id)
	}
	out, err := SafeInvoke(ctx, p, hook, payload, h.timeout)
	if err != nil {
		return nil, &HookError{PluginID: id, Hook: hook, Err: err}
	}
	return out, nil
}

// RunHook runs hook on every enabled plugin in ascending ID order, threading
// the payload through: each plugin receives the previous plugin's output (a
// nil output leaves the payload unchanged). The first failure — error, panic
// or timeout — aborts the chain and is returned as a *HookError.
func (h *NativeHost) RunHook(ctx context.Context, hook Hook, payload map[string]interface{}) (map[string]interface{}, error) {
	if !hook.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidHook, hook)
	}
	h.mu.RLock()
	ids := make([]string, 0, len(h.plugins))
	for id := range h.plugins {
		ids = append(ids, id)
	}
	h.mu.RUnlock()
	sort.Strings(ids)

	current := CopyPayload(payload)
	if current == nil {
		current = map[string]interface{}{}
	}
	for _, id := range ids {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		out, err := h.Invoke(ctx, id, hook, current)
		if errors.Is(err, ErrPluginDisabled) || errors.Is(err, ErrPluginNotFound) {
			continue // disabled, or removed concurrently
		}
		if err != nil {
			return nil, err
		}
		if out != nil {
			current = out
		}
	}
	return current, nil
}

var _ Host = (*NativeHost)(nil)
