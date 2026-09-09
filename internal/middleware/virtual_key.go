package middleware

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/keymanager"
)

// virtualKeyContextKey is the context key for the validated virtual key.
type virtualKeyContextKey struct{}

// VirtualKeyFromContext retrieves the validated virtual key from the request context.
func VirtualKeyFromContext(ctx context.Context) (*keymanager.VirtualKey, bool) {
	vk, ok := ctx.Value(virtualKeyContextKey{}).(*keymanager.VirtualKey)
	return vk, ok
}

// VirtualKeyAuthMiddleware validates virtual keys against the key manager.
// If the Bearer token starts with "sk-", it is treated as a virtual key.
// Otherwise, the static API key list from the guardrails scoper is checked.
type VirtualKeyAuthMiddleware struct {
	Next       http.HandlerFunc
	KeyManager *keymanager.Manager
	StaticKeys map[string]bool
	mu         sync.RWMutex
}

// NewVirtualKeyAuthMiddleware creates a middleware that validates virtual keys.
func NewVirtualKeyAuthMiddleware(next http.HandlerFunc, km *keymanager.Manager, staticKeys map[string]bool) *VirtualKeyAuthMiddleware {
	return &VirtualKeyAuthMiddleware{
		Next:       next,
		KeyManager: km,
		StaticKeys: staticKeys,
	}
}

// ServeHTTP validates the incoming API key.
func (m *VirtualKeyAuthMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
		return
	}

	// Strip "Bearer " prefix.
	apiKey := auth
	if len(apiKey) > 7 && apiKey[:7] == "Bearer " {
		apiKey = apiKey[7:]
	}

	if keymanager.IsVirtualKey(auth) && m.KeyManager != nil {
		vk, err := m.KeyManager.Validate(r.Context(), apiKey)
		if err != nil {
			http.Error(w, `{"error":"invalid virtual key"}`, http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), virtualKeyContextKey{}, vk)
		m.Next(w, r.WithContext(ctx))
		return
	}

	// Static key fallback.
	m.mu.RLock()
	valid := m.StaticKeys[apiKey]
	m.mu.RUnlock()
	if valid {
		m.Next(w, r)
		return
	}

	http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
}

// AddStaticKey adds or removes a static key (for backward compat).
func (m *VirtualKeyAuthMiddleware) AddStaticKey(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.StaticKeys[key] = true
}

// RemoveStaticKey removes a static key.
func (m *VirtualKeyAuthMiddleware) RemoveStaticKey(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.StaticKeys, key)
}

// IsVirtualKeyAuth reports whether the auth token is a virtual key.
func IsVirtualKeyAuth(auth string) bool {
	if len(auth) > 7 && auth[:7] == "Bearer " {
		auth = auth[7:]
	}
	return strings.HasPrefix(auth, "sk-")
}
