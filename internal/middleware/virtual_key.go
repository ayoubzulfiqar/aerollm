package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/keymanager"
)

// virtualKeyContextKey is the context key for the validated virtual key.
type virtualKeyContextKey struct{}

// VirtualKeyFromContext retrieves the validated virtual key from the request context.
func VirtualKeyFromContext(ctx context.Context) (*keymanager.VirtualKey, bool) {
	vk, ok := ctx.Value(virtualKeyContextKey{}).(*keymanager.VirtualKey)
	return vk, ok && vk != nil
}

// VirtualKeyAuthMiddleware validates static keys and key-manager virtual keys.
// Static keys are checked first (by digest, see Authenticator), so a static
// key that starts with "sk-" is not mistaken for an invalid virtual key.
//
// Deprecated: use Authenticator.RequireKey.
type VirtualKeyAuthMiddleware struct {
	Next       http.HandlerFunc
	KeyManager *keymanager.Manager
	StaticKeys map[string]bool

	auth *Authenticator
}

// NewVirtualKeyAuthMiddleware creates a middleware that validates virtual keys.
func NewVirtualKeyAuthMiddleware(next http.HandlerFunc, km *keymanager.Manager, staticKeys map[string]bool) *VirtualKeyAuthMiddleware {
	keys := make([]string, 0, len(staticKeys))
	for k, ok := range staticKeys {
		if ok {
			keys = append(keys, k)
		}
	}
	var validator VirtualKeyValidator
	if km != nil {
		validator = km
	}
	return &VirtualKeyAuthMiddleware{
		Next:       next,
		KeyManager: km,
		StaticKeys: staticKeys,
		auth:       NewAuthenticator(nil, keys, validator),
	}
}

// ServeHTTP validates the incoming API key.
func (m *VirtualKeyAuthMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.auth.RequireKey()(m.Next).ServeHTTP(w, r)
}

// AddStaticKey adds a static key.
func (m *VirtualKeyAuthMiddleware) AddStaticKey(key string) {
	m.auth.AddAPIKey(key)
}

// RemoveStaticKey removes a static key.
func (m *VirtualKeyAuthMiddleware) RemoveStaticKey(key string) {
	m.auth.RemoveAPIKey(key)
}

// IsVirtualKeyAuth reports whether the auth token looks like a virtual key.
func IsVirtualKeyAuth(auth string) bool {
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		auth = auth[7:]
	}
	return strings.HasPrefix(auth, "sk-")
}
