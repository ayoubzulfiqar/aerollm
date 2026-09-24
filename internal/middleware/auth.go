package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/keymanager"
)

// Principal is the authenticated caller of a request.
type Principal struct {
	// Key is the raw credential. Never log it; use KeyID instead.
	Key string
	// KeyID is a short, stable, non-reversible identifier for the key, safe
	// for logs, metrics, rate-limit buckets and cache namespaces.
	KeyID string
	// Admin is true for master/admin keys.
	Admin bool
	// Virtual is set when the key is a key-manager issued virtual key.
	Virtual *keymanager.VirtualKey
}

type principalKey struct{}

// PrincipalFromContext returns the authenticated principal, if any.
func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(*Principal)
	return p, ok && p != nil
}

// WithPrincipal stores p in ctx (also exposing a virtual key through
// VirtualKeyFromContext for older call sites).
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	ctx = context.WithValue(ctx, principalKey{}, p)
	if p != nil && p.Virtual != nil {
		ctx = context.WithValue(ctx, virtualKeyContextKey{}, p.Virtual)
	}
	return ctx
}

// KeyID derives the non-reversible identifier used for a key.
func KeyID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "key_" + hex.EncodeToString(sum[:8])
}

// VirtualKeyValidator validates key-manager issued keys.
type VirtualKeyValidator interface {
	Validate(ctx context.Context, key string) (*keymanager.VirtualKey, error)
}

// Authentication errors.
var (
	ErrMissingKey = errors.New("missing api key")
	ErrInvalidKey = errors.New("invalid api key")
	// ErrKeyOverBudget is returned for a valid virtual key whose budget is spent.
	ErrKeyOverBudget = errors.New("api key budget exceeded")
)

// Authenticator validates API keys against static admin keys, static client
// keys and (optionally) the virtual key manager. Static keys are compared by
// SHA-256 digest so lookups never depend on secret-derived timing.
type Authenticator struct {
	mu        sync.RWMutex
	adminKeys map[[32]byte]struct{}
	apiKeys   map[[32]byte]struct{}
	Virtual   VirtualKeyValidator
}

// NewAuthenticator builds an Authenticator. Empty keys are ignored.
func NewAuthenticator(adminKeys, apiKeys []string, virtual VirtualKeyValidator) *Authenticator {
	a := &Authenticator{
		adminKeys: make(map[[32]byte]struct{}),
		apiKeys:   make(map[[32]byte]struct{}),
		Virtual:   virtual,
	}
	for _, k := range adminKeys {
		a.AddAdminKey(k)
	}
	for _, k := range apiKeys {
		a.AddAPIKey(k)
	}
	return a
}

// AddAdminKey registers an admin key.
func (a *Authenticator) AddAdminKey(key string) {
	if key = strings.TrimSpace(key); key == "" {
		return
	}
	a.mu.Lock()
	a.adminKeys[sha256.Sum256([]byte(key))] = struct{}{}
	a.mu.Unlock()
}

// AddAPIKey registers a static client key.
func (a *Authenticator) AddAPIKey(key string) {
	if key = strings.TrimSpace(key); key == "" {
		return
	}
	a.mu.Lock()
	a.apiKeys[sha256.Sum256([]byte(key))] = struct{}{}
	a.mu.Unlock()
}

// RemoveAPIKey unregisters a static client key.
func (a *Authenticator) RemoveAPIKey(key string) {
	a.mu.Lock()
	delete(a.apiKeys, sha256.Sum256([]byte(key)))
	a.mu.Unlock()
}

// HasAdminKeys reports whether any admin key is configured.
func (a *Authenticator) HasAdminKeys() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.adminKeys) > 0
}

// Authenticate resolves the caller of r. Static keys are checked before the
// virtual key manager, so a static key that happens to share the virtual key
// prefix ("sk-") still works.
func (a *Authenticator) Authenticate(r *http.Request) (*Principal, error) {
	key := APIKeyFromRequest(r)
	if key == "" {
		return nil, ErrMissingKey
	}
	digest := sha256.Sum256([]byte(key))
	a.mu.RLock()
	_, admin := a.adminKeys[digest]
	_, static := a.apiKeys[digest]
	a.mu.RUnlock()
	if admin || static {
		return &Principal{Key: key, KeyID: KeyID(key), Admin: admin}, nil
	}
	if a.Virtual != nil {
		vk, err := a.Virtual.Validate(r.Context(), key)
		if err == nil && vk != nil {
			return &Principal{Key: key, KeyID: KeyID(key), Virtual: vk}, nil
		}
		if errors.Is(err, keymanager.ErrBudgetExceeded) {
			return nil, ErrKeyOverBudget
		}
	}
	return nil, ErrInvalidKey
}

func (a *Authenticator) require(admin bool) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}
			p, err := a.Authenticate(r)
			if errors.Is(err, ErrKeyOverBudget) {
				WriteJSONError(w, http.StatusPaymentRequired, err.Error(), "insufficient_quota")
				return
			}
			if err != nil {
				w.Header().Set("WWW-Authenticate", `Bearer realm="aerollm"`)
				WriteJSONError(w, http.StatusUnauthorized, err.Error(), "")
				return
			}
			if admin && !p.Admin {
				WriteJSONError(w, http.StatusForbidden, "admin access required", "")
				return
			}
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
		})
	}
}

// RequireKey admits any valid admin, static or virtual key.
func (a *Authenticator) RequireKey() Middleware { return a.require(false) }

// RequireAdmin admits only admin keys.
func (a *Authenticator) RequireAdmin() Middleware { return a.require(true) }
