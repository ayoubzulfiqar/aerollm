package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
)

// ErrAPIKeyNotFound is returned when an API key cannot be resolved.
var ErrAPIKeyNotFound = errors.New("tenant: api key not found")

// HashAPIKey returns the lowercase hex SHA-256 of a plaintext API key.
func HashAPIKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// normalizeAPIKey trims whitespace and an optional "Bearer " scheme.
func normalizeAPIKey(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 7 && strings.EqualFold(s[:7], "bearer ") {
		s = strings.TrimSpace(s[7:])
	}
	return s
}

// InMemoryTenantResolver resolves API keys to tenants from an in-memory map.
// Only SHA-256 hashes of keys are stored; ResolveByAPIKey always hashes its
// input, so possessing a stored hash does not allow authentication.
type InMemoryTenantResolver struct {
	mu      sync.RWMutex
	apiKeys map[string]*APIKey // by sha256(raw key)
}

// NewInMemoryTenantResolver creates a new in-memory tenant resolver.
func NewInMemoryTenantResolver() *InMemoryTenantResolver {
	return &InMemoryTenantResolver{apiKeys: make(map[string]*APIKey)}
}

// Add registers an API key. For backward compatibility key.HashedKey is
// interpreted as the PLAINTEXT bearer token presented by clients; it is
// hashed before storage and the stored entry's HashedKey is replaced by the
// hash. Prefer AddKey (plaintext) or AddHashed (pre-hashed).
func (r *InMemoryTenantResolver) Add(key *APIKey) {
	if key == nil || normalizeAPIKey(key.HashedKey) == "" {
		return
	}
	r.AddKey(key.HashedKey, key)
}

// AddKey registers key under the plaintext token raw.
func (r *InMemoryTenantResolver) AddKey(raw string, key *APIKey) {
	raw = normalizeAPIKey(raw)
	if key == nil || raw == "" {
		return
	}
	r.AddHashed(HashAPIKey(raw), key)
}

// AddHashed registers key under a pre-computed SHA-256 hex hash.
func (r *InMemoryTenantResolver) AddHashed(hash string, key *APIKey) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	if key == nil || hash == "" {
		return
	}
	entry := key.clone()
	entry.HashedKey = hash
	r.mu.Lock()
	defer r.mu.Unlock()
	r.apiKeys[hash] = entry
}

// Remove unregisters the plaintext token raw.
func (r *InMemoryTenantResolver) Remove(raw string) {
	h := HashAPIKey(normalizeAPIKey(raw))
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.apiKeys, h)
}

// ResolveByAPIKey looks up the API key. Inactive keys are not resolved.
func (r *InMemoryTenantResolver) ResolveByAPIKey(_ context.Context, apiKey string) (*APIKey, error) {
	raw := normalizeAPIKey(apiKey)
	if raw == "" {
		return nil, ErrAPIKeyNotFound
	}
	h := HashAPIKey(raw)
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, ok := r.apiKeys[h]
	if !ok || !key.Active {
		return nil, ErrAPIKeyNotFound
	}
	return key.clone(), nil
}
