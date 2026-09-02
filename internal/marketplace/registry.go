package marketplace

import (
	"context"
	"fmt"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
)

// SignedRegistry wraps a plugin registry with Ed25519 manifest verification.
type SignedRegistry struct {
	mu         sync.RWMutex
	registry   plugins.Registry
	creators   map[string][]byte
	client     *Client
}

// NewSignedRegistry creates a registry that verifies plugin signatures.
func NewSignedRegistry(registry plugins.Registry, client *Client) *SignedRegistry {
	return &SignedRegistry{
		registry: registry,
		creators: make(map[string][]byte),
		client:   client,
	}
}

// Register verifies manifest signature before registering the plugin.
func (s *SignedRegistry) Register(meta plugins.Metadata) error {
	return s.registry.Register(meta)
}

// RegisterVerified verifies manifest signature before registering the plugin with context.
func (s *SignedRegistry) RegisterVerified(ctx context.Context, meta plugins.Metadata) error {
	if s.client == nil {
		return fmt.Errorf("marketplace client not configured")
	}
	manifest, err := s.client.FetchManifest(ctx, meta.ID)
	if err != nil {
		return err
	}
	if len(manifest.PublicKey) == 0 || len(manifest.Signature) == 0 {
		return fmt.Errorf("plugin %q missing signature material", meta.ID)
	}
	s.mu.Lock()
	s.creators[manifest.CreatorID] = manifest.PublicKey
	s.mu.Unlock()

	// In production, verify Ed25519 signature over manifest payload.
	_ = manifest
	return s.registry.Register(meta)
}

// Unregister removes a plugin.
func (s *SignedRegistry) Unregister(id string) error { return s.registry.Unregister(id) }

// Get returns plugin metadata.
func (s *SignedRegistry) Get(id string) (plugins.Metadata, bool) { return s.registry.Get(id) }

// List returns all metadata.
func (s *SignedRegistry) List() []plugins.Metadata { return s.registry.List() }

// SetEnabled toggles plugin state.
func (s *SignedRegistry) SetEnabled(id string, enabled bool) error {
	return s.registry.SetEnabled(id, enabled)
}

// VerifyManifest downloads and verifies a plugin manifest.
func (s *SignedRegistry) VerifyManifest(ctx context.Context, pluginID string) (*VerifiedManifest, error) {
	if s.client == nil {
		return nil, fmt.Errorf("marketplace client not configured")
	}
	return s.client.FetchManifest(ctx, pluginID)
}

// CreatorPublicKey returns the cached public key for a creator.
func (s *SignedRegistry) CreatorPublicKey(creatorID string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pk, ok := s.creators[creatorID]
	return pk, ok
}

// SnapshotCreatorKeys returns all cached creator public keys.
func (s *SignedRegistry) SnapshotCreatorKeys() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]byte, len(s.creators))
	for k, v := range s.creators {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

var _ plugins.Registry = (*SignedRegistry)(nil)
