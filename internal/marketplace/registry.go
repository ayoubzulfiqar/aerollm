package marketplace

import (
	"context"
	"crypto/subtle"
	"fmt"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
)

// signedRegisterTimeout bounds the network verification done by Register,
// which has no context parameter.
const signedRegisterTimeout = 30 * time.Second

// SignedRegistry wraps a plugin registry with Ed25519 manifest verification.
// Every registration path fetches the plugin's manifest from the marketplace
// and verifies its signature; there is no unverified Register bypass.
// Creator keys are pinned on first use: a later manifest for the same creator
// signed with a different key is rejected.
type SignedRegistry struct {
	mu       sync.RWMutex
	registry plugins.Registry
	creators map[string][]byte
	client   *Client
}

// NewSignedRegistry creates a registry that verifies plugin signatures.
func NewSignedRegistry(registry plugins.Registry, client *Client) *SignedRegistry {
	if registry == nil {
		registry = plugins.NewInMemoryRegistry()
	}
	return &SignedRegistry{
		registry: registry,
		creators: make(map[string][]byte),
		client:   client,
	}
}

// Register verifies the plugin's signed manifest (with a bounded timeout)
// before registering it. It is equivalent to RegisterVerified with a
// background context.
func (s *SignedRegistry) Register(meta plugins.Metadata) error {
	ctx, cancel := context.WithTimeout(context.Background(), signedRegisterTimeout)
	defer cancel()
	return s.RegisterVerified(ctx, meta)
}

// RegisterVerified fetches the plugin manifest, verifies its Ed25519 signature
// and creator key pinning, checks it matches meta, and registers the plugin.
func (s *SignedRegistry) RegisterVerified(ctx context.Context, meta plugins.Metadata) error {
	if s.client == nil {
		return fmt.Errorf("marketplace client not configured")
	}
	if err := ValidatePluginID(meta.ID); err != nil {
		return err
	}
	manifest, err := s.client.FetchManifest(ctx, meta.ID)
	if err != nil {
		return err
	}
	if meta.Version != "" && meta.Version != manifest.Version {
		return fmt.Errorf("%w: registering version %q but manifest is %q", ErrInvalidManifest, meta.Version, manifest.Version)
	}
	s.mu.Lock()
	if pinned, ok := s.creators[manifest.CreatorID]; ok {
		if subtle.ConstantTimeCompare(pinned, manifest.PublicKey) != 1 {
			s.mu.Unlock()
			return fmt.Errorf("%w: creator %q key changed", ErrUntrustedKey, manifest.CreatorID)
		}
	} else {
		s.creators[manifest.CreatorID] = append([]byte(nil), manifest.PublicKey...)
	}
	s.mu.Unlock()

	if meta.Version == "" {
		meta.Version = manifest.Version
	}
	if meta.Name == "" {
		meta.Name = manifest.Name
	}
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

// VerifyManifest downloads and verifies a plugin manifest, enforcing the
// creator key pinned by earlier registrations.
func (s *SignedRegistry) VerifyManifest(ctx context.Context, pluginID string) (*VerifiedManifest, error) {
	if s.client == nil {
		return nil, fmt.Errorf("marketplace client not configured")
	}
	m, err := s.client.FetchManifest(ctx, pluginID)
	if err != nil {
		return nil, err
	}
	if pinned, ok := s.CreatorPublicKey(m.CreatorID); ok && subtle.ConstantTimeCompare(pinned, m.PublicKey) != 1 {
		return nil, fmt.Errorf("%w: creator %q key changed", ErrUntrustedKey, m.CreatorID)
	}
	return m, nil
}

// CreatorPublicKey returns the cached public key for a creator.
func (s *SignedRegistry) CreatorPublicKey(creatorID string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pk, ok := s.creators[creatorID]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), pk...), true
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
