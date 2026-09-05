package tenant

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// APIKeyAlreadyExistsError is returned when inserting a duplicate key ID.
type APIKeyAlreadyExistsError struct {
	APIKeyID string
}

func (e *APIKeyAlreadyExistsError) Error() string {
	return "api key already exists: " + e.APIKeyID
}

// APIKeyNotFoundError is returned when a key cannot be found.
type APIKeyNotFoundError struct {
	APIKeyID string
}

func (e *APIKeyNotFoundError) Error() string {
	return "api key not found: " + e.APIKeyID
}

// InMemoryStore is a thread-safe in-memory API key store for testing and small deployments.
type InMemoryStore struct {
	mu       sync.RWMutex
	apiKeys  map[string]*APIKey
	orgs     map[TenantID]*Organization
	teams    map[TenantID]*Team
	users    map[TenantID]*User
}

// NewInMemoryStore creates a new in-memory store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		apiKeys: make(map[string]*APIKey),
		orgs:    make(map[TenantID]*Organization),
		teams:   make(map[TenantID]*Team),
		users:   make(map[TenantID]*User),
	}
}

// CreateAPIKey creates a new API key with a hashed key value.
func (s *InMemoryStore) CreateAPIKey(ctx context.Context, apiKey *APIKey) (*APIKey, error) {
	if apiKey == nil || apiKey.ID == "" {
		return nil, errors.New("invalid api key")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.apiKeys[apiKey.ID]; exists {
		return nil, &APIKeyAlreadyExistsError{APIKeyID: apiKey.ID}
	}
	keyBytes := make([]byte, 32)
	_, _ = rand.Read(keyBytes)
	apiKey.HashedKey = hex.EncodeToString(keyBytes)
	apiKey.CreatedAt = time.Now().Unix()
	s.apiKeys[apiKey.ID] = apiKey
	return apiKey, nil
}

// GetAPIKey retrieves an API key by ID.
func (s *InMemoryStore) GetAPIKey(ctx context.Context, id string) (*APIKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.apiKeys[id]
	if !ok {
		return nil, &APIKeyNotFoundError{APIKeyID: id}
	}
	return key, nil
}

// ListAPIKeys returns all API keys.
func (s *InMemoryStore) ListAPIKeys(ctx context.Context) ([]*APIKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*APIKey, 0, len(s.apiKeys))
	for _, key := range s.apiKeys {
		out = append(out, key)
	}
	return out, nil
}

// CreateOrganization creates a new organization.
func (s *InMemoryStore) CreateOrganization(ctx context.Context, org *Organization) (*Organization, error) {
	if org == nil || org.ID == "" {
		return nil, errors.New("invalid organization")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.orgs[org.ID]; exists {
		return nil, &TenantAlreadyExistsError{TenantID: org.ID}
	}
	org.CreatedAt = time.Now().Unix()
	s.orgs[org.ID] = org
	return org, nil
}

// GetOrganization retrieves an organization by ID.
func (s *InMemoryStore) GetOrganization(ctx context.Context, id TenantID) (*Organization, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	org, ok := s.orgs[id]
	if !ok {
		return nil, &TenantNotFoundError{TenantID: id}
	}
	return org, nil
}

// CreateTeam creates a new team within an organization.
func (s *InMemoryStore) CreateTeam(ctx context.Context, team *Team) (*Team, error) {
	if team == nil || team.ID == "" || team.OrgID == "" {
		return nil, errors.New("invalid team")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.teams[team.ID]; exists {
		return nil, &TenantAlreadyExistsError{TenantID: team.ID}
	}
	if _, ok := s.orgs[team.OrgID]; !ok {
		return nil, &TenantNotFoundError{TenantID: team.OrgID}
	}
	team.CreatedAt = time.Now().Unix()
	s.teams[team.ID] = team
	return team, nil
}

// CreateUser creates a new user within a team.
func (s *InMemoryStore) CreateUser(ctx context.Context, user *User) (*User, error) {
	if user == nil || user.ID == "" || user.TeamID == "" {
		return nil, errors.New("invalid user")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.users[user.ID]; exists {
		return nil, &TenantAlreadyExistsError{TenantID: user.ID}
	}
	if _, ok := s.teams[user.TeamID]; !ok {
		return nil, &TenantNotFoundError{TenantID: user.TeamID}
	}
	user.CreatedAt = time.Now().Unix()
	s.users[user.ID] = user
	return user, nil
}

// TenantAlreadyExistsError is returned when a tenant already exists.
type TenantAlreadyExistsError struct {
	TenantID TenantID
}

func (e *TenantAlreadyExistsError) Error() string {
	return "tenant already exists: " + string(e.TenantID)
}

// TenantNotFoundError is returned when a tenant cannot be found.
type TenantNotFoundError struct {
	TenantID TenantID
}

func (e *TenantNotFoundError) Error() string {
	return "tenant not found: " + string(e.TenantID)
}
