package tenant

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"maps"
	"strings"
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

// TenantAPIKeyPrefix prefixes plaintext keys issued by InMemoryStore.
const TenantAPIKeyPrefix = "sk-tenant-"

// InMemoryStore is a thread-safe in-memory API key and tenant-hierarchy
// store for testing and small deployments. It stores and returns copies, so
// callers cannot mutate stored state, and keeps only SHA-256 hashes of keys.
// It implements Resolver.
type InMemoryStore struct {
	mu      sync.RWMutex
	apiKeys map[string]*APIKey // by ID
	byHash  map[string]string  // sha256(raw) -> ID
	orgs    map[TenantID]*Organization
	teams   map[TenantID]*Team
	users   map[TenantID]*User
}

// NewInMemoryStore creates a new in-memory store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		apiKeys: make(map[string]*APIKey),
		byHash:  make(map[string]string),
		orgs:    make(map[TenantID]*Organization),
		teams:   make(map[TenantID]*Team),
		users:   make(map[TenantID]*User),
	}
}

func newRawAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return TenantAPIKeyPrefix + base64.RawURLEncoding.EncodeToString(b), nil
}

// IssueAPIKey creates a new API key and returns its plaintext exactly once.
// apiKey.ID is required; apiKey.HashedKey is overwritten with the hash of
// the generated key.
func (s *InMemoryStore) IssueAPIKey(ctx context.Context, apiKey *APIKey) (string, *APIKey, error) {
	if apiKey == nil || strings.TrimSpace(apiKey.ID) == "" {
		return "", nil, errors.New("invalid api key")
	}
	raw, err := newRawAPIKey()
	if err != nil {
		return "", nil, err
	}
	entry := apiKey.clone()
	entry.HashedKey = HashAPIKey(raw)
	stored, err := s.insertAPIKey(entry)
	if err != nil {
		return "", nil, err
	}
	return raw, stored, nil
}

// CreateAPIKey stores an API key. If apiKey.HashedKey is set it must be the
// SHA-256 hex hash of the plaintext key (see HashAPIKey). If it is empty a
// random key is generated, but its plaintext is not returned; use
// IssueAPIKey to obtain a usable key. The returned value is a copy.
func (s *InMemoryStore) CreateAPIKey(ctx context.Context, apiKey *APIKey) (*APIKey, error) {
	if apiKey == nil || strings.TrimSpace(apiKey.ID) == "" {
		return nil, errors.New("invalid api key")
	}
	entry := apiKey.clone()
	if entry.HashedKey == "" {
		raw, err := newRawAPIKey()
		if err != nil {
			return nil, err
		}
		entry.HashedKey = HashAPIKey(raw)
	}
	return s.insertAPIKey(entry)
}

func (s *InMemoryStore) insertAPIKey(entry *APIKey) (*APIKey, error) {
	entry.HashedKey = strings.ToLower(entry.HashedKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.apiKeys[entry.ID]; exists {
		return nil, &APIKeyAlreadyExistsError{APIKeyID: entry.ID}
	}
	if _, exists := s.byHash[entry.HashedKey]; exists {
		return nil, &APIKeyAlreadyExistsError{APIKeyID: entry.ID}
	}
	entry.CreatedAt = time.Now().Unix()
	s.apiKeys[entry.ID] = entry
	s.byHash[entry.HashedKey] = entry.ID
	return entry.clone(), nil
}

// GetAPIKey retrieves a copy of an API key by ID.
func (s *InMemoryStore) GetAPIKey(ctx context.Context, id string) (*APIKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.apiKeys[id]
	if !ok {
		return nil, &APIKeyNotFoundError{APIKeyID: id}
	}
	return key.clone(), nil
}

// SetAPIKeyActive activates or deactivates (revokes) a key by ID.
func (s *InMemoryStore) SetAPIKeyActive(ctx context.Context, id string, active bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.apiKeys[id]
	if !ok {
		return &APIKeyNotFoundError{APIKeyID: id}
	}
	key.Active = active
	return nil
}

// ResolveByAPIKey resolves a plaintext key (optionally "Bearer "-prefixed)
// to a copy of its active API key record and records its last use.
func (s *InMemoryStore) ResolveByAPIKey(ctx context.Context, apiKey string) (*APIKey, error) {
	raw := normalizeAPIKey(apiKey)
	if raw == "" {
		return nil, ErrAPIKeyNotFound
	}
	h := HashAPIKey(raw)
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byHash[h]
	if !ok {
		return nil, ErrAPIKeyNotFound
	}
	key := s.apiKeys[id]
	if key == nil || !key.Active {
		return nil, ErrAPIKeyNotFound
	}
	key.LastUsedAt = time.Now().Unix()
	return key.clone(), nil
}

// ListAPIKeys returns copies of all API keys.
func (s *InMemoryStore) ListAPIKeys(ctx context.Context) ([]*APIKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*APIKey, 0, len(s.apiKeys))
	for _, key := range s.apiKeys {
		out = append(out, key.clone())
	}
	return out, nil
}

func cloneOrg(o *Organization) *Organization {
	c := *o
	c.Settings = maps.Clone(o.Settings)
	return &c
}

func cloneTeam(t *Team) *Team {
	c := *t
	c.Settings = maps.Clone(t.Settings)
	if t.ParentTeamID != nil {
		p := *t.ParentTeamID
		c.ParentTeamID = &p
	}
	return &c
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
	s.orgs[org.ID] = cloneOrg(org)
	return cloneOrg(org), nil
}

// GetOrganization retrieves a copy of an organization by ID.
func (s *InMemoryStore) GetOrganization(ctx context.Context, id TenantID) (*Organization, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	org, ok := s.orgs[id]
	if !ok {
		return nil, &TenantNotFoundError{TenantID: id}
	}
	return cloneOrg(org), nil
}

// CreateTeam creates a new team within an existing organization. A parent
// team, if given, must exist and belong to the same organization.
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
	if team.ParentTeamID != nil {
		parent, ok := s.teams[*team.ParentTeamID]
		if !ok {
			return nil, &TenantNotFoundError{TenantID: *team.ParentTeamID}
		}
		if parent.OrgID != team.OrgID {
			return nil, errors.New("parent team belongs to a different organization")
		}
	}
	team.CreatedAt = time.Now().Unix()
	s.teams[team.ID] = cloneTeam(team)
	return cloneTeam(team), nil
}

// GetTeam retrieves a copy of a team by ID.
func (s *InMemoryStore) GetTeam(ctx context.Context, id TenantID) (*Team, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.teams[id]
	if !ok {
		return nil, &TenantNotFoundError{TenantID: id}
	}
	return cloneTeam(t), nil
}

// CreateUser creates a new user within an existing team.
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
	c := *user
	s.users[user.ID] = &c
	out := c
	return &out, nil
}

// GetUser retrieves a copy of a user by ID.
func (s *InMemoryStore) GetUser(ctx context.Context, id TenantID) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[id]
	if !ok {
		return nil, &TenantNotFoundError{TenantID: id}
	}
	c := *u
	return &c, nil
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
