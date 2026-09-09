package keymanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// User represents an agency user.
type User struct {
	ID        string                 `json:"id"`
	Email     string                 `json:"email"`
	Role      string                 `json:"role"`
	Metadata  map[string]interface{} `json:"metadata"`
	CreatedAt time.Time              `json:"created_at"`
}

// Team represents a team within an agency.
type Team struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Members     []string              `json:"members"`
	Budget      float64               `json:"budget"`
	Metadata    map[string]interface{} `json:"metadata"`
	CreatedAt   time.Time              `json:"created_at"`
}

// UserStore persists users.
type UserStore interface {
	Create(ctx context.Context, u *User) error
	Get(ctx context.Context, id string) (*User, error)
	Update(ctx context.Context, u *User) error
}

// TeamStore persists teams.
type TeamStore interface {
	Create(ctx context.Context, t *Team) error
	Get(ctx context.Context, id string) (*Team, error)
	Update(ctx context.Context, t *Team) error
}

// KeyHandler holds the manager and stores for HTTP handlers.
type KeyHandler struct {
	Manager  *Manager
	Users    UserStore
	Teams    TeamStore
	Logger   func(msg string, kv ...interface{})
}

// NewKeyHandler creates a new key handler.
func NewKeyHandler(mgr *Manager, users UserStore, teams TeamStore, logger func(msg string, kv ...interface{})) *KeyHandler {
	if logger == nil {
		logger = func(string, ...interface{}) {}
	}
	return &KeyHandler{Manager: mgr, Users: users, Teams: teams, Logger: logger}
}

// GenerateKeys handles POST /key/generate.
// @Summary Generate virtual key
// @Description Create a new virtual key with allowed models, TTL, and budget limits.
// @Tags keys
// @Accept json
// @Produce json
// @Param req body GenerateRequest true "Generate key request"
// @Success 200 {object} GenerateResponse
// @Router /key/generate [post]
func (h *KeyHandler) GenerateKeys(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req GenerateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	resp, err := h.Manager.Generate(ctx, &req)
	if err != nil {
		h.Logger("key generation failed", "error", err)
		http.Error(w, `{"error":"key generation failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// DeleteKey handles POST /key/delete.
// @Summary Delete virtual key
// @Description Soft-delete a virtual key.
// @Tags keys
// @Accept json
// @Produce json
// @Param req body DeleteRequest true "Delete key request"
// @Success 200 {object} map[string]string
// @Router /key/delete [post]
func (h *KeyHandler) DeleteKey(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	keyHash := ParseKeyOrHash(h.Manager, req.Key, req.KeyHash, req.Token)
	if keyHash == "" {
		http.Error(w, `{"error":"missing key_hash"}`, http.StatusBadRequest)
		return
	}
	if err := h.Manager.Delete(ctx, keyHash); err != nil {
		http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

// InfoKey handles POST /key/info.
// @Summary Get key info
// @Description Get usage, budget, and metadata for a specific virtual key.
// @Tags keys
// @Accept json
// @Produce json
// @Param req body InfoRequest true "Key info request"
// @Success 200 {object} InfoResponse
// @Router /key/info [post]
func (h *KeyHandler) InfoKey(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req InfoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	keyHash := ParseKeyOrHash(h.Manager, req.Key, req.KeyHash, req.Token)
	if keyHash == "" {
		http.Error(w, `{"error":"missing key_hash"}`, http.StatusBadRequest)
		return
	}
	resp, err := h.Manager.Info(ctx, keyHash)
	if err != nil {
		http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// UserInfo handles POST /user/info.
// @Summary Get user info
// @Tags users
// @Accept json
// @Produce json
// @Param req body keymanager.InfoRequest true "User info request"
// @Success 200 {object} User
// @Router /user/info [post]
func (h *KeyHandler) UserInfo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	u, err := h.Users.Get(ctx, req.UserID)
	if err != nil {
		http.Error(w, `{"error":"user not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(u)
}

// CreateUser handles user creation (internal helper).
func (h *KeyHandler) CreateUser(w http.ResponseWriter, r *http.Request) {
	var u User
	if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	u.ID = generateID()
	u.CreatedAt = time.Now().UTC()
	if err := h.Users.Create(r.Context(), &u); err != nil {
		http.Error(w, `{"error":"failed to create user"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(u)
}

// TeamCreate handles POST /team/create.
// @Summary Create team
// @Description Create a new team for an agency.
// @Tags teams
// @Accept json
// @Produce json
// @Param req body Team true "Team creation request"
// @Success 200 {object} Team
// @Router /team/create [post]
func (h *KeyHandler) TeamCreate(w http.ResponseWriter, r *http.Request) {
	var t Team
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	t.ID = generateID()
	t.CreatedAt = time.Now().UTC()
	if err := h.Teams.Create(r.Context(), &t); err != nil {
		http.Error(w, `{"error":"failed to create team"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(t)
}

// TeamUpdate handles POST /team/update.
// @Summary Update team
// @Description Update an existing team's details.
// @Tags teams
// @Accept json
// @Produce json
// @Param req body Team true "Team update request"
// @Success 200 {object} Team
// @Router /team/update [post]
func (h *KeyHandler) TeamUpdate(w http.ResponseWriter, r *http.Request) {
	var t Team
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	if err := h.Teams.Update(r.Context(), &t); err != nil {
		http.Error(w, `{"error":"team not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(t)
}

// InMemoryKeyStore is a simple in-memory implementation of KeyStore.
type InMemoryKeyStore struct {
	mu   sync.RWMutex
	keys map[string]*VirtualKey
}

// NewInMemoryKeyStore creates a new in-memory key store.
func NewInMemoryKeyStore() *InMemoryKeyStore {
	return &InMemoryKeyStore{keys: make(map[string]*VirtualKey)}
}

// Create stores a virtual key.
func (s *InMemoryKeyStore) Create(ctx context.Context, key *VirtualKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.keys[key.KeyHash]; exists {
		return ErrKeyExists
	}
	s.keys[key.KeyHash] = key
	return nil
}

// Get retrieves a virtual key by hash.
func (s *InMemoryKeyStore) Get(ctx context.Context, keyHash string) (*VirtualKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.keys[keyHash]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return key, nil
}

// GetByPrefix returns the first key matching the given prefix.
func (s *InMemoryKeyStore) GetByPrefix(ctx context.Context, prefix string) (*VirtualKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, k := range s.keys {
		if k.Prefix == prefix {
			return k, nil
		}
	}
	return nil, ErrKeyNotFound
}

// Update modifies a virtual key.
func (s *InMemoryKeyStore) Update(ctx context.Context, key *VirtualKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[key.KeyHash]; !ok {
		return ErrKeyNotFound
	}
	s.keys[key.KeyHash] = key
	return nil
}

// Delete removes a virtual key.
func (s *InMemoryKeyStore) Delete(ctx context.Context, keyHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[keyHash]; !ok {
		return ErrKeyNotFound
	}
	delete(s.keys, keyHash)
	return nil
}

// List returns all virtual keys.
func (s *InMemoryKeyStore) List(ctx context.Context) ([]*VirtualKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*VirtualKey, 0, len(s.keys))
	for _, k := range s.keys {
		out = append(out, k)
	}
	return out, nil
}

// ErrKeyExists is returned when a key already exists.
var ErrKeyExists = newKeyError("key already exists")

// ErrKeyNotFound is returned when a key is not found.
var ErrKeyNotFound = newKeyError("key not found")

// KeyError is a lightweight error type.
type KeyError struct{ msg string }

func (e *KeyError) Error() string { return e.msg }

func newKeyError(msg string) *KeyError { return &KeyError{msg: msg} }

// generateID creates a unique ID.
func generateID() string {
	return fmt.Sprintf("id_%d", time.Now().UnixNano())
}
