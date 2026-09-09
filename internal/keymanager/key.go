package keymanager

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// KeyStatus represents the lifecycle state of a virtual key.
type KeyStatus string

const (
	StatusActive    KeyStatus = "active"
	StatusDeleted   KeyStatus = "deleted"
	StatusExpired   KeyStatus = "expired"
)

// VirtualKey is the in-memory representation of a virtual key.
type VirtualKey struct {
	KeyHash     string    `json:"key_hash"`
	HashedKey   string    `json:"-"`
	Prefix      string    `json:"prefix"`
	Models      []string  `json:"models"`
	MaxBudget   float64   `json:"max_budget"`
	Aliases     []string  `json:"aliases"`
	Metadata    map[string]interface{} `json:"metadata"`
	UserID      string    `json:"user_id"`
	TeamID      string    `json:"team_id"`
	TenantID    string    `json:"tenant_id"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	Status      KeyStatus `json:"status"`
	CreatedAtMs int64     `json:"created_at_ms"`
}

// KeyStore is the interface for persisting virtual keys.
type KeyStore interface {
	Create(ctx context.Context, key *VirtualKey) error
	Get(ctx context.Context, keyHash string) (*VirtualKey, error)
	GetByPrefix(ctx context.Context, prefix string) (*VirtualKey, error)
	Update(ctx context.Context, key *VirtualKey) error
	Delete(ctx context.Context, keyHash string) error
	List(ctx context.Context) ([]*VirtualKey, error)
}

// GenerateRequest is the payload for POST /key/generate.
type GenerateRequest struct {
	Models    []string                `json:"models"`
	Duration  string                `json:"duration"`
	MaxBudget float64               `json:"max_budget"`
	Aliases   []string              `json:"aliases"`
	Metadata  map[string]interface{} `json:"metadata"`
	UserID    string                `json:"user_id"`
	TeamID    string                `json:"team_id"`
}

// GenerateResponse is the response for POST /key/generate.
type GenerateResponse struct {
	KeyHash string `json:"key_hash"`
	Key     string `json:"key"`
	Expires string `json:"expires"`
	Token   string `json:"token"`
}

// InfoRequest is the payload for POST /key/info.
type InfoRequest struct {
	KeyHash string `json:"key_hash"`
	Key     string `json:"key"`
	Token   string `json:"token"`
}

// DeleteRequest is the payload for POST /key/delete.
type DeleteRequest struct {
	KeyHash string `json:"key_hash"`
	Key     string `json:"key"`
	Token   string `json:"token"`
}

// InfoResponse is the response for POST /key/info.
type InfoResponse struct {
	KeyHash  string                 `json:"key_hash"`
	Models   []string               `json:"models"`
	MaxBudget float64                `json:"max_budget"`
	Aliases  []string               `json:"aliases"`
	Metadata map[string]interface{} `json:"metadata"`
	Status   KeyStatus              `json:"status"`
	Created  time.Time              `json:"created_at"`
	Expires  time.Time              `json:"expires_at"`
}

// Manager handles virtual key lifecycle operations.
type Manager struct {
	store    KeyStore
	masterKey string
}

// NewManager creates a new key manager.
func NewManager(store KeyStore, masterKey string) *Manager {
	return &Manager{store: store, masterKey: masterKey}
}

// Generate creates a new virtual key, persists it, and returns the plaintext key.
func (m *Manager) Generate(ctx context.Context, req *GenerateRequest) (*GenerateResponse, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("failed to generate random bytes: %w", err)
	}
	prefix := "sk-" + hex.EncodeToString(raw[:8])
	key := prefix + "_" + hex.EncodeToString(raw[8:])
	keyHash := m.hashKey(key)

	dur, err := time.ParseDuration(req.Duration)
	if err != nil {
		dur = 24 * time.Hour
	}
	expiresAt := time.Now().Add(dur)
	if req.Duration == "" || req.Duration == "0" {
		expiresAt = time.Time{}
	}

	vk := &VirtualKey{
		KeyHash:   keyHash,
		HashedKey: keyHash,
		Prefix:    prefix,
		Models:    req.Models,
		MaxBudget: req.MaxBudget,
		Aliases:   req.Aliases,
		Metadata:  req.Metadata,
		UserID:    req.UserID,
		TeamID:    req.TeamID,
		Status:    StatusActive,
		CreatedAt: time.Now().UTC(),
		ExpiresAt: expiresAt,
	}
	if err := m.store.Create(ctx, vk); err != nil {
		return nil, fmt.Errorf("failed to store key: %w", err)
	}
	return &GenerateResponse{
		KeyHash: keyHash,
		Key:     key,
		Expires: expiresAt.Format(time.RFC3339),
		Token:   key,
	}, nil
}

// Delete soft-deletes a virtual key by hash.
func (m *Manager) Delete(ctx context.Context, keyHash string) error {
	vk, err := m.store.Get(ctx, keyHash)
	if err != nil {
		return err
	}
	vk.Status = StatusDeleted
	return m.store.Update(ctx, vk)
}

// Info retrieves key metadata by hash.
func (m *Manager) Info(ctx context.Context, keyHash string) (*InfoResponse, error) {
	vk, err := m.store.Get(ctx, keyHash)
	if err != nil {
		return nil, err
	}
	return &InfoResponse{
		KeyHash:   vk.KeyHash,
		Models:    vk.Models,
		MaxBudget: vk.MaxBudget,
		Aliases:   vk.Aliases,
		Metadata:  vk.Metadata,
		Status:    vk.Status,
		Created:   vk.CreatedAt,
		Expires:   vk.ExpiresAt,
	}, nil
}

// Validate checks a key against the store for auth/usage.
func (m *Manager) Validate(ctx context.Context, key string) (*VirtualKey, error) {
	if key == "" {
		return nil, errors.New("empty key")
	}
	keyHash := m.hashKey(key)
	vk, err := m.store.Get(ctx, keyHash)
	if err != nil {
		return nil, err
	}
	if vk.Status != StatusActive {
		return nil, fmt.Errorf("key status: %s", vk.Status)
	}
	if !vk.ExpiresAt.IsZero() && time.Now().After(vk.ExpiresAt) {
		return nil, errors.New("key expired")
	}
	return vk, nil
}

// hashKey produces a deterministic SHA-256 hash of the key.
func (m *Manager) hashKey(key string) string {
	h := hmac.New(sha256.New, []byte(m.masterKey))
	h.Write([]byte(key))
	return hex.EncodeToString(h.Sum(nil))
}

// ParseKeyOrHash extracts the key hash from a request.
func ParseKeyOrHash(mgr *Manager, key, keyHash, token string) string {
	if keyHash != "" {
		return keyHash
	}
	if token != "" {
		return mgr.hashKey(token)
	}
	if key != "" {
		return mgr.hashKey(key)
	}
	return ""
}

// IsVirtualKey reports whether the given Bearer token is a virtual key (sk- prefix).
func IsVirtualKey(auth string) bool {
	if len(auth) > 7 && auth[:7] == "Bearer " {
		auth = auth[7:]
	}
	return strings.HasPrefix(auth, "sk-")
}
