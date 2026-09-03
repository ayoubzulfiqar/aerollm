package marketplace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store persists plugin registry manifests and metadata.
type Store interface {
	Put(ctx context.Context, manifest VerifiedManifest, metadata Metadata) error
	Get(ctx context.Context, pluginID string) (VerifiedManifest, Metadata, bool)
	List(ctx context.Context) ([]Metadata, error)
}

// Metadata captures a marketplace plugin listing entry.
type Metadata struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Version   string    `json:"version"`
	CreatorID string    `json:"creator_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PublishRequest is the body for publishing a plugin to the registry.
type PublishRequest struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Version   string `json:"version"`
	CreatorID string `json:"creator_id"`
	WASMHash  string `json:"wasm_hash"`
	PublicKey string `json:"public_key"`
	Signature string `json:"signature"`
}

// InMemoryStore is a simple non-durable registry store.
type InMemoryStore struct {
	mu         sync.RWMutex
	manifests  map[string]VerifiedManifest
	meta       map[string]Metadata
}

// NewInMemoryStore returns an in-memory registry store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{manifests: make(map[string]VerifiedManifest), meta: make(map[string]Metadata)}
}

func (s *InMemoryStore) Put(_ context.Context, m VerifiedManifest, meta Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manifests[m.ID] = m
	meta.UpdatedAt = time.Now()
	s.meta[m.ID] = meta
	return nil
}

func (s *InMemoryStore) Get(_ context.Context, pluginID string) (VerifiedManifest, Metadata, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok1 := s.manifests[pluginID]
	meta, ok2 := s.meta[pluginID]
	return m, meta, ok1 && ok2
}

func (s *InMemoryStore) List(_ context.Context) ([]Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Metadata, 0, len(s.meta))
	for _, m := range s.meta {
		out = append(out, m)
	}
	return out, nil
}

// RegistryService exposes marketplace registry HTTP handlers.
type RegistryService struct {
	client *Client
	store  Store
}

// NewRegistryService creates a registry service.
func NewRegistryService(client *Client, store Store) *RegistryService {
	if store == nil {
		store = NewInMemoryStore()
	}
	return &RegistryService{client: client, store: store}
}

// RegisterRoutes mounts marketplace routes on the provided mux.
func (s *RegistryService) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/marketplace/plugins", s.handlePlugins)
	mux.HandleFunc("/v1/marketplace/plugins/", s.handlePluginByID)
	mux.HandleFunc("/v1/marketplace/openstandard/capability", s.handleCapabilityManifest)
	mux.HandleFunc("/v1/marketplace/openstandard/receipt", s.handleBillingReceipt)
}

// PluginsHandler returns the raw list/publish handler.
func (s *RegistryService) PluginsHandler() http.HandlerFunc { return s.handlePlugins }

// PluginByIDHandler returns the raw get-by-id handler.
func (s *RegistryService) PluginByIDHandler() http.HandlerFunc { return s.handlePluginByID }

// CapabilityManifestHandler returns the raw capability handler.
func (s *RegistryService) CapabilityManifestHandler() http.HandlerFunc { return s.handleCapabilityManifest }

// BillingReceiptHandler returns the raw receipt handler.
func (s *RegistryService) BillingReceiptHandler() http.HandlerFunc { return s.handleBillingReceipt }

func (s *RegistryService) handlePlugins(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listPlugins(w, r)
	case http.MethodPost:
		s.publishPlugin(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *RegistryService) handlePluginByID(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.getPlugin(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *RegistryService) publishPlugin(w http.ResponseWriter, r *http.Request) {
	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	required := map[string]string{"id": req.ID, "name": req.Name, "version": req.Version, "creator_id": req.CreatorID, "public_key": req.PublicKey, "signature": req.Signature}
	for k, v := range required {
		if v == "" {
			http.Error(w, fmt.Sprintf("missing %s", k), http.StatusBadRequest)
			return
		}
	}
	manifest := VerifiedManifest{ID: req.ID, Name: req.Name, Version: req.Version, CreatorID: req.CreatorID, WASMHash: req.WASMHash, PublicKey: []byte(req.PublicKey), Signature: []byte(req.Signature), Payload: []byte(fmt.Sprintf(`{"id":"%s","version":"%s"}`, req.ID, req.Version))}
	meta := Metadata{ID: req.ID, Name: req.Name, Version: req.Version, CreatorID: req.CreatorID, UpdatedAt: time.Now()}
	if err := s.store.Put(r.Context(), manifest, meta); err != nil {
		http.Error(w, fmt.Sprintf("store error: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(meta)
}

func (s *RegistryService) listPlugins(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.List(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("store error: %v", err), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(items)
}

func (s *RegistryService) getPlugin(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/v1/marketplace/plugins/"):]
	if id == "" {
		http.Error(w, "missing plugin id", http.StatusBadRequest)
		return
	}
	manifest, meta, ok := s.store.Get(r.Context(), id)
	if !ok {
		http.Error(w, "plugin not found", http.StatusNotFound)
		return
	}
	resp := struct {
		Metadata Metadata        `json:"metadata"`
		Manifest VerifiedManifest `json:"manifest"`
	}{Metadata: meta, Manifest: manifest}
	_ = json.NewEncoder(w).Encode(resp)
}

// VerifyManifest ensures a manifest has required registry fields.
func (s *RegistryService) VerifyManifest(ctx context.Context, pluginID string) (*VerifiedManifest, error) {
	manifest, _, ok := s.store.Get(ctx, pluginID)
	if !ok {
		return nil, errors.New("plugin not found")
	}
	return &manifest, nil
}

// PublishManifest writes a manifest JSON file for a plugin package.
func PublishManifest(destDir, id, name, version, creatorID, wasmPath, publicKey, signature string) (string, error) {
	hash, err := hashFile(wasmPath)
	if err != nil {
		return "", err
	}
	manifest := PublishRequest{ID: id, Name: name, Version: version, CreatorID: creatorID, WASMHash: hash, PublicKey: publicKey, Signature: signature}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	out := filepath.Join(destDir, "manifest.json")
	if err := os.WriteFile(out, payload, 0o644); err != nil {
		return "", err
	}
	return out, nil
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}

func (s *RegistryService) handleCapabilityManifest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var m CapabilityManifest
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
			return
		}
		m.UpdatedAt = time.Now()
		if err := m.Validate(); err != nil {
			http.Error(w, fmt.Sprintf("invalid manifest: %v", err), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(m)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *RegistryService) handleBillingReceipt(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var rec BillingReceipt
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
			return
		}
		rec.RecordedAt = time.Now()
		if err := rec.Validate(); err != nil {
			http.Error(w, fmt.Sprintf("invalid receipt: %v", err), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(rec)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
