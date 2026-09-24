package marketplace

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Pagination limits for the list endpoint.
const (
	DefaultListLimit = 50
	MaxListLimit     = 200
	maxListOffset    = 1_000_000
)

// Store persists plugin registry manifests and metadata.
type Store interface {
	Put(ctx context.Context, manifest VerifiedManifest, metadata Metadata) error
	Get(ctx context.Context, pluginID string) (VerifiedManifest, Metadata, bool)
	List(ctx context.Context) ([]Metadata, error)
}

// LookupStore is an optional Store extension that distinguishes a missing
// plugin (ErrNotFound) from a backend failure.
type LookupStore interface {
	Lookup(ctx context.Context, pluginID string) (VerifiedManifest, Metadata, error)
}

// Metadata captures a marketplace plugin listing entry.
type Metadata struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Version   string    `json:"version"`
	CreatorID string    `json:"creator_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PublishRequest is the body for publishing a plugin to the registry. It is
// also the on-the-wire signed manifest document: Signature is a base64
// Ed25519 signature by PublicKey over CanonicalBytes (see SignManifest).
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
	mu        sync.RWMutex
	manifests map[string]VerifiedManifest
	meta      map[string]Metadata
}

// NewInMemoryStore returns an in-memory registry store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{manifests: make(map[string]VerifiedManifest), meta: make(map[string]Metadata)}
}

// Put stores (or replaces) a manifest and its metadata.
func (s *InMemoryStore) Put(_ context.Context, m VerifiedManifest, meta Metadata) error {
	if m.ID == "" {
		return fmt.Errorf("%w: missing id", ErrInvalidManifest)
	}
	if meta.ID != "" && meta.ID != m.ID {
		return fmt.Errorf("%w: metadata id %q does not match manifest id %q", ErrInvalidManifest, meta.ID, m.ID)
	}
	meta.ID = m.ID
	if meta.UpdatedAt.IsZero() {
		meta.UpdatedAt = time.Now().UTC()
	}
	m = cloneManifest(m)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manifests[m.ID] = m
	s.meta[m.ID] = meta
	return nil
}

// Lookup returns the manifest and metadata or ErrNotFound.
func (s *InMemoryStore) Lookup(_ context.Context, pluginID string) (VerifiedManifest, Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok1 := s.manifests[pluginID]
	meta, ok2 := s.meta[pluginID]
	if !ok1 || !ok2 {
		return VerifiedManifest{}, Metadata{}, ErrNotFound
	}
	return cloneManifest(m), meta, nil
}

// Get returns the manifest and metadata for pluginID.
func (s *InMemoryStore) Get(ctx context.Context, pluginID string) (VerifiedManifest, Metadata, bool) {
	m, meta, err := s.Lookup(ctx, pluginID)
	return m, meta, err == nil
}

// List returns all metadata sorted by ID.
func (s *InMemoryStore) List(_ context.Context) ([]Metadata, error) {
	s.mu.RLock()
	out := make([]Metadata, 0, len(s.meta))
	for _, m := range s.meta {
		out = append(out, m)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func cloneManifest(m VerifiedManifest) VerifiedManifest {
	m.Signature = append([]byte(nil), m.Signature...)
	m.PublicKey = append([]byte(nil), m.PublicKey...)
	m.Payload = append([]byte(nil), m.Payload...)
	return m
}

// lookup resolves a plugin through LookupStore when available.
func lookup(ctx context.Context, store Store, id string) (VerifiedManifest, Metadata, error) {
	if ls, ok := store.(LookupStore); ok {
		return ls.Lookup(ctx, id)
	}
	m, meta, ok := store.Get(ctx, id)
	if !ok {
		return VerifiedManifest{}, Metadata{}, ErrNotFound
	}
	return m, meta, nil
}

// RegistryService exposes marketplace registry HTTP handlers.
//
// Publishing requires a manifest whose Ed25519 signature verifies over its
// canonical encoding and whose key is trusted for its creator_id (by default
// trust-on-first-use: the first key a creator publishes with is pinned).
// A plugin ID stays owned by the creator that first published it, and a new
// publish must carry a strictly higher semantic version (re-publishing the
// identical signed manifest is idempotent).
type RegistryService struct {
	client *Client
	store  Store
	trust  *TrustStore

	// publishMu serialises the read-check-write publish sequence so two
	// concurrent publishes cannot both pass the ownership/version checks.
	publishMu sync.Mutex
	seeded    bool
	now       func() time.Time
}

// NewRegistryService creates a registry service with a trust-on-first-use
// key store. Use SetTrustStore to pin keys explicitly.
func NewRegistryService(client *Client, store Store) *RegistryService {
	if store == nil {
		store = NewInMemoryStore()
	}
	return &RegistryService{client: client, store: store, trust: NewTOFUTrustStore(), now: time.Now}
}

// SetTrustStore replaces the publisher key trust store (nil restores TOFU).
func (s *RegistryService) SetTrustStore(t *TrustStore) {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	if t == nil {
		t = NewTOFUTrustStore()
	}
	s.trust = t
	s.seeded = false
}

// seedTrustLocked pins, once, the keys of every manifest already in the store
// when running in trust-on-first-use mode. Without this a restart would forget
// every pinned key and let anyone publish under an existing creator_id with a
// fresh key. Caller must hold publishMu.
func (s *RegistryService) seedTrustLocked(ctx context.Context) error {
	if s.seeded || !s.trust.tofu {
		s.seeded = true
		return nil
	}
	items, err := s.store.List(ctx)
	if err != nil {
		return err
	}
	for _, it := range items {
		m, _, err := lookup(ctx, s.store, it.ID)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if ValidatePluginID(m.CreatorID) != nil || len(m.PublicKey) != ed25519.PublicKeySize {
			continue
		}
		if err := s.trust.Pin(m.CreatorID, m.PublicKey); err != nil {
			return err
		}
	}
	s.seeded = true
	return nil
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
func (s *RegistryService) CapabilityManifestHandler() http.HandlerFunc {
	return s.handleCapabilityManifest
}

// BillingReceiptHandler returns the raw receipt handler.
func (s *RegistryService) BillingReceiptHandler() http.HandlerFunc { return s.handleBillingReceipt }

// WriteJSONError writes {"error": msg} with the given status.
func WriteJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func methodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
}

// decodeBody strictly decodes a capped JSON request body into v and writes
// the error response itself; it reports whether decoding succeeded.
func decodeBody(w http.ResponseWriter, r *http.Request, maxBytes int64, v interface{}) bool {
	if r.Body == nil {
		WriteJSONError(w, http.StatusBadRequest, "missing request body")
		return false
	}
	body := http.MaxBytesReader(w, r.Body, maxBytes)
	defer body.Close()
	if err := decodeStrictJSON(body, maxBytes, v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) || errors.Is(err, ErrTooLarge) {
			WriteJSONError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", maxBytes))
			return false
		}
		WriteJSONError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return false
	}
	return true
}

func (s *RegistryService) handlePlugins(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		s.listPlugins(w, r)
	case http.MethodPost:
		s.publishPlugin(w, r)
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodHead, http.MethodPost)
	}
}

func (s *RegistryService) handlePluginByID(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		s.getPlugin(w, r)
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
	}
}

// Publish verifies and stores a signed manifest. It returns the stored
// metadata and whether a new record was written (false for an idempotent
// re-publish of the identical manifest).
func (s *RegistryService) Publish(ctx context.Context, req PublishRequest) (Metadata, bool, error) {
	manifest, err := VerifyPublishRequest(req)
	if err != nil {
		return Metadata{}, false, err
	}
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	if err := s.seedTrustLocked(ctx); err != nil {
		return Metadata{}, false, err
	}

	existing, existingMeta, err := lookup(ctx, s.store, manifest.ID)
	switch {
	case errors.Is(err, ErrNotFound):
	case err != nil:
		return Metadata{}, false, err
	default:
		if existing.CreatorID != manifest.CreatorID {
			return Metadata{}, false, ErrOwnership
		}
		cmp, ok := CompareVersions(manifest.Version, existing.Version)
		if ok && cmp == 0 && bytes.Equal(existing.Payload, manifest.Payload) && bytes.Equal(existing.Signature, manifest.Signature) {
			return existingMeta, false, nil
		}
		if !ok || cmp <= 0 {
			return Metadata{}, false, fmt.Errorf("%w: version %s must be greater than published %s", ErrVersionConflict, manifest.Version, existing.Version)
		}
	}
	if err := s.trust.Check(manifest.CreatorID, manifest.PublicKey); err != nil {
		return Metadata{}, false, err
	}
	meta := Metadata{ID: manifest.ID, Name: manifest.Name, Version: manifest.Version, CreatorID: manifest.CreatorID, UpdatedAt: s.now().UTC()}
	if err := s.store.Put(ctx, *manifest, meta); err != nil {
		return Metadata{}, false, err
	}
	return meta, true, nil
}

func (s *RegistryService) publishPlugin(w http.ResponseWriter, r *http.Request) {
	var req PublishRequest
	if !decodeBody(w, r, MaxManifestBytes, &req) {
		return
	}
	meta, created, err := s.Publish(r.Context(), req)
	switch {
	case err == nil && created:
		writeJSON(w, http.StatusCreated, meta)
	case err == nil:
		writeJSON(w, http.StatusOK, meta)
	case errors.Is(err, ErrInvalidSignature):
		WriteJSONError(w, http.StatusBadRequest, "invalid manifest signature")
	case errors.Is(err, ErrInvalidManifest):
		WriteJSONError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrUntrustedKey), errors.Is(err, ErrOwnership):
		WriteJSONError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, ErrVersionConflict):
		WriteJSONError(w, http.StatusConflict, err.Error())
	default:
		WriteJSONError(w, http.StatusInternalServerError, "registry store unavailable")
	}
}

func parseBoundedInt(raw string, def, min, max int) (int, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("must be an integer between %d and %d", min, max)
	}
	return n, nil
}

// listPlugins returns a page of plugins sorted by ID as a JSON array.
// Query: limit (1..200, default 50), offset (>=0), creator_id (optional filter).
// Pagination metadata is returned in X-Total-Count and, when more results
// exist, X-Next-Offset headers.
func (s *RegistryService) listPlugins(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := parseBoundedInt(q.Get("limit"), DefaultListLimit, 1, MaxListLimit)
	if err != nil {
		WriteJSONError(w, http.StatusBadRequest, "invalid limit: "+err.Error())
		return
	}
	offset, err := parseBoundedInt(q.Get("offset"), 0, 0, maxListOffset)
	if err != nil {
		WriteJSONError(w, http.StatusBadRequest, "invalid offset: "+err.Error())
		return
	}
	creator := q.Get("creator_id")
	if creator != "" && ValidatePluginID(creator) != nil {
		WriteJSONError(w, http.StatusBadRequest, "invalid creator_id")
		return
	}
	items, err := s.store.List(r.Context())
	if err != nil {
		WriteJSONError(w, http.StatusInternalServerError, "registry store unavailable")
		return
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	if creator != "" {
		filtered := items[:0]
		for _, it := range items {
			if it.CreatorID == creator {
				filtered = append(filtered, it)
			}
		}
		items = filtered
	}
	total := len(items)
	page := []Metadata{}
	if offset < total {
		end := offset + limit
		if end > total {
			end = total
		}
		page = items[offset:end]
		if end < total {
			w.Header().Set("X-Next-Offset", strconv.Itoa(end))
		}
	}
	w.Header().Set("X-Total-Count", strconv.Itoa(total))
	writeJSON(w, http.StatusOK, page)
}

// pluginIDFromPath extracts {id} from ".../plugins/{id}" without panicking on
// short or unexpected paths.
func pluginIDFromPath(path string) (string, bool) {
	const marker = "/plugins/"
	i := strings.Index(path, marker)
	if i < 0 {
		return "", false
	}
	id := path[i+len(marker):]
	id = strings.TrimSuffix(id, "/")
	if id == "" || strings.Contains(id, "/") {
		return "", false
	}
	return id, true
}

func (s *RegistryService) getPlugin(w http.ResponseWriter, r *http.Request) {
	id, ok := pluginIDFromPath(r.URL.Path)
	if !ok {
		WriteJSONError(w, http.StatusBadRequest, "missing plugin id")
		return
	}
	if ValidatePluginID(id) != nil {
		WriteJSONError(w, http.StatusBadRequest, "invalid plugin id")
		return
	}
	manifest, meta, err := lookup(r.Context(), s.store, id)
	if errors.Is(err, ErrNotFound) {
		WriteJSONError(w, http.StatusNotFound, "plugin not found")
		return
	}
	if err != nil {
		WriteJSONError(w, http.StatusInternalServerError, "registry store unavailable")
		return
	}
	resp := struct {
		Metadata Metadata         `json:"metadata"`
		Manifest VerifiedManifest `json:"manifest"`
	}{Metadata: meta, Manifest: manifest}
	writeJSON(w, http.StatusOK, resp)
}

// VerifyManifest loads a stored manifest and re-verifies its signature and
// publisher trust, so tampering with the backing store is detected.
func (s *RegistryService) VerifyManifest(ctx context.Context, pluginID string) (*VerifiedManifest, error) {
	manifest, _, err := lookup(ctx, s.store, pluginID)
	if err != nil {
		return nil, err
	}
	if err := manifest.Verify(); err != nil {
		return nil, err
	}
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	if err := s.seedTrustLocked(ctx); err != nil {
		return nil, err
	}
	if err := s.trust.Check(manifest.CreatorID, manifest.PublicKey); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// PublishManifest writes a manifest.json for a plugin package into destDir.
// The WASM file is hashed with SHA-256 (streamed, capped at MaxWASMBytes).
// publicKey and signature are written as given; use SignManifest to produce
// them. The file is written with 0644 permissions and never follows an
// existing symlink at the destination.
func PublishManifest(destDir, id, name, version, creatorID, wasmPath, publicKey, signature string) (string, error) {
	hash, err := hashFile(wasmPath)
	if err != nil {
		return "", err
	}
	manifest := PublishRequest{ID: id, Name: name, Version: version, CreatorID: creatorID, WASMHash: hash, PublicKey: publicKey, Signature: signature}
	if err := manifest.ValidateFields(); err != nil {
		return "", err
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	out := filepath.Join(destDir, "manifest.json")
	if fi, err := os.Lstat(out); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("marketplace: refusing to overwrite symlink %s", out)
	}
	if err := os.WriteFile(out, payload, 0o644); err != nil {
		return "", err
	}
	return out, nil
}

// hashFile returns the lowercase hex SHA-256 of the file at path. The
// previous implementation returned the hex of the whole file contents rather
// than a digest.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, MaxWASMBytes+1))
	if err != nil {
		return "", err
	}
	if n > MaxWASMBytes {
		return "", fmt.Errorf("marketplace: %s exceeds %d bytes", path, MaxWASMBytes)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (s *RegistryService) handleCapabilityManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var m CapabilityManifest
	if !decodeBody(w, r, MaxOpenStandardBytes, &m) {
		return
	}
	m.UpdatedAt = s.now().UTC()
	if err := m.Validate(); err != nil {
		WriteJSONError(w, http.StatusBadRequest, "invalid manifest: "+err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, m)
}

func (s *RegistryService) handleBillingReceipt(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var rec BillingReceipt
	if !decodeBody(w, r, MaxOpenStandardBytes, &rec) {
		return
	}
	rec.RecordedAt = s.now().UTC()
	if err := rec.Validate(); err != nil {
		WriteJSONError(w, http.StatusBadRequest, "invalid receipt: "+err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, rec)
}
