// Package secrets is a secret store with encryption at rest.
//
// Values are encrypted with AES-256-GCM using a random 96-bit nonce per
// version and the secret ID/version as additional authenticated data, so
// ciphertexts cannot be swapped between secrets. The key comes from
// AEROLLM_SECRETS_KEY (32 bytes, hex or base64). If the variable is unset an
// ephemeral random key is generated (IsEphemeral reports this); if it is set
// but invalid the store refuses all writes (KeyError reports why).
//
// By default secrets live in memory only. EnablePersistence (or
// NewPersistentStore) writes every change through to a persist.Store; only
// ciphertexts, nonces and non-secret metadata are persisted, together with
// a fingerprint of the encryption key, so a store opened with the wrong key
// fails loudly instead of returning undecryptable data. Persistence refuses
// ephemeral keys. See persistent.go for the crash-safe key rotation.
//
// List and Get never return secret values; values are only returned by
// Reveal / RevealVersion (or the HTTP handler with ?reveal=true).
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// EnvKey is the environment variable holding the 32-byte encryption key.
const EnvKey = "AEROLLM_SECRETS_KEY"

// Limits.
const (
	MaxValueBytes      = 64 << 10
	MaxBodyBytes       = 1 << 20
	DefaultMaxVersions = 10
	maxMetadataEntries = 32
	maxMetadataKeyLen  = 64
	maxMetadataValLen  = 256
	maxTypeLen         = 64
)

// Secret is the write/reveal representation of a secret. Value is only
// populated by Reveal/RevealVersion.
type Secret struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Value     string            `json:"value,omitempty"`
	Type      string            `json:"type"`
	Metadata  map[string]string `json:"metadata"`
	CreatedAt int64             `json:"created_at"`
	UpdatedAt int64             `json:"updated_at,omitempty"`
	Version   int               `json:"version,omitempty"`
}

// SecretMetadata is the non-sensitive view of a secret returned by listings.
type SecretMetadata struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Metadata  map[string]string `json:"metadata"`
	CreatedAt int64             `json:"created_at"`
	UpdatedAt int64             `json:"updated_at"`
	Version   int               `json:"version"`
	Versions  []int             `json:"versions"`
}

// Errors.
var (
	ErrNotFound       = errors.New("secrets: not found")
	ErrKeyUnavailable = errors.New("secrets: encryption key unavailable")
	ErrDecrypt        = errors.New("secrets: decryption failed")
)

// ValidationError reports invalid input; its message is safe for clients.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

type version struct {
	n          int
	nonce      []byte
	ciphertext []byte
	createdAt  int64
}

type entry struct {
	id, name, typ string
	metadata      map[string]string
	createdAt     int64
	updatedAt     int64
	versions      []version // ascending
}

// clone returns a copy of e that can be modified without affecting e.
// Version ciphertexts are immutable and shared.
func (e *entry) clone() *entry {
	c := *e
	c.metadata = maps.Clone(e.metadata)
	c.versions = slices.Clone(e.versions)
	return &c
}

// Store manages encrypted secrets. It is safe for concurrent use.
type Store struct {
	mu          sync.RWMutex
	secrets     map[string]*entry
	aead        cipher.AEAD
	keyID       string // fingerprint of the current key (see keyFingerprint)
	ephemeral   bool
	keyErr      error
	maxVersions int
	audit       func(action, id string, version int)

	disk *secretsDisk // non-nil when persistence is enabled
}

// keyFingerprint identifies a key without revealing it (HMAC-SHA256 of a
// fixed label under the key, truncated to 128 bits).
func keyFingerprint(key []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("aerollm-secrets-key-id-v1"))
	return hex.EncodeToString(m.Sum(nil)[:16])
}

// NewStore creates a secret store keyed from AEROLLM_SECRETS_KEY (see the
// package documentation for the fallback behaviour).
func NewStore() *Store {
	raw := strings.TrimSpace(os.Getenv(EnvKey))
	if raw == "" {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return &Store{secrets: map[string]*entry{}, keyErr: fmt.Errorf("secrets: generate key: %w", err)}
		}
		s, err := NewStoreWithKey(key)
		if err != nil {
			return &Store{secrets: map[string]*entry{}, keyErr: err}
		}
		s.ephemeral = true
		return s
	}
	key, err := ParseKey(raw)
	if err == nil {
		var s *Store
		if s, err = NewStoreWithKey(key); err == nil {
			return s
		}
	}
	return &Store{secrets: map[string]*entry{}, maxVersions: DefaultMaxVersions,
		keyErr: fmt.Errorf("%w: invalid %s: %v", ErrKeyUnavailable, EnvKey, err)}
}

// ParseKey decodes a 32-byte key given as hex or (raw/std/url) base64.
func ParseKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if b, err := hex.DecodeString(s); err == nil && len(b) == 32 {
		return b, nil
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil && len(b) == 32 {
			return b, nil
		}
	}
	return nil, errors.New("key must be 32 bytes encoded as hex or base64")
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, errors.New("secrets: key must be 32 bytes (AES-256)")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// NewStoreWithKey creates a store with an explicit 32-byte key.
func NewStoreWithKey(key []byte) (*Store, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	return &Store{secrets: map[string]*entry{}, aead: aead, keyID: keyFingerprint(key), maxVersions: DefaultMaxVersions}, nil
}

// IsEphemeral reports whether the store uses a random per-process key
// (AEROLLM_SECRETS_KEY unset): encrypted values cannot outlive the process.
func (s *Store) IsEphemeral() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ephemeral
}

// KeyError returns why the store has no usable key (nil if it has one).
func (s *Store) KeyError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.keyErr
}

// SetMaxVersions sets how many versions are retained per secret (>= 1).
func (s *Store) SetMaxVersions(n int) {
	if n < 1 {
		n = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxVersions = n
}

// SetAuditHook registers a callback invoked on "upsert", "reveal" and
// "delete" (never with secret values).
func (s *Store) SetAuditHook(fn func(action, id string, version int)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = fn
}

var (
	nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$`)
	idRe   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,159}$`)
)

func validate(sec *Secret) error {
	sec.Name = strings.TrimSpace(sec.Name)
	sec.ID = strings.TrimSpace(sec.ID)
	if sec.ID == "" && sec.Name == "" {
		return &ValidationError{"name is required"}
	}
	if sec.Name != "" && (!nameRe.MatchString(sec.Name) || strings.Contains(sec.Name, "..")) {
		return &ValidationError{"invalid name: use 1-128 of [A-Za-z0-9._/-], starting alphanumeric, no '..'"}
	}
	if sec.ID != "" && (!idRe.MatchString(sec.ID) || strings.Contains(sec.ID, "..")) {
		return &ValidationError{"invalid id"}
	}
	if sec.Value == "" {
		return &ValidationError{"value is required"}
	}
	if len(sec.Value) > MaxValueBytes {
		return &ValidationError{fmt.Sprintf("value exceeds %d bytes", MaxValueBytes)}
	}
	if len(sec.Type) > maxTypeLen || strings.IndexFunc(sec.Type, unicode.IsControl) >= 0 {
		return &ValidationError{"invalid type"}
	}
	if len(sec.Metadata) > maxMetadataEntries {
		return &ValidationError{fmt.Sprintf("at most %d metadata entries", maxMetadataEntries)}
	}
	for k, v := range sec.Metadata {
		if k == "" || len(k) > maxMetadataKeyLen || len(v) > maxMetadataValLen {
			return &ValidationError{"invalid metadata entry"}
		}
	}
	return nil
}

func aad(id string, n int) []byte {
	return []byte("aerollm-secret-v1|" + id + "|" + strconv.Itoa(n))
}

// Upsert adds a secret or, if the ID exists, stores a new version of it
// (rotation). The ID defaults to "sec_" + name. The value is encrypted
// before storage; the plaintext is not retained.
func (s *Store) Upsert(secret Secret) error {
	if err := validate(&secret); err != nil {
		return err
	}
	if secret.ID == "" {
		secret.ID = "sec_" + secret.Name
	}
	now := time.Now().Unix()

	s.mu.Lock()
	if s.aead == nil {
		err := s.keyErr
		s.mu.Unlock()
		if err == nil {
			err = ErrKeyUnavailable
		}
		return err
	}
	if err := s.writableLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("secrets: nonce: %w", err)
	}
	// Build the new state on a copy so a failed persistence write leaves
	// the store unchanged.
	cur, ok := s.secrets[secret.ID]
	var e *entry
	if ok {
		e = cur.clone()
	} else {
		name := secret.Name
		if name == "" {
			name = secret.ID
		}
		created := secret.CreatedAt
		if created <= 0 {
			created = now
		}
		e = &entry{id: secret.ID, name: name, createdAt: created}
	}
	if secret.Name != "" {
		e.name = secret.Name
	}
	e.typ = secret.Type
	if secret.Metadata != nil || !ok {
		e.metadata = maps.Clone(secret.Metadata)
	}
	n := 1
	if len(e.versions) > 0 {
		n = e.versions[len(e.versions)-1].n + 1
	}
	ct := s.aead.Seal(nil, nonce, []byte(secret.Value), aad(e.id, n))
	e.versions = append(e.versions, version{n: n, nonce: nonce, ciphertext: ct, createdAt: now})
	if max := s.maxVersions; max > 0 && len(e.versions) > max {
		e.versions = slices.Clone(e.versions[len(e.versions)-max:])
	}
	e.updatedAt = now
	if s.disk != nil {
		if err := s.disk.put(e, s.keyID); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	s.secrets[secret.ID] = e
	audit := s.audit
	s.mu.Unlock()
	if audit != nil {
		audit("upsert", secret.ID, n)
	}
	return nil
}

func (e *entry) meta() SecretMetadata {
	m := SecretMetadata{
		ID: e.id, Name: e.name, Type: e.typ, Metadata: maps.Clone(e.metadata),
		CreatedAt: e.createdAt, UpdatedAt: e.updatedAt,
	}
	for _, v := range e.versions {
		m.Versions = append(m.Versions, v.n)
	}
	if len(e.versions) > 0 {
		m.Version = e.versions[len(e.versions)-1].n
	}
	return m
}

func (m SecretMetadata) secret() Secret {
	return Secret{ID: m.ID, Name: m.Name, Type: m.Type, Metadata: m.Metadata,
		CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt, Version: m.Version}
}

// Metadata returns the non-sensitive view of a secret.
func (s *Store) Metadata(id string) (SecretMetadata, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.secrets[id]
	if !ok {
		return SecretMetadata{}, false
	}
	return e.meta(), true
}

// Get retrieves a secret's metadata by id. The Value field is always empty;
// use Reveal to obtain the plaintext.
func (s *Store) Get(id string) (Secret, bool) {
	m, ok := s.Metadata(id)
	if !ok {
		return Secret{}, false
	}
	return m.secret(), true
}

// Reveal decrypts and returns the latest version of a secret.
func (s *Store) Reveal(id string) (Secret, error) {
	return s.RevealVersion(id, 0)
}

// RevealVersion decrypts a specific version (0 = latest).
func (s *Store) RevealVersion(id string, n int) (Secret, error) {
	s.mu.RLock()
	if s.aead == nil {
		s.mu.RUnlock()
		return Secret{}, ErrKeyUnavailable
	}
	e, ok := s.secrets[id]
	if !ok || len(e.versions) == 0 {
		s.mu.RUnlock()
		return Secret{}, ErrNotFound
	}
	var v *version
	if n == 0 {
		v = &e.versions[len(e.versions)-1]
	} else {
		for i := range e.versions {
			if e.versions[i].n == n {
				v = &e.versions[i]
			}
		}
	}
	if v == nil {
		s.mu.RUnlock()
		return Secret{}, ErrNotFound
	}
	sec := e.meta().secret()
	nonce, ct, vn := v.nonce, v.ciphertext, v.n
	audit := s.audit
	aead := s.aead
	s.mu.RUnlock()

	pt, err := aead.Open(nil, nonce, ct, aad(id, vn))
	if err != nil {
		return Secret{}, ErrDecrypt
	}
	sec.Value = string(pt)
	sec.Version = vn
	if audit != nil {
		audit("reveal", id, vn)
	}
	return sec, nil
}

// List returns metadata for all secrets (Value is always empty), sorted by
// ID.
func (s *Store) List() []Secret {
	metas := s.ListMetadata()
	out := make([]Secret, len(metas))
	for i, m := range metas {
		out[i] = m.secret()
	}
	return out
}

// ListMetadata returns metadata for all secrets sorted by ID.
func (s *Store) ListMetadata() []SecretMetadata {
	s.mu.RLock()
	out := make([]SecretMetadata, 0, len(s.secrets))
	for _, e := range s.secrets {
		out = append(out, e.meta())
	}
	s.mu.RUnlock()
	slices.SortFunc(out, func(a, b SecretMetadata) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// Delete removes a secret (all versions) by id and reports whether it
// existed. With persistence enabled a failed write keeps the secret and
// returns false; use Remove to obtain the error.
func (s *Store) Delete(id string) bool {
	ok, err := s.Remove(id)
	return ok && err == nil
}

// Remove removes a secret (all versions) by id. It reports whether the
// secret existed and any persistence error (in which case the secret is
// kept).
func (s *Store) Remove(id string) (bool, error) {
	s.mu.Lock()
	_, ok := s.secrets[id]
	if ok {
		if err := s.writableLocked(); err != nil {
			s.mu.Unlock()
			return true, err
		}
		if s.disk != nil {
			if err := s.disk.remove(id); err != nil {
				s.mu.Unlock()
				return true, err
			}
		}
		// Ciphertexts are not wiped: RevealVersion may still be decrypting
		// them outside the lock, and they are not secret.
		delete(s.secrets, id)
	}
	audit := s.audit
	s.mu.Unlock()
	if ok && audit != nil {
		audit("delete", id, 0)
	}
	return ok, nil
}

// RotateKey re-encrypts every stored version under newKey. On error the
// store is unchanged, except for errors matching ErrRotationIncomplete,
// which mean the new key is already in effect (see persistent.go). With
// persistence enabled the persisted ciphertexts are re-encrypted as well;
// the process must be restarted with AEROLLM_SECRETS_KEY set to newKey.
func (s *Store) RotateKey(newKey []byte) error {
	newAead, err := newAEAD(newKey)
	if err != nil {
		return err
	}
	newID := keyFingerprint(newKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.aead == nil {
		return ErrKeyUnavailable
	}
	if err := s.writableLocked(); err != nil {
		return err
	}
	next := make(map[string]*entry, len(s.secrets))
	for id, e := range s.secrets {
		ne := e.clone()
		for i, v := range ne.versions {
			pt, err := s.aead.Open(nil, v.nonce, v.ciphertext, aad(e.id, v.n))
			if err != nil {
				return ErrDecrypt
			}
			nonce := make([]byte, newAead.NonceSize())
			if _, err := rand.Read(nonce); err != nil {
				return err
			}
			nv := v
			nv.nonce = nonce
			nv.ciphertext = newAead.Seal(nil, nonce, pt, aad(e.id, v.n))
			clear(pt)
			ne.versions[i] = nv
		}
		next[id] = ne
	}
	var rotErr error
	if s.disk != nil {
		rotErr = s.disk.rotate(s.keyID, newID, s.secrets, next)
		if rotErr != nil && !errors.Is(rotErr, ErrRotationIncomplete) {
			return rotErr
		}
	}
	s.secrets = next
	s.aead = newAead
	s.keyID = newID
	s.ephemeral = false
	s.keyErr = nil
	return rotErr
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// WebhookHandler exposes JSON CRUD for secrets. It performs no
// authentication itself: mount it behind admin authentication.
//
//	POST|PUT /       body {"name","value","type","metadata"[, "id"]} -> metadata
//	                 (an existing id gets a new version: rotation)
//	GET  /           -> list of metadata (never values)
//	GET  /?id=X      -> metadata
//	GET  /?id=X&reveal=true[&version=N] -> secret including its value
//	DELETE /?id=X    -> {"status":"deleted"}
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if store == nil {
			writeError(w, http.StatusServiceUnavailable, "secret store unavailable")
			return
		}
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			var sec Secret
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
			if err := dec.Decode(&sec); err != nil {
				var mbe *http.MaxBytesError
				if errors.As(err, &mbe) {
					writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
					return
				}
				writeError(w, http.StatusBadRequest, "bad request")
				return
			}
			if err := dec.Decode(&struct{}{}); err != io.EOF {
				writeError(w, http.StatusBadRequest, "bad request")
				return
			}
			if err := store.Upsert(sec); err != nil {
				var ve *ValidationError
				if errors.As(err, &ve) {
					writeError(w, http.StatusBadRequest, ve.Msg)
					return
				}
				writeError(w, http.StatusServiceUnavailable, "secret store unavailable")
				return
			}
			id := strings.TrimSpace(sec.ID)
			if id == "" {
				id = "sec_" + strings.TrimSpace(sec.Name)
			}
			m, _ := store.Metadata(id)
			writeJSON(w, http.StatusOK, m)
		case http.MethodGet:
			q := r.URL.Query()
			id := q.Get("id")
			if id == "" {
				writeJSON(w, http.StatusOK, store.ListMetadata())
				return
			}
			if reveal, _ := strconv.ParseBool(q.Get("reveal")); reveal {
				n := 0
				if v := q.Get("version"); v != "" {
					var err error
					if n, err = strconv.Atoi(v); err != nil || n < 1 {
						writeError(w, http.StatusBadRequest, "invalid version")
						return
					}
				}
				sec, err := store.RevealVersion(id, n)
				switch {
				case errors.Is(err, ErrNotFound):
					writeError(w, http.StatusNotFound, "not found")
				case err != nil:
					writeError(w, http.StatusInternalServerError, "failed to reveal secret")
				default:
					writeJSON(w, http.StatusOK, sec)
				}
				return
			}
			if m, ok := store.Metadata(id); ok {
				writeJSON(w, http.StatusOK, m)
				return
			}
			writeError(w, http.StatusNotFound, "not found")
		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" {
				writeError(w, http.StatusBadRequest, "missing id")
				return
			}
			ok, err := store.Remove(id)
			switch {
			case err != nil:
				writeError(w, http.StatusServiceUnavailable, "secret store unavailable")
			case ok:
				writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
			default:
				writeError(w, http.StatusNotFound, "not found")
			}
		default:
			w.Header().Set("Allow", "GET, POST, PUT, DELETE")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}
