package secrets

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used by a persistent secret store.
const (
	BucketSecrets  = "secrets_entries"
	BucketRotation = "secrets_rotation"
	BucketMeta     = "secrets_meta"
	metaStateKey   = "state"
)

// Persistence errors.
var (
	// ErrEphemeralKey is returned when persistence is requested for a store
	// whose key was generated randomly because AEROLLM_SECRETS_KEY is unset.
	ErrEphemeralKey = errors.New("secrets: persistence requires " + EnvKey + " (32 bytes, hex or base64): " +
		"with an ephemeral key the persisted secrets would be unreadable after a restart")
	// ErrKeyMismatch is returned when persisted secrets were encrypted with
	// a different key than the configured one.
	ErrKeyMismatch = errors.New("secrets: persisted secrets were encrypted with a different key; set " +
		EnvKey + " to the key they were written with")
	// ErrRotationIncomplete reports that a key rotation took effect but its
	// on-disk bookkeeping did not finish, or that a rotation was interrupted.
	// The rotation completes automatically when the store is next opened
	// with the new key.
	ErrRotationIncomplete = errors.New("secrets: key rotation incomplete; restart with the new " + EnvKey + " to complete it")
	// ErrPersistenceBroken is returned for writes after a failed rotation
	// could not be rolled back; restart with the new key.
	ErrPersistenceBroken = errors.New("secrets: persisted state is inconsistent after a failed key rotation; restart with the new " + EnvKey)
)

// storedVersion is the persisted form of one encrypted version.
type storedVersion struct {
	N          int    `json:"n"`
	Nonce      []byte `json:"nonce"`
	Ciphertext []byte `json:"ciphertext"`
	CreatedAt  int64  `json:"created_at"`
}

// storedSecret is the persisted form of a secret: metadata, ciphertexts and
// the fingerprint of the key they are encrypted with. It never contains a
// plaintext value.
type storedSecret struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Type      string            `json:"type,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt int64             `json:"created_at"`
	UpdatedAt int64             `json:"updated_at"`
	KeyID     string            `json:"key_id"`
	Versions  []storedVersion   `json:"versions"`
}

// secretsState records which key the persisted secrets are encrypted with
// and whether a rotation to NewKeyID is in progress.
type secretsState struct {
	KeyID    string `json:"key_id"`
	Rotating bool   `json:"rotating,omitempty"`
	NewKeyID string `json:"new_key_id,omitempty"`
}

func toStored(e *entry, keyID string) storedSecret {
	doc := storedSecret{ID: e.id, Name: e.name, Type: e.typ, Metadata: e.metadata,
		CreatedAt: e.createdAt, UpdatedAt: e.updatedAt, KeyID: keyID}
	for _, v := range e.versions {
		doc.Versions = append(doc.Versions, storedVersion{N: v.n, Nonce: v.nonce, Ciphertext: v.ciphertext, CreatedAt: v.createdAt})
	}
	return doc
}

func (d storedSecret) entry() *entry {
	e := &entry{id: d.ID, name: d.Name, typ: d.Type, metadata: d.Metadata, createdAt: d.CreatedAt, updatedAt: d.UpdatedAt}
	for _, v := range d.Versions {
		e.versions = append(e.versions, version{n: v.N, nonce: v.Nonce, ciphertext: v.Ciphertext, createdAt: v.CreatedAt})
	}
	return e
}

// secretsDisk mirrors a Store to a persist.Store. Used under the Store lock.
type secretsDisk struct {
	ps     persist.Store
	broken error // set when the persisted state can no longer be updated safely
}

func (d *secretsDisk) put(e *entry, keyID string) error {
	if err := d.ps.Put(BucketSecrets, e.id, toStored(e, keyID)); err != nil {
		return fmt.Errorf("secrets: persist %s: %w", e.id, err)
	}
	return nil
}

func (d *secretsDisk) remove(id string) error {
	if err := d.ps.Delete(BucketSecrets, id); err != nil {
		return fmt.Errorf("secrets: persist delete %s: %w", id, err)
	}
	return nil
}

// writableLocked reports whether the store may be modified.
func (s *Store) writableLocked() error {
	if s.disk != nil && s.disk.broken != nil {
		return s.disk.broken
	}
	return nil
}

// rotate re-keys the persisted secrets crash-safely:
//
//  1. the re-encrypted documents are staged in BucketRotation;
//  2. the state is marked {rotating to newID};
//  3. every document in BucketSecrets is overwritten with its staged copy;
//  4. the state is set to {newID};
//  5. the staged copies are deleted.
//
// A crash after step 2 is completed on the next open with the new key (see
// recoverRotation). A failure before step 3 leaves everything unchanged; a
// failure during step 3 is rolled back by rewriting the old documents.
// Failures in steps 4-5 return ErrRotationIncomplete: the new key is in
// effect and the bookkeeping is finished on the next open.
func (d *secretsDisk) rotate(oldID, newID string, old, next map[string]*entry) error {
	if oldID == newID {
		// Same key (fresh nonces only): every document is readable with the
		// key whether or not it was rewritten.
		for _, e := range next {
			if err := d.put(e, newID); err != nil {
				return err
			}
		}
		return nil
	}
	cleanupStaging := func() {
		for id := range next {
			_ = d.ps.Delete(BucketRotation, id)
		}
	}
	for id, e := range next {
		if err := d.ps.Put(BucketRotation, id, toStored(e, newID)); err != nil {
			cleanupStaging()
			return fmt.Errorf("secrets: stage rotation of %s: %w", id, err)
		}
	}
	if err := d.ps.Put(BucketMeta, metaStateKey, secretsState{KeyID: oldID, Rotating: true, NewKeyID: newID}); err != nil {
		cleanupStaging()
		return fmt.Errorf("secrets: record rotation start: %w", err)
	}
	var applied []string
	for id, e := range next {
		if err := d.put(e, newID); err != nil {
			// Roll back to the old key.
			for _, done := range applied {
				if rbErr := d.put(old[done], oldID); rbErr != nil {
					d.broken = fmt.Errorf("%w (rotation failed: %v; rollback failed: %v)", ErrPersistenceBroken, err, rbErr)
					return d.broken
				}
			}
			if rbErr := d.ps.Put(BucketMeta, metaStateKey, secretsState{KeyID: oldID}); rbErr != nil {
				// Documents are all under the old key again, but the state
				// still says "rotating": the next open with the old key rolls
				// back (no document uses the new key). Drop the staged copies
				// so an open with the new key fails instead of resurrecting
				// them over later writes.
				cleanupStaging()
				return fmt.Errorf("secrets: rotation failed and was rolled back (state update failed: %v): %w", rbErr, err)
			}
			cleanupStaging()
			return fmt.Errorf("secrets: rotation failed and was rolled back: %w", err)
		}
		applied = append(applied, id)
	}
	if err := d.ps.Put(BucketMeta, metaStateKey, secretsState{KeyID: newID}); err != nil {
		return fmt.Errorf("%w: %v", ErrRotationIncomplete, err)
	}
	for id := range next {
		if err := d.ps.Delete(BucketRotation, id); err != nil {
			return fmt.Errorf("%w: delete staged copy: %v", ErrRotationIncomplete, err)
		}
	}
	return nil
}

func loadStored(ps persist.Store, bucket string) (map[string]storedSecret, error) {
	docs, err := persist.LoadAll[storedSecret](ps, bucket)
	if err != nil {
		return nil, fmt.Errorf("secrets: load %s: %w", bucket, err)
	}
	for id, d := range docs {
		if d.ID != id {
			return nil, fmt.Errorf("secrets: load %s/%s: document does not match its id", bucket, id)
		}
	}
	return docs, nil
}

// recoverRotation finishes or rolls back an interrupted rotation so that
// every document in BucketSecrets is encrypted with keyID.
func recoverRotation(ps persist.Store, st secretsState, keyID string) error {
	main, err := loadStored(ps, BucketSecrets)
	if err != nil {
		return err
	}
	staged, err := loadStored(ps, BucketRotation)
	if err != nil {
		return err
	}
	switch keyID {
	case st.NewKeyID: // roll forward
		for id, doc := range main {
			if doc.KeyID == keyID {
				continue // already rotated (or rewritten after rotating)
			}
			sd, ok := staged[id]
			if !ok || sd.KeyID != keyID {
				return fmt.Errorf("%w: no re-encrypted copy of %s", ErrKeyMismatch, id)
			}
			if err := ps.Put(BucketSecrets, id, sd); err != nil {
				return fmt.Errorf("secrets: complete rotation of %s: %w", id, err)
			}
		}
	case st.KeyID: // roll back, possible only if nothing was rotated yet
		for _, doc := range main {
			if doc.KeyID == st.NewKeyID {
				return ErrRotationIncomplete
			}
		}
	default:
		return ErrKeyMismatch
	}
	if err := ps.Put(BucketMeta, metaStateKey, secretsState{KeyID: keyID}); err != nil {
		return fmt.Errorf("secrets: record rotation state: %w", err)
	}
	return nil
}

// EnablePersistence loads the secrets persisted in ps and writes every
// subsequent change through to it. Only ciphertexts and non-secret metadata
// are persisted.
//
// It fails with ErrEphemeralKey when the store's key is ephemeral
// (AEROLLM_SECRETS_KEY unset), with the key error when the key is invalid,
// and with ErrKeyMismatch when the persisted data was encrypted with a
// different key. An interrupted key rotation is completed (when opened with
// the new key) or rolled back. Secrets already in memory are written to ps
// and take precedence over persisted ones with the same ID.
func (s *Store) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("secrets: nil persist store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.disk != nil:
		return errors.New("secrets: persistence already enabled")
	case s.aead == nil:
		if s.keyErr != nil {
			return s.keyErr
		}
		return ErrKeyUnavailable
	case s.ephemeral:
		return ErrEphemeralKey
	}

	var st secretsState
	if _, err := ps.Get(BucketMeta, metaStateKey, &st); err != nil {
		return fmt.Errorf("secrets: load state: %w", err)
	}
	if st.Rotating {
		if err := recoverRotation(ps, st, s.keyID); err != nil {
			return err
		}
		st = secretsState{KeyID: s.keyID}
	}
	if st.KeyID != "" && st.KeyID != s.keyID {
		return ErrKeyMismatch
	}
	docs, err := loadStored(ps, BucketSecrets)
	if err != nil {
		return err
	}
	for id, doc := range docs {
		if doc.KeyID != s.keyID {
			return fmt.Errorf("%w (secret %s)", ErrKeyMismatch, id)
		}
	}
	// Staged copies of a finished (or rolled back) rotation are obsolete.
	if err := ps.ForEach(BucketRotation, func(id string, _ json.RawMessage) error {
		return ps.Delete(BucketRotation, id)
	}); err != nil {
		return fmt.Errorf("secrets: clean up rotation staging: %w", err)
	}
	if st.KeyID == "" {
		if err := ps.Put(BucketMeta, metaStateKey, secretsState{KeyID: s.keyID}); err != nil {
			return fmt.Errorf("secrets: record key: %w", err)
		}
	}
	d := &secretsDisk{ps: ps}
	for _, e := range s.secrets {
		if err := d.put(e, s.keyID); err != nil {
			return err
		}
	}
	for id, doc := range docs {
		if _, ok := s.secrets[id]; !ok {
			s.secrets[id] = doc.entry()
		}
	}
	s.disk = d
	return nil
}

// Persistent reports whether the store writes through to a persist.Store.
func (s *Store) Persistent() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.disk != nil
}

// NewPersistentStore creates a store keyed from AEROLLM_SECRETS_KEY and
// enables persistence to ps. Unlike NewStore it fails when the variable is
// unset (ErrEphemeralKey) or invalid, since persisted secrets must stay
// readable after a restart.
func NewPersistentStore(ps persist.Store) (*Store, error) {
	s := NewStore()
	if err := s.EnablePersistence(ps); err != nil {
		return nil, err
	}
	return s, nil
}

// NewPersistentStoreWithKey creates a store with an explicit 32-byte key and
// enables persistence to ps.
func NewPersistentStoreWithKey(key []byte, ps persist.Store) (*Store, error) {
	s, err := NewStoreWithKey(key)
	if err != nil {
		return nil, err
	}
	if err := s.EnablePersistence(ps); err != nil {
		return nil, err
	}
	return s, nil
}
