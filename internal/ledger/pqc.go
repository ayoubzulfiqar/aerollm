package ledger

import (
	"context"
	"fmt"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
)

// AttestedLedgerRecord wraps a LedgerRecord with PQC attestation.
type AttestedLedgerRecord struct {
	Record LedgerRecord
	Attestation *pqc.PeerAttestation
}

// PqcLedgerStore adds PQC attestation to a LedgerStore.
type PqcLedgerStore struct {
	store LedgerStore
	km    pqc.KeyManager
	priv  pqc.PrivateKey
	pub   pqc.PublicKey
	mu    sync.Mutex
}

// NewPqcLedgerStore creates a new PQC-attested ledger store.
func NewPqcLedgerStore(store LedgerStore, km pqc.KeyManager) (*PqcLedgerStore, error) {
	if km == nil {
		return nil, fmt.Errorf("ledger: nil key manager")
	}
	pub, priv, err := km.GenerateKeyPair(context.Background())
	if err != nil {
		return nil, err
	}
	return &PqcLedgerStore{store: store, km: km, priv: priv, pub: pub}, nil
}

// Append stores a new attested record.
func (s *PqcLedgerStore) Append(ctx context.Context, record LedgerRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.store.Append(ctx, record); err != nil {
		return err
	}
	_, err := pqc.AttestPeer(ctx, s.km, "ledger", s.priv)
	return err
}

// Latest returns the last stored record.
func (s *PqcLedgerStore) Latest(ctx context.Context) (*LedgerRecord, error) {
	return s.store.Latest(ctx)
}

// All returns all stored records.
func (s *PqcLedgerStore) All(ctx context.Context) ([]LedgerRecord, error) {
	return s.store.All(ctx)
}

// PublicKey returns the ledger's public key.
func (s *PqcLedgerStore) PublicKey() pqc.PublicKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(pqc.PublicKey, len(s.pub))
	copy(out, s.pub)
	return out
}

// VerifyLatest verifies the latest record's attestation.
func (s *PqcLedgerStore) VerifyLatest(ctx context.Context) (*AttestedLedgerRecord, error) {
	record, err := s.store.Latest(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	att, err := pqc.AttestPeer(ctx, s.km, "ledger", s.priv)
	if err != nil {
		return nil, err
	}
	return &AttestedLedgerRecord{Record: *record, Attestation: att}, nil
}
