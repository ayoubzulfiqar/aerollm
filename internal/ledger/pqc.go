package ledger

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
)

// Metadata keys written by PqcLedgerStore.
const (
	MetadataSignature          = "pqc_signature"
	MetadataSignatureAlgorithm = "pqc_algorithm"
)

// AttestedLedgerRecord wraps a LedgerRecord with PQC attestation.
type AttestedLedgerRecord struct {
	Record      LedgerRecord
	Attestation *pqc.PeerAttestation
}

// PqcLedgerStore signs every appended record's chain hash with the store's
// key and can verify those signatures. The signature (base64) is stored in
// Metadata[MetadataSignature]; Metadata is not part of the chain hash, so
// signing does not alter the chain.
//
// Note: with the hybrid pqc suites the signature is Ed25519 (see package
// pqc: ML-DSA is not available), so signatures are classical, not
// post-quantum.
type PqcLedgerStore struct {
	store LedgerStore
	km    pqc.KeyManager
	priv  pqc.PrivateKey
	pub   pqc.PublicKey
	alg   string
	mu    sync.Mutex // serializes appends for stores without atomic chaining
}

// NewPqcLedgerStore creates a new signing ledger store. It fails if the key
// manager cannot produce and verify signatures.
func NewPqcLedgerStore(store LedgerStore, km pqc.KeyManager) (*PqcLedgerStore, error) {
	if km == nil {
		return nil, fmt.Errorf("ledger: nil key manager")
	}
	if store == nil {
		return nil, fmt.Errorf("ledger: nil store")
	}
	ctx := context.Background()
	pub, priv, err := km.GenerateKeyPair(ctx)
	if err != nil {
		return nil, err
	}
	probe := []byte("aerollm-ledger-probe")
	sig, err := km.Sign(ctx, priv, probe)
	if err != nil {
		return nil, fmt.Errorf("ledger: key manager cannot sign: %w", err)
	}
	if err := km.Verify(ctx, pub, probe, sig); err != nil {
		return nil, fmt.Errorf("ledger: key manager cannot verify: %w", err)
	}
	alg := ""
	if a, ok := km.(interface{ Algorithm() string }); ok {
		alg = a.Algorithm()
	}
	return &PqcLedgerStore{store: store, km: km, priv: priv, pub: pub, alg: alg}, nil
}

func signedMessage(r *LedgerRecord) []byte {
	return []byte("aerollm-ledger-sig-v1|" + r.PrevHash + "|" + r.ChainHash)
}

func (s *PqcLedgerStore) sign(ctx context.Context, rec *LedgerRecord) error {
	sig, err := s.km.Sign(ctx, s.priv, signedMessage(rec))
	if err != nil {
		return err
	}
	md := make(map[string]interface{}, len(rec.Metadata)+2)
	for k, v := range rec.Metadata {
		md[k] = v
	}
	md[MetadataSignature] = base64.StdEncoding.EncodeToString(sig)
	if s.alg != "" {
		md[MetadataSignatureAlgorithm] = s.alg
	}
	rec.Metadata = md
	return nil
}

type sealedAppender interface {
	AppendChainedSealed(ctx context.Context, requestPayload, responsePayload string, metadata map[string]interface{}, seal SealFunc) (LedgerRecord, error)
}

// AppendChainedWithMetadata atomically chains, signs and stores a record.
func (s *PqcLedgerStore) AppendChainedWithMetadata(ctx context.Context, requestPayload, responsePayload string, metadata map[string]interface{}) (LedgerRecord, error) {
	if sa, ok := s.store.(sealedAppender); ok {
		return sa.AppendChainedSealed(ctx, requestPayload, responsePayload, metadata, func(rec *LedgerRecord) error {
			return s.sign(ctx, rec)
		})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := ""
	if latest, err := s.store.Latest(ctx); err == nil && latest != nil {
		prev = latest.ChainHash
	}
	rec := LedgerRecord{
		PrevHash:        prev,
		RequestHash:     hashHex(requestPayload),
		ResponseHash:    hashHex(responsePayload),
		ChainHash:       ComputeChainHash(prev, requestPayload, responsePayload),
		Metadata:        metadata,
		RequestPayload:  requestPayload,
		ResponsePayload: responsePayload,
	}
	if err := s.sign(ctx, &rec); err != nil {
		return LedgerRecord{}, err
	}
	if err := s.store.Append(ctx, rec); err != nil {
		return LedgerRecord{}, err
	}
	return rec, nil
}

// AppendChained atomically chains, signs and stores a record.
func (s *PqcLedgerStore) AppendChained(ctx context.Context, requestPayload, responsePayload string) (LedgerRecord, error) {
	return s.AppendChainedWithMetadata(ctx, requestPayload, responsePayload, nil)
}

// Append stores a signed record. When the underlying store supports atomic
// chaining the record is (re)chained and signed under its lock; otherwise
// the given hashes are signed as-is.
func (s *PqcLedgerStore) Append(ctx context.Context, record LedgerRecord) error {
	if _, ok := s.store.(sealedAppender); ok {
		_, err := s.AppendChainedWithMetadata(ctx, record.RequestPayload, record.ResponsePayload, record.Metadata)
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.sign(ctx, &record); err != nil {
		return err
	}
	return s.store.Append(ctx, record)
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
	out := make(pqc.PublicKey, len(s.pub))
	copy(out, s.pub)
	return out
}

// ErrUnsigned is returned when a record carries no signature.
var ErrUnsigned = errors.New("ledger: record is not signed")

// VerifyRecord checks a record's chain hash and signature.
func (s *PqcLedgerStore) VerifyRecord(ctx context.Context, rec LedgerRecord) error {
	if rec.ChainHash != ComputeChainHash(rec.PrevHash, rec.RequestPayload, rec.ResponsePayload) {
		return &ChainError{Reason: "chain hash mismatch"}
	}
	enc, _ := rec.Metadata[MetadataSignature].(string)
	if enc == "" {
		return ErrUnsigned
	}
	sig, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return fmt.Errorf("ledger: malformed signature: %w", err)
	}
	return s.km.Verify(ctx, s.pub, signedMessage(&rec), sig)
}

// VerifyLatest verifies the latest record's chain hash and signature and
// returns it together with a fresh signed attestation of the ledger's key.
func (s *PqcLedgerStore) VerifyLatest(ctx context.Context) (*AttestedLedgerRecord, error) {
	record, err := s.store.Latest(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.VerifyRecord(ctx, *record); err != nil {
		return nil, err
	}
	att, err := pqc.AttestPeer(ctx, s.km, "ledger", s.priv)
	if err != nil {
		return nil, err
	}
	return &AttestedLedgerRecord{Record: *record, Attestation: att}, nil
}

// Verify checks the chain links of the underlying store (when it supports
// Verify) and every record's signature.
func (s *PqcLedgerStore) Verify(ctx context.Context) error {
	if v, ok := s.store.(interface{ Verify(context.Context) error }); ok {
		if err := v.Verify(ctx); err != nil {
			return err
		}
	}
	records, err := s.store.All(ctx)
	if err != nil {
		return err
	}
	for i, r := range records {
		if err := s.VerifyRecord(ctx, r); err != nil {
			return fmt.Errorf("ledger: record %d: %w", i, err)
		}
	}
	return nil
}
