package main

import (
	"context"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

const (
	receiptBucket      = "gateway_receipts"
	maxMemoryReceipts  = 100000
	receiptOwnerAdmins = "*admin*"
)

// storedReceipt binds a billing receipt to the key that recorded it.
type storedReceipt struct {
	Owner   string                     `json:"owner"`
	Receipt marketplace.BillingReceipt `json:"receipt"`
}

// receiptSink stores open-standard billing receipts. Non-admin callers may
// only record receipts for themselves: customer_id must be empty (it is
// filled with the caller's key ID) or equal their key ID. Re-recording the
// same receipt is idempotent; reusing an ID with different content conflicts.
type receiptSink struct {
	mu  sync.Mutex
	ps  persist.Store
	mem map[string]storedReceipt
}

func newReceiptSink(ps persist.Store) *receiptSink {
	return &receiptSink{ps: ps, mem: make(map[string]storedReceipt)}
}

func (s *receiptSink) RecordReceipt(ctx context.Context, rec marketplace.BillingReceipt) (marketplace.BillingReceipt, bool, error) {
	p, ok := middleware.PrincipalFromContext(ctx)
	if !ok || p.KeyID == "" {
		return rec, false, marketplace.ErrReceiptForbidden
	}
	owner := p.KeyID
	if p.Admin {
		owner = receiptOwnerAdmins
	} else {
		if rec.CustomerID == "" {
			rec.CustomerID = p.KeyID
		}
		if rec.CustomerID != p.KeyID {
			return rec, false, marketplace.ErrReceiptForbidden
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	existing, found, err := s.load(rec.ReceiptID)
	if err != nil {
		return rec, false, err
	}
	if found {
		if existing.Owner != owner && owner != receiptOwnerAdmins {
			return rec, false, marketplace.ErrReceiptForbidden
		}
		if !sameReceipt(existing.Receipt, rec) {
			return rec, false, marketplace.ErrReceiptConflict
		}
		return existing.Receipt, false, nil
	}
	doc := storedReceipt{Owner: owner, Receipt: rec}
	if s.ps != nil {
		if err := s.ps.Put(receiptBucket, rec.ReceiptID, doc); err != nil {
			return rec, false, err
		}
	} else {
		if len(s.mem) >= maxMemoryReceipts {
			for k := range s.mem { // bounded memory: drop an arbitrary old receipt
				delete(s.mem, k)
				break
			}
		}
		s.mem[rec.ReceiptID] = doc
	}
	return rec, true, nil
}

func (s *receiptSink) load(id string) (storedReceipt, bool, error) {
	if s.ps == nil {
		r, ok := s.mem[id]
		return r, ok, nil
	}
	var r storedReceipt
	ok, err := s.ps.Get(receiptBucket, id, &r)
	return r, ok, err
}

// sameReceipt compares the client-supplied fields; RecordedAt is stamped by
// the server on every call, so a replay keeps the original timestamp.
func sameReceipt(a, b marketplace.BillingReceipt) bool {
	return a.ReceiptID == b.ReceiptID && a.CustomerID == b.CustomerID && a.ProviderID == b.ProviderID &&
		a.EventName == b.EventName && a.Value == b.Value && a.Currency == b.Currency
}
