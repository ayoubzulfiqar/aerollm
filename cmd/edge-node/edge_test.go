package main

import (
	"testing"

	"go.etcd.io/bbolt"
)

func TestBboltWalletStoreRoundTrip(t *testing.T) {
	db, err := bbolt.Open(t.TempDir()+"/edge.db", 0o600, &bbolt.Options{Timeout: 1})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	defer db.Close()
	_ = db.Update(func(tx *bbolt.Tx) error {
		for _, b := range [][]byte{[]byte("edge"), []byte("wallets")} {
			_, _ = tx.CreateBucketIfNotExists(b)
		}
		return nil
	})
	store := newBboltWalletStore(db)
	w, err := store.Wallet(nil, "w1")
	if err != nil {
		t.Fatalf("wallet: %v", err)
	}
	if _, err := w.Balance(nil); err != nil {
		t.Fatalf("balance: %v", err)
	}
	if _, err := w.Credit(nil, 10, "seed"); err != nil {
		t.Fatalf("credit: %v", err)
	}
	if _, err := w.Debit(nil, 3, "spend"); err != nil {
		t.Fatalf("debit: %v", err)
	}
	if bal, _ := w.Balance(nil); bal != 7 {
		t.Fatalf("expected balance 7, got %.2f", bal)
	}
	if _, err := w.History(nil, 10); err != nil {
		t.Fatalf("history: %v", err)
	}
}
