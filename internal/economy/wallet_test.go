package economy

import (
	"context"
	"errors"
	"testing"
)

func TestDefaultWalletCreditDebit(t *testing.T) {
	store := NewInMemoryWalletStore()
	wallet := NewDefaultWallet("tenant-1", store)

	_, err := wallet.Credit(context.Background(), 10, "deposit")
	if err != nil {
		t.Fatalf("credit failed: %v", err)
	}

	balance, err := wallet.Balance(context.Background())
	if err != nil {
		t.Fatalf("balance failed: %v", err)
	}
	if balance != 10 {
		t.Fatalf("expected balance 10, got %f", balance)
	}

	tx, err := wallet.Debit(context.Background(), 3, "tool-call")
	if err != nil {
		t.Fatalf("debit failed: %v", err)
	}
	if tx.Amount != 3 || tx.From != "tenant-1" {
		t.Fatalf("unexpected tx: %+v", tx)
	}

	balance, err = wallet.Balance(context.Background())
	if err != nil {
		t.Fatalf("balance failed: %v", err)
	}
	if balance != 7 {
		t.Fatalf("expected balance 7, got %f", balance)
	}

	if _, err := wallet.Debit(context.Background(), 10, "overdraft"); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("expected insufficient balance, got %v", err)
	}
}
