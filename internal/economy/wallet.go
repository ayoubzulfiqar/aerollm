package economy

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrInsufficientBalance is returned when a wallet does not have enough funds.
var ErrInsufficientBalance = errors.New("economy: insufficient balance")

// WalletID identifies an economy participant.
type WalletID string

// Transaction records a balance change.
type Transaction struct {
	ID        string
	From      WalletID
	To        WalletID
	Amount    float64
	Reason    string
	Timestamp time.Time
}

// Wallet stores balance and transaction history for an economy participant.
type Wallet interface {
	ID() WalletID
	Balance(ctx context.Context) (float64, error)
	Credit(ctx context.Context, amount float64, reason string) (*Transaction, error)
	Debit(ctx context.Context, amount float64, reason string) (*Transaction, error)
	History(ctx context.Context, limit int) ([]Transaction, error)
}

// LedgerStore persists wallet state across restarts.
type LedgerStore interface {
	Balance(ctx context.Context, id WalletID) (float64, error)
	SetBalance(ctx context.Context, id WalletID, balance float64) error
	AppendTransaction(ctx context.Context, tx Transaction) error
	Transactions(ctx context.Context, id WalletID, limit int) ([]Transaction, error)
}

// InMemoryWalletStore is a simple non-CRDT wallet backing store for development.
type InMemoryWalletStore struct {
	mu         sync.RWMutex
	balances   map[WalletID]float64
	transactions []Transaction
}

// NewInMemoryWalletStore creates a new in-memory wallet store.
func NewInMemoryWalletStore() *InMemoryWalletStore {
	return &InMemoryWalletStore{balances: make(map[WalletID]float64), transactions: make([]Transaction, 0)}
}

// Wallet returns a Wallet handle for the given identifier.
func (s *InMemoryWalletStore) Wallet(_ context.Context, id WalletID) (Wallet, error) {
	if id == "" {
		return nil, fmt.Errorf("economy: missing wallet id")
	}
	return NewDefaultWallet(id, s), nil
}

// Balance returns the current balance.
func (s *InMemoryWalletStore) Balance(_ context.Context, id WalletID) (float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.balances[id], nil
}

// SetBalance sets the balance.
func (s *InMemoryWalletStore) SetBalance(_ context.Context, id WalletID, balance float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.balances[id] = balance
	return nil
}

// AppendTransaction appends a transaction.
func (s *InMemoryWalletStore) AppendTransaction(_ context.Context, tx Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transactions = append(s.transactions, tx)
	return nil
}

// Transactions returns recent transactions for a wallet.
func (s *InMemoryWalletStore) Transactions(_ context.Context, id WalletID, limit int) ([]Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > len(s.transactions) {
		limit = len(s.transactions)
	}
	out := make([]Transaction, 0, limit)
	for i := len(s.transactions) - 1; i >= 0; i-- {
		tx := s.transactions[i]
		if tx.From == id || tx.To == id {
			out = append(out, tx)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// DefaultWallet is a simple wallet backed by a LedgerStore.
type DefaultWallet struct {
	id     WalletID
	store  LedgerStore
	prefix string
}

// NewDefaultWallet creates a new wallet.
func NewDefaultWallet(id WalletID, store LedgerStore) *DefaultWallet {
	return &DefaultWallet{id: id, store: store, prefix: fmt.Sprintf("wallet-%s", id)}
}

// ID returns the wallet identifier.
func (w *DefaultWallet) ID() WalletID { return w.id }

// Balance returns the current wallet balance.
func (w *DefaultWallet) Balance(ctx context.Context) (float64, error) {
	return w.store.Balance(ctx, w.id)
}

// Credit adds funds to the wallet.
func (w *DefaultWallet) Credit(ctx context.Context, amount float64, reason string) (*Transaction, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("economy: credit amount must be positive")
	}
	balance, err := w.store.Balance(ctx, w.id)
	if err != nil {
		return nil, err
	}
	balance += amount
	if err := w.store.SetBalance(ctx, w.id, balance); err != nil {
		return nil, err
	}
	tx := w.newTransaction("", w.id, amount, reason)
	if err := w.store.AppendTransaction(ctx, tx); err != nil {
		return nil, err
	}
	return &tx, nil
}

// Debit removes funds from the wallet.
func (w *DefaultWallet) Debit(ctx context.Context, amount float64, reason string) (*Transaction, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("economy: debit amount must be positive")
	}
	balance, err := w.store.Balance(ctx, w.id)
	if err != nil {
		return nil, err
	}
	if balance < amount {
		return nil, fmt.Errorf("%w: %s has %.4f, need %.4f", ErrInsufficientBalance, w.id, balance, amount)
	}
	balance -= amount
	if err := w.store.SetBalance(ctx, w.id, balance); err != nil {
		return nil, err
	}
	tx := w.newTransaction(w.id, "", amount, reason)
	if err := w.store.AppendTransaction(ctx, tx); err != nil {
		return nil, err
	}
	return &tx, nil
}

// History returns recent transactions for the wallet.
func (w *DefaultWallet) History(ctx context.Context, limit int) ([]Transaction, error) {
	return w.store.Transactions(ctx, w.id, limit)
}

func (w *DefaultWallet) newTransaction(from, to WalletID, amount float64, reason string) Transaction {
	tx := Transaction{
		ID:        fmt.Sprintf("%s-%d-%d", w.prefix, time.Now().UnixNano(), randomUint64()),
		Amount:    amount,
		Reason:    reason,
		Timestamp: time.Now().UTC(),
	}
	if from != "" {
		tx.From = from
	}
	if to != "" {
		tx.To = to
	}
	return tx
}

func randomUint64() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.LittleEndian.Uint64(b[:])
}
