package economy

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"time"
)

// ErrInsufficientBalance is returned when a wallet does not have enough funds.
var ErrInsufficientBalance = errors.New("economy: insufficient balance")

// ErrInvalidAmount is returned for NaN, infinite, negative, zero (where a
// positive amount is required) or out-of-range amounts.
var ErrInvalidAmount = errors.New("economy: invalid amount")

// NanosPerUnit is the fixed-point scale used internally: all balances are
// kept as int64 nano-units (1e-9 of a currency unit), so concurrent credits
// and debits never accumulate floating-point error while sub-micro tool
// prices stay representable.
const NanosPerUnit = 1_000_000_000

// maxAmount bounds amounts so nano-unit values fit in int64.
const maxAmount = 9e9

// ToNanos converts an amount to integer nano-units, rejecting NaN,
// infinities, negative and out-of-range values.
func ToNanos(amount float64) (int64, error) {
	if math.IsNaN(amount) || math.IsInf(amount, 0) || amount < 0 || amount > maxAmount {
		return 0, fmt.Errorf("%w: %v", ErrInvalidAmount, amount)
	}
	return int64(math.Round(amount * NanosPerUnit)), nil
}

// FromNanos converts nano-units back to a float amount.
func FromNanos(m int64) float64 { return float64(m) / NanosPerUnit }

// positiveNanos validates a strictly positive amount.
func positiveNanos(amount float64, op string) (int64, error) {
	m, err := ToNanos(amount)
	if err != nil {
		return 0, err
	}
	if m <= 0 {
		return 0, fmt.Errorf("%w: %s amount must be positive", ErrInvalidAmount, op)
	}
	return m, nil
}

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
	// AmountNanos is the exact amount in nano-units.
	AmountNanos int64
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

// AtomicLedgerStore is an optional LedgerStore extension. Stores that
// implement it apply balance changes and record the transaction as one
// atomic step, which makes Credit/Debit/transfers safe across processes.
type AtomicLedgerStore interface {
	LedgerStore
	// TransferNanos atomically moves amount nano-units from `from` to
	// `to` and records tx. An empty from mints into `to` (credit); an
	// empty to burns from `from` (debit). It returns an error wrapping
	// ErrInsufficientBalance, without changing anything, when from's
	// balance is below amount.
	TransferNanos(ctx context.Context, from, to WalletID, amount int64, tx Transaction) error
}

// walletLocks serialises the read-modify-write fallback used for plain
// LedgerStores, per wallet ID (striped). It protects against concurrent
// updates within one process; use an AtomicLedgerStore across processes.
var walletLocks [64]sync.Mutex

func lockFor(id WalletID) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return &walletLocks[h.Sum32()%uint32(len(walletLocks))]
}

// lockPair locks the stripes of two wallets in a fixed order (no deadlock).
func lockPair(a, b WalletID) func() {
	la, lb := lockFor(a), lockFor(b)
	if la == lb {
		la.Lock()
		return la.Unlock
	}
	ha, hb := fnv.New32a(), fnv.New32a()
	_, _ = ha.Write([]byte(a))
	_, _ = hb.Write([]byte(b))
	if ha.Sum32()%64 > hb.Sum32()%64 {
		la, lb = lb, la
	}
	la.Lock()
	lb.Lock()
	return func() { lb.Unlock(); la.Unlock() }
}

// maxInMemoryTransactions bounds the in-memory transaction log.
const maxInMemoryTransactions = 100_000

// InMemoryWalletStore is a thread-safe in-memory wallet backing store. It
// implements AtomicLedgerStore with exact integer nano-unit balances.
type InMemoryWalletStore struct {
	mu           sync.RWMutex
	balances     map[WalletID]int64
	transactions []Transaction
}

// NewInMemoryWalletStore creates a new in-memory wallet store.
func NewInMemoryWalletStore() *InMemoryWalletStore {
	return &InMemoryWalletStore{balances: make(map[WalletID]int64), transactions: make([]Transaction, 0)}
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
	return FromNanos(s.balances[id]), nil
}

// BalanceNanos returns the exact balance in nano-units.
func (s *InMemoryWalletStore) BalanceNanos(_ context.Context, id WalletID) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.balances[id], nil
}

// SetBalance sets the balance (an administrative override). Negative, NaN
// and infinite balances are rejected.
func (s *InMemoryWalletStore) SetBalance(_ context.Context, id WalletID, balance float64) error {
	m, err := ToNanos(balance)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.balances[id] = m
	return nil
}

// AppendTransaction appends a transaction.
func (s *InMemoryWalletStore) AppendTransaction(_ context.Context, tx Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendLocked(tx)
	return nil
}

func (s *InMemoryWalletStore) appendLocked(tx Transaction) {
	if len(s.transactions) >= maxInMemoryTransactions {
		// Drop the oldest 10% in one go to amortise the copy.
		drop := maxInMemoryTransactions / 10
		s.transactions = append(s.transactions[:0], s.transactions[drop:]...)
	}
	s.transactions = append(s.transactions, tx)
}

// TransferNanos implements AtomicLedgerStore.
func (s *InMemoryWalletStore) TransferNanos(_ context.Context, from, to WalletID, amount int64, tx Transaction) error {
	if amount <= 0 {
		return fmt.Errorf("%w: transfer amount must be positive", ErrInvalidAmount)
	}
	if from == "" && to == "" {
		return fmt.Errorf("economy: transfer needs a source or destination")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if from != "" {
		if bal := s.balances[from]; bal < amount {
			return fmt.Errorf("%w: %s has %.9f, need %.9f", ErrInsufficientBalance, from, FromNanos(bal), FromNanos(amount))
		}
	}
	if to != "" && s.balances[to] > math.MaxInt64-amount {
		return fmt.Errorf("%w: balance overflow", ErrInvalidAmount)
	}
	if from != "" {
		s.balances[from] -= amount
	}
	if to != "" {
		s.balances[to] += amount
	}
	s.appendLocked(tx)
	return nil
}

// Transactions returns recent transactions for a wallet, newest first.
func (s *InMemoryWalletStore) Transactions(_ context.Context, id WalletID, limit int) ([]Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > len(s.transactions) {
		limit = len(s.transactions)
	}
	out := make([]Transaction, 0, min(limit, 64))
	for i := len(s.transactions) - 1; i >= 0 && len(out) < limit; i-- {
		tx := s.transactions[i]
		if tx.From == id || tx.To == id {
			out = append(out, tx)
		}
	}
	return out, nil
}

var _ AtomicLedgerStore = (*InMemoryWalletStore)(nil)

// DefaultWallet is a wallet backed by a LedgerStore. When the store is an
// AtomicLedgerStore every balance change is a single atomic store
// operation; otherwise changes are serialised per wallet within the
// process and the transaction append is rolled back into the balance if it
// fails.
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

// Credit adds funds to the wallet. The amount must be finite and positive.
func (w *DefaultWallet) Credit(ctx context.Context, amount float64, reason string) (*Transaction, error) {
	m, err := positiveNanos(amount, "credit")
	if err != nil {
		return nil, err
	}
	tx := w.newTransaction("", w.id, m, reason)
	if as, ok := w.store.(AtomicLedgerStore); ok {
		if err := as.TransferNanos(ctx, "", w.id, m, tx); err != nil {
			return nil, err
		}
		return &tx, nil
	}
	unlock := lockPair(w.id, w.id)
	defer unlock()
	if err := adjustBalance(ctx, w.store, w.id, m); err != nil {
		return nil, err
	}
	if err := w.store.AppendTransaction(ctx, tx); err != nil {
		_ = adjustBalance(ctx, w.store, w.id, -m)
		return nil, err
	}
	return &tx, nil
}

// Debit removes funds from the wallet. The amount must be finite and
// positive; overdrafts return ErrInsufficientBalance.
func (w *DefaultWallet) Debit(ctx context.Context, amount float64, reason string) (*Transaction, error) {
	m, err := positiveNanos(amount, "debit")
	if err != nil {
		return nil, err
	}
	tx := w.newTransaction(w.id, "", m, reason)
	if as, ok := w.store.(AtomicLedgerStore); ok {
		if err := as.TransferNanos(ctx, w.id, "", m, tx); err != nil {
			return nil, err
		}
		return &tx, nil
	}
	unlock := lockPair(w.id, w.id)
	defer unlock()
	if err := adjustBalance(ctx, w.store, w.id, -m); err != nil {
		return nil, err
	}
	if err := w.store.AppendTransaction(ctx, tx); err != nil {
		_ = adjustBalance(ctx, w.store, w.id, m)
		return nil, err
	}
	return &tx, nil
}

// adjustBalance performs a read-modify-write on a plain LedgerStore in
// nano-units; callers must hold the wallet lock.
func adjustBalance(ctx context.Context, store LedgerStore, id WalletID, delta int64) error {
	bal, err := store.Balance(ctx, id)
	if err != nil {
		return err
	}
	if math.IsNaN(bal) || math.IsInf(bal, 0) {
		return fmt.Errorf("%w: stored balance of %s is corrupt", ErrInvalidAmount, id)
	}
	cur := int64(math.Round(bal * NanosPerUnit))
	next := cur + delta
	if delta < 0 && next < 0 {
		return fmt.Errorf("%w: %s has %.9f, need %.9f", ErrInsufficientBalance, id, FromNanos(cur), FromNanos(-delta))
	}
	return store.SetBalance(ctx, id, FromNanos(next))
}

// Transfer atomically (per AtomicLedgerStore) moves amount from one wallet
// to another and returns the recorded transaction. With a plain
// LedgerStore both wallets are locked in-process and the debit is refunded
// if the credit fails.
func Transfer(ctx context.Context, store LedgerStore, from, to WalletID, amount float64, reason string) (*Transaction, error) {
	if from == "" || to == "" {
		return nil, fmt.Errorf("economy: transfer requires both wallets")
	}
	m, err := positiveNanos(amount, "transfer")
	if err != nil {
		return nil, err
	}
	tx := newTx(fmt.Sprintf("transfer-%s-%s", from, to), from, to, m, reason)
	if from == to {
		return &tx, nil
	}
	if as, ok := store.(AtomicLedgerStore); ok {
		if err := as.TransferNanos(ctx, from, to, m, tx); err != nil {
			return nil, err
		}
		return &tx, nil
	}
	unlock := lockPair(from, to)
	defer unlock()
	if err := adjustBalance(ctx, store, from, -m); err != nil {
		return nil, err
	}
	if err := adjustBalance(ctx, store, to, m); err != nil {
		_ = adjustBalance(ctx, store, from, m)
		return nil, err
	}
	if err := store.AppendTransaction(ctx, tx); err != nil {
		_ = adjustBalance(ctx, store, to, -m)
		_ = adjustBalance(ctx, store, from, m)
		return nil, err
	}
	return &tx, nil
}

// History returns recent transactions for the wallet.
func (w *DefaultWallet) History(ctx context.Context, limit int) ([]Transaction, error) {
	return w.store.Transactions(ctx, w.id, limit)
}

func (w *DefaultWallet) newTransaction(from, to WalletID, nanos int64, reason string) Transaction {
	return newTx(w.prefix, from, to, nanos, reason)
}

func newTx(prefix string, from, to WalletID, nanos int64, reason string) Transaction {
	return Transaction{
		ID:          fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), randomUint64()),
		From:        from,
		To:          to,
		Amount:      FromNanos(nanos),
		AmountNanos: nanos,
		Reason:      reason,
		Timestamp:   time.Now().UTC(),
	}
}

func randomUint64() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.LittleEndian.Uint64(b[:])
}
