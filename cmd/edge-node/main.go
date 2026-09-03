package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/economy"
	"github.com/ayoubzulfiqar/aerollm/internal/hardware"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
	"github.com/ayoubzulfiqar/aerollm/internal/sandbox"
	"go.etcd.io/bbolt"
)

const statePath = "edge-state.db"

func main() {
	ctx := context.Background()

	db, err := bbolt.Open(statePath, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "edge state open error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	peerID := localPeerID(db)
	transport := mesh.NewInMemoryTransport(peerID)
	discovery := mesh.NewDiscovery(mesh.DiscoveryConfig{
		LocalID:     peerID,
		BindAddress: "/ip4/127.0.0.1/tcp/0",
		Peers:       []mesh.PeerDescriptor{},
		Transport:   transport,
	})

	detector := hardware.NewLocalDetector()
	caps := detector.Detect()
	_ = caps

	walletStore := newBboltWalletStore(db)
	wallet, err := walletStore.Wallet(ctx, "edge-wallet")
	if err != nil {
		fmt.Fprintf(os.Stderr, "wallet init error: %v\n", err)
		os.Exit(1)
	}
	_ = wallet

	wasmExecutor := sandbox.NewWasmExecutor()
	_ = wasmExecutor

	registryStore := marketplace.NewInMemoryStore()
	_ = registryStore
	_ = marketplace.NewRegistryService(nil, registryStore)

	discovery.Start(ctx)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			_ = discovery.Peers()
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("edge-node shutting down")
}

func localPeerID(db *bbolt.DB) mesh.PeerID {
	var id string
	_ = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("edge"))
		if b == nil {
			return nil
		}
		sb := b.Bucket([]byte("state"))
		if sb != nil {
			id = string(sb.Get([]byte("peer_id")))
		}
		return nil
	})
	if id != "" {
		return mesh.PeerID(id)
	}
	id = fmt.Sprintf("edge-%d", time.Now().UnixNano())
	_ = db.Update(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("edge"))
		sb, _ := b.CreateBucketIfNotExists([]byte("state"))
		return sb.Put([]byte("peer_id"), []byte(id))
	})
	return mesh.PeerID(id)
}

type bboltWalletStore struct {
	db *bbolt.DB
}

func newBboltWalletStore(db *bbolt.DB) *bboltWalletStore {
	return &bboltWalletStore{db: db}
}

func (s *bboltWalletStore) Wallet(_ context.Context, id economy.WalletID) (economy.Wallet, error) {
	if id == "" {
		return nil, fmt.Errorf("economy: missing wallet id")
	}
	return economy.NewDefaultWallet(id, &bboltLedgerStore{db: s.db, prefix: fmt.Sprintf("wallet-%s", id)}), nil
}

type bboltLedgerStore struct {
	db     *bbolt.DB
	prefix string
}

func (s *bboltLedgerStore) Balance(_ context.Context, id economy.WalletID) (float64, error) {
	var balance float64
	key := []byte(s.prefix)
	_ = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("edge"))
		if b == nil {
			return nil
		}
		wb := b.Bucket([]byte("wallets"))
		if wb == nil {
			return nil
		}
		v := wb.Get(key)
		if v != nil {
			_, _ = fmt.Sscanf(string(v), "%f", &balance)
		}
		return nil
	})
	return balance, nil
}

func (s *bboltLedgerStore) SetBalance(_ context.Context, id economy.WalletID, balance float64) error {
	key := []byte(s.prefix)
	payload := []byte(fmt.Sprintf("%f", balance))
	return s.db.Update(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("edge"))
		wb, _ := b.CreateBucketIfNotExists([]byte("wallets"))
		return wb.Put(key, payload)
	})
}

func (s *bboltLedgerStore) AppendTransaction(_ context.Context, tx economy.Transaction) error {
	payload, _ := jsonMarshal(tx)
	return s.db.Update(func(btx *bbolt.Tx) error {
		b, _ := btx.CreateBucketIfNotExists([]byte("edge"))
		qb, _ := b.CreateBucketIfNotExists([]byte("queue"))
		k := []byte(tx.ID)
		return qb.Put(k, payload)
	})
}

func (s *bboltLedgerStore) Transactions(_ context.Context, id economy.WalletID, limit int) ([]economy.Transaction, error) {
	_ = id
	_ = limit
	return nil, nil
}

func jsonMarshal(v interface{}) ([]byte, error) {
	// minimal json marshal for primitive/struct values to avoid importing encoding/json in main.
	switch vv := v.(type) {
	case economy.Transaction:
		return []byte(fmt.Sprintf(`{"id":"%s","from":"%s","to":"%s","amount":%f,"reason":"%s","timestamp":"%s"}`,
			vv.ID, vv.From, vv.To, vv.Amount, vv.Reason, vv.Timestamp.Format(time.RFC3339Nano))), nil
	default:
		return []byte(fmt.Sprintf("%v", v)), nil
	}
}
