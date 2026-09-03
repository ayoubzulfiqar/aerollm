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
	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
	"github.com/ayoubzulfiqar/aerollm/internal/sandbox"
	"go.etcd.io/bbolt"
)

const statePath = "edge-state.db"

func localPeerID(db *bbolt.DB) mesh.PeerID {
	_ = db
	return mesh.PeerID(fmt.Sprintf("edge-%d", time.Now().UnixNano()))
}

func newSyncWorker(db *bbolt.DB, transport mesh.SecureTransport, discovery *mesh.Discovery) *syncWorker {
	return &syncWorker{db: db, transport: transport, discovery: discovery}
}

type syncWorker struct {
	db        *bbolt.DB
	transport mesh.SecureTransport
	discovery *mesh.Discovery
}

func (w *syncWorker) run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = w.discovery.Peers()
		}
	}
}

func main() {
	ctx := context.Background()

	db, err := bbolt.Open(statePath, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "edge state open error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	transport := mesh.NewInMemoryTransport(localPeerID(db))
	discovery := mesh.NewDiscovery(mesh.DiscoveryConfig{
		LocalID:     localPeerID(db),
		BindAddress: "/ip4/127.0.0.1/tcp/0",
		Peers:       []mesh.PeerDescriptor{},
		Transport:   transport,
	})

	syncWorker := newSyncWorker(db, transport, discovery)
	detector := hardware.NewLocalDetector()
	caps := detector.Detect()
	_ = caps

	walletStore := economy.NewInMemoryWalletStore()
	_ = walletStore
	wallet, _ := walletStore.Wallet(ctx, "edge-wallet")
	_ = wallet

	wasmExecutor := sandbox.NewWasmExecutor()
	_ = wasmExecutor

	discovery.Start(ctx)
	go syncWorker.run(ctx)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("edge-node shutting down")
}
