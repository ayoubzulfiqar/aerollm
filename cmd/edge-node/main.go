package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/economy"
	"github.com/ayoubzulfiqar/aerollm/internal/hardware"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/ayoubzulfiqar/aerollm/internal/sandbox"
	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
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

	walletStore := newBboltWalletStore(db)
	wallet, err := walletStore.Wallet(ctx, "edge-wallet")
	if err != nil {
		fmt.Fprintf(os.Stderr, "wallet init error: %v\n", err)
		os.Exit(1)
	}
	_ = wallet

	_ = sandbox.NewWasmExecutor()

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

	openManifest := toOpenStandardCapabilityManifest(caps)
	openReceipt := marketplace.BillingReceipt{
		ReceiptID:  "edge-invoice",
		CustomerID: string(peerID),
		ProviderID: "edge",
		EventName:  "compute",
		Value:      0,
		Currency:   "USD",
		RecordedAt: time.Now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/marketplace/openstandard/capability", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var m marketplace.CapabilityManifest
			if err := json.NewDecoder(r.Body).Decode(&m); err != nil { respondErr(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest); return }
			m.UpdatedAt = time.Now()
			if err := m.Validate(); err != nil { respondErr(w, fmt.Sprintf("invalid manifest: %v", err), http.StatusBadRequest); return }
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(m)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/marketplace/openstandard/capability/self", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(openManifest)
			return
		}
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			var m marketplace.CapabilityManifest
			if err := json.NewDecoder(r.Body).Decode(&m); err != nil { respondErr(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest); return }
			m.UpdatedAt = time.Now()
			if err := m.Validate(); err != nil { respondErr(w, fmt.Sprintf("invalid manifest: %v", err), http.StatusBadRequest); return }
			openManifest = m
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(openManifest)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})
	mux.HandleFunc("/v1/marketplace/openstandard/receipt", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var rec marketplace.BillingReceipt
			if err := json.NewDecoder(r.Body).Decode(&rec); err != nil { respondErr(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest); return }
			rec.RecordedAt = time.Now()
			if err := rec.Validate(); err != nil { respondErr(w, fmt.Sprintf("invalid receipt: %v", err), http.StatusBadRequest); return }
			_ = queueReceipt(db, rec)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(rec)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/marketplace/openstandard/receipt/self", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(openReceipt)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/v1/edge/capabilities", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"peer_id": string(peerID),
			"hardware": hardware.AdvertisedCapabilities(caps),
			"wallet": func() string {
				b, _ := wallet.Balance(ctx)
				return fmt.Sprintf("%.2f", b)
			}(),
		})
	})

	pqcKM := pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridEd25519MLDSA65)
	mux.HandleFunc("/v1/edge/pqc/handshake", pqc.HandshakeHandler(pqcKM))
	mux.HandleFunc("/v1/edge/spatial/stream", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		spatial.NewVideo3DStreamHandler().StreamResponse(w, r, r.Body)
	})

	addr := ":7910"
	if env := strings.TrimSpace(os.Getenv("EDGE_LISTEN")); env != "" { addr = env }
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
			fmt.Printf("edge http server stopped: %v\n", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("edge-node shutting down")
}

func respondErr(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct{ Error string `json:"error"` }{Error: msg})
}

func toOpenStandardCapabilityManifest(caps []hardware.Capability) marketplace.CapabilityManifest {
	gpuName := ""
	osName := ""
	memGB := 0
	for _, c := range caps {
		switch c.Name {
		case "cuda", "metal", "rocm", "vulkan":
			if c.Available { gpuName = c.Name }
		case "cpu":
			osName = c.Detail
		}
	}
	return marketplace.CapabilityManifest{
		Version: "1.0",
		Hardware: marketplace.Hardware{
			HasLocalGPU: gpuName != "",
			GPUName:     gpuName,
			OS:          osName,
			MemoryGB:    memGB,
		},
		Billing: marketplace.Billing{
			SupportsMetered: true,
			Currency:        "USD",
			InvoiceURL:      fmt.Sprintf("http://localhost%s/v1/marketplace/openstandard/receipt/self", envOr(":7910", "EDGE_LISTEN")),
		},
		Capabilities: []string{"mesh", "wasm", "billing", "privacy"},
		UpdatedAt:    time.Now(),
	}
}

func queueReceipt(db *bbolt.DB, rec marketplace.BillingReceipt) error {
	b, _ := jsonMarshal(rec)
	return db.Update(func(tx *bbolt.Tx) error {
		q, _ := tx.CreateBucketIfNotExists([]byte("edge"))
		r, _ := q.CreateBucketIfNotExists([]byte("receipts"))
		return r.Put([]byte(rec.ReceiptID), b)
	})
}

func envOr(def, key string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" { return v }
	return def
}

func localPeerID(db *bbolt.DB) mesh.PeerID {
	var id string
	_ = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("edge"))
		if b == nil { return nil }
		sb := b.Bucket([]byte("state"))
		if sb != nil { id = string(sb.Get([]byte("peer_id"))) }
		return nil
	})
	if id != "" { return mesh.PeerID(id) }
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

func newBboltWalletStore(db *bbolt.DB) *bboltWalletStore { return &bboltWalletStore{db: db} }

func (s *bboltWalletStore) Wallet(_ context.Context, id economy.WalletID) (economy.Wallet, error) {
	if id == "" { return nil, fmt.Errorf("economy: missing wallet id") }
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
		if b == nil { return nil }
		wb := b.Bucket([]byte("wallets"))
		if wb == nil { return nil }
		v := wb.Get(key)
		if v != nil { _, _ = fmt.Sscanf(string(v), "%f", &balance) }
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
	switch vv := v.(type) {
	case economy.Transaction:
		return []byte(fmt.Sprintf(`{"id":"%s","from":"%s","to":"%s","amount":%f,"reason":"%s","timestamp":"%s"}`, vv.ID, vv.From, vv.To, vv.Amount, vv.Reason, vv.Timestamp.Format(time.RFC3339Nano))), nil
	default:
		return []byte(fmt.Sprintf("%v", v)), nil
	}
}
