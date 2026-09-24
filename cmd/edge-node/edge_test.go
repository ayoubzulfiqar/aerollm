package main

import (
	"context"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/economy"
	"go.etcd.io/bbolt"
)

func openTestDB(t *testing.T) *bbolt.DB {
	t.Helper()
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "edge.db"), 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestBboltWalletStoreRoundTrip(t *testing.T) {
	db := openTestDB(t)
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
	hist, err := w.History(nil, 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 2 || hist[0].Reason != "spend" || hist[1].Reason != "seed" {
		t.Fatalf("expected newest-first history [spend seed], got %+v", hist)
	}
	if _, err := store.Wallet(nil, ""); err == nil {
		t.Fatal("expected error for empty wallet id")
	}
}

func TestBboltWalletKeepsSubMicroPrecision(t *testing.T) {
	db := openTestDB(t)
	w, _ := newBboltWalletStore(db).Wallet(context.Background(), "w1")
	if _, err := w.Credit(context.Background(), 0.0000001, "tiny"); err != nil {
		t.Fatal(err)
	}
	// The previous "%f" encoding stored this as 0.000000.
	if bal, _ := w.Balance(context.Background()); bal != 0.0000001 {
		t.Fatalf("precision lost: %v", bal)
	}
	if _, err := w.Credit(context.Background(), math.NaN(), "nan"); err == nil {
		t.Fatal("expected NaN credit to be rejected")
	}
}

func TestBboltWalletTransactionJSONIsEscaped(t *testing.T) {
	db := openTestDB(t)
	w, _ := newBboltWalletStore(db).Wallet(context.Background(), "w1")
	reason := `evil","amount":1e9,"x":"`
	if _, err := w.Credit(context.Background(), 1, reason); err != nil {
		t.Fatal(err)
	}
	hist, err := w.History(context.Background(), 1)
	if err != nil || len(hist) != 1 {
		t.Fatalf("history: %v %v", hist, err)
	}
	if hist[0].Reason != reason || hist[0].Amount != 1 {
		t.Fatalf("transaction corrupted by injection: %+v", hist[0])
	}
}

func TestBboltWalletConcurrentCreditsAreSerialized(t *testing.T) {
	db := openTestDB(t)
	store := newBboltWalletStore(db)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine gets its own wallet handle for the same ID,
			// like concurrent HTTP handlers would.
			w, _ := store.Wallet(context.Background(), "shared")
			if _, err := w.Credit(context.Background(), 1, "c"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	w, _ := store.Wallet(context.Background(), "shared")
	if bal, _ := w.Balance(context.Background()); bal != 20 {
		t.Fatalf("lost updates: balance %v, want 20", bal)
	}
}

func TestBboltLedgerCorruptBalanceSurfacesError(t *testing.T) {
	db := openTestDB(t)
	_ = db.Update(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists(bucketEdge)
		wb, _ := b.CreateBucketIfNotExists(bucketWallets)
		return wb.Put([]byte("wallet-w1"), []byte("not-a-number"))
	})
	w, _ := newBboltWalletStore(db).Wallet(context.Background(), "w1")
	if _, err := w.Balance(context.Background()); err == nil {
		t.Fatal("expected corrupt balance error instead of silent zero")
	}
}

func TestLocalPeerIDIsStableAndRandom(t *testing.T) {
	db := openTestDB(t)
	id1, err := localPeerID(db)
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := localPeerID(db)
	if id1 != id2 || !strings.HasPrefix(string(id1), "edge-") || len(id1) != len("edge-")+24 {
		t.Fatalf("unexpected peer ids %q %q", id1, id2)
	}
	other, _ := localPeerID(openTestDB(t))
	if other == id1 {
		t.Fatal("peer ids must differ between nodes")
	}
}

func TestLoadConfig(t *testing.T) {
	env := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	cfg, err := loadConfig(nil, env(nil))
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if cfg.listenAddr != defaultListenAddr || cfg.statePath != defaultStatePath || cfg.maxStreamBytes != defaultMaxStreamBytes || cfg.shutdownTimeout != 15*time.Second {
		t.Fatalf("unexpected defaults %+v", cfg)
	}

	token := strings.Repeat("t", 32)
	cfg, err = loadConfig([]string{"-listen", ":9000", "-max-stream-bytes", "1024"}, env(map[string]string{"EDGE_API_TOKEN": token, "EDGE_ALLOWED_ORIGINS": "https://app.example, http://localhost:3000"}))
	if err != nil {
		t.Fatalf("token config: %v", err)
	}
	if cfg.apiToken != token || cfg.maxStreamBytes != 1024 || len(cfg.allowedOrigins) != 2 {
		t.Fatalf("unexpected config %+v", cfg)
	}

	bad := []struct {
		args []string
		env  map[string]string
	}{
		{[]string{"-listen", ":9000"}, nil},                                   // public without token
		{[]string{"-listen", "0.0.0.0:9000"}, nil},                            // public without token
		{nil, map[string]string{"EDGE_API_TOKEN": "short"}},                   // weak token
		{[]string{"-listen", "nope"}, nil},                                    // bad address
		{nil, map[string]string{"EDGE_MAX_STREAM_BYTES": "-5"}},               // bad cap
		{nil, map[string]string{"EDGE_SHUTDOWN_TIMEOUT": "soon"}},             // bad duration
		{nil, map[string]string{"EDGE_ALLOWED_ORIGINS": "not a url"}},         // bad origin
		{[]string{"extra"}, nil},                                              // stray args
		{[]string{"-state", ""}, nil},                                         // empty state path
		{[]string{"-token-file", filepath.Join(t.TempDir(), "missing")}, nil}, // unreadable token file
	}
	for i, c := range bad {
		if _, err := loadConfig(c.args, env(c.env)); err == nil {
			t.Errorf("case %d (%v %v): expected error", i, c.args, c.env)
		}
	}
	if _, err := loadConfig([]string{"-listen", ":9000", "-insecure-no-auth"}, env(nil)); err != nil {
		t.Fatalf("explicit insecure override rejected: %v", err)
	}
}

func TestInvoiceURLAndLoopback(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:7910": "http://127.0.0.1:7910/v1/marketplace/openstandard/receipt/self",
		":7910":          "http://localhost:7910/v1/marketplace/openstandard/receipt/self",
		"0.0.0.0:8080":   "http://localhost:8080/v1/marketplace/openstandard/receipt/self",
		"[::1]:7910":     "http://[::1]:7910/v1/marketplace/openstandard/receipt/self",
	}
	for in, want := range cases {
		if got := invoiceURL(in); got != want {
			t.Errorf("invoiceURL(%q)=%q want %q", in, got, want)
		}
	}
	for addr, want := range map[string]bool{"127.0.0.1:1": true, "localhost:1": true, "[::1]:1": true, ":1": false, "0.0.0.0:1": false, "10.0.0.1:1": false} {
		if got := isLoopbackListen(addr); got != want {
			t.Errorf("isLoopbackListen(%q)=%v want %v", addr, got, want)
		}
	}
	m := toOpenStandardCapabilityManifest(nil, "127.0.0.1:7910")
	if err := m.Validate(); err != nil {
		t.Fatalf("generated manifest invalid: %v", err)
	}
}

// lockedWallet must keep satisfying economy.Wallet.
var _ economy.Wallet = (*lockedWallet)(nil)
