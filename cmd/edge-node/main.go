// Command edge-node is the local-first AeroLLM edge runtime: bbolt state,
// hardware detection, an in-process mesh, Open Standard marketplace routes,
// PQC handshake, spatial streaming and a realtime WebSocket.
//
// Configuration (flags override environment):
//
//	-listen            EDGE_LISTEN             listen address (default 127.0.0.1:7910)
//	-state             EDGE_STATE_PATH         bbolt state file (default edge-state.db)
//	-token-file        EDGE_API_TOKEN_FILE     file holding the API bearer token
//	                   EDGE_API_TOKEN          API bearer token (min 16 chars)
//	-allowed-origins   EDGE_ALLOWED_ORIGINS    extra WebSocket origins (comma separated)
//	-max-stream-bytes  EDGE_MAX_STREAM_BYTES   cap for /v1/edge/spatial/stream bodies
//	-shutdown-timeout  EDGE_SHUTDOWN_TIMEOUT   graceful shutdown budget
//	-insecure-no-auth  EDGE_INSECURE_NO_AUTH   allow a non-loopback listener without a token
//
// Without a token the node only serves loopback clients and rejects requests
// whose Host header is not a loopback name (DNS-rebinding protection). With a
// token every route requires "Authorization: Bearer <token>".
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/economy"
	"github.com/ayoubzulfiqar/aerollm/internal/hardware"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/ayoubzulfiqar/aerollm/internal/realtime"
	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
	"go.etcd.io/bbolt"
)

const (
	defaultListenAddr     = "127.0.0.1:7910"
	defaultStatePath      = "edge-state.db"
	defaultMaxStreamBytes = 64 << 20
	minTokenLen           = 16
	edgeWalletID          = "edge-wallet"
)

var (
	bucketEdge     = []byte("edge")
	bucketState    = []byte("state")
	bucketReceipts = []byte("receipts")
	bucketWallets  = []byte("wallets")
	bucketQueue    = []byte("queue")
	bucketTxLog    = []byte("wallet_tx")
	keyPeerID      = []byte("peer_id")
	keyCapability  = []byte("capability_manifest")
)

// errReceiptConflict reports a receipt ID reused with different content.
var errReceiptConflict = errors.New("edge: receipt id already used with different content")

type edgeConfig struct {
	listenAddr      string
	statePath       string
	apiToken        string
	allowedOrigins  []string
	insecureNoAuth  bool
	maxStreamBytes  int64
	shutdownTimeout time.Duration
}

func main() {
	cfg, err := loadConfig(os.Args[1:], os.Getenv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "edge-node: %v\n", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "edge-node: %v\n", err)
		os.Exit(1)
	}
}

func loadConfig(args []string, getenv func(string) string) (edgeConfig, error) {
	env := func(key, def string) string {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
		return def
	}
	fs := flag.NewFlagSet("edge-node", flag.ContinueOnError)
	listen := fs.String("listen", env("EDGE_LISTEN", defaultListenAddr), "listen address")
	state := fs.String("state", env("EDGE_STATE_PATH", defaultStatePath), "bbolt state file")
	tokenFile := fs.String("token-file", env("EDGE_API_TOKEN_FILE", ""), "file holding the API bearer token")
	origins := fs.String("allowed-origins", env("EDGE_ALLOWED_ORIGINS", ""), "comma-separated extra WebSocket origins")
	insecure := fs.Bool("insecure-no-auth", strings.EqualFold(env("EDGE_INSECURE_NO_AUTH", ""), "true"), "allow a non-loopback listener without an API token")
	maxStream := fs.Int64("max-stream-bytes", 0, "maximum spatial stream body size")
	shutdown := fs.Duration("shutdown-timeout", 0, "graceful shutdown timeout")
	if err := fs.Parse(args); err != nil {
		return edgeConfig{}, err
	}
	if fs.NArg() > 0 {
		return edgeConfig{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}

	cfg := edgeConfig{
		listenAddr:     strings.TrimSpace(*listen),
		statePath:      strings.TrimSpace(*state),
		insecureNoAuth: *insecure,
	}
	if cfg.statePath == "" {
		return edgeConfig{}, errors.New("state path must not be empty")
	}
	if _, _, err := net.SplitHostPort(cfg.listenAddr); err != nil {
		return edgeConfig{}, fmt.Errorf("invalid listen address %q: %w", cfg.listenAddr, err)
	}

	cfg.maxStreamBytes = *maxStream
	if cfg.maxStreamBytes == 0 {
		v, err := parsePositiveInt(env("EDGE_MAX_STREAM_BYTES", strconv.Itoa(defaultMaxStreamBytes)))
		if err != nil {
			return edgeConfig{}, fmt.Errorf("EDGE_MAX_STREAM_BYTES: %w", err)
		}
		cfg.maxStreamBytes = v
	}
	if cfg.maxStreamBytes <= 0 {
		return edgeConfig{}, errors.New("max stream bytes must be positive")
	}

	cfg.shutdownTimeout = *shutdown
	if cfg.shutdownTimeout == 0 {
		d, err := time.ParseDuration(env("EDGE_SHUTDOWN_TIMEOUT", "15s"))
		if err != nil || d <= 0 {
			return edgeConfig{}, fmt.Errorf("invalid EDGE_SHUTDOWN_TIMEOUT")
		}
		cfg.shutdownTimeout = d
	}
	if cfg.shutdownTimeout < 0 {
		return edgeConfig{}, errors.New("shutdown timeout must be positive")
	}

	cfg.apiToken = strings.TrimSpace(getenv("EDGE_API_TOKEN"))
	if *tokenFile != "" {
		b, err := os.ReadFile(*tokenFile)
		if err != nil {
			return edgeConfig{}, fmt.Errorf("read token file: %w", err)
		}
		cfg.apiToken = strings.TrimSpace(string(b))
	}
	if cfg.apiToken != "" && len(cfg.apiToken) < minTokenLen {
		return edgeConfig{}, fmt.Errorf("API token must be at least %d characters", minTokenLen)
	}

	for _, o := range strings.Split(*origins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			u, err := url.Parse(o)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return edgeConfig{}, fmt.Errorf("invalid allowed origin %q", o)
			}
			cfg.allowedOrigins = append(cfg.allowedOrigins, strings.ToLower(u.Scheme+"://"+u.Host))
		}
	}

	if cfg.apiToken == "" && !isLoopbackListen(cfg.listenAddr) && !cfg.insecureNoAuth {
		return edgeConfig{}, fmt.Errorf("refusing to listen on non-loopback address %q without EDGE_API_TOKEN (use -insecure-no-auth to override)", cfg.listenAddr)
	}
	return cfg, nil
}

func parsePositiveInt(s string) (int64, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("must be a positive integer")
	}
	return v, nil
}

// isLoopbackListen reports whether addr binds only loopback interfaces. An
// empty host or an unspecified address (0.0.0.0, ::) binds everything.
func isLoopbackListen(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return isLoopbackHost(host)
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func run(ctx context.Context, cfg edgeConfig) error {
	db, err := bbolt.Open(cfg.statePath, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("open edge state %s: %w", cfg.statePath, err)
	}
	defer db.Close()

	peerID, err := localPeerID(db)
	if err != nil {
		return err
	}

	transport := mesh.NewInMemoryTransport(peerID)
	defer transport.Close()
	discovery := mesh.NewDiscovery(mesh.DiscoveryConfig{
		LocalID:     peerID,
		BindAddress: "/ip4/127.0.0.1/tcp/0",
		Transport:   transport,
	})
	discovery.Start(ctx)
	defer func() {
		discovery.Stop()
		select {
		case <-discovery.Stopped():
		case <-time.After(5 * time.Second):
		}
	}()

	srv, err := newEdgeServer(ctx, cfg, db, peerID, hardware.NewLocalDetector().Detect(), discovery)
	if err != nil {
		return err
	}

	httpSrv := &http.Server{
		Handler:           srv.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
		// No WriteTimeout: it would cut off spatial streams and WebSockets.
	}
	httpSrv.RegisterOnShutdown(srv.hub.CancelAll)

	ln, err := net.Listen("tcp", cfg.listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.listenAddr, err)
	}
	fmt.Printf("edge-node %s listening on %s (auth: %s)\n", peerID, ln.Addr(), authMode(cfg))

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	fmt.Println("edge-node shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		_ = httpSrv.Close()
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	return nil
}

func authMode(cfg edgeConfig) string {
	switch {
	case cfg.apiToken != "":
		return "bearer token"
	case cfg.insecureNoAuth:
		return "NONE (insecure)"
	default:
		return "loopback only"
	}
}

// edgeServer holds the HTTP-facing state of the node.
type edgeServer struct {
	cfg       edgeConfig
	db        *bbolt.DB
	peerID    mesh.PeerID
	caps      []hardware.Capability
	wallet    economy.Wallet
	registry  *marketplace.RegistryService
	pqcKM     *pqc.QuantumSafeKeyManager
	hub       *realtime.Hub
	discovery *mesh.Discovery
	now       func() time.Time

	mu       sync.RWMutex
	manifest marketplace.CapabilityManifest
}

func newEdgeServer(ctx context.Context, cfg edgeConfig, db *bbolt.DB, peerID mesh.PeerID, caps []hardware.Capability, discovery *mesh.Discovery) (*edgeServer, error) {
	wallet, err := newBboltWalletStore(db).Wallet(ctx, edgeWalletID)
	if err != nil {
		return nil, fmt.Errorf("wallet init: %w", err)
	}
	s := &edgeServer{
		cfg:       cfg,
		db:        db,
		peerID:    peerID,
		caps:      caps,
		wallet:    wallet,
		registry:  marketplace.NewRegistryService(nil, marketplace.NewInMemoryStore()),
		pqcKM:     pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridMLKEM768X25519Ed25519),
		hub:       realtime.NewHub(),
		discovery: discovery,
		now:       time.Now,
	}
	s.manifest = toOpenStandardCapabilityManifest(caps, cfg.listenAddr)
	if stored, ok, err := loadCapabilityManifest(db); err != nil {
		return nil, err
	} else if ok {
		s.manifest = stored
	}
	return s, nil
}

func (s *edgeServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/marketplace/openstandard/capability", s.registry.CapabilityManifestHandler())
	mux.HandleFunc("/v1/marketplace/openstandard/capability/self", s.handleCapabilitySelf)
	mux.HandleFunc("/v1/marketplace/openstandard/receipt", s.handleReceipt)
	mux.HandleFunc("/v1/marketplace/openstandard/receipt/self", s.handleReceiptSelf)
	mux.HandleFunc("/v1/edge/capabilities", s.handleCapabilities)
	mux.Handle("/v1/edge/pqc/handshake", onlyMethods(pqc.HandshakeHandler(s.pqcKM), http.MethodPost))
	stream := spatial.NewVideo3DStreamHandler()
	stream.MaxBytes = s.cfg.maxStreamBytes
	mux.Handle("/v1/edge/spatial/stream", onlyMethods(s.capBody(stream, s.cfg.maxStreamBytes), http.MethodPost))
	rtCfg := realtime.DefaultConfig()
	rtCfg.AllowedOrigins = append(rtCfg.AllowedOrigins, s.cfg.allowedOrigins...)
	rtCfg.MaxSessions = 64
	mux.Handle("/v1/edge/realtime/ws", s.checkOrigin(realtime.ServeWSWithConfig(s.hub, newEdgeRealtimeProvider(), rtCfg)))
	return s.guard(mux)
}

// guard applies DNS-rebinding protection, authentication and common headers.
func (s *edgeServer) guard(next http.Handler) http.Handler {
	var tokenSum [32]byte
	if s.cfg.apiToken != "" {
		tokenSum = sha256.Sum256([]byte(s.cfg.apiToken))
	}
	loopback := isLoopbackListen(s.cfg.listenAddr)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		if loopback && !isLoopbackHost(hostOnly(r.Host)) {
			// A browser page on another origin that rebinds its DNS name to
			// 127.0.0.1 still sends its own name in Host.
			respondErr(w, "host not allowed", http.StatusMisdirectedRequest)
			return
		}
		if s.cfg.apiToken != "" {
			got := bearerToken(r.Header.Get("Authorization"))
			gotSum := sha256.Sum256([]byte(got))
			if got == "" || subtle.ConstantTimeCompare(gotSum[:], tokenSum[:]) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				respondErr(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return hostport
}

func bearerToken(h string) string {
	h = strings.TrimSpace(h)
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

// checkOrigin rejects cross-site WebSocket handshakes. realtime's upgrader
// accepts every Origin, so without this any web page could drive the local
// node through the visitor's browser.
func (s *edgeServer) checkOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && !s.originAllowed(origin, r.Host) {
			respondErr(w, "origin not allowed", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *edgeServer) originAllowed(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	if strings.EqualFold(u.Host, host) {
		return true
	}
	norm := strings.ToLower(u.Scheme + "://" + u.Host)
	for _, o := range s.cfg.allowedOrigins {
		if o == norm {
			return true
		}
	}
	return false
}

func onlyMethods(h http.Handler, methods ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, m := range methods {
			if r.Method == m {
				h.ServeHTTP(w, r)
				return
			}
		}
		w.Header().Set("Allow", strings.Join(methods, ", "))
		respondErr(w, "method not allowed", http.StatusMethodNotAllowed)
	})
}

func (s *edgeServer) capBody(h http.Handler, limit int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		h.ServeHTTP(w, r)
	})
}

func (s *edgeServer) handleCapabilitySelf(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		s.mu.RLock()
		m := s.manifest
		s.mu.RUnlock()
		respondJSON(w, http.StatusOK, m)
	case http.MethodPut, http.MethodPost:
		m, ok := decodeCapped(w, r, marketplace.ParseCapabilityManifest)
		if !ok {
			return
		}
		m.UpdatedAt = s.now().UTC()
		if err := saveCapabilityManifest(s.db, m); err != nil {
			respondErr(w, "failed to persist manifest", http.StatusInternalServerError)
			return
		}
		s.mu.Lock()
		s.manifest = m
		s.mu.Unlock()
		respondJSON(w, http.StatusAccepted, m)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, POST")
		respondErr(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *edgeServer) handleReceipt(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		respondErr(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rec, ok := decodeCapped(w, r, marketplace.ParseBillingReceipt)
	if !ok {
		return
	}
	rec.RecordedAt = s.now().UTC()
	stored, created, err := storeReceipt(s.db, rec)
	switch {
	case errors.Is(err, errReceiptConflict):
		respondErr(w, err.Error(), http.StatusConflict)
	case err != nil:
		respondErr(w, "failed to persist receipt", http.StatusInternalServerError)
	case created:
		respondJSON(w, http.StatusCreated, stored)
	default:
		respondJSON(w, http.StatusOK, stored)
	}
}

func (s *edgeServer) handleReceiptSelf(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		respondErr(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	respondJSON(w, http.StatusOK, marketplace.BillingReceipt{
		ReceiptID:  "edge-invoice",
		CustomerID: string(s.peerID),
		ProviderID: "edge",
		EventName:  "compute",
		Value:      0,
		Currency:   "USD",
		RecordedAt: s.now().UTC(),
	})
}

func (s *edgeServer) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		respondErr(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := map[string]interface{}{
		"peer_id":  string(s.peerID),
		"hardware": hardware.AdvertisedCapabilities(s.caps),
		"system":   hardware.DetectSystemInfo(),
	}
	if b, err := s.wallet.Balance(r.Context()); err != nil {
		resp["wallet"] = nil
		resp["wallet_error"] = "unavailable"
	} else {
		resp["wallet"] = strconv.FormatFloat(b, 'f', 2, 64)
	}
	if s.discovery != nil {
		resp["mesh_peers"] = len(s.discovery.Peers())
	}
	respondJSON(w, http.StatusOK, resp)
}

// decodeCapped parses a capped JSON body with parse and writes 400/413 itself.
func decodeCapped[T any](w http.ResponseWriter, r *http.Request, parse func(io.Reader) (T, error)) (T, bool) {
	var zero T
	if r.Body == nil {
		respondErr(w, "missing request body", http.StatusBadRequest)
		return zero, false
	}
	body := http.MaxBytesReader(w, r.Body, marketplace.MaxOpenStandardBytes)
	defer body.Close()
	v, err := parse(body)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) || errors.Is(err, marketplace.ErrTooLarge) {
			respondErr(w, "request body too large", http.StatusRequestEntityTooLarge)
			return zero, false
		}
		respondErr(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return zero, false
	}
	return v, true
}

func respondJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func respondErr(w http.ResponseWriter, msg string, code int) {
	respondJSON(w, code, struct {
		Error string `json:"error"`
	}{Error: msg})
}

func toOpenStandardCapabilityManifest(caps []hardware.Capability, listenAddr string) marketplace.CapabilityManifest {
	gpuName := ""
	// Prefer the most capable accelerator deterministically.
	rank := map[string]int{"cuda": 4, "rocm": 3, "metal": 2, "vulkan": 1}
	for _, c := range caps {
		if c.Available && rank[c.Name] > rank[gpuName] {
			gpuName = c.Name
		}
	}
	return marketplace.CapabilityManifest{
		Version: "1.0",
		Hardware: marketplace.Hardware{
			HasLocalGPU: gpuName != "",
			GPUName:     gpuName,
			OS:          runtime.GOOS,
			MemoryGB:    hardware.DetectSystemInfo().MemoryGB(),
		},
		Billing: marketplace.Billing{
			SupportsMetered: true,
			Currency:        "USD",
			InvoiceURL:      invoiceURL(listenAddr),
		},
		Capabilities: []string{"mesh", "wasm", "billing", "privacy"},
		UpdatedAt:    time.Now().UTC(),
	}
}

// invoiceURL derives the self-receipt URL from the listen address. Wildcard
// or empty hosts are advertised as localhost.
func invoiceURL(listenAddr string) string {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		host, port = "localhost", "7910"
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "localhost"
	}
	u := url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: "/v1/marketplace/openstandard/receipt/self"}
	return u.String()
}

func loadCapabilityManifest(db *bbolt.DB) (marketplace.CapabilityManifest, bool, error) {
	var raw []byte
	err := db.View(func(tx *bbolt.Tx) error {
		if b := tx.Bucket(bucketEdge); b != nil {
			if sb := b.Bucket(bucketState); sb != nil {
				if v := sb.Get(keyCapability); v != nil {
					raw = append([]byte(nil), v...)
				}
			}
		}
		return nil
	})
	if err != nil || raw == nil {
		return marketplace.CapabilityManifest{}, false, err
	}
	var m marketplace.CapabilityManifest
	if err := json.Unmarshal(raw, &m); err != nil || m.Validate() != nil {
		// A corrupt stored manifest falls back to the detected one.
		return marketplace.CapabilityManifest{}, false, nil
	}
	return m, true, nil
}

func saveCapabilityManifest(db *bbolt.DB, m marketplace.CapabilityManifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bbolt.Tx) error {
		eb, err := tx.CreateBucketIfNotExists(bucketEdge)
		if err != nil {
			return err
		}
		sb, err := eb.CreateBucketIfNotExists(bucketState)
		if err != nil {
			return err
		}
		return sb.Put(keyCapability, b)
	})
}

// queueReceipt persists rec keyed by its receipt ID. Re-queuing an identical
// receipt is a no-op; reusing an ID for different content returns
// errReceiptConflict.
func queueReceipt(db *bbolt.DB, rec marketplace.BillingReceipt) error {
	_, _, err := storeReceipt(db, rec)
	return err
}

// storeReceipt is queueReceipt that also returns the stored receipt and
// whether it was newly created.
func storeReceipt(db *bbolt.DB, rec marketplace.BillingReceipt) (marketplace.BillingReceipt, bool, error) {
	if err := rec.Validate(); err != nil {
		return marketplace.BillingReceipt{}, false, err
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return marketplace.BillingReceipt{}, false, err
	}
	stored, created := rec, false
	err = db.Update(func(tx *bbolt.Tx) error {
		eb, err := tx.CreateBucketIfNotExists(bucketEdge)
		if err != nil {
			return err
		}
		rb, err := eb.CreateBucketIfNotExists(bucketReceipts)
		if err != nil {
			return err
		}
		key := []byte(rec.ReceiptID)
		if existing := rb.Get(key); existing != nil {
			var prev marketplace.BillingReceipt
			if err := json.Unmarshal(existing, &prev); err != nil {
				return errReceiptConflict
			}
			if !sameReceipt(prev, rec) {
				return errReceiptConflict
			}
			stored = prev
			return nil
		}
		created = true
		return rb.Put(key, payload)
	})
	if err != nil {
		return marketplace.BillingReceipt{}, false, err
	}
	return stored, created, nil
}

func sameReceipt(a, b marketplace.BillingReceipt) bool {
	a.RecordedAt, b.RecordedAt = time.Time{}, time.Time{}
	return reflect.DeepEqual(a, b)
}

func localPeerID(db *bbolt.DB) (mesh.PeerID, error) {
	var id string
	err := db.Update(func(tx *bbolt.Tx) error {
		eb, err := tx.CreateBucketIfNotExists(bucketEdge)
		if err != nil {
			return err
		}
		sb, err := eb.CreateBucketIfNotExists(bucketState)
		if err != nil {
			return err
		}
		if v := sb.Get(keyPeerID); len(v) > 0 {
			id = string(v)
			return nil
		}
		var b [12]byte
		if _, err := rand.Read(b[:]); err != nil {
			return err
		}
		id = "edge-" + hex.EncodeToString(b[:])
		return sb.Put(keyPeerID, []byte(id))
	})
	if err != nil {
		return "", fmt.Errorf("load peer id: %w", err)
	}
	return mesh.PeerID(id), nil
}

// bboltWalletStore hands out wallets backed by bbolt. Wallets with the same
// ID share one lock, so the read-modify-write in economy.DefaultWallet's
// Credit/Debit cannot interleave within this process.
type bboltWalletStore struct {
	db    *bbolt.DB
	mu    sync.Mutex
	locks map[economy.WalletID]*sync.Mutex
}

func newBboltWalletStore(db *bbolt.DB) *bboltWalletStore {
	return &bboltWalletStore{db: db, locks: make(map[economy.WalletID]*sync.Mutex)}
}

func (s *bboltWalletStore) Wallet(_ context.Context, id economy.WalletID) (economy.Wallet, error) {
	if id == "" {
		return nil, fmt.Errorf("economy: missing wallet id")
	}
	if len(id) > 128 {
		return nil, fmt.Errorf("economy: wallet id too long")
	}
	s.mu.Lock()
	lock, ok := s.locks[id]
	if !ok {
		lock = &sync.Mutex{}
		s.locks[id] = lock
	}
	s.mu.Unlock()
	inner := economy.NewDefaultWallet(id, &bboltLedgerStore{db: s.db, prefix: fmt.Sprintf("wallet-%s", id)})
	return &lockedWallet{mu: lock, inner: inner}, nil
}

type lockedWallet struct {
	mu    *sync.Mutex
	inner economy.Wallet
}

func (w *lockedWallet) ID() economy.WalletID { return w.inner.ID() }

func (w *lockedWallet) Balance(ctx context.Context) (float64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.inner.Balance(ctx)
}

func (w *lockedWallet) Credit(ctx context.Context, amount float64, reason string) (*economy.Transaction, error) {
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		return nil, fmt.Errorf("economy: amount must be finite")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.inner.Credit(ctx, amount, reason)
}

func (w *lockedWallet) Debit(ctx context.Context, amount float64, reason string) (*economy.Transaction, error) {
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		return nil, fmt.Errorf("economy: amount must be finite")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.inner.Debit(ctx, amount, reason)
}

func (w *lockedWallet) History(ctx context.Context, limit int) ([]economy.Transaction, error) {
	return w.inner.History(ctx, limit)
}

type bboltLedgerStore struct {
	db     *bbolt.DB
	prefix string
}

func (s *bboltLedgerStore) Balance(_ context.Context, _ economy.WalletID) (float64, error) {
	var balance float64
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketEdge)
		if b == nil {
			return nil
		}
		wb := b.Bucket(bucketWallets)
		if wb == nil {
			return nil
		}
		v := wb.Get([]byte(s.prefix))
		if v == nil {
			return nil
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(string(v)), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("economy: corrupt balance for %s", s.prefix)
		}
		balance = f
		return nil
	})
	return balance, err
}

func (s *bboltLedgerStore) SetBalance(_ context.Context, _ economy.WalletID, balance float64) error {
	if math.IsNaN(balance) || math.IsInf(balance, 0) {
		return fmt.Errorf("economy: balance must be finite")
	}
	// Shortest round-trip formatting: the previous "%f" silently dropped
	// everything below 1e-6.
	payload := []byte(strconv.FormatFloat(balance, 'g', -1, 64))
	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketEdge)
		if err != nil {
			return err
		}
		wb, err := b.CreateBucketIfNotExists(bucketWallets)
		if err != nil {
			return err
		}
		return wb.Put([]byte(s.prefix), payload)
	})
}

// AppendTransaction records tx in the wallet's history (ordered by time) and
// in the sync outbox ("queue"), atomically.
func (s *bboltLedgerStore) AppendTransaction(_ context.Context, t economy.Transaction) error {
	if t.ID == "" {
		return fmt.Errorf("economy: transaction id is required")
	}
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return s.db.Update(func(btx *bbolt.Tx) error {
		b, err := btx.CreateBucketIfNotExists(bucketEdge)
		if err != nil {
			return err
		}
		qb, err := b.CreateBucketIfNotExists(bucketQueue)
		if err != nil {
			return err
		}
		if err := qb.Put([]byte(t.ID), payload); err != nil {
			return err
		}
		logs, err := b.CreateBucketIfNotExists(bucketTxLog)
		if err != nil {
			return err
		}
		wl, err := logs.CreateBucketIfNotExists([]byte(s.prefix))
		if err != nil {
			return err
		}
		return wl.Put(txLogKey(t), payload)
	})
}

// txLogKey orders history entries by timestamp, then ID.
func txLogKey(t economy.Transaction) []byte {
	key := make([]byte, 8, 8+len(t.ID))
	ts := t.Timestamp.UnixNano()
	if ts < 0 {
		ts = 0
	}
	binary.BigEndian.PutUint64(key, uint64(ts))
	return append(key, t.ID...)
}

// Transactions returns up to limit transactions, newest first (limit <= 0
// means 50; capped at 1000).
func (s *bboltLedgerStore) Transactions(_ context.Context, _ economy.WalletID, limit int) ([]economy.Transaction, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}
	out := []economy.Transaction{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketEdge)
		if b == nil {
			return nil
		}
		logs := b.Bucket(bucketTxLog)
		if logs == nil {
			return nil
		}
		wl := logs.Bucket([]byte(s.prefix))
		if wl == nil {
			return nil
		}
		c := wl.Cursor()
		for k, v := c.Last(); k != nil && len(out) < limit; k, v = c.Prev() {
			var t economy.Transaction
			if err := json.Unmarshal(v, &t); err != nil {
				return fmt.Errorf("economy: corrupt transaction %x: %w", k, err)
			}
			out = append(out, t)
		}
		return nil
	})
	return out, err
}
