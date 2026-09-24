package mesh

// This file implements the network SecureTransport: mutually authenticated
// TLS 1.3 over TCP.
//
// Peer identity: every node presents a certificate that carries its peer id,
// either as a URI SAN "urn:aerollm:mesh:<id>" (preferred) or, when no such
// SAN is present, as the subject Common Name. Certificates are verified
// against a configured CA pool and/or pinned SHA-256 fingerprints. The id
// read from the verified certificate is authoritative: a dialer only accepts
// a server whose certificate names the peer id it meant to reach, and every
// inbound envelope's From is set from the sender's certificate, so a peer
// cannot claim another peer's id.
//
// Wire format: after the TLS handshake the server writes a single accept byte
// (TLS 1.3 servers verify the client certificate after the client considers
// the handshake done, so this makes a rejected client certificate fail Dial
// instead of the first Send). Envelopes are then exchanged as frames: a 4-byte
// big-endian length followed by that many bytes of JSON
// {"type": <state type>, "payload": <raw JSON>}. Frames above the configured
// maximum are rejected before they are read and the connection is closed.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	// MeshALPN is the TLS application protocol mesh peers must negotiate.
	MeshALPN = "aerollm-mesh/1"
	// PeerIDURIPrefix prefixes the URI SAN that carries a peer id in a mesh
	// certificate, e.g. "urn:aerollm:mesh:node-a".
	PeerIDURIPrefix = "urn:aerollm:mesh:"

	// DefaultTLSMaxConns bounds concurrent connections (inbound, outbound and
	// handshaking) of one TLS transport.
	DefaultTLSMaxConns = 256
	// DefaultTLSHandshakeTimeout bounds TCP connect + TLS handshake.
	DefaultTLSHandshakeTimeout = 10 * time.Second
	// DefaultTLSIdleTimeout closes a connection on which no frame starts
	// within this window.
	DefaultTLSIdleTimeout = 2 * time.Minute
	// DefaultTLSReadTimeout bounds reading one frame once its header arrived.
	DefaultTLSReadTimeout = 30 * time.Second
	// DefaultTLSWriteTimeout bounds writing one frame.
	DefaultTLSWriteTimeout = 10 * time.Second
	// DefaultTLSBackoffBase is the first reconnect delay after a failed dial.
	DefaultTLSBackoffBase = 250 * time.Millisecond
	// DefaultTLSBackoffMax caps the reconnect delay.
	DefaultTLSBackoffMax = 30 * time.Second

	// Environment variables read by TLSTransportConfigFromEnv. CERT, KEY and
	// CA hold a file path or inline PEM; PINS holds "id=sha256hex" entries
	// separated by commas or whitespace (an id may appear more than once).
	EnvMeshTLSCert = "AEROLLM_MESH_TLS_CERT"
	EnvMeshTLSKey  = "AEROLLM_MESH_TLS_KEY"
	EnvMeshTLSCA   = "AEROLLM_MESH_TLS_CA"
	EnvMeshTLSPins = "AEROLLM_MESH_TLS_PINS"

	frameHeaderLen    = 4
	maxFrameLimit     = 1 << 30
	maxStateTypeLen   = 256
	maxPEMBytes       = 1 << 20
	tlsInboxSize      = 16
	tlsAcceptBacklog  = 64
	maxBackoffEntries = 4096
	helloAccept       = 0x01
)

var (
	// ErrPeerIdentity is returned by Dial when the certificate presented at
	// the peer's address names a different peer id.
	ErrPeerIdentity = errors.New("mesh: peer certificate identity mismatch")
	// ErrUntrustedPeer is returned when a peer certificate fails CA
	// verification, pinning or protocol negotiation.
	ErrUntrustedPeer = errors.New("mesh: untrusted peer certificate")
	// ErrPeerBackoff is returned (wrapped with ErrPeerUnreachable) by Dial
	// while a peer is in reconnect backoff after failed dials.
	ErrPeerBackoff = errors.New("mesh: peer in reconnect backoff")
	// ErrTooManyConns is returned by Dial when the transport's connection
	// limit is reached.
	ErrTooManyConns = errors.New("mesh: connection limit reached")
	// ErrConnClosed is returned by Send on a closed connection.
	ErrConnClosed = errors.New("mesh: connection closed")

	aLongTimeAgo = time.Unix(1, 0)
)

// Fingerprint is the SHA-256 digest of a DER-encoded certificate.
type Fingerprint [sha256.Size]byte

// CertificateFingerprint returns the SHA-256 fingerprint of a DER certificate.
func CertificateFingerprint(der []byte) Fingerprint { return sha256.Sum256(der) }

// String returns the lowercase hex encoding.
func (f Fingerprint) String() string { return hex.EncodeToString(f[:]) }

// ParseFingerprint parses a hex SHA-256 fingerprint. Colons, spaces and an
// optional "sha256:" prefix are ignored; case does not matter.
func ParseFingerprint(s string) (Fingerprint, error) {
	var f Fingerprint
	s = strings.TrimSpace(s)
	if len(s) > 7 && strings.EqualFold(s[:7], "sha256:") {
		s = s[7:]
	}
	s = strings.NewReplacer(":", "", " ", "").Replace(s)
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(f) {
		return f, fmt.Errorf("mesh: invalid SHA-256 fingerprint %q", truncateStr(s, 80))
	}
	copy(f[:], b)
	return f, nil
}

// ParsePeerPins parses "id=fingerprint" entries separated by commas,
// semicolons or whitespace into a pin map. An id may be listed more than once
// (for certificate rotation).
func ParsePeerPins(spec string) (map[PeerID][]Fingerprint, error) {
	out := make(map[PeerID][]Fingerprint)
	for _, entry := range strings.FieldsFunc(spec, func(r rune) bool { return r == ',' || r == ';' || unicode.IsSpace(r) }) {
		id, fp, ok := strings.Cut(entry, "=")
		if !ok || id == "" {
			return nil, fmt.Errorf("mesh: pin entry %q must be id=sha256hex", truncateStr(entry, 80))
		}
		if err := validatePeerIDString(id); err != nil {
			return nil, err
		}
		f, err := ParseFingerprint(fp)
		if err != nil {
			return nil, err
		}
		out[PeerID(id)] = append(out[PeerID(id)], f)
	}
	return out, nil
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func validatePeerIDString(id string) error {
	if id == "" || len(id) > MaxPeerIDLen || hasControl(id) || strings.TrimSpace(id) != id {
		return fmt.Errorf("%w: invalid peer id %q", ErrInvalidPeer, truncateStr(id, 80))
	}
	return nil
}

// PeerIDFromCertificate returns the peer id a mesh certificate is issued to:
// the id in its "urn:aerollm:mesh:<id>" URI SAN or, when it has none, its
// subject Common Name. Certificates with several different mesh URI SANs are
// rejected as ambiguous.
func PeerIDFromCertificate(cert *x509.Certificate) (PeerID, error) {
	if cert == nil {
		return "", fmt.Errorf("%w: no certificate", ErrUntrustedPeer)
	}
	var id string
	for _, u := range cert.URIs {
		if u == nil || !strings.EqualFold(u.Scheme, "urn") {
			continue
		}
		const nid = "aerollm:mesh:"
		if len(u.Opaque) <= len(nid) || !strings.EqualFold(u.Opaque[:len(nid)], nid) {
			continue
		}
		v, err := url.PathUnescape(u.Opaque[len(nid):])
		if err != nil {
			return "", fmt.Errorf("%w: malformed mesh URI SAN", ErrUntrustedPeer)
		}
		if id != "" && id != v {
			return "", fmt.Errorf("%w: certificate names several mesh peer ids", ErrUntrustedPeer)
		}
		id = v
	}
	if id == "" {
		id = cert.Subject.CommonName
	}
	if err := validatePeerIDString(id); err != nil {
		return "", fmt.Errorf("%w: certificate carries no usable peer id: %v", ErrUntrustedPeer, err)
	}
	return PeerID(id), nil
}

// ParseTCPAddress converts a mesh address into host:port. Accepted forms are
// "host:port", "tcp://host:port", "tls://host:port" and the multiaddr-style
// "/ip4/<ip>/tcp/<port>", "/ip6/<ip>/tcp/<port>" and
// "/dns|dns4|dns6/<name>/tcp/<port>".
func ParseTCPAddress(addr string) (string, error) {
	a := strings.TrimSpace(addr)
	bad := func(why string) (string, error) {
		return "", fmt.Errorf("%w: address %q: %s", ErrInvalidPeer, truncateStr(a, 80), why)
	}
	if a == "" || len(a) > MaxPeerAddressLen || hasControl(a) {
		return bad("empty, too long or contains control characters")
	}
	var host, port string
	switch {
	case strings.HasPrefix(a, "/"):
		parts := strings.Split(a[1:], "/")
		if len(parts) != 4 || parts[2] != "tcp" {
			return bad("expected /ip4|ip6|dns|dns4|dns6/<host>/tcp/<port>")
		}
		host, port = parts[1], parts[3]
		switch parts[0] {
		case "ip4":
			if ip := net.ParseIP(host); ip == nil || ip.To4() == nil {
				return bad("invalid IPv4 address")
			}
		case "ip6":
			if ip := net.ParseIP(host); ip == nil || ip.To4() != nil {
				return bad("invalid IPv6 address")
			}
		case "dns", "dns4", "dns6":
			if host == "" {
				return bad("empty host name")
			}
		default:
			return bad("unsupported protocol " + strconv.Quote(parts[0]))
		}
	default:
		for _, scheme := range []string{"tcp://", "tls://"} {
			if len(a) > len(scheme) && strings.EqualFold(a[:len(scheme)], scheme) {
				a = a[len(scheme):]
				break
			}
		}
		if strings.Contains(a, "/") {
			return bad("unexpected path")
		}
		var err error
		host, port, err = net.SplitHostPort(a)
		if err != nil {
			return bad(err.Error())
		}
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil || strconv.FormatUint(p, 10) != port {
		return bad("invalid port")
	}
	return net.JoinHostPort(host, port), nil
}

// TLSTransportConfig configures NewTLSTransport. Certificate and at least
// one of RootCAs or PinnedPeers are required.
type TLSTransportConfig struct {
	// LocalID is this node's peer id. It must match the id carried by
	// Certificate (see PeerIDFromCertificate); when empty it is taken from
	// the certificate.
	LocalID PeerID
	// Certificate is this node's certificate chain and private key, used as
	// both TLS server and client certificate. It should carry the
	// serverAuth and clientAuth extended key usages (or none).
	Certificate tls.Certificate
	// RootCAs verifies peer certificate chains (serverAuth for peers this
	// node dials, clientAuth for peers that dial it).
	RootCAs *x509.CertPool
	// PinnedPeers, when non-empty, is an allow-list: a peer is accepted only
	// if the SHA-256 fingerprint of its leaf certificate is pinned for the
	// peer id that certificate carries. Without RootCAs, pinned certificates
	// may be self-signed (their validity period is still enforced).
	PinnedPeers map[PeerID][]Fingerprint

	// MaxFrameBytes caps one encoded envelope (default
	// DefaultMaxEnvelopeBytes). Larger inbound frames close the connection.
	MaxFrameBytes int
	// MaxConns bounds concurrent connections, including handshakes in
	// progress (default DefaultTLSMaxConns).
	MaxConns int
	// HandshakeTimeout bounds TCP connect + TLS handshake (default
	// DefaultTLSHandshakeTimeout).
	HandshakeTimeout time.Duration
	// IdleTimeout closes a connection on which no frame starts within this
	// window (default DefaultTLSIdleTimeout).
	IdleTimeout time.Duration
	// ReadTimeout bounds reading one frame after its header (default
	// DefaultTLSReadTimeout).
	ReadTimeout time.Duration
	// WriteTimeout bounds writing one frame (default DefaultTLSWriteTimeout).
	WriteTimeout time.Duration
	// BackoffBase and BackoffMax shape the per-peer reconnect backoff: after
	// n consecutive failed dials to a peer, dials fail fast with
	// ErrPeerBackoff for a jittered min(BackoffBase·2^(n-1), BackoffMax).
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

// TLSTransport is a SecureTransport over TCP with mutual TLS 1.3. It is safe
// for concurrent use.
//
// Dial opens a new authenticated connection per call (bounded by ctx and
// HandshakeTimeout); repeated failures to reach a peer put it into
// exponential reconnect backoff. Each connection has one reader goroutine;
// Close closes the listener and every connection and waits for all of the
// transport's goroutines to exit.
type TLSTransport struct {
	id        PeerID
	roots     *x509.CertPool
	pins      map[PeerID][]Fingerprint
	serverCfg *tls.Config
	clientCfg *tls.Config

	maxFrame         int
	handshakeTimeout time.Duration
	idleTimeout      time.Duration
	readTimeout      time.Duration
	writeTimeout     time.Duration
	backoffBase      time.Duration
	backoffMax       time.Duration

	sem chan struct{}
	wg  sync.WaitGroup

	mu       sync.Mutex
	closed   bool
	listener *tlsListener
	conns    map[*tlsPeerConn]struct{}
	pending  map[net.Conn]struct{}

	bmu     sync.Mutex
	backoff map[string]*backoffEntry
}

type backoffEntry struct {
	failures int
	until    time.Time
}

var _ SecureTransport = (*TLSTransport)(nil)

// NewTLSTransport validates cfg and returns a transport. The certificate's
// peer id must match cfg.LocalID when that is set.
func NewTLSTransport(cfg TLSTransportConfig) (*TLSTransport, error) {
	if len(cfg.Certificate.Certificate) == 0 || cfg.Certificate.PrivateKey == nil {
		return nil, errors.New("mesh: TLS transport requires a certificate and private key")
	}
	leaf := cfg.Certificate.Leaf
	if leaf == nil {
		var err error
		if leaf, err = x509.ParseCertificate(cfg.Certificate.Certificate[0]); err != nil {
			return nil, fmt.Errorf("mesh: parse local certificate: %w", err)
		}
	}
	id, err := PeerIDFromCertificate(leaf)
	if err != nil {
		return nil, fmt.Errorf("mesh: local certificate: %w", err)
	}
	if cfg.LocalID != "" && cfg.LocalID != id {
		return nil, fmt.Errorf("%w: local id %q does not match certificate id %q", ErrPeerIdentity, cfg.LocalID, id)
	}
	if cfg.RootCAs == nil && len(cfg.PinnedPeers) == 0 {
		return nil, errors.New("mesh: TLS transport requires RootCAs or PinnedPeers to authenticate peers")
	}
	pins := make(map[PeerID][]Fingerprint, len(cfg.PinnedPeers))
	for pid, fps := range cfg.PinnedPeers {
		if err := validatePeerIDString(string(pid)); err != nil {
			return nil, err
		}
		if len(fps) > 0 {
			pins[pid] = append([]Fingerprint(nil), fps...)
		}
	}
	if len(cfg.PinnedPeers) > 0 && len(pins) == 0 {
		return nil, errors.New("mesh: PinnedPeers has no fingerprints")
	}
	if cfg.MaxFrameBytes <= 0 {
		cfg.MaxFrameBytes = DefaultMaxEnvelopeBytes
	}
	if cfg.MaxFrameBytes > maxFrameLimit {
		cfg.MaxFrameBytes = maxFrameLimit
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = DefaultTLSMaxConns
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&cfg.HandshakeTimeout, DefaultTLSHandshakeTimeout)
	def(&cfg.IdleTimeout, DefaultTLSIdleTimeout)
	def(&cfg.ReadTimeout, DefaultTLSReadTimeout)
	def(&cfg.WriteTimeout, DefaultTLSWriteTimeout)
	def(&cfg.BackoffBase, DefaultTLSBackoffBase)
	def(&cfg.BackoffMax, DefaultTLSBackoffMax)
	if cfg.BackoffMax < cfg.BackoffBase {
		cfg.BackoffMax = cfg.BackoffBase
	}

	t := &TLSTransport{
		id:               id,
		roots:            cfg.RootCAs,
		pins:             pins,
		maxFrame:         cfg.MaxFrameBytes,
		handshakeTimeout: cfg.HandshakeTimeout,
		idleTimeout:      cfg.IdleTimeout,
		readTimeout:      cfg.ReadTimeout,
		writeTimeout:     cfg.WriteTimeout,
		backoffBase:      cfg.BackoffBase,
		backoffMax:       cfg.BackoffMax,
		sem:              make(chan struct{}, cfg.MaxConns),
		conns:            make(map[*tlsPeerConn]struct{}),
		pending:          make(map[net.Conn]struct{}),
		backoff:          make(map[string]*backoffEntry),
	}
	cert := cfg.Certificate
	cert.Leaf = leaf
	base := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{MeshALPN},
	}
	t.serverCfg = base.Clone()
	// Chain, pin and identity checks happen in VerifyConnection so that
	// pinned self-signed certificates work and the id is extracted in one
	// place. Resumption is disabled so every connection re-verifies.
	t.serverCfg.ClientAuth = tls.RequireAnyClientCert
	t.serverCfg.SessionTicketsDisabled = true
	t.serverCfg.VerifyConnection = func(cs tls.ConnectionState) error {
		_, err := t.verifyPeer(cs, x509.ExtKeyUsageClientAuth, "")
		return err
	}
	t.clientCfg = base.Clone()
	// Peers are authenticated by peer id (VerifyConnection), not by host
	// name, so Go's host name verification is replaced, not skipped.
	t.clientCfg.InsecureSkipVerify = true //nolint:gosec // verified in VerifyConnection
	return t, nil
}

// LocalID returns the peer id bound to this transport's certificate.
func (t *TLSTransport) LocalID() PeerID { return t.id }

// verifyPeer authenticates the peer of a completed handshake and returns the
// peer id carried by its certificate. expect, when non-empty, must match it.
func (t *TLSTransport) verifyPeer(cs tls.ConnectionState, usage x509.ExtKeyUsage, expect PeerID) (PeerID, error) {
	if cs.Version != tls.VersionTLS13 {
		return "", fmt.Errorf("%w: TLS 1.3 required", ErrUntrustedPeer)
	}
	if cs.NegotiatedProtocol != MeshALPN {
		return "", fmt.Errorf("%w: peer did not negotiate %s", ErrUntrustedPeer, MeshALPN)
	}
	if len(cs.PeerCertificates) == 0 {
		return "", fmt.Errorf("%w: no peer certificate", ErrUntrustedPeer)
	}
	leaf := cs.PeerCertificates[0]
	if t.roots != nil {
		inter := x509.NewCertPool()
		for _, c := range cs.PeerCertificates[1:] {
			inter.AddCert(c)
		}
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: t.roots, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return "", fmt.Errorf("%w: %v", ErrUntrustedPeer, err)
		}
	} else if now := time.Now(); now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return "", fmt.Errorf("%w: certificate expired or not yet valid", ErrUntrustedPeer)
	}
	id, err := PeerIDFromCertificate(leaf)
	if err != nil {
		return "", err
	}
	if len(t.pins) > 0 {
		fp := CertificateFingerprint(leaf.Raw)
		match := 0
		for _, pinned := range t.pins[id] {
			match |= subtle.ConstantTimeCompare(pinned[:], fp[:])
		}
		if match != 1 {
			return "", fmt.Errorf("%w: certificate for %q is not pinned", ErrUntrustedPeer, id)
		}
	}
	if expect != "" && id != expect {
		return "", fmt.Errorf("%w: expected %q, certificate is for %q", ErrPeerIdentity, expect, id)
	}
	return id, nil
}

func (t *TLSTransport) acquire() bool {
	select {
	case t.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (t *TLSTransport) release() { <-t.sem }

// trackPending registers a raw connection whose handshake is in progress so
// Close can interrupt it.
func (t *TLSTransport) trackPending(raw net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.pending[raw] = struct{}{}
	return true
}

func (t *TLSTransport) untrackPending(raw net.Conn) {
	t.mu.Lock()
	delete(t.pending, raw)
	t.mu.Unlock()
}

// newConn promotes a handshaken connection to a tracked peer connection and
// starts its reader. On error raw is closed; the caller releases the slot.
func (t *TLSTransport) newConn(tc *tls.Conn, raw net.Conn, remote PeerID) (*tlsPeerConn, error) {
	c := &tlsPeerConn{
		t:      t,
		conn:   tc,
		raw:    raw,
		remote: remote,
		inbox:  make(chan Envelope, tlsInboxSize),
		done:   make(chan struct{}),
	}
	t.mu.Lock()
	delete(t.pending, raw)
	if t.closed {
		t.mu.Unlock()
		_ = raw.Close()
		return nil, ErrTransportClosed
	}
	t.conns[c] = struct{}{}
	t.wg.Add(1)
	t.mu.Unlock()
	go c.readLoop()
	return c, nil
}

func (t *TLSTransport) forget(c *tlsPeerConn) {
	t.mu.Lock()
	_, tracked := t.conns[c]
	delete(t.conns, c)
	t.mu.Unlock()
	if tracked {
		t.release()
	}
}

// identityUnknown reports whether desc is a bare-address seed (id equal to
// its address, as produced by MeshConfig.PeerDescriptors for entries without
// "id@"). Such a peer is dialed without an id expectation; any peer the
// trust configuration accepts may answer, and its certificate id is reported
// by the connection's RemotePeer.
func identityUnknown(desc PeerDescriptor) bool {
	return string(desc.ID) == strings.TrimSpace(desc.Address)
}

// Dial opens an authenticated connection to peer. The certificate at
// peer.Address must carry peer.ID (unless the descriptor is a bare-address
// seed, see MeshConfig.PeerDescriptors). ctx bounds connect and handshake
// only. After a failed dial the peer is in reconnect backoff and Dial fails
// fast with ErrPeerBackoff until it expires.
func (t *TLSTransport) Dial(ctx context.Context, peer PeerDescriptor) (PeerConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if peer.ID == "" {
		return nil, fmt.Errorf("%w: empty peer id", ErrPeerUnreachable)
	}
	hostport, err := ParseTCPAddress(peer.Address)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPeerUnreachable, err)
	}
	if _, port, _ := net.SplitHostPort(hostport); port == "0" {
		return nil, fmt.Errorf("%w: address %q has no port", ErrPeerUnreachable, peer.Address)
	}
	key := string(peer.ID) + "\x00" + hostport
	if wait := t.backoffRemaining(key); wait > 0 {
		return nil, fmt.Errorf("%w: %w: %q for another %s", ErrPeerUnreachable, ErrPeerBackoff, peer.ID, wait.Round(time.Millisecond))
	}
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil, ErrTransportClosed
	}
	if !t.acquire() {
		return nil, ErrTooManyConns
	}
	c, err := t.dial(ctx, peer, hostport)
	if err != nil {
		t.release()
		if ctx.Err() == nil && !errors.Is(err, ErrTransportClosed) {
			t.recordFailure(key)
		}
		return nil, err
	}
	t.recordSuccess(key)
	return c, nil
}

func (t *TLSTransport) dial(ctx context.Context, peer PeerDescriptor, hostport string) (*tlsPeerConn, error) {
	hctx, cancel := context.WithTimeout(ctx, t.handshakeTimeout)
	defer cancel()
	var d net.Dialer
	raw, err := d.DialContext(hctx, "tcp", hostport)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %q: %v", ErrPeerUnreachable, peer.ID, err)
	}
	if !t.trackPending(raw) {
		_ = raw.Close()
		return nil, ErrTransportClosed
	}
	fail := func(err error) (*tlsPeerConn, error) {
		t.untrackPending(raw)
		_ = raw.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	expect := peer.ID
	if identityUnknown(peer) {
		expect = ""
	}
	cfg := t.clientCfg.Clone()
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		_, err := t.verifyPeer(cs, x509.ExtKeyUsageServerAuth, expect)
		return err
	}
	tc := tls.Client(raw, cfg)
	if err := tc.HandshakeContext(hctx); err != nil {
		if errors.Is(err, ErrPeerIdentity) || errors.Is(err, ErrUntrustedPeer) {
			return fail(err)
		}
		return fail(fmt.Errorf("%w: %q: handshake: %v", ErrPeerUnreachable, peer.ID, err))
	}
	// Wait for the server's accept byte: a TLS 1.3 server verifies the
	// client certificate after the client finished its handshake.
	if dl, ok := hctx.Deadline(); ok {
		_ = raw.SetReadDeadline(dl)
	}
	stop := context.AfterFunc(hctx, func() { _ = raw.SetReadDeadline(aLongTimeAgo) })
	var hello [1]byte
	_, err = io.ReadFull(tc, hello[:])
	if !stop() && err == nil {
		// The deadline fired concurrently and may still poison the read
		// deadline the reader is about to set; give up on this connection.
		err = hctx.Err()
	}
	if err != nil || hello[0] != helloAccept {
		if err == nil {
			err = errors.New("unexpected accept byte")
		}
		return fail(fmt.Errorf("%w: %q rejected the connection: %v", ErrUntrustedPeer, peer.ID, err))
	}
	_ = raw.SetReadDeadline(time.Time{})
	remote, err := PeerIDFromCertificate(tc.ConnectionState().PeerCertificates[0])
	if err != nil {
		return fail(err)
	}
	return t.newConn(tc, raw, remote)
}

func (t *TLSTransport) backoffRemaining(key string) time.Duration {
	t.bmu.Lock()
	defer t.bmu.Unlock()
	e, ok := t.backoff[key]
	if !ok {
		return 0
	}
	return time.Until(e.until)
}

func (t *TLSTransport) recordFailure(key string) {
	now := time.Now()
	t.bmu.Lock()
	defer t.bmu.Unlock()
	e, ok := t.backoff[key]
	if !ok {
		if len(t.backoff) >= maxBackoffEntries {
			// Drop entries whose backoff expired long ago, then anything, to
			// keep the table bounded.
			for k, v := range t.backoff {
				if now.Sub(v.until) > t.backoffMax {
					delete(t.backoff, k)
				}
			}
			for k := range t.backoff {
				if len(t.backoff) < maxBackoffEntries {
					break
				}
				delete(t.backoff, k)
			}
		}
		e = &backoffEntry{}
		t.backoff[key] = e
	}
	e.failures++
	d := t.backoffMax
	if shift := e.failures - 1; shift < 30 {
		if v := t.backoffBase << shift; v > 0 && v < d {
			d = v
		}
	}
	// Equal jitter: at least half the delay, so a failing peer is never
	// retried immediately, while peers do not retry in lockstep.
	d = d/2 + rand.N(d/2+1)
	e.until = now.Add(d)
}

func (t *TLSTransport) recordSuccess(key string) {
	t.bmu.Lock()
	delete(t.backoff, key)
	t.bmu.Unlock()
}

// Listen starts accepting mesh connections on address (see
// ParseTCPAddress; port 0 picks a free port, reported by the listener's Addr
// method). Only one listener may be active. The listener is closed when ctx
// is done, when it is closed, or when the transport is closed.
func (t *TLSTransport) Listen(ctx context.Context, address string) (PeerListener, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	hostport, err := ParseTCPAddress(address)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrTransportClosed
	}
	if t.listener != nil {
		return nil, ErrAlreadyListening
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", hostport)
	if err != nil {
		return nil, fmt.Errorf("mesh: listen %s: %w", hostport, err)
	}
	l := &tlsListener{
		t:     t,
		ln:    ln,
		ready: make(chan *tlsPeerConn, tlsAcceptBacklog),
		done:  make(chan struct{}),
	}
	t.listener = l
	t.wg.Add(1)
	go l.acceptLoop()
	l.stopCtx = context.AfterFunc(ctx, func() { _ = l.Close() })
	return l, nil
}

// Close closes the listener and every connection, and waits for all
// transport goroutines to exit. It is idempotent.
func (t *TLSTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		t.wg.Wait()
		return nil
	}
	t.closed = true
	l := t.listener
	conns := make([]*tlsPeerConn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	pending := make([]net.Conn, 0, len(t.pending))
	for raw := range t.pending {
		pending = append(pending, raw)
	}
	t.mu.Unlock()

	if l != nil {
		_ = l.Close()
	}
	for _, c := range conns {
		c.shutdown(false)
	}
	for _, raw := range pending {
		_ = raw.Close()
	}
	t.wg.Wait()
	return nil
}

type tlsListener struct {
	t       *TLSTransport
	ln      net.Listener
	ready   chan *tlsPeerConn
	done    chan struct{}
	once    sync.Once
	stopCtx func() bool

	mu     sync.Mutex
	closed bool
}

// Addr returns the bound network address.
func (l *tlsListener) Addr() net.Addr { return l.ln.Addr() }

func (l *tlsListener) acceptLoop() {
	defer l.t.wg.Done()
	var delay time.Duration
	for {
		raw, err := l.ln.Accept()
		if err != nil {
			select {
			case <-l.done:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// Transient failure (e.g. EMFILE): back off like net/http.
			if delay == 0 {
				delay = 5 * time.Millisecond
			} else if delay *= 2; delay > time.Second {
				delay = time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-l.done:
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		delay = 0
		if !l.t.acquire() {
			// Over the limit: refuse before spending anything on a handshake.
			_ = raw.Close()
			continue
		}
		if !l.t.trackPending(raw) {
			_ = raw.Close()
			l.t.release()
			return
		}
		l.t.wg.Add(1)
		go l.serveHandshake(raw)
	}
}

func (l *tlsListener) serveHandshake(raw net.Conn) {
	defer l.t.wg.Done()
	t := l.t
	fail := func() {
		t.untrackPending(raw)
		_ = raw.Close()
		t.release()
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.handshakeTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	tc := tls.Server(raw, t.serverCfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		fail()
		return
	}
	remote, err := PeerIDFromCertificate(tc.ConnectionState().PeerCertificates[0])
	if err != nil {
		fail()
		return
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = raw.SetWriteDeadline(dl)
	}
	if _, err := tc.Write([]byte{helloAccept}); err != nil {
		fail()
		return
	}
	_ = raw.SetWriteDeadline(time.Time{})
	if !stop() {
		// The handshake deadline fired and closed raw.
		fail()
		return
	}
	c, err := t.newConn(tc, raw, remote)
	if err != nil {
		t.release()
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		c.shutdown(false)
		return
	}
	select {
	case l.ready <- c:
	default:
		// Nobody is accepting: shed the connection instead of queueing it.
		c.shutdown(false)
	}
}

// Accept returns the next authenticated inbound connection.
func (l *tlsListener) Accept(ctx context.Context) (PeerConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-l.done:
		return nil, ErrListenerClosed
	default:
	}
	select {
	case c := <-l.ready:
		return c, nil
	case <-l.done:
		return nil, ErrListenerClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close stops accepting and closes connections that were never accepted.
func (l *tlsListener) Close() error {
	l.once.Do(func() {
		l.mu.Lock()
		l.closed = true
		l.mu.Unlock()
		_ = l.ln.Close() // the accept loop exits on net.ErrClosed
		if l.stopCtx != nil {
			l.stopCtx()
		}
		// Free the transport's listener slot before signalling closure, so
		// anyone who observes ErrListenerClosed can Listen again at once.
		l.t.mu.Lock()
		if l.t.listener == l {
			l.t.listener = nil
		}
		l.t.mu.Unlock()
		l.mu.Lock()
		close(l.done)
		l.mu.Unlock()
		for {
			select {
			case c := <-l.ready:
				c.shutdown(false)
			default:
				return
			}
		}
	})
	return nil
}

// tlsPeerConn is one authenticated mesh connection.
type tlsPeerConn struct {
	t      *TLSTransport
	conn   *tls.Conn
	raw    net.Conn
	remote PeerID
	inbox  chan Envelope
	done   chan struct{}
	once   sync.Once
	wmu    sync.Mutex
}

// wireEnvelope is the JSON body of a frame. From is deliberately absent: the
// receiver sets it from the sender's certificate.
type wireEnvelope struct {
	StateType string          `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// RemotePeer returns the peer id carried by the remote certificate.
func (c *tlsPeerConn) RemotePeer() PeerID { return c.remote }

// Send writes env as one frame. env.From is ignored (the receiver takes the
// sender id from its certificate). The write is bounded by WriteTimeout and
// ctx; a failed or interrupted write closes the connection, since a partial
// frame corrupts the stream.
func (c *tlsPeerConn) Send(ctx context.Context, env Envelope) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.done:
		return ErrConnClosed
	default:
	}
	if env.StateType == "" || len(env.StateType) > maxStateTypeLen || hasControl(env.StateType) {
		return fmt.Errorf("mesh: invalid state type %q", truncateStr(env.StateType, 80))
	}
	body, err := json.Marshal(wireEnvelope{StateType: env.StateType, Payload: env.Payload})
	if err != nil {
		return fmt.Errorf("mesh: encode envelope: %w", err)
	}
	if len(body) > c.t.maxFrame {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrEnvelopeTooLarge, len(body), c.t.maxFrame)
	}
	frame := make([]byte, frameHeaderLen+len(body))
	binary.BigEndian.PutUint32(frame, uint32(len(body)))
	copy(frame[frameHeaderLen:], body)

	c.wmu.Lock()
	defer c.wmu.Unlock()
	deadline := time.Now().Add(c.t.writeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.raw.SetWriteDeadline(deadline)
	// Cancellation interrupts a blocked write. interrupt is disarmed before
	// Send returns, so a late callback cannot poison the next Send's deadline.
	var imu sync.Mutex
	armed := true
	stop := context.AfterFunc(ctx, func() {
		imu.Lock()
		defer imu.Unlock()
		if armed {
			_ = c.raw.SetWriteDeadline(aLongTimeAgo)
		}
	})
	_, err = c.conn.Write(frame)
	if !stop() {
		imu.Lock()
		armed = false
		imu.Unlock()
	}
	if err != nil {
		c.shutdown(false)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("mesh: send to %q: %w", c.remote, err)
	}
	return nil
}

// Receive returns the channel of inbound envelopes. It is closed when the
// connection ends (remote close, error, idle timeout or Close), after any
// buffered envelopes.
func (c *tlsPeerConn) Receive(_ context.Context) (<-chan Envelope, error) {
	return c.inbox, nil
}

// Close closes the connection, sending a TLS close_notify (bounded by 5s).
// It is idempotent.
func (c *tlsPeerConn) Close() error {
	c.shutdown(true)
	return nil
}

func (c *tlsPeerConn) shutdown(graceful bool) {
	c.once.Do(func() {
		close(c.done)
		if graceful {
			// tls.Conn.Close skips the close_notify if a write is in
			// flight, and otherwise bounds it by a 5s write deadline.
			_ = c.conn.Close()
		} else {
			_ = c.raw.Close()
		}
		c.t.forget(c)
	})
}

func (c *tlsPeerConn) readLoop() {
	defer c.t.wg.Done()
	defer close(c.inbox)
	defer c.shutdown(false)
	br := bufio.NewReaderSize(c.conn, 16<<10)
	var hdr [frameHeaderLen]byte
	for {
		_ = c.raw.SetReadDeadline(time.Now().Add(c.t.idleTimeout))
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n == 0 || uint64(n) > uint64(c.t.maxFrame) {
			// Oversized (or empty) frame: drop the connection without
			// reading or allocating the body.
			return
		}
		_ = c.raw.SetReadDeadline(time.Now().Add(c.t.readTimeout))
		// Grow with the data actually received rather than trusting n.
		var body bytes.Buffer
		body.Grow(int(min(n, 64<<10)))
		if _, err := io.CopyN(&body, br, int64(n)); err != nil {
			return
		}
		var w wireEnvelope
		dec := json.NewDecoder(bytes.NewReader(body.Bytes()))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&w); err != nil || dec.More() {
			return
		}
		if w.StateType == "" || len(w.StateType) > maxStateTypeLen || hasControl(w.StateType) {
			return
		}
		env := Envelope{From: c.remote, StateType: w.StateType, Payload: w.Payload, Received: time.Now()}
		select {
		case c.inbox <- env:
		case <-c.done:
			return
		}
	}
}

// LoadTLSTransportConfig builds a config from a certificate chain, private
// key and optional CA bundle. Each argument is either inline PEM or a path to
// a PEM file; caSrc may be empty when peers are authenticated by pins only.
func LoadTLSTransportConfig(certSrc, keySrc, caSrc string) (TLSTransportConfig, error) {
	certPEM, err := readPEMSource("certificate", certSrc)
	if err != nil {
		return TLSTransportConfig{}, err
	}
	keyPEM, err := readPEMSource("private key", keySrc)
	if err != nil {
		return TLSTransportConfig{}, err
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return TLSTransportConfig{}, fmt.Errorf("mesh: load certificate/key: %w", err)
	}
	cfg := TLSTransportConfig{Certificate: cert}
	if strings.TrimSpace(caSrc) != "" {
		caPEM, err := readPEMSource("CA bundle", caSrc)
		if err != nil {
			return TLSTransportConfig{}, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return TLSTransportConfig{}, errors.New("mesh: CA bundle contains no certificates")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// readPEMSource returns src itself when it is inline PEM, else the contents
// of the file it names (at most 1 MiB).
func readPEMSource(what, src string) ([]byte, error) {
	s := strings.TrimSpace(src)
	if s == "" {
		return nil, fmt.Errorf("mesh: %s not configured", what)
	}
	if strings.HasPrefix(s, "-----BEGIN") {
		if len(s) > maxPEMBytes {
			return nil, fmt.Errorf("mesh: inline %s exceeds %d bytes", what, maxPEMBytes)
		}
		return []byte(s), nil
	}
	f, err := os.Open(s)
	if err != nil {
		return nil, fmt.Errorf("mesh: read %s: %w", what, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxPEMBytes+1))
	if err != nil {
		return nil, fmt.Errorf("mesh: read %s: %w", what, err)
	}
	if len(b) > maxPEMBytes {
		return nil, fmt.Errorf("mesh: %s file %s exceeds %d bytes", what, s, maxPEMBytes)
	}
	if block, _ := pem.Decode(b); block == nil {
		return nil, fmt.Errorf("mesh: %s file %s is not PEM", what, s)
	}
	return b, nil
}

// TLSTransportConfigFromEnv reads AEROLLM_MESH_TLS_CERT, _KEY, _CA and _PINS.
// ok is false (with a nil error) when neither CERT nor KEY is set, meaning
// the network mesh is not configured. Setting only one of CERT/KEY, or
// neither CA nor PINS, is an error. Timeouts and limits keep their defaults.
func TLSTransportConfigFromEnv(getenv func(string) string) (cfg TLSTransportConfig, ok bool, err error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	certSrc := strings.TrimSpace(getenv(EnvMeshTLSCert))
	keySrc := strings.TrimSpace(getenv(EnvMeshTLSKey))
	caSrc := strings.TrimSpace(getenv(EnvMeshTLSCA))
	pinSpec := strings.TrimSpace(getenv(EnvMeshTLSPins))
	if certSrc == "" && keySrc == "" {
		if caSrc != "" || pinSpec != "" {
			return TLSTransportConfig{}, false, fmt.Errorf("mesh: %s/%s are required when %s or %s is set", EnvMeshTLSCert, EnvMeshTLSKey, EnvMeshTLSCA, EnvMeshTLSPins)
		}
		return TLSTransportConfig{}, false, nil
	}
	if certSrc == "" || keySrc == "" {
		return TLSTransportConfig{}, false, fmt.Errorf("mesh: both %s and %s must be set", EnvMeshTLSCert, EnvMeshTLSKey)
	}
	if caSrc == "" && pinSpec == "" {
		return TLSTransportConfig{}, false, fmt.Errorf("mesh: set %s and/or %s to authenticate peers", EnvMeshTLSCA, EnvMeshTLSPins)
	}
	cfg, err = LoadTLSTransportConfig(certSrc, keySrc, caSrc)
	if err != nil {
		return TLSTransportConfig{}, false, err
	}
	if pinSpec != "" {
		if cfg.PinnedPeers, err = ParsePeerPins(pinSpec); err != nil {
			return TLSTransportConfig{}, false, err
		}
	}
	return cfg, true, nil
}

// NewTLSTransportFromEnv builds a TLS transport from the AEROLLM_MESH_TLS_*
// variables (see TLSTransportConfigFromEnv). localID may be empty to use the
// certificate's peer id; otherwise it must match it. ok is false when the
// variables are not set.
func NewTLSTransportFromEnv(localID PeerID, getenv func(string) string) (*TLSTransport, bool, error) {
	cfg, ok, err := TLSTransportConfigFromEnv(getenv)
	if err != nil || !ok {
		return nil, ok, err
	}
	cfg.LocalID = localID
	t, err := NewTLSTransport(cfg)
	if err != nil {
		return nil, false, err
	}
	return t, true, nil
}
