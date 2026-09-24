package mesh_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
)

func newTLSNode(t *testing.T, ca *testCA, id string, mutate func(*mesh.TLSTransportConfig)) *mesh.TLSTransport {
	t.Helper()
	cert, _, _ := issue(t, ca, id, leafOpts{})
	cfg := mesh.TLSTransportConfig{Certificate: cert, HandshakeTimeout: 3 * time.Second, BackoffBase: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond}
	if ca != nil {
		cfg.RootCAs = ca.pool
	}
	if mutate != nil {
		mutate(&cfg)
	}
	tr, err := mesh.NewTLSTransport(cfg)
	if err != nil {
		t.Fatalf("new transport %s: %v", id, err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func listenTLS(t *testing.T, tr *mesh.TLSTransport) (mesh.PeerListener, string) {
	t.Helper()
	l, err := tr.Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return l, l.(interface{ Addr() net.Addr }).Addr().String()
}

func dialTLS(t *testing.T, tr *mesh.TLSTransport, id, addr string) mesh.PeerConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := tr.Dial(ctx, mesh.PeerDescriptor{ID: mesh.PeerID(id), Address: addr})
	if err != nil {
		t.Fatalf("dial %s at %s: %v", id, addr, err)
	}
	return c
}

// expectNoAccept asserts that nothing reaches the listener.
func expectNoAccept(t *testing.T, l mesh.PeerListener) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if c, err := l.Accept(ctx); err == nil {
		_ = c.Close()
		t.Fatal("an unauthenticated connection was accepted")
	}
}

// expectClosed asserts that c's inbound channel closes without delivering.
func expectClosed(t *testing.T, c mesh.PeerConn) {
	t.Helper()
	ch, _ := c.Receive(context.Background())
	select {
	case env, ok := <-ch:
		if ok {
			t.Fatalf("unexpected envelope delivered: %+v", env)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not closed")
	}
}

func TestTLSTransportRoundTripBindsSenderIdentity(t *testing.T) {
	baseline := runtime.NumGoroutine()
	ca := newTestCA(t)
	a := newTLSNode(t, ca, "node-a", nil)
	b := newTLSNode(t, ca, "node-b", nil)
	if a.LocalID() != "node-a" {
		t.Fatalf("local id from certificate: %q", a.LocalID())
	}
	lb, addrB := listenTLS(t, b)

	conn := dialTLS(t, a, "node-b", addrB)
	if rp := conn.(interface{ RemotePeer() mesh.PeerID }).RemotePeer(); rp != "node-b" {
		t.Fatalf("RemotePeer = %q", rp)
	}
	server := acceptOne(t, lb)

	// The sender's claimed From is ignored; the certificate id is used.
	if err := conn.Send(context.Background(), mesh.Envelope{From: "node-b", StateType: "t", Payload: json.RawMessage(`{"x":1}`)}); err != nil {
		t.Fatalf("send: %v", err)
	}
	env, ok := recvOne(t, server)
	if !ok || env.From != "node-a" || env.StateType != "t" || string(env.Payload) != `{"x":1}` || env.Received.IsZero() {
		t.Fatalf("unexpected envelope %+v ok=%v", env, ok)
	}
	if err := server.Send(context.Background(), mesh.Envelope{StateType: "reply"}); err != nil {
		t.Fatalf("reply: %v", err)
	}
	if env, ok := recvOne(t, conn); !ok || env.From != "node-b" || env.StateType != "reply" || len(env.Payload) != 0 {
		t.Fatalf("unexpected reply %+v ok=%v", env, ok)
	}
	if err := conn.Send(context.Background(), mesh.Envelope{}); err == nil {
		t.Fatal("expected an empty state type to be rejected")
	}

	_ = conn.Close()
	_ = conn.Close()
	expectClosed(t, server)
	if err := conn.Send(context.Background(), mesh.Envelope{StateType: "t"}); !errors.Is(err, mesh.ErrConnClosed) {
		t.Fatalf("send after close: %v", err)
	}
	_ = server.Close()

	_ = a.Close()
	_ = b.Close()
	if _, err := lb.Accept(context.Background()); !errors.Is(err, mesh.ErrListenerClosed) {
		t.Fatalf("accept after close: %v", err)
	}
	if _, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "node-b", Address: addrB}); !errors.Is(err, mesh.ErrTransportClosed) {
		t.Fatalf("dial after close: %v", err)
	}
	if _, err := b.Listen(context.Background(), "127.0.0.1:0"); !errors.Is(err, mesh.ErrTransportClosed) {
		t.Fatalf("listen after close: %v", err)
	}
	requireNoGoroutineLeak(t, baseline)
}

type tlsMeshNode struct {
	id        mesh.PeerID
	state     *mesh.PluginRegistrySync
	transport *mesh.TLSTransport
	discovery *mesh.Discovery
	worker    *mesh.SyncWorker
	done      chan struct{}
}

func startTLSMeshNode(t *testing.T, ctx context.Context, ca *testCA, id mesh.PeerID, peerAddresses []string) *tlsMeshNode {
	t.Helper()
	tr := newTLSNode(t, ca, string(id), nil)
	cfg := mesh.MeshConfig{Enabled: true, LocalPeerID: id, BindAddress: "127.0.0.1:0", PeerAddresses: peerAddresses, GossipInterval: 50 * time.Millisecond}
	n := &tlsMeshNode{id: id, state: mesh.NewPluginRegistrySyncForNode(id), transport: tr, done: make(chan struct{})}
	n.discovery = mesh.NewDiscovery(mesh.DiscoveryConfig{
		LocalID:     id,
		BindAddress: cfg.BindAddress,
		Peers:       cfg.PeerDescriptors(),
		Transport:   tr,
		Interval:    50 * time.Millisecond,
	})
	n.worker = mesh.NewSyncWorker(mesh.SyncWorkerConfig{State: n.state, Discovery: n.discovery, Interval: cfg.GossipInterval})
	n.state.Registry.Add("plugin-"+string(id), mesh.Now(), []byte(`{"owner":"`+string(id)+`"}`))
	go func() {
		defer close(n.done)
		n.worker.Start(ctx)
	}()
	waitFor(t, 5*time.Second, "listener bound", func() bool {
		return !strings.HasSuffix(n.discovery.Self().Address, ":0")
	})
	return n
}

// TestTLSMeshCRDTSyncOverRealNetwork runs Discovery + SyncWorker over real
// mutual-TLS connections on 127.0.0.1: b seeds a as "id@address", c seeds a
// by bare address (its identity is learned from the certificate).
func TestTLSMeshCRDTSyncOverRealNetwork(t *testing.T) {
	baseline := runtime.NumGoroutine()
	ca := newTestCA(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := startTLSMeshNode(t, ctx, ca, "node-a", nil)
	addrA := a.discovery.Self().Address
	b := startTLSMeshNode(t, ctx, ca, "node-b", []string{"node-a@" + addrA})
	c := startTLSMeshNode(t, ctx, ca, "node-c", []string{addrA})
	nodes := []*tlsMeshNode{a, b, c}

	waitFor(t, 10*time.Second, "registry convergence over TLS", func() bool {
		for _, n := range nodes {
			if len(n.state.Registry.Elements()) != 3 {
				return false
			}
		}
		return true
	})
	// c's bare-address seed was re-keyed to the certificate identity.
	waitFor(t, 5*time.Second, "bare seed re-keyed", func() bool {
		peers := c.discovery.Peers()
		return len(peers) == 1 && peers[0].ID == "node-a" && peers[0].Address == addrA
	})
	if got := len(a.discovery.Peers()); got != 2 {
		t.Fatalf("node-a knows %d peers, want 2: %+v", got, a.discovery.Peers())
	}

	c.state.Registry.Remove("plugin-node-a", mesh.Now())
	waitFor(t, 10*time.Second, "removal convergence over TLS", func() bool {
		for _, n := range nodes {
			if _, ok := n.state.Registry.Lookup("plugin-node-a"); ok {
				return false
			}
		}
		return true
	})
	for _, n := range nodes {
		if st := n.worker.Stats(); st.Sent == 0 || st.Received == 0 {
			t.Fatalf("%s: expected traffic, got %+v", n.id, st)
		}
	}

	cancel()
	for _, n := range nodes {
		select {
		case <-n.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: worker did not stop", n.id)
		}
		select {
		case <-n.discovery.Stopped():
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: discovery did not stop", n.id)
		}
		_ = n.transport.Close()
	}
	requireNoGoroutineLeak(t, baseline)
}

func TestTLSTransportRejectsUntrustedCA(t *testing.T) {
	ca1, ca2 := newTestCA(t), newTestCA(t)
	b := newTLSNode(t, ca1, "node-b", nil)
	lb, addrB := listenTLS(t, b)

	// A client whose certificate comes from another CA is refused by the
	// server, and Dial reports it (not the first Send).
	evil := newTLSNode(t, ca2, "evil", func(c *mesh.TLSTransportConfig) { c.RootCAs = ca1.pool })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := evil.Dial(ctx, mesh.PeerDescriptor{ID: "node-b", Address: addrB}); !errors.Is(err, mesh.ErrUntrustedPeer) {
		t.Fatalf("expected the server to reject a foreign-CA client, got %v", err)
	}
	expectNoAccept(t, lb)

	// A server whose certificate comes from another CA is refused by the
	// client.
	evilSrv := newTLSNode(t, ca2, "node-x", func(c *mesh.TLSTransportConfig) { c.RootCAs = ca1.pool })
	lx, addrX := listenTLS(t, evilSrv)
	a := newTLSNode(t, ca1, "node-a", nil)
	if _, err := a.Dial(ctx, mesh.PeerDescriptor{ID: "node-x", Address: addrX}); !errors.Is(err, mesh.ErrUntrustedPeer) {
		t.Fatalf("expected the client to reject a foreign-CA server, got %v", err)
	}
	expectNoAccept(t, lx)

	// A client without any certificate is refused.
	raw, err := tls.Dial("tcp", addrB, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{mesh.MeshALPN}, MinVersion: tls.VersionTLS13}) //nolint:gosec // test client
	if err == nil {
		_ = raw.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, rerr := raw.Read(make([]byte, 1)); rerr == nil {
			t.Fatal("server accepted a client without a certificate")
		}
		_ = raw.Close()
	}
	expectNoAccept(t, lb)

	// A valid client that does not negotiate the mesh protocol is refused.
	cert, _, _ := issue(t, ca1, "node-c", leafOpts{})
	raw, err = tls.Dial("tcp", addrB, &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}) //nolint:gosec // test client
	if err == nil {
		_ = raw.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, rerr := raw.Read(make([]byte, 1)); rerr == nil {
			t.Fatal("server accepted a client without the mesh ALPN")
		}
		_ = raw.Close()
	}
	expectNoAccept(t, lb)

	// The trusted peer still gets through.
	good := newTLSNode(t, ca1, "node-a2", nil)
	conn := dialTLS(t, good, "node-b", addrB)
	defer conn.Close()
	s := acceptOne(t, lb)
	_ = s.Close()
}

func TestTLSTransportRejectsIDSpoofing(t *testing.T) {
	ca := newTestCA(t)
	mallory := newTLSNode(t, ca, "mallory", nil)
	lm, addrM := listenTLS(t, mallory)
	a := newTLSNode(t, ca, "node-a", nil)

	// mallory holds a valid certificate, but for her own id: dialing "bob"
	// at her address fails.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := a.Dial(ctx, mesh.PeerDescriptor{ID: "bob", Address: addrM}); !errors.Is(err, mesh.ErrPeerIdentity) {
		t.Fatalf("expected ErrPeerIdentity, got %v", err)
	}
	expectNoAccept(t, lm)

	// A node cannot claim an id its certificate does not carry.
	cert, _, _ := issue(t, ca, "mallory", leafOpts{})
	if _, err := mesh.NewTLSTransport(mesh.TLSTransportConfig{LocalID: "bob", Certificate: cert, RootCAs: ca.pool}); !errors.Is(err, mesh.ErrPeerIdentity) {
		t.Fatalf("expected local id mismatch, got %v", err)
	}
	// Certificates naming several mesh ids are ambiguous and rejected.
	multi, _, _ := issue(t, ca, "", leafOpts{uris: []string{"urn:aerollm:mesh:bob", "urn:aerollm:mesh:mallory"}})
	if _, err := mesh.NewTLSTransport(mesh.TLSTransportConfig{Certificate: multi, RootCAs: ca.pool}); !errors.Is(err, mesh.ErrUntrustedPeer) {
		t.Fatalf("expected ambiguous certificate to be rejected, got %v", err)
	}
	// A server presenting such a certificate is rejected by clients too.
	multiNode, err := mesh.NewTLSTransport(mesh.TLSTransportConfig{Certificate: multi, RootCAs: ca.pool, LocalID: ""})
	if err == nil {
		_ = multiNode.Close()
		t.Fatal("ambiguous certificate accepted")
	}
}

func TestPeerIDFromCertificate(t *testing.T) {
	ca := newTestCA(t)
	parse := func(c tls.Certificate) *x509.Certificate {
		x, err := x509.ParseCertificate(c.Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		return x
	}
	cases := []struct {
		name string
		opts leafOpts
		id   string
		want mesh.PeerID
		ok   bool
	}{
		{"uri san", leafOpts{cn: "ignored"}, "node-1", "node-1", true},
		{"common name fallback", leafOpts{uris: []string{"https://example.com"}, cn: "node-cn"}, "", "node-cn", true},
		{"duplicate same id", leafOpts{uris: []string{"urn:aerollm:mesh:n", "URN:AEROLLM:MESH:n"}}, "", "n", true},
		{"escaped", leafOpts{uris: []string{"urn:aerollm:mesh:a%20b"}}, "", "a b", true},
		{"no identity", leafOpts{uris: []string{}}, "", "", false},
		{"ambiguous", leafOpts{uris: []string{"urn:aerollm:mesh:a", "urn:aerollm:mesh:b"}}, "", "", false},
		{"control chars", leafOpts{uris: []string{}, cn: "bad\nid"}, "", "", false},
	}
	for _, c := range cases {
		cert, _, _ := issue(t, ca, c.id, c.opts)
		got, err := mesh.PeerIDFromCertificate(parse(cert))
		if (err == nil) != c.ok || got != c.want {
			t.Errorf("%s: got %q, %v; want %q ok=%v", c.name, got, err, c.want, c.ok)
		}
	}
	if _, err := mesh.PeerIDFromCertificate(nil); err == nil {
		t.Fatal("nil certificate accepted")
	}
}

func TestTLSTransportPinnedPeers(t *testing.T) {
	// Self-signed certificates, no CA: trust comes from pins only.
	certA, _, _ := issue(t, nil, "node-a", leafOpts{})
	certB, _, _ := issue(t, nil, "node-b", leafOpts{})
	certC, _, _ := issue(t, nil, "node-c", leafOpts{})
	fakeB, _, _ := issue(t, nil, "node-b", leafOpts{}) // same id, different key
	expired, _, _ := issue(t, nil, "node-e", leafOpts{notAfter: time.Now().Add(-time.Minute)})
	fp := func(c tls.Certificate) mesh.Fingerprint { return mesh.CertificateFingerprint(c.Certificate[0]) }
	pins := map[mesh.PeerID][]mesh.Fingerprint{
		"node-a": {fp(certA)},
		"node-b": {fp(certB)},
		"node-e": {fp(expired)},
	}
	mk := func(cert tls.Certificate) *mesh.TLSTransport {
		tr, err := mesh.NewTLSTransport(mesh.TLSTransportConfig{Certificate: cert, PinnedPeers: pins, HandshakeTimeout: 3 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tr.Close() })
		return tr
	}
	a, b := mk(certA), mk(certB)
	lb, addrB := listenTLS(t, b)
	conn := dialTLS(t, a, "node-b", addrB)
	s := acceptOne(t, lb)
	if err := conn.Send(context.Background(), mesh.Envelope{StateType: "x"}); err != nil {
		t.Fatal(err)
	}
	if env, ok := recvOne(t, s); !ok || env.From != "node-a" {
		t.Fatalf("unexpected %+v", env)
	}
	_ = conn.Close()
	_ = s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// An unpinned peer is refused by the server.
	if _, err := mk(certC).Dial(ctx, mesh.PeerDescriptor{ID: "node-b", Address: addrB}); !errors.Is(err, mesh.ErrUntrustedPeer) {
		t.Fatalf("unpinned client: %v", err)
	}
	// An expired pinned certificate is refused.
	if _, err := mk(expired).Dial(ctx, mesh.PeerDescriptor{ID: "node-b", Address: addrB}); !errors.Is(err, mesh.ErrUntrustedPeer) {
		t.Fatalf("expired client: %v", err)
	}
	expectNoAccept(t, lb)
	// A server with the right id but an unpinned key is refused by the client.
	_, addrFake := listenTLS(t, mk(fakeB))
	if _, err := a.Dial(ctx, mesh.PeerDescriptor{ID: "node-b", Address: addrFake}); !errors.Is(err, mesh.ErrUntrustedPeer) {
		t.Fatalf("impostor server: %v", err)
	}
}

func TestTLSTransportRejectsOversizedFrames(t *testing.T) {
	ca := newTestCA(t)
	a := newTLSNode(t, ca, "node-a", nil)
	b := newTLSNode(t, ca, "node-b", func(c *mesh.TLSTransportConfig) { c.MaxFrameBytes = 1024 })
	lb, addrB := listenTLS(t, b)

	conn := dialTLS(t, a, "node-b", addrB)
	defer conn.Close()
	server := acceptOne(t, lb)
	defer server.Close()
	if err := conn.Send(context.Background(), mesh.Envelope{StateType: "small", Payload: json.RawMessage(`"ok"`)}); err != nil {
		t.Fatal(err)
	}
	if env, ok := recvOne(t, server); !ok || env.StateType != "small" {
		t.Fatalf("unexpected %+v", env)
	}
	// The receiver refuses a frame whose header exceeds its limit and drops
	// the connection without delivering it.
	big := json.RawMessage(`"` + strings.Repeat("x", 4096) + `"`)
	_ = conn.Send(context.Background(), mesh.Envelope{StateType: "big", Payload: big})
	expectClosed(t, server)

	// The sender refuses to encode a frame above its own limit.
	small := newTLSNode(t, ca, "node-s", func(c *mesh.TLSTransportConfig) { c.MaxFrameBytes = 64 })
	conn2 := dialTLS(t, small, "node-b", addrB)
	defer conn2.Close()
	if err := conn2.Send(context.Background(), mesh.Envelope{StateType: "big", Payload: big}); !errors.Is(err, mesh.ErrEnvelopeTooLarge) {
		t.Fatalf("expected ErrEnvelopeTooLarge, got %v", err)
	}
	// Invalid JSON payloads are rejected before they reach the wire.
	if err := conn2.Send(context.Background(), mesh.Envelope{StateType: "x", Payload: json.RawMessage(`{`)}); err == nil {
		t.Fatal("expected invalid payload to be rejected")
	}
}

func TestTLSTransportConnectionLimitAndHandshakeTimeout(t *testing.T) {
	ca := newTestCA(t)
	b := newTLSNode(t, ca, "node-b", func(c *mesh.TLSTransportConfig) {
		c.MaxConns = 1
		c.HandshakeTimeout = 300 * time.Millisecond
	})
	lb, addrB := listenTLS(t, b)

	// A client that connects and never handshakes holds the only slot until
	// the handshake timeout drops it.
	raw, err := net.Dial("tcp", addrB)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	time.Sleep(50 * time.Millisecond)
	a := newTLSNode(t, ca, "node-a", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := a.Dial(ctx, mesh.PeerDescriptor{ID: "node-b", Address: addrB}); err == nil {
		t.Fatal("expected refusal while the connection limit is reached")
	}
	_ = raw.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := raw.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("stalled handshake was not dropped by the server: %v", err)
	}

	// The slot is free again.
	var conn mesh.PeerConn
	waitFor(t, 5*time.Second, "slot released", func() bool {
		c, err := a.Dial(ctx, mesh.PeerDescriptor{ID: "node-b", Address: addrB})
		if err != nil {
			return false
		}
		conn = c
		return true
	})
	defer conn.Close()
	server := acceptOne(t, lb)
	defer server.Close()

	// Garbage instead of a TLS ClientHello is dropped too.
	junk, err := net.Dial("tcp", addrB)
	if err == nil {
		_, _ = junk.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
		_ = junk.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = io.Copy(io.Discard, junk)
		_ = junk.Close()
	}

	// The dialer enforces its own limit as well.
	one := newTLSNode(t, ca, "node-one", func(c *mesh.TLSTransportConfig) { c.MaxConns = 1 })
	lo, addrO := listenTLS(t, newTLSNode(t, ca, "node-o", nil))
	first := dialTLS(t, one, "node-o", addrO)
	defer first.Close()
	if _, err := one.Dial(ctx, mesh.PeerDescriptor{ID: "node-o", Address: addrO}); !errors.Is(err, mesh.ErrTooManyConns) {
		t.Fatalf("expected ErrTooManyConns, got %v", err)
	}
	s := acceptOne(t, lo)
	_ = s.Close()
}

func TestTLSTransportReconnectBackoff(t *testing.T) {
	ca := newTestCA(t)
	a := newTLSNode(t, ca, "node-a", func(c *mesh.TLSTransportConfig) {
		c.BackoffBase = 200 * time.Millisecond
		c.BackoffMax = time.Second
		c.HandshakeTimeout = 2 * time.Second
	})
	// A port with nothing listening.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := ln.Addr().String()
	_ = ln.Close()

	peer := mesh.PeerDescriptor{ID: "node-b", Address: dead}
	if _, err := a.Dial(context.Background(), peer); err == nil || errors.Is(err, mesh.ErrPeerBackoff) {
		t.Fatalf("first dial should really fail, got %v", err)
	}
	start := time.Now()
	_, err = a.Dial(context.Background(), peer)
	if !errors.Is(err, mesh.ErrPeerBackoff) || !errors.Is(err, mesh.ErrPeerUnreachable) {
		t.Fatalf("expected fast backoff failure, got %v", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("backoff dial touched the network")
	}
	// The backoff is per peer id and address.
	b := newTLSNode(t, ca, "node-b", nil)
	lb, addrB := listenTLS(t, b)
	if _, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "wrong", Address: addrB}); !errors.Is(err, mesh.ErrPeerIdentity) {
		t.Fatalf("expected identity failure, got %v", err)
	}
	if _, err := a.Dial(context.Background(), mesh.PeerDescriptor{ID: "wrong", Address: addrB}); !errors.Is(err, mesh.ErrPeerBackoff) {
		t.Fatalf("expected backoff after identity failure, got %v", err)
	}
	conn := dialTLS(t, a, "node-b", addrB)
	_ = conn.Close()
	s := acceptOne(t, lb)
	_ = s.Close()

	// Once the delay passes the peer is dialed again.
	waitFor(t, 3*time.Second, "backoff expiry", func() bool {
		_, err := a.Dial(context.Background(), peer)
		return err != nil && !errors.Is(err, mesh.ErrPeerBackoff)
	})

	// A canceled dial does not count as a peer failure.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Dial(ctx, mesh.PeerDescriptor{ID: "node-b", Address: addrB}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	conn = dialTLS(t, a, "node-b", addrB)
	_ = conn.Close()
}

func TestTLSTransportListenLifecycle(t *testing.T) {
	baseline := runtime.NumGoroutine()
	ca := newTestCA(t)
	b := newTLSNode(t, ca, "node-b", nil)
	ctx, cancel := context.WithCancel(context.Background())
	l, err := b.Listen(ctx, "/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Listen(context.Background(), "127.0.0.1:0"); !errors.Is(err, mesh.ErrAlreadyListening) {
		t.Fatalf("expected ErrAlreadyListening, got %v", err)
	}
	addr := l.(interface{ Addr() net.Addr }).Addr().String()
	a := newTLSNode(t, ca, "node-a", nil)
	conn := dialTLS(t, a, "node-b", addr)
	server := acceptOne(t, l)

	// Canceling the Listen context closes the listener but not established
	// connections.
	cancel()
	waitFor(t, 3*time.Second, "listener closed by ctx", func() bool {
		actx, acancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer acancel()
		_, err := l.Accept(actx)
		return errors.Is(err, mesh.ErrListenerClosed)
	})
	if err := conn.Send(context.Background(), mesh.Envelope{StateType: "still-open"}); err != nil {
		t.Fatal(err)
	}
	if env, ok := recvOne(t, server); !ok || env.StateType != "still-open" {
		t.Fatalf("unexpected %+v", env)
	}
	// A new listener may be started after the old one closed.
	l2, err := b.Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("relisten: %v", err)
	}

	// Closing the transport closes every connection and goroutine.
	_ = b.Close()
	expectClosed(t, server)
	expectClosed(t, conn)
	if _, err := l2.Accept(context.Background()); !errors.Is(err, mesh.ErrListenerClosed) {
		t.Fatalf("expected ErrListenerClosed, got %v", err)
	}
	_ = a.Close()
	_ = a.Close()
	requireNoGoroutineLeak(t, baseline)
}

func TestTLSTransportConfigFromEnv(t *testing.T) {
	ca := newTestCA(t)
	_, certPEM, keyPEM := issue(t, ca, "node-env", leafOpts{})
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	certPath, keyPath, caPath := write("node.crt", certPEM), write("node.key", keyPEM), write("ca.crt", ca.pem)
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	tr, ok, err := mesh.NewTLSTransportFromEnv("", env(map[string]string{
		mesh.EnvMeshTLSCert: certPath, mesh.EnvMeshTLSKey: keyPath, mesh.EnvMeshTLSCA: caPath,
	}))
	if err != nil || !ok || tr.LocalID() != "node-env" {
		t.Fatalf("files: %v %v", ok, err)
	}
	_ = tr.Close()

	// Inline PEM works too, and pins can replace the CA.
	cert, _, _ := issue(t, ca, "node-peer", leafOpts{})
	pin := "node-peer=" + mesh.CertificateFingerprint(cert.Certificate[0]).String()
	tr, ok, err = mesh.NewTLSTransportFromEnv("node-env", env(map[string]string{
		mesh.EnvMeshTLSCert: string(certPEM), mesh.EnvMeshTLSKey: string(keyPEM), mesh.EnvMeshTLSPins: pin,
	}))
	if err != nil || !ok {
		t.Fatalf("inline pem + pins: %v %v", ok, err)
	}
	_ = tr.Close()

	if tr, ok, err := mesh.NewTLSTransportFromEnv("", env(nil)); tr != nil || ok || err != nil {
		t.Fatalf("unset: expected not configured, got %v %v %v", tr, ok, err)
	}
	bad := []map[string]string{
		{mesh.EnvMeshTLSCert: certPath},
		{mesh.EnvMeshTLSCert: certPath, mesh.EnvMeshTLSKey: keyPath},
		{mesh.EnvMeshTLSCA: caPath},
		{mesh.EnvMeshTLSCert: filepath.Join(dir, "missing.crt"), mesh.EnvMeshTLSKey: keyPath, mesh.EnvMeshTLSCA: caPath},
		{mesh.EnvMeshTLSCert: certPath, mesh.EnvMeshTLSKey: keyPath, mesh.EnvMeshTLSCA: write("junk.pem", []byte("not pem"))},
		{mesh.EnvMeshTLSCert: certPath, mesh.EnvMeshTLSKey: keyPath, mesh.EnvMeshTLSPins: "node-peer=zz"},
		{mesh.EnvMeshTLSCert: keyPath, mesh.EnvMeshTLSKey: certPath, mesh.EnvMeshTLSCA: caPath},
	}
	for i, m := range bad {
		if _, ok, err := mesh.NewTLSTransportFromEnv("", env(m)); err == nil || ok {
			t.Errorf("case %d: expected error, got ok=%v err=%v", i, ok, err)
		}
	}
	if _, _, err := mesh.NewTLSTransportFromEnv("someone-else", env(map[string]string{
		mesh.EnvMeshTLSCert: certPath, mesh.EnvMeshTLSKey: keyPath, mesh.EnvMeshTLSCA: caPath,
	})); !errors.Is(err, mesh.ErrPeerIdentity) {
		t.Fatalf("expected local id mismatch, got %v", err)
	}
	if _, err := mesh.NewTLSTransport(mesh.TLSTransportConfig{}); err == nil {
		t.Fatal("expected error without certificate")
	}
	if _, err := mesh.NewTLSTransport(mesh.TLSTransportConfig{Certificate: cert}); err == nil {
		t.Fatal("expected error without CA or pins")
	}
}

func TestParseTCPAddressAndPins(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:7946":              "127.0.0.1:7946",
		"tcp://example.com:1":         "example.com:1",
		"TLS://[::1]:80":              "[::1]:80",
		"/ip4/10.0.0.1/tcp/7946":      "10.0.0.1:7946",
		"/ip6/::1/tcp/0":              "[::1]:0",
		"/dns/mesh.example.com/tcp/5": "mesh.example.com:5",
		":7946":                       ":7946",
	}
	for in, want := range cases {
		if got, err := mesh.ParseTCPAddress(in); err != nil || got != want {
			t.Errorf("ParseTCPAddress(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "mem://a", "/ip4/::1/tcp/1", "/ip6/1.2.3.4/tcp/1", "/ip4/1.2.3.4/udp/1", "/ip4/1.2.3.4/tcp/99999", "host", "host:-1", "tcp://h:1/path", "a\n:1", "/dns//tcp/1", "h:01"} {
		if got, err := mesh.ParseTCPAddress(in); err == nil {
			t.Errorf("ParseTCPAddress(%q) = %q, expected error", in, got)
		}
	}

	fp := strings.Repeat("ab", 32)
	pins, err := mesh.ParsePeerPins("a=" + fp + ", a=SHA256:" + strings.ToUpper(fp) + " b=" + strings.Repeat("ab:", 31) + "ab")
	if err != nil || len(pins["a"]) != 2 || len(pins["b"]) != 1 || pins["a"][0].String() != fp {
		t.Fatalf("pins: %+v %v", pins, err)
	}
	for _, spec := range []string{"a", "=" + fp, "a=abc", "a=" + fp + "00"} {
		if _, err := mesh.ParsePeerPins(spec); err == nil {
			t.Errorf("ParsePeerPins(%q): expected error", spec)
		}
	}
}

// TestDiscoveryListensOnBindAndAdvertisesAdvertise checks that an explicit
// advertise address is announced while the listener binds BindAddress.
func TestDiscoveryListensOnBindAndAdvertisesAdvertise(t *testing.T) {
	ca := newTestCA(t)
	tr := newTLSNode(t, ca, "node-a", nil)
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{
		LocalID:     "node-a",
		BindAddress: "127.0.0.1:0",
		Advertise:   mesh.PeerDescriptor{ID: "node-a", Address: "mesh-a.example.com:7946"},
		Transport:   tr,
		Interval:    time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)
	defer func() {
		cancel()
		<-d.Stopped()
	}()
	if got := d.Self().Address; got != "mesh-a.example.com:7946" {
		t.Fatalf("advertised address changed to %q", got)
	}
	if err := d.LastError(); err != nil {
		t.Fatalf("listen on bind address failed: %v", err)
	}
	// The transport is listening (a second Listen is refused).
	if _, err := tr.Listen(context.Background(), "127.0.0.1:0"); !errors.Is(err, mesh.ErrAlreadyListening) {
		t.Fatalf("expected the discovery listener to be active, got %v", err)
	}
}
