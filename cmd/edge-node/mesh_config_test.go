package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
)

// meshPKI writes a CA and certificates for the given peer ids into a temp
// dir and returns an env lookup per id (AEROLLM_MESH_TLS_* set).
func meshPKI(t *testing.T, ids ...string) map[string]map[string]string {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "edge test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	write := func(name string, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	caPath := write("ca.pem", "CERTIFICATE", caDER)
	out := make(map[string]map[string]string)
	for i, id := range ids {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		u, _ := url.Parse(mesh.PeerIDURIPrefix + id)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)),
			NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			URIs:        []*url.URL{u},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, _ := x509.MarshalECPrivateKey(key)
		out[id] = map[string]string{
			mesh.EnvMeshTLSCert: write(id+".crt", "CERTIFICATE", der),
			mesh.EnvMeshTLSKey:  write(id+".key", "EC PRIVATE KEY", keyDER),
			mesh.EnvMeshTLSCA:   caPath,
		}
	}
	return out
}

func envOf(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func withEnv(base map[string]string, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func TestLoadConfigStreamAndReceiptLimits(t *testing.T) {
	cfg, err := loadConfig(nil, envOf(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.streamIdleTimeout != spatial.DefaultStreamIdleTimeout || cfg.streamMaxDuration != spatial.DefaultStreamMaxDuration ||
		cfg.receipts.maxCount != defaultMaxReceipts || cfg.receipts.maxAge != 0 || cfg.meshListen != "" || cfg.meshTLS != nil {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	cfg, err = loadConfig([]string{"-stream-idle-timeout", "5s", "-max-receipts", "10"}, envOf(map[string]string{
		"EDGE_STREAM_MAX_DURATION": "1m", "EDGE_RECEIPT_MAX_AGE": "720h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.streamIdleTimeout != 5*time.Second || cfg.streamMaxDuration != time.Minute || cfg.receipts.maxCount != 10 || cfg.receipts.maxAge != 720*time.Hour {
		t.Fatalf("unexpected parsed config: %+v", cfg)
	}
	for _, args := range [][]string{
		{"-stream-idle-timeout", "0"},
		{"-stream-idle-timeout", "-1s"},
		{"-stream-max-duration", "soon"},
		{"-max-receipts", "0"},
		{"-max-receipts", "-5"},
		{"-receipt-max-age", "-1h"},
	} {
		if _, err := loadConfig(args, envOf(nil)); err == nil {
			t.Errorf("%v: expected error", args)
		}
	}
}

func TestLoadConfigMesh(t *testing.T) {
	pki := meshPKI(t, "edge-a")
	for name, c := range map[string]struct {
		args []string
		env  map[string]string
	}{
		"peers without listen":     {[]string{"-mesh-peers", "x@127.0.0.1:1"}, nil},
		"advertise without listen": {[]string{"-mesh-advertise", "h:1"}, nil},
		"listen without tls":       {[]string{"-mesh-listen", "127.0.0.1:0"}, nil},
		"bad listen":               {[]string{"-mesh-listen", "nope"}, pki["edge-a"]},
		"advertise port 0":         {[]string{"-mesh-listen", "127.0.0.1:0", "-mesh-advertise", "h:0"}, pki["edge-a"]},
		"partial tls":              {[]string{"-mesh-listen", "127.0.0.1:0"}, map[string]string{mesh.EnvMeshTLSCert: pki["edge-a"][mesh.EnvMeshTLSCert]}},
	} {
		if _, err := loadConfig(c.args, envOf(c.env)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	// TLS variables alone do not switch the node to the network mesh.
	if cfg, err := loadConfig(nil, envOf(pki["edge-a"])); err != nil || cfg.meshTLS != nil {
		t.Fatalf("tls env without listen: %+v %v", cfg.meshTLS, err)
	}
	cfg, err := loadConfig([]string{"-mesh-listen", "/ip4/127.0.0.1/tcp/0"}, envOf(withEnv(pki["edge-a"], map[string]string{
		"EDGE_MESH_PEERS": "edge-b@127.0.0.1:7946, edge-c@127.0.0.1:7947", "EDGE_MESH_ADVERTISE": "edge-a.example:7946",
	})))
	if err != nil || cfg.meshTLS == nil || len(cfg.meshPeers) != 2 || cfg.meshAdvertise != "edge-a.example:7946" {
		t.Fatalf("mesh config: %+v %v", cfg, err)
	}
}

// TestEdgeNetworkMesh wires two edge nodes' meshes the way run() does and
// checks they discover each other over mutual TLS, under their certificate
// ids.
func TestEdgeNetworkMesh(t *testing.T) {
	pki := meshPKI(t, "edge-a", "edge-b")
	cfgA, err := loadConfig([]string{"-mesh-listen", "127.0.0.1:0"}, envOf(pki["edge-a"]))
	if err != nil {
		t.Fatal(err)
	}
	trA, dcfgA, err := newMesh(cfgA, "stored-id-a")
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Close()
	if dcfgA.LocalID != "edge-a" {
		t.Fatalf("network mesh must use the certificate id, got %q", dcfgA.LocalID)
	}
	dcfgA.Interval = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := mesh.NewDiscovery(dcfgA)
	a.Start(ctx)
	defer func() { a.Stop(); <-a.Stopped() }()
	addrA := a.Self().Address
	if strings.HasSuffix(addrA, ":0") {
		t.Fatalf("bound address not resolved: %q", addrA)
	}

	cfgB, err := loadConfig([]string{"-mesh-listen", "127.0.0.1:0", "-mesh-peers", "edge-a@" + addrA}, envOf(pki["edge-b"]))
	if err != nil {
		t.Fatal(err)
	}
	trB, dcfgB, err := newMesh(cfgB, "stored-id-b")
	if err != nil {
		t.Fatal(err)
	}
	defer trB.Close()
	dcfgB.Interval = 50 * time.Millisecond
	b := mesh.NewDiscovery(dcfgB)
	b.Start(ctx)
	defer func() { b.Stop(); <-b.Stopped() }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		peers := a.Peers()
		if len(peers) == 1 && peers[0].ID == "edge-b" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("edge-a never learned edge-b: %+v (last error %v / %v)", peers, a.LastError(), b.LastError())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Without -mesh-listen the node keeps the in-process mesh and its
	// stored id.
	tr, dcfg, err := newMesh(edgeConfig{}, "stored-id")
	if err != nil || dcfg.LocalID != "stored-id" {
		t.Fatalf("in-process mesh: %v %q", err, dcfg.LocalID)
	}
	_ = tr.Close()
}

// TestEdgeSpatialStreamStalledBody checks that a client that stops sending
// mid-body is cut off by the stream idle timeout on the real edge route.
func TestEdgeSpatialStreamStalledBody(t *testing.T) {
	_, srv := newTestEdge(t, func(c *edgeConfig) { c.streamIdleTimeout = 200 * time.Millisecond })
	pr, pw := io.Pipe()
	defer pw.Close()
	go func() { _, _ = pw.Write([]byte("partial")) }()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/edge/spatial/stream", pr)
	start := time.Now()
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if time.Since(start) > 5*time.Second {
		t.Fatal("stalled stream was not cut off by its idle timeout")
	}
	if string(body) != "partial" || resp.Trailer.Get(spatial.StreamStatusTrailer) != "timeout" {
		t.Fatalf("unexpected stream result %q, trailer %q", body, resp.Trailer.Get(spatial.StreamStatusTrailer))
	}
}

// TestReadDeadlineEndsStalledJSONBody checks the body read deadline used on
// the JSON routes.
func TestReadDeadlineEndsStalledJSONBody(t *testing.T) {
	readErr := make(chan error, 1)
	srv := httptest.NewServer(readDeadlineFor(150*time.Millisecond, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		readErr <- err
	})))
	defer srv.Close()
	pr, pw := io.Pipe()
	defer pw.Close()
	go func() { _, _ = pw.Write([]byte(`{"receipt_id":`)) }()
	req, _ := http.NewRequest(http.MethodPost, srv.URL, pr)
	go func() {
		if resp, err := srv.Client().Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("expected the stalled body read to fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled body read was not ended by the read deadline")
	}
}
