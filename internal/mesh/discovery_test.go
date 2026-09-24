package mesh_test

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
)

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func requireNoGoroutineLeak(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	t.Fatalf("goroutine leak: %d running, baseline %d\n%s", runtime.NumGoroutine(), baseline, buf[:n])
}

type meshNode struct {
	id        mesh.PeerID
	state     *mesh.PluginRegistrySync
	transport mesh.SecureTransport
	discovery *mesh.Discovery
	worker    *mesh.SyncWorker
	done      chan struct{}
}

func TestSyncWorkerGossipConvergesAcrossNetwork(t *testing.T) {
	baseline := runtime.NumGoroutine()
	network := mesh.NewInMemoryNetwork()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// b and c only know a; a knows nobody. Membership and state must still
	// spread to every node through announcements and gossip via a.
	seeds := map[mesh.PeerID][]mesh.PeerDescriptor{
		"a": nil,
		"b": {{ID: "a", Address: "mem://a"}},
		"c": {{ID: "a", Address: "mem://a"}},
	}
	var nodes []*meshNode
	for _, id := range []mesh.PeerID{"a", "b", "c"} {
		tr, err := network.Transport(id)
		if err != nil {
			t.Fatal(err)
		}
		n := &meshNode{id: id, state: mesh.NewPluginRegistrySyncForNode(id), transport: tr, done: make(chan struct{})}
		n.discovery = mesh.NewDiscovery(mesh.DiscoveryConfig{
			LocalID:     id,
			BindAddress: "mem://" + string(id),
			Peers:       seeds[id],
			Transport:   tr,
			Interval:    20 * time.Millisecond,
		})
		n.worker = mesh.NewSyncWorker(mesh.SyncWorkerConfig{State: n.state, Discovery: n.discovery, Interval: 25 * time.Millisecond})
		n.state.Registry.Add("plugin-"+string(id), mesh.Now(), []byte(`{"owner":"`+string(id)+`"}`))
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		go func(n *meshNode) {
			defer close(n.done)
			n.worker.Start(ctx)
		}(n)
	}

	waitFor(t, 5*time.Second, "registry convergence", func() bool {
		for _, n := range nodes {
			if len(n.state.Registry.Elements()) != 3 {
				return false
			}
		}
		return true
	})
	// a learned b and c from their announcements; b and c only know a
	// (membership is direct-announce only, there is no peer exchange).
	wantPeers := map[mesh.PeerID]int{"a": 2, "b": 1, "c": 1}
	for _, n := range nodes {
		if got := len(n.discovery.Peers()); got != wantPeers[n.id] {
			t.Fatalf("node %s knows %d peers, want %d: %+v", n.id, got, wantPeers[n.id], n.discovery.Peers())
		}
	}

	// A removal on one node propagates too.
	nodes[1].state.Registry.Remove("plugin-a", mesh.Now())
	waitFor(t, 5*time.Second, "removal convergence", func() bool {
		for _, n := range nodes {
			if _, ok := n.state.Registry.Lookup("plugin-a"); ok {
				return false
			}
		}
		return true
	})

	cancel()
	for _, n := range nodes {
		select {
		case <-n.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("sync worker %s did not stop on ctx cancel", n.id)
		}
		select {
		case <-n.discovery.Stopped():
		case <-time.After(2 * time.Second):
			t.Fatalf("discovery %s goroutines did not exit", n.id)
		}
		if st := n.worker.Stats(); st.Sent == 0 || st.Received == 0 {
			t.Fatalf("node %s stats show no traffic: %+v", n.id, st)
		}
		_ = n.transport.Close()
	}
	requireNoGoroutineLeak(t, baseline)
}

func TestSyncWorkerStopTerminatesEverything(t *testing.T) {
	baseline := runtime.NumGoroutine()
	tr := mesh.NewInMemoryTransport("solo")
	defer tr.Close()
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{LocalID: "solo", BindAddress: "mem://solo", Transport: tr, Interval: 10 * time.Millisecond})
	w := mesh.NewSyncWorker(mesh.SyncWorkerConfig{State: mesh.NewPluginRegistrySync(), Discovery: d, Interval: 10 * time.Millisecond})
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Start(context.Background())
	}()
	time.Sleep(50 * time.Millisecond)
	w.Stop()
	w.Stop() // idempotent
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
	select {
	case <-d.Stopped():
	case <-time.After(2 * time.Second):
		t.Fatal("discovery not stopped")
	}
	// Start after Stop must not run again.
	finished := make(chan struct{})
	go func() {
		w.Start(context.Background())
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Start after Stop blocked")
	}
	_ = tr.Close()
	requireNoGoroutineLeak(t, baseline)
}

func TestDiscoveryStopBeforeStartAndCancelledContext(t *testing.T) {
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{LocalID: "x"})
	d.Stop()
	d.Start(context.Background())
	select {
	case <-d.Stopped():
	case <-time.After(time.Second):
		t.Fatal("expected Stopped to close")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tr := mesh.NewInMemoryTransport("y")
	defer tr.Close()
	d2 := mesh.NewDiscovery(mesh.DiscoveryConfig{LocalID: "y", BindAddress: "mem://y", Transport: tr})
	d2.Start(ctx)
	select {
	case <-d2.Stopped():
	case <-time.After(2 * time.Second):
		t.Fatal("discovery with cancelled ctx did not stop")
	}
}

func TestDiscoveryFiltersSeeds(t *testing.T) {
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{
		LocalID:     "self",
		BindAddress: "mem://self",
		Peers: []mesh.PeerDescriptor{
			{ID: "", Address: "mem://noid"},
			{ID: "noaddr", Address: ""},
			{ID: "self", Address: "mem://self"},
			{ID: "  ", Address: "mem://blank"},
			{ID: "bad\nid", Address: "mem://x"},
			{ID: mesh.PeerID(strings.Repeat("x", mesh.MaxPeerIDLen+1)), Address: "mem://long"},
			{ID: "ok", Address: "mem://ok"},
			{ID: "ok", Address: "mem://ok"},
		},
	})
	peers := d.Peers()
	if len(peers) != 1 || peers[0].ID != "ok" {
		t.Fatalf("expected only the valid seed, got %+v", peers)
	}
}

func TestDiscoveryMaxPeers(t *testing.T) {
	var seeds []mesh.PeerDescriptor
	for i := 0; i < 10; i++ {
		seeds = append(seeds, mesh.PeerDescriptor{ID: mesh.PeerID(fmt.Sprintf("p%d", i)), Address: "mem://p"})
	}
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{LocalID: "self", Peers: seeds, MaxPeers: 3})
	if got := len(d.Peers()); got != 3 {
		t.Fatalf("expected peer table capped at 3, got %d", got)
	}
}

func TestDiscoveryAnnounceCannotSpoofIdentity(t *testing.T) {
	network := mesh.NewInMemoryNetwork()
	victimTr, _ := network.Transport("victim")
	evil, _ := network.Transport("evil")
	defer victimTr.Close()
	defer evil.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{LocalID: "victim", BindAddress: "mem://victim", Transport: victimTr, Interval: time.Hour})
	d.Start(ctx)
	defer d.Stop()

	conn, err := evil.Dial(ctx, mesh.PeerDescriptor{ID: "victim"})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(mesh.PeerDescriptor{ID: "trusted-node", Address: "mem://evil"})
	if err := conn.Send(ctx, mesh.Envelope{StateType: mesh.AnnounceStateType, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	// Garbage for a type without a handler, and an oversized payload.
	_ = conn.Send(ctx, mesh.Envelope{StateType: "unknown", Payload: json.RawMessage(`{}`)})
	_ = conn.Close()

	waitFor(t, 2*time.Second, "announce processed", func() bool { return len(d.Peers()) == 1 })
	if p := d.Peers()[0]; p.ID != "evil" || p.Address != "mem://evil" {
		t.Fatalf("expected announce attributed to transport identity, got %+v", p)
	}
	waitFor(t, 2*time.Second, "unknown type rejected", func() bool { return d.Stats().MergeErrors >= 1 })
}

func TestDiscoveryHandleRejectsReservedType(t *testing.T) {
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{LocalID: "x"})
	noop := func(context.Context, mesh.Envelope) error { return nil }
	if err := d.Handle(mesh.AnnounceStateType, noop); err == nil {
		t.Fatal("expected reserved type to be rejected")
	}
	if err := d.Handle("", noop); err == nil {
		t.Fatal("expected empty type to be rejected")
	}
	if err := d.Handle("t", noop); err != nil {
		t.Fatal(err)
	}
}

func TestSyncWorkerRejectsBadRemoteState(t *testing.T) {
	network := mesh.NewInMemoryNetwork()
	tr, _ := network.Transport("node")
	peer, _ := network.Transport("peer")
	defer tr.Close()
	defer peer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := mesh.NewPNCounterForNode("node")
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{LocalID: "node", BindAddress: "mem://node", Transport: tr, Interval: time.Hour})
	w := mesh.NewSyncWorker(mesh.SyncWorkerConfig{State: state, Discovery: d, Interval: time.Hour})
	done := make(chan struct{})
	go func() { defer close(done); w.Start(ctx) }()

	waitFor(t, 2*time.Second, "listener up", func() bool {
		c, err := peer.Dial(ctx, mesh.PeerDescriptor{ID: "node"})
		if err != nil {
			return false
		}
		defer c.Close()
		_ = c.Send(ctx, mesh.Envelope{StateType: state.Type(), Payload: json.RawMessage(`{"k":{"peer":{"p":-5,"n":0}}}`)})
		_ = c.Send(ctx, mesh.Envelope{StateType: state.Type(), Payload: json.RawMessage(`{"k":{"peer":{"p":4,"n":1}}}`)})
		return true
	})
	waitFor(t, 2*time.Second, "valid merge applied", func() bool { return state.Value("k") == 3 })
	waitFor(t, 2*time.Second, "invalid merge recorded", func() bool { return w.Stats().MergeErrors == 1 && w.LastError() != nil })

	cancel()
	<-done
}

func TestGossipWorkerRecordsErrors(t *testing.T) {
	tr := mesh.NewInMemoryTransport("local")
	defer tr.Close()
	w := mesh.NewGossipWorker(mesh.GossipWorkerConfig{
		State:     mesh.NewPNCounter(),
		Peers:     []mesh.PeerDescriptor{{ID: ""}, {ID: "ghost"}, {ID: "ghost"}},
		Transport: tr,
		Interval:  10 * time.Millisecond,
	})
	if got := len(w.Peers()); got != 1 {
		t.Fatalf("expected empty/duplicate peers dropped, got %d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	w.Start(ctx)
	st := w.Stats()
	if st.Rounds == 0 || st.SendErrors == 0 || w.LastError() == nil {
		t.Fatalf("expected recorded send errors, got %+v", st)
	}
}

func TestGossipWorkerDeliversToListeningPeer(t *testing.T) {
	network := mesh.NewInMemoryNetwork()
	a, _ := network.Transport("a")
	b, _ := network.Transport("b")
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remote := mesh.NewPNCounterForNode("b")
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{LocalID: "b", BindAddress: "mem://b", Transport: b, Interval: time.Hour})
	if err := d.Handle(remote.Type(), func(ctx context.Context, env mesh.Envelope) error { return remote.Merge(ctx, env.Payload) }); err != nil {
		t.Fatal(err)
	}
	d.Start(ctx)
	defer d.Stop()

	local := mesh.NewPNCounterForNode("a")
	local.Increment("hits", 42)
	w := mesh.NewGossipWorker(mesh.GossipWorkerConfig{State: local, Peers: []mesh.PeerDescriptor{{ID: "b"}}, Transport: a, Interval: 10 * time.Millisecond})
	go w.Start(ctx)
	defer w.Stop()
	waitFor(t, 2*time.Second, "gossip delivery", func() bool { return remote.Value("hits") == 42 })
}

func TestParsePeerAddresses(t *testing.T) {
	if got := mesh.ParsePeerAddresses(""); got != nil {
		t.Fatalf("expected nil for empty input, got %v", got)
	}
	if got := mesh.ParsePeerAddresses(" , ;\n"); got != nil {
		t.Fatalf("expected nil for separators only, got %v", got)
	}
	got := mesh.ParsePeerAddresses(" a:1, b:2 ;a:1\tc:3\n" + strings.Repeat("z", mesh.MaxPeerAddressLen+1))
	want := []string{"a:1", "b:2", "c:3"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %v want %v", got, want)
	}
	var many []string
	for i := 0; i < mesh.MaxPeerAddresses+50; i++ {
		many = append(many, fmt.Sprintf("h%d:1", i))
	}
	if n := len(mesh.ParsePeerAddresses(strings.Join(many, ","))); n != mesh.MaxPeerAddresses {
		t.Fatalf("expected cap %d, got %d", mesh.MaxPeerAddresses, n)
	}
}

func TestMeshConfigPeerDescriptorsDropsEmptyAndSelf(t *testing.T) {
	cfg := mesh.DefaultMeshConfig()
	cfg.LocalPeerID = "me"
	// This is exactly what cmd/server/main.go builds when AEROLLM_MESH_PEERS is unset.
	cfg.PeerAddresses = append(cfg.PeerAddresses, "")
	if got := cfg.PeerDescriptors(); len(got) != 0 {
		t.Fatalf("expected no peers for empty address, got %+v", got)
	}
	cfg.PeerAddresses = []string{"n1@/ip4/10.0.0.1/tcp/4001, /ip4/10.0.0.2/tcp/4001", "me@/ip4/10.0.0.3/tcp/1", "n1@dup", "@bare"}
	got := cfg.PeerDescriptors()
	if len(got) != 3 {
		t.Fatalf("expected 3 peers, got %+v", got)
	}
	if got[0].ID != "n1" || got[0].Address != "/ip4/10.0.0.1/tcp/4001" {
		t.Fatalf("unexpected first peer %+v", got[0])
	}
	if got[1].ID != "/ip4/10.0.0.2/tcp/4001" || got[1].Address != "/ip4/10.0.0.2/tcp/4001" {
		t.Fatalf("unexpected bare-address peer %+v", got[1])
	}
	if got[2].ID != "@bare" {
		t.Fatalf("unexpected third peer %+v", got[2])
	}
}

func TestDefaultMeshConfigRandomID(t *testing.T) {
	a, b := mesh.DefaultMeshConfig(), mesh.DefaultMeshConfig()
	if a.LocalPeerID == b.LocalPeerID {
		t.Fatal("expected unique peer ids")
	}
	if !strings.HasPrefix(string(a.LocalPeerID), "node-") || len(a.LocalPeerID) != len("node-")+32 {
		t.Fatalf("unexpected id %q", a.LocalPeerID)
	}
	if err := a.Validate(); err != nil {
		t.Fatalf("disabled config should validate: %v", err)
	}
	a.Enabled = true
	a.GossipInterval = 0
	if err := a.Validate(); err == nil {
		t.Fatal("expected invalid gossip interval")
	}
}

func TestMainServerWiringIsHarmless(t *testing.T) {
	// Mirrors cmd/server/main.go: standalone transport, self as the only seed.
	baseline := runtime.NumGoroutine()
	cfg := mesh.DefaultMeshConfig()
	tr := mesh.NewInMemoryTransport(cfg.LocalPeerID)
	defer tr.Close()
	d := mesh.NewDiscovery(mesh.DiscoveryConfig{
		LocalID:     cfg.LocalPeerID,
		BindAddress: cfg.BindAddress,
		Peers:       []mesh.PeerDescriptor{{ID: cfg.LocalPeerID, Address: cfg.BindAddress}},
		Transport:   tr,
	})
	if len(d.Peers()) != 0 {
		t.Fatalf("self must not be a peer: %+v", d.Peers())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	w := mesh.NewSyncWorker(mesh.SyncWorkerConfig{State: mesh.NewPluginRegistrySync(), Discovery: d, Interval: 10 * time.Millisecond})
	w.Start(ctx)
	<-d.Stopped()
	if err := w.LastError(); err != nil {
		t.Fatalf("unexpected sync error: %v", err)
	}
	_ = tr.Close()
	requireNoGoroutineLeak(t, baseline)
}
