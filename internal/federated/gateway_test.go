package federated

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestGatewayRegistryRegisterAndNode(t *testing.T) {
	g := NewGatewayRegistry()
	pub, _, _ := ed25519.GenerateKey(nil)
	reg := &NodeRegistration{NodeID: "n1", Endpoint: "http://n1", PublicKey: pub, Algorithms: []string{"a"}}
	if err := g.Register(context.Background(), reg); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	n, ok := g.Node("n1")
	if !ok {
		t.Fatalf("expected node n1")
	}
	if n.Endpoint != "http://n1" {
		t.Fatalf("endpoint mismatch")
	}
	if string(n.PublicKey) != string(pub) {
		t.Fatalf("public key mismatch")
	}
	// Returned copies must not alias internal state.
	n.PublicKey[0] ^= 0xff
	again, _ := g.Node("n1")
	if again.PublicKey[0] == n.PublicKey[0] {
		t.Fatalf("Node must return a deep copy")
	}
}

func TestGatewayRegistryLatestAndHistory(t *testing.T) {
	g := NewGatewayRegistry()
	_ = g.Register(context.Background(), &NodeRegistration{NodeID: "n1"})
	_ = g.Register(context.Background(), &NodeRegistration{NodeID: "n2"})
	if g.Latest().NodeID != "n2" {
		t.Fatalf("expected latest n2")
	}
	if len(g.History()) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(g.History()))
	}
}

func TestGatewayRegistryRejectsNil(t *testing.T) {
	g := NewGatewayRegistry()
	if err := g.Register(context.Background(), nil); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("nil registration should be rejected, got %v", err)
	}
	if g.Latest() != nil {
		t.Fatalf("expected nil latest for nil registration")
	}
}

func TestGatewayRegistryValidation(t *testing.T) {
	g := NewGatewayRegistry()
	cases := map[string]*NodeRegistration{
		"empty id":         {NodeID: ""},
		"bad id chars":     {NodeID: "n1/../x"},
		"long id":          {NodeID: strings.Repeat("a", MaxNodeIDLength+1)},
		"bad scheme":       {NodeID: "n1", Endpoint: "file:///etc/passwd"},
		"no host":          {NodeID: "n1", Endpoint: "http://"},
		"relative url":     {NodeID: "n1", Endpoint: "/foo"},
		"credentials":      {NodeID: "n1", Endpoint: "https://user:pass@host"},
		"long endpoint":    {NodeID: "n1", Endpoint: "https://h/" + strings.Repeat("a", MaxEndpointLength)},
		"short key":        {NodeID: "n1", PublicKey: []byte("pk")},
		"too many algs":    {NodeID: "n1", Algorithms: make([]string, MaxAlgorithms+1)},
		"empty alg":        {NodeID: "n1", Algorithms: []string{""}},
		"alg with spaces":  {NodeID: "n1", Algorithms: []string{"a b"}},
		"alg too long":     {NodeID: "n1", Algorithms: []string{strings.Repeat("a", MaxAlgorithmLength+1)}},
		"endpoint newline": {NodeID: "n1", Endpoint: "http://h/\nx"},
	}
	for name, reg := range cases {
		t.Run(name, func(t *testing.T) {
			if err := g.Register(context.Background(), reg); !errors.Is(err, ErrInvalidRegistration) {
				t.Fatalf("expected ErrInvalidRegistration, got %v", err)
			}
		})
	}
	if g.Len() != 0 {
		t.Fatalf("no invalid registration should be stored")
	}
}

// A node's key is pinned: an unauthenticated re-registration must not be able
// to swap in an attacker-controlled key.
func TestGatewayRegistryKeyPinning(t *testing.T) {
	g := NewGatewayRegistry()
	pub1, _, _ := ed25519.GenerateKey(nil)
	pub2, _, _ := ed25519.GenerateKey(nil)
	ctx := context.Background()
	if err := g.Register(ctx, &NodeRegistration{NodeID: "n1", PublicKey: pub1}); err != nil {
		t.Fatal(err)
	}
	if err := g.Register(ctx, &NodeRegistration{NodeID: "n1", PublicKey: pub2}); !errors.Is(err, ErrNodeKeyConflict) {
		t.Fatalf("expected ErrNodeKeyConflict, got %v", err)
	}
	if err := g.Register(ctx, &NodeRegistration{NodeID: "n1"}); !errors.Is(err, ErrNodeKeyConflict) {
		t.Fatalf("stripping the key should be rejected, got %v", err)
	}
	// Same key may update the endpoint.
	if err := g.Register(ctx, &NodeRegistration{NodeID: "n1", PublicKey: pub1, Endpoint: "https://new"}); err != nil {
		t.Fatalf("same-key update failed: %v", err)
	}
	k, ok := g.PublicKey("n1")
	if !ok || string(k) != string(pub1) {
		t.Fatalf("pinned key changed")
	}
	// Explicit deregistration allows rotation.
	if !g.Deregister(ctx, "n1") {
		t.Fatal("deregister failed")
	}
	if err := g.Register(ctx, &NodeRegistration{NodeID: "n1", PublicKey: pub2}); err != nil {
		t.Fatalf("rotation after deregister failed: %v", err)
	}
}

func TestGatewayRegistryBoundedHistoryAndNodes(t *testing.T) {
	g := NewGatewayRegistry()
	ctx := context.Background()
	for i := 0; i < MaxRegistrationHistory+50; i++ {
		if err := g.Register(ctx, &NodeRegistration{NodeID: "same"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(g.History()); got != MaxRegistrationHistory {
		t.Fatalf("history not bounded: %d", got)
	}
	if cap(g.history) > 2*MaxRegistrationHistory {
		t.Fatalf("history backing array grew unbounded: cap=%d", cap(g.history))
	}

	g2 := NewGatewayRegistry()
	for i := 0; i < MaxNodes; i++ {
		g2.nodes[fmt.Sprintf("n%d", i)] = &NodeRegistration{NodeID: fmt.Sprintf("n%d", i)}
	}
	if err := g2.Register(ctx, &NodeRegistration{NodeID: "overflow"}); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("expected ErrRegistryFull, got %v", err)
	}
	// Updating an existing node is still allowed when full.
	if err := g2.Register(ctx, &NodeRegistration{NodeID: "n1", Endpoint: "https://x"}); err != nil {
		t.Fatalf("update when full failed: %v", err)
	}
}

func TestGatewayRegistryConcurrent(t *testing.T) {
	g := NewGatewayRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = g.Register(context.Background(), &NodeRegistration{NodeID: fmt.Sprintf("n%d-%d", i, j)})
				_ = g.Latest()
				_ = g.History()
				_, _ = g.Node("n1-1")
			}
		}(i)
	}
	wg.Wait()
	if g.Len() != 16*50 {
		t.Fatalf("expected %d nodes, got %d", 16*50, g.Len())
	}
}

func TestGatewayRegistryNilSafe(t *testing.T) {
	var g *GatewayRegistry
	if g.Latest() != nil || g.History() != nil || g.Len() != 0 {
		t.Fatal("nil registry accessors should be safe")
	}
	if _, ok := g.Node("x"); ok {
		t.Fatal("nil registry has no nodes")
	}
	if err := g.Register(context.Background(), &NodeRegistration{NodeID: "x"}); err == nil {
		t.Fatal("expected error registering on nil registry")
	}
}
