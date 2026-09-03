package federated

import (
	"context"
	"testing"
)

func TestGatewayRegistryRegisterAndNode(t *testing.T) {
	g := NewGatewayRegistry()
	reg := &NodeRegistration{NodeID: "n1", Endpoint: "http://n1", PublicKey: []byte("pk"), Algorithms: []string{"a"}}
	if err := g.Register(context.Background(), reg); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	n, ok := g.Node("n1")
	if !ok { t.Fatalf("expected node n1") }
	if n.Endpoint != "http://n1" { t.Fatalf("endpoint mismatch") }
	if string(n.PublicKey) != "pk" { t.Fatalf("public key mismatch") }
}

func TestGatewayRegistryLatestAndHistory(t *testing.T) {
	g := NewGatewayRegistry()
	_ = g.Register(context.Background(), &NodeRegistration{NodeID: "n1"})
	_ = g.Register(context.Background(), &NodeRegistration{NodeID: "n2"})
	if g.Latest().NodeID != "n2" { t.Fatalf("expected latest n2") }
	if len(g.History()) != 2 { t.Fatalf("expected 2 history entries, got %d", len(g.History())) }
}

func TestGatewayRegistryRejectsNil(t *testing.T) {
	g := NewGatewayRegistry()
	if err := g.Register(context.Background(), nil); err != nil {
		t.Fatalf("nil registration should not error, got %v", err)
	}
	if g.Latest() != nil {
		t.Fatalf("expected nil latest for nil registration")
	}
}
