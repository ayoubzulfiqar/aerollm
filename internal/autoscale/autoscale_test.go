package autoscale

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestAWSProvisionGPU(t *testing.T) {
	p := NewAWSProvisioner()
	node, err := p.ProvisionGPU(context.Background(), NodeSpec{InstanceType: "A100", GPUCount: 1, Region: "us-east-1"})
	if err != nil { t.Fatalf("provision failed: %v", err) }
	if node == nil || node.Provider != "aws" { t.Fatalf("unexpected node: %v", node) }
}

func TestGCPProvisionGPU(t *testing.T) {
	p := NewGCPProvisioner()
	node, err := p.ProvisionGPU(context.Background(), NodeSpec{InstanceType: "A100", GPUCount: 1, Region: "us-east-1"})
	if err != nil { t.Fatalf("provision failed: %v", err) }
	if node == nil || node.Provider != "gcp" { t.Fatalf("unexpected node: %v", node) }
}

func TestBootstrapScriptContainsNodeID(t *testing.T) {
	script := BootstrapScript([]string{"peer1"}, "node-1")
	if !strings.Contains(script, "AEROLLM_MESH_NODE_ID=node-1") { t.Fatalf("missing node id in script") }
	if !strings.Contains(script, "AEROLLM_MESH_PEERS=peer1") { t.Fatalf("missing peers in script") }
}

func TestMetaAgentInfraLoopTriggers(t *testing.T) {
	called := false
	p := &stubProvisioner{
		onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
			called = true
			return &Node{ID: "n1", Provider: spec.Provider, InstanceType: spec.InstanceType}, nil
		},
	}
	loop := NewMetaAgentInfraLoop(p, 0.1)
	_, err := loop.Evaluate(context.Background(), 0.3)
	if err != nil { t.Fatalf("evaluate failed: %v", err) }
	if !called { t.Fatalf("expected provisioner to be called") }
}

func TestMetaAgentInfraLoopNoTrigger(t *testing.T) {
	called := false
	p := &stubProvisioner{
		onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
			called = true
			return &Node{}, nil
		},
	}
	loop := NewMetaAgentInfraLoop(p, 0.2)
	_, err := loop.Evaluate(context.Background(), 0.1)
	if err != nil { t.Fatalf("evaluate failed: %v", err) }
	if called { t.Fatalf("expected no provisioning below threshold") }
}

func TestServerMetaAgentLoopFallback(t *testing.T) {
	primary := &stubProvisioner{onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
		return nil, fmt.Errorf("aws down")
	}}
	fallback := &stubProvisioner{onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
		return &Node{ID: "gcp-1", Provider: "gcp", InstanceType: spec.InstanceType}, nil
	}}
	loop := NewServerMetaAgentLoopWith(primary, fallback, 0.1)
	node, err := loop.Evaluate(context.Background(), 0.3)
	if err != nil { t.Fatalf("evaluate failed: %v", err) }
	if node == nil || node.Provider != "gcp" { t.Fatalf("expected fallback provider, got %v", node) }
}

func TestServerMetaAgentLoopTerminateUsesFallback(t *testing.T) {
	primary := &stubProvisioner{onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
		return nil, fmt.Errorf("aws down")
	}}
	fallback := &stubProvisioner{}
	loop := NewServerMetaAgentLoopWith(primary, fallback, 0.1)
	_, _ = loop.Evaluate(context.Background(), 0.3)
	fp := loop.provisioner.(*failoverProvisioner)
	if err := fp.Terminate(context.Background(), "n1"); err != nil {
		t.Fatalf("terminate failed: %v", err)
	}
	if !fallback.terminateCalled { t.Fatalf("expected fallback terminate") }
}

type stubProvisioner struct {
	onProvision    func(ctx context.Context, spec NodeSpec) (*Node, error)
	terminateCalled bool
}

func (s *stubProvisioner) ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error) {
	if s.onProvision != nil {
		return s.onProvision(ctx, spec)
	}
	return &Node{}, nil
}
func (s *stubProvisioner) Terminate(ctx context.Context, nodeID string) error {
	s.terminateCalled = true
	return nil
}
func (s *stubProvisioner) List(ctx context.Context) ([]Node, error) { return nil, nil }
