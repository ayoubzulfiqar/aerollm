package autoscale

import (
	"context"
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

type stubProvisioner struct {
	onProvision func(ctx context.Context, spec NodeSpec) (*Node, error)
}

func (s *stubProvisioner) ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error) {
	return s.onProvision(ctx, spec)
}
func (s *stubProvisioner) Terminate(ctx context.Context, nodeID string) error { return nil }
func (s *stubProvisioner) List(ctx context.Context) ([]Node, error) { return nil, nil }
