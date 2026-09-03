package autoscale

import (
	"context"
	"fmt"
	"time"
)

// NodeSpec describes the desired infrastructure node.
type NodeSpec struct {
	Provider     string
	InstanceType string
	GPUCount     int
	Region       string
	SSHKey       string
}

// Node represents a provisioned infrastructure node.
type Node struct {
	ID         string
	Provider   string
	PublicIP   string
	InstanceType string
	LaunchedAt int64
}

// InfraProvisioner defines the contract for cloud providers.
type InfraProvisioner interface {
	ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error)
	Terminate(ctx context.Context, nodeID string) error
	List(ctx context.Context) ([]Node, error)
}

// AWSProvisioner is a stub AWS provisioner.
type AWSProvisioner struct{}

// NewAWSProvisioner creates a new AWS provisioner.
func NewAWSProvisioner() *AWSProvisioner {
	return &AWSProvisioner{}
}

// ProvisionGPU requests a GPU instance.
func (p *AWSProvisioner) ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error) {
	_ = ctx
	if spec.Provider == "" {
		spec.Provider = "aws"
	}
	return &Node{
		ID:          fmt.Sprintf("aws-%s-%d", spec.InstanceType, timeNow()),
		Provider:    "aws",
		InstanceType: spec.InstanceType,
		LaunchedAt: timeNow(),
	}, nil
}

// Terminate shuts down an AWS node.
func (p *AWSProvisioner) Terminate(ctx context.Context, nodeID string) error {
	_ = ctx
	_ = nodeID
	return nil
}

// List returns AWS nodes.
func (p *AWSProvisioner) List(ctx context.Context) ([]Node, error) {
	_ = ctx
	return nil, nil
}

// GCPProvisioner is a stub GCP provisioner.
type GCPProvisioner struct{}

// NewGCPProvisioner creates a new GCP provisioner.
func NewGCPProvisioner() *GCPProvisioner {
	return &GCPProvisioner{}
}

// ProvisionGPU requests a GPU instance.
func (p *GCPProvisioner) ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error) {
	_ = ctx
	if spec.Provider == "" {
		spec.Provider = "gcp"
	}
	return &Node{
		ID:          fmt.Sprintf("gcp-%s-%d", spec.InstanceType, timeNow()),
		Provider:    "gcp",
		InstanceType: spec.InstanceType,
		LaunchedAt: timeNow(),
	}, nil
}

// Terminate shuts down a GCP node.
func (p *GCPProvisioner) Terminate(ctx context.Context, nodeID string) error {
	_ = ctx
	_ = nodeID
	return nil
}

// List returns GCP nodes.
func (p *GCPProvisioner) List(ctx context.Context) ([]Node, error) {
	_ = ctx
	return nil, nil
}

// BootstrapScript generates a cloud-init script that installs the AeroLLM Edge Companion.
func BootstrapScript(meshPeers []string, nodeID string) string {
	peers := ""
	for i, p := range meshPeers {
		if i > 0 {
			peers += ","
		}
		peers += p
	}
	return fmt.Sprintf(`#!/bin/bash
set -euo pipefail
export AEROLLM_MESH_ENABLED=true
export AEROLLM_MESH_NODE_ID=%s
export AEROLLM_MESH_PEERS=%s
curl -fsSL https://install.aerollm.io/edge.sh | bash
systemctl enable --now aerollm-edge
`, nodeID, peers)
}

// MetaAgentInfraLoop polls mesh compute deficit and triggers provisioning.
type MetaAgentInfraLoop struct {
	provisioner InfraProvisioner
	threshold   float64
}

// NewMetaAgentInfraLoop creates a new loop.
func NewMetaAgentInfraLoop(provisioner InfraProvisioner, threshold float64) *MetaAgentInfraLoop {
	if threshold <= 0 {
		threshold = 0.2
	}
	return &MetaAgentInfraLoop{provisioner: provisioner, threshold: threshold}
}

// Evaluate checks the deficit and provisions if needed.
func (l *MetaAgentInfraLoop) Evaluate(ctx context.Context, deficit float64) (*Node, error) {
	if deficit < l.threshold {
		return nil, nil
	}
	if l.provisioner == nil {
		return nil, fmt.Errorf("no provisioner configured")
	}
	return l.provisioner.ProvisionGPU(ctx, NodeSpec{InstanceType: "A100", GPUCount: 1, Region: "us-east-1"})
}

// NewServerMetaAgentLoop creates a loop with AWS primary and GCP fallback.
func NewServerMetaAgentLoop() *MetaAgentInfraLoop {
	return NewServerMetaAgentLoopWith(NewAWSProvisioner(), NewGCPProvisioner(), 0.2)
}

// NewServerMetaAgentLoopWith creates a loop with explicit provisioners.
func NewServerMetaAgentLoopWith(primary, fallback InfraProvisioner, threshold float64) *MetaAgentInfraLoop {
	if threshold <= 0 {
		threshold = 0.2
	}
	return &MetaAgentInfraLoop{
		provisioner: &failoverProvisioner{primary: primary, fallback: fallback},
		threshold:   threshold,
	}
}

type failoverProvisioner struct {
	primary   InfraProvisioner
	fallback  InfraProvisioner
	usedFallback bool
}

func (f *failoverProvisioner) ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error) {
	if f.primary != nil {
		n, err := f.primary.ProvisionGPU(ctx, spec)
		if err == nil && n != nil {
			return n, nil
		}
	}
	f.usedFallback = true
	if f.fallback != nil {
		return f.fallback.ProvisionGPU(ctx, spec)
	}
	return nil, fmt.Errorf("no provisioner available")
}

func (f *failoverProvisioner) Terminate(ctx context.Context, nodeID string) error {
	if !f.usedFallback && f.primary != nil {
		return f.primary.Terminate(ctx, nodeID)
	}
	if f.fallback != nil {
		return f.fallback.Terminate(ctx, nodeID)
	}
	return nil
}

func (f *failoverProvisioner) List(ctx context.Context) ([]Node, error) {
	if !f.usedFallback && f.primary != nil {
		return f.primary.List(ctx)
	}
	if f.fallback != nil {
		return f.fallback.List(ctx)
	}
	return nil, nil
}

func timeNow() int64 {
	return time.Now().Unix()
}
