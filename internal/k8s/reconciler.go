package k8s

import (
	"context"
	"fmt"
)

// ResourceKind identifies the Aero control-plane resource type.
type ResourceKind string

const (
	KindAeroRoute         ResourceKind = "AeroRoute"
	KindAeroBudget        ResourceKind = "AeroBudget"
	KindAeroAgentPipeline ResourceKind = "AeroAgentPipeline"
)

// ApplyResult is the outcome of applying a control-plane resource.
type ApplyResult struct {
	Kind    ResourceKind
	Name    string
	Applied bool
	Message string
}

// ResourceApplyFunc applies control-plane changes to the gateway runtime.
// Implementations may update Redis, a gRPC control plane, or a Kubernetes CRD.
type ResourceApplyFunc func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error)

// Reconciler watches control-plane resources and applies routing/config changes.
type Reconciler struct {
	Apply ResourceApplyFunc
}

// Reconcile performs a single reconciliation for an Aero control-plane resource.
func (r *Reconciler) Reconcile(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
	if r == nil || r.Apply == nil {
		return ApplyResult{}, fmt.Errorf("reconciler or apply function is nil")
	}
	return r.Apply(ctx, kind, name, spec)
}

// SetupManager is a placeholder for controller-runtime registration.
// It preserves the extension point without requiring kubebuilder imports in this package.
func SetupManager() error {
	return nil
}
