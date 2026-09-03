package k8s

import (
	"context"
	"fmt"
)

// ManifestReconciler applies AeroLLM operator manifest resources.
type ManifestReconciler struct {
	RouterApply      ApplyFunc
	BudgetApply      ApplyFunc
	PipelineApply    ApplyFunc
}

// Reconcile applies the manifest resource based on its kind.
func (m *ManifestReconciler) Reconcile(ctx context.Context, object interface{}) error {
	switch v := object.(type) {
	case map[string]interface{}:
		kindVal := fmt.Sprint(v["kind"])
		kind := ResourceKind(kindVal)
		name, _ := v["metadata"].(map[string]interface{})["name"].(string)
		if name == "" { name = "unknown" }
		spec, _ := v["spec"].(map[string]interface{})
		if spec == nil { spec = map[string]interface{}{} }
		res, err := m.applyForKind(ctx, kind, name, spec)
		if err != nil { return err }
		if statuser, ok := v["status"].(map[string]interface{}); ok && statuser != nil {
			statuser["message"] = res.Message
			statuser["applied"] = res.Applied
			statuser["source"] = "manifest-reconciler"
		} else {
			v["status"] = map[string]interface{}{
				"message": res.Message,
				"applied": res.Applied,
				"source": "manifest-reconciler",
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported manifest object type: %T", object)
	}
}

func (m *ManifestReconciler) applyForKind(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
	switch kind {
	case KindAeroRoute:
		if m.RouterApply != nil { return m.RouterApply(ctx, kind, name, spec) }
	case KindAeroBudget:
		if m.BudgetApply != nil { return m.BudgetApply(ctx, kind, name, spec) }
	case KindAeroAgentPipeline:
		if m.PipelineApply != nil { return m.PipelineApply(ctx, kind, name, spec) }
	default:
		return ApplyResult{Kind: kind, Name: name, Applied: false, Message: "unsupported kind"}, fmt.Errorf("unsupported kind: %s", kind)
	}
	return ApplyResult{Kind: kind, Name: name, Applied: false, Message: "no apply func configured"}, nil
}
