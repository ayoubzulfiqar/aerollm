package k8s

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/api/v1alpha1"
)

const manifestSource = "manifest-reconciler"

// ManifestReconciler applies AeroLLM operator manifest resources.
//
// Every manifest is validated (metadata.name, kind, and the typed
// api/v1alpha1 spec including unknown-field detection) before its ApplyFunc
// runs. Reconciliation is idempotent: once a spec has been applied
// successfully, re-delivering the identical spec for the same kind/name is a
// no-op reported as "unchanged".
type ManifestReconciler struct {
	RouterApply   ApplyFunc
	BudgetApply   ApplyFunc
	PipelineApply ApplyFunc

	mu      sync.Mutex
	applied map[string]string // kind/name -> hash of last applied spec
}

// Reconcile validates and applies the manifest resource based on its kind.
// Map objects get their "status" field updated with the outcome.
func (m *ManifestReconciler) Reconcile(ctx context.Context, object interface{}) error {
	var v map[string]interface{}
	switch o := object.(type) {
	case map[string]interface{}:
		v = o
	case []byte:
		if err := json.Unmarshal(o, &v); err != nil {
			return fmt.Errorf("invalid manifest JSON: %w", err)
		}
	case json.RawMessage:
		if err := json.Unmarshal(o, &v); err != nil {
			return fmt.Errorf("invalid manifest JSON: %w", err)
		}
	default:
		return fmt.Errorf("unsupported manifest object type: %T", object)
	}
	if v == nil {
		return errors.New("manifest: null object")
	}

	kind, name, spec, err := manifestParts(v)
	if err != nil {
		setStatus(v, "invalid: "+err.Error(), false)
		return err
	}
	if err := validateSpec(kind, spec); err != nil {
		err = fmt.Errorf("%s/%s: invalid spec: %w", kind, name, err)
		setStatus(v, err.Error(), false)
		return err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	key := string(kind) + "/" + name
	hash, err := specHash(spec)
	if err != nil {
		setStatus(v, "invalid: "+err.Error(), false)
		return err
	}
	m.mu.Lock()
	unchanged := m.applied != nil && m.applied[key] == hash
	m.mu.Unlock()
	if unchanged {
		setStatus(v, "unchanged", true)
		return nil
	}

	res, err := m.applyForKind(ctx, kind, name, spec)
	if err != nil {
		msg := res.Message
		if msg == "" {
			msg = err.Error()
		}
		setStatus(v, msg, false)
		return err
	}
	if res.Message == "" && res.Applied {
		res.Message = "applied"
	}
	if res.Applied {
		m.mu.Lock()
		if m.applied == nil {
			m.applied = make(map[string]string)
		}
		m.applied[key] = hash
		m.mu.Unlock()
	}
	setStatus(v, res.Message, res.Applied)
	return nil
}

// ValidateObject checks a decoded AeroLLM resource the same way Reconcile
// does (kind, metadata.name and the typed api/v1alpha1 spec, rejecting
// unknown spec fields) and returns its parts. A missing spec is returned
// as an empty map.
func ValidateObject(obj map[string]interface{}) (kind ResourceKind, name string, spec map[string]interface{}, err error) {
	if obj == nil {
		return "", "", nil, errors.New("manifest: null object")
	}
	kind, name, spec, err = manifestParts(obj)
	if err != nil {
		return kind, name, spec, err
	}
	if err := validateSpec(kind, spec); err != nil {
		return kind, name, spec, fmt.Errorf("%s/%s: invalid spec: %w", kind, name, err)
	}
	return kind, name, spec, nil
}

// SpecHash returns a stable hash of a spec (for change detection).
func SpecHash(spec map[string]interface{}) (string, error) { return specHash(spec) }

// Forget drops the idempotence record for kind/name so the next delivery of
// its spec is applied again (e.g. after the resource was deleted).
func (m *ManifestReconciler) Forget(kind ResourceKind, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.applied, string(kind)+"/"+name)
}

func manifestParts(v map[string]interface{}) (ResourceKind, string, map[string]interface{}, error) {
	kindStr, _ := v["kind"].(string)
	if kindStr == "" {
		return "", "", nil, errors.New("manifest: missing kind")
	}
	kind := ResourceKind(kindStr)
	switch kind {
	case KindAeroRoute, KindAeroBudget, KindAeroAgentPipeline:
	default:
		return kind, "", nil, fmt.Errorf("unsupported kind: %s", kindStr)
	}
	var name string
	switch md := v["metadata"].(type) {
	case map[string]interface{}:
		name, _ = md["name"].(string)
	case nil:
	default:
		return kind, "", nil, errors.New("manifest: metadata must be an object")
	}
	if err := v1alpha1.ValidateName(name); err != nil {
		return kind, name, nil, fmt.Errorf("%s: %w", kind, err)
	}
	var spec map[string]interface{}
	switch s := v["spec"].(type) {
	case map[string]interface{}:
		spec = s
	case nil:
		spec = map[string]interface{}{}
	default:
		return kind, name, nil, fmt.Errorf("%s/%s: spec must be an object", kind, name)
	}
	return kind, name, spec, nil
}

// validateSpec decodes spec into the typed v1alpha1 spec for kind (rejecting
// unknown fields, which usually indicate a typo that would otherwise be
// silently ignored) and runs its Validate method.
func validateSpec(kind ResourceKind, spec map[string]interface{}) error {
	raw, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("spec is not serialisable: %w", err)
	}
	decode := func(dst interface{}) error {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		return dec.Decode(dst)
	}
	switch kind {
	case KindAeroRoute:
		var s v1alpha1.AeroRouteSpec
		if err := decode(&s); err != nil {
			return err
		}
		return s.Validate()
	case KindAeroBudget:
		var s v1alpha1.AeroBudgetSpec
		if err := decode(&s); err != nil {
			return err
		}
		return s.Validate()
	case KindAeroAgentPipeline:
		var s v1alpha1.AeroAgentPipelineSpec
		if err := decode(&s); err != nil {
			return err
		}
		return s.Validate()
	default:
		return fmt.Errorf("unsupported kind: %s", kind)
	}
}

func specHash(spec map[string]interface{}) (string, error) {
	raw, err := json.Marshal(spec) // map keys are sorted: canonical
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func setStatus(v map[string]interface{}, message string, applied bool) {
	if st, ok := v["status"].(map[string]interface{}); ok && st != nil {
		st["message"] = message
		st["applied"] = applied
		st["source"] = manifestSource
		return
	}
	v["status"] = map[string]interface{}{
		"message": message,
		"applied": applied,
		"source":  manifestSource,
	}
}

func (m *ManifestReconciler) applyForKind(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
	var fn ApplyFunc
	switch kind {
	case KindAeroRoute:
		fn = m.RouterApply
	case KindAeroBudget:
		fn = m.BudgetApply
	case KindAeroAgentPipeline:
		fn = m.PipelineApply
	default:
		return ApplyResult{Kind: kind, Name: name, Applied: false, Message: "unsupported kind"}, fmt.Errorf("unsupported kind: %s", kind)
	}
	if fn == nil {
		return ApplyResult{Kind: kind, Name: name, Applied: false, Message: msgNoApplyFunc}, nil
	}
	return fn(ctx, kind, name, spec)
}
