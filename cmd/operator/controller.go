package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/api/v1alpha1"
	"github.com/ayoubzulfiqar/aerollm/internal/k8s"
)

// Ready condition reasons.
const (
	reasonApplied            = "Applied"
	reasonInvalidSpec        = "InvalidSpec"
	reasonUnsupported        = "Unsupported"
	reasonConflict           = "Conflict"
	reasonSecretUnavailable  = "SecretUnavailable"
	reasonGatewayRejected    = "GatewayRejected"
	reasonGatewayUnavailable = "GatewayUnavailable"
	reasonValidated          = "Validated"
	reasonValidationFailed   = "ValidationFailed"
	maxConditionMessage      = 1024
)

// retryBaseDelay is the first retry delay after a transient failure
// (doubling up to 5 minutes); replaceable in tests.
var retryBaseDelay = time.Second

// kubeAPI is the subset of *k8s.Client the controller uses.
type kubeAPI interface {
	PatchStatus(ctx context.Context, gvr k8s.GroupVersionResource, namespace, name string, mergePatch []byte) (map[string]interface{}, error)
	SecretValue(ctx context.Context, namespace, name, key string) ([]byte, error)
}

// entry is the controller's state for one custom resource.
type entry struct {
	kind k8s.ResourceKind
	gvr  k8s.GroupVersionResource
	obj  map[string]interface{}

	deleted bool
	force   bool // re-apply even if generation/spec are unchanged

	done     bool // doneGen/doneHash reached a terminal outcome
	doneGen  int64
	doneHash string

	failures   int
	retryTimer *time.Timer

	target         string // gateway target owned by this resource
	conflictTarget string // target this resource is waiting for
	keyID          string // per-key budget applied for this resource
}

// outcome of reconciling one resource.
type outcome struct {
	valid     bool
	validMsg  string
	ready     bool
	reason    string
	msg       string
	transient bool
}

// controller reconciles AeroLLM custom resources against the gateway and
// reports the outcome in status. One worker processes a deduplicating
// queue, so gateway updates are serialized.
type controller struct {
	kube      kubeAPI
	gw        *gatewayClient
	log       *slog.Logger
	now       func() time.Time
	retryBase time.Duration
	retryMax  time.Duration

	mu      sync.Mutex
	entries map[string]*entry
	owners  map[string]string // target -> entry key
	queue   []string
	queued  map[string]bool
	wake    chan struct{}
	stopped bool
}

func newController(kube kubeAPI, gw *gatewayClient, log *slog.Logger) *controller {
	return &controller{
		kube:      kube,
		gw:        gw,
		log:       log,
		now:       time.Now,
		retryBase: retryBaseDelay,
		retryMax:  5 * time.Minute,
		entries:   make(map[string]*entry),
		owners:    make(map[string]string),
		queued:    make(map[string]bool),
		wake:      make(chan struct{}, 1),
	}
}

func entryKey(kind k8s.ResourceKind, obj map[string]interface{}) string {
	return string(kind) + "/" + k8s.ObjectNamespace(obj) + "/" + k8s.ObjectName(obj)
}

// observe records an informer event and queues the resource.
func (c *controller) observe(kind k8s.ResourceKind, gvr k8s.GroupVersionResource, ev k8s.WatchEvent) {
	if ev.Object == nil || k8s.ObjectName(ev.Object) == "" {
		return
	}
	key := entryKey(kind, ev.Object)
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		e = &entry{kind: kind, gvr: gvr}
		c.entries[key] = e
	}
	e.obj = ev.Object
	e.deleted = ev.Type == k8s.EventDeleted
	c.enqueueLocked(key)
}

func (c *controller) enqueueLocked(key string) {
	if c.stopped || c.queued[key] {
		return
	}
	c.queued[key] = true
	c.queue = append(c.queue, key)
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *controller) pop() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queue) == 0 {
		return "", false
	}
	key := c.queue[0]
	c.queue = c.queue[1:]
	delete(c.queued, key)
	return key, true
}

// runWorker processes the queue until ctx is done.
func (c *controller) runWorker(ctx context.Context) {
	for {
		for {
			if ctx.Err() != nil {
				return
			}
			key, ok := c.pop()
			if !ok {
				break
			}
			c.process(ctx, key)
		}
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		}
	}
}

// runResync periodically re-applies every resource so gateway restarts
// (which drop runtime config updates) are healed.
func (c *controller) runResync(ctx context.Context, every time.Duration) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.mu.Lock()
			for key, e := range c.entries {
				e.force = true
				c.enqueueLocked(key)
			}
			c.mu.Unlock()
		}
	}
}

// stop cancels pending retries.
func (c *controller) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	for _, e := range c.entries {
		if e.retryTimer != nil {
			e.retryTimer.Stop()
		}
	}
}

func (c *controller) retryDelay(failures int) time.Duration {
	d := c.retryBase
	for i := 1; i < failures && d < c.retryMax; i++ {
		d *= 2
	}
	if d > c.retryMax {
		d = c.retryMax
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func (c *controller) process(ctx context.Context, key string) {
	c.mu.Lock()
	e := c.entries[key]
	if e == nil {
		c.mu.Unlock()
		return
	}
	obj, deleted, force := e.obj, e.deleted, e.force
	e.force = false
	if e.retryTimer != nil {
		e.retryTimer.Stop()
		e.retryTimer = nil
	}
	c.mu.Unlock()

	if deleted {
		c.finalize(ctx, key, e)
		return
	}
	gen := k8s.ObjectGeneration(obj)
	spec, _ := obj["spec"].(map[string]interface{})
	hash, err := k8s.SpecHash(spec)
	if err != nil {
		hash = ""
	}
	c.mu.Lock()
	skip := !force && e.done && e.doneGen == gen && e.doneHash == hash
	c.mu.Unlock()
	if skip {
		return
	}

	out := c.reconcile(ctx, key, e, obj)
	ns, name := k8s.ObjectNamespace(obj), k8s.ObjectName(obj)
	conds := buildConditions(currentConditions(obj), gen, c.now(), out)
	if needsStatusPatch(obj, gen, conds) {
		patched, err := c.patchStatus(ctx, e.gvr, ns, name, gen, conds)
		switch {
		case err == nil:
			c.mu.Lock()
			if patched != nil && k8s.ObjectResourceVersion(e.obj) == k8s.ObjectResourceVersion(obj) {
				e.obj = patched
			}
			c.mu.Unlock()
		case k8s.IsNotFound(err):
			// Deleted meanwhile; the DELETED event will clean up.
		default:
			if ctx.Err() == nil {
				c.log.Warn("status patch failed", "kind", string(e.kind), "namespace", ns, "name", name, "error", err.Error())
			}
			out.transient = true
		}
	}

	level := slog.LevelInfo
	if !out.valid || (!out.ready && out.reason != reasonUnsupported) {
		level = slog.LevelWarn
	}
	c.log.Log(ctx, level, "reconciled", "kind", string(e.kind), "namespace", ns, "name", name,
		"generation", gen, "valid", out.valid, "ready", out.ready, "reason", out.reason, "status", out.msg)

	c.mu.Lock()
	defer c.mu.Unlock()
	if out.transient && ctx.Err() == nil {
		e.done = false
		e.failures++
		d := c.retryDelay(e.failures)
		e.retryTimer = time.AfterFunc(d, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.entries[key] == e {
				e.force = true
				c.enqueueLocked(key)
			}
		})
		return
	}
	e.failures = 0
	e.done, e.doneGen, e.doneHash = true, gen, hash
}

// reconcile validates obj and applies it to the gateway.
func (c *controller) reconcile(ctx context.Context, key string, e *entry, obj map[string]interface{}) outcome {
	kind, _, spec, err := k8s.ValidateObject(obj)
	if err == nil && kind != e.kind {
		err = fmt.Errorf("kind: expected %s, got %s", e.kind, kind)
	}
	if err != nil {
		c.release(key, e)
		return outcome{validMsg: err.Error(), reason: reasonInvalidSpec, msg: "spec failed validation; nothing was applied"}
	}

	var p plan
	switch e.kind {
	case k8s.KindAeroRoute:
		var s v1alpha1.AeroRouteSpec
		if err = decodeSpec(spec, &s); err == nil {
			p, err = planRoute(s)
		}
	case k8s.KindAeroBudget:
		var s v1alpha1.AeroBudgetSpec
		if err = decodeSpec(spec, &s); err == nil {
			apiKey := strings.TrimSpace(s.APIKey)
			if s.APIKeySecretRef != "" {
				secretName, secretKey, _ := strings.Cut(s.APIKeySecretRef, "/")
				val, serr := c.kube.SecretValue(ctx, k8s.ObjectNamespace(obj), secretName, secretKey)
				if serr != nil {
					c.release(key, e)
					return outcome{valid: true, reason: reasonSecretUnavailable, transient: true,
						msg: sanitize(fmt.Sprintf("cannot read api_key_secret_ref %q: %v", s.APIKeySecretRef, serr), maxConditionMessage)}
				}
				apiKey = strings.TrimSpace(string(val))
				if apiKey == "" {
					c.release(key, e)
					return outcome{valid: true, reason: reasonSecretUnavailable, transient: true,
						msg: fmt.Sprintf("api_key_secret_ref %q resolves to an empty value", s.APIKeySecretRef)}
				}
			}
			p = planBudget(s, apiKey)
		}
	case k8s.KindAeroAgentPipeline:
		p = planPipeline()
	}
	if err != nil {
		c.release(key, e)
		return outcome{validMsg: err.Error(), reason: reasonInvalidSpec, msg: "spec failed validation; nothing was applied"}
	}
	if p.unsupported != "" {
		c.release(key, e)
		return outcome{valid: true, reason: reasonUnsupported, msg: p.unsupported}
	}

	// Two resources must not fight over the same gateway state.
	c.mu.Lock()
	if owner, ok := c.owners[p.target]; ok && owner != key {
		if oe := c.entries[owner]; oe != nil && !oe.deleted {
			if e.target != p.target {
				c.releaseLocked(key, e)
			}
			e.conflictTarget = p.target
			c.mu.Unlock()
			return outcome{valid: true, reason: reasonConflict,
				msg: fmt.Sprintf("gateway %s is already managed by %s; delete one of them", describeTarget(p.target), owner)}
		}
	}
	e.conflictTarget = ""
	oldTarget, oldKeyID := e.target, e.keyID
	if oldTarget != "" && oldTarget != p.target {
		c.releaseLocked(key, e)
	}
	c.owners[p.target] = key
	e.target = p.target
	c.mu.Unlock()

	if err := c.gw.apply(ctx, p); err != nil {
		reason, transient := reasonGatewayRejected, false
		var ge *gatewayError
		if errors.As(err, &ge) && ge.Transient {
			reason, transient = reasonGatewayUnavailable, true
		}
		return outcome{valid: true, reason: reason, transient: transient, msg: sanitize(err.Error(), maxConditionMessage)}
	}
	c.mu.Lock()
	e.keyID = p.keyID
	c.mu.Unlock()
	if oldKeyID != "" && oldKeyID != p.keyID {
		// The budget moved to another key: drop the old key's budget.
		if err := c.gw.deleteBudget(ctx, oldKeyID); err != nil {
			c.log.Warn("could not remove previous per-key budget", "key_id", oldKeyID, "error", err.Error())
		}
	}
	return outcome{valid: true, ready: true, reason: reasonApplied, msg: sanitize("applied "+p.summary+p.note(), maxConditionMessage)}
}

func describeTarget(t string) string {
	switch {
	case t == "router":
		return "router config"
	case t == "finops":
		return "gateway-wide budget (finops)"
	case strings.HasPrefix(t, "budget:"):
		return "budget for " + strings.TrimPrefix(t, "budget:")
	}
	return t
}

// release gives up the target owned by e (if any).
func (c *controller) release(key string, e *entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseLocked(key, e)
	e.conflictTarget = ""
}

// releaseLocked frees e's target and requeues resources waiting for it.
func (c *controller) releaseLocked(key string, e *entry) {
	t := e.target
	if t == "" {
		return
	}
	e.target = ""
	if c.owners[t] != key {
		return
	}
	delete(c.owners, t)
	for k, other := range c.entries {
		if k != key && other.conflictTarget == t && !other.deleted {
			other.force = true
			c.enqueueLocked(k)
		}
	}
}

// finalize handles a deleted resource: per-key budgets are removed from
// the gateway; global router/finops settings are left as they are (their
// previous values are unknown).
func (c *controller) finalize(ctx context.Context, key string, e *entry) {
	c.mu.Lock()
	keyID := e.keyID
	c.releaseLocked(key, e)
	if e.retryTimer != nil {
		e.retryTimer.Stop()
	}
	delete(c.entries, key)
	c.mu.Unlock()
	if keyID != "" {
		if err := c.gw.deleteBudget(ctx, keyID); err != nil {
			c.log.Warn("could not remove per-key budget of deleted resource", "resource", key, "key_id", keyID, "error", err.Error())
		}
	}
	c.log.Info("resource deleted", "resource", key)
}

func (c *controller) patchStatus(ctx context.Context, gvr k8s.GroupVersionResource, ns, name string, gen int64, conds []v1alpha1.Condition) (map[string]interface{}, error) {
	patch, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"observedGeneration": gen,
			"conditions":         conds,
		},
	})
	if err != nil {
		return nil, err
	}
	return c.kube.PatchStatus(ctx, gvr, ns, name, patch)
}

// currentConditions reads status.conditions from obj.
func currentConditions(obj map[string]interface{}) []v1alpha1.Condition {
	st, _ := obj["status"].(map[string]interface{})
	raw, ok := st["conditions"]
	if !ok {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var conds []v1alpha1.Condition
	if json.Unmarshal(b, &conds) != nil {
		return nil
	}
	return conds
}

func observedGeneration(obj map[string]interface{}) int64 {
	st, _ := obj["status"].(map[string]interface{})
	f, _ := st["observedGeneration"].(float64)
	return int64(f)
}

func condStatus(ok bool) string {
	if ok {
		return v1alpha1.ConditionTrue
	}
	return v1alpha1.ConditionFalse
}

// buildConditions computes Valid and Ready, keeping lastTransitionTime when
// a condition's status did not change.
func buildConditions(prev []v1alpha1.Condition, gen int64, now time.Time, out outcome) []v1alpha1.Condition {
	validMsg, validReason := "spec is valid", reasonValidated
	if !out.valid {
		validMsg, validReason = out.validMsg, reasonValidationFailed
	}
	conds := []v1alpha1.Condition{
		{Type: v1alpha1.ConditionValid, Status: condStatus(out.valid), Reason: validReason, Message: sanitize(validMsg, maxConditionMessage), ObservedGeneration: gen},
		{Type: v1alpha1.ConditionReady, Status: condStatus(out.ready), Reason: out.reason, Message: sanitize(out.msg, maxConditionMessage), ObservedGeneration: gen},
	}
	for i := range conds {
		conds[i].LastTransitionTime = v1alpha1.FormatTime(now)
		for _, p := range prev {
			if p.Type == conds[i].Type && p.Status == conds[i].Status && p.LastTransitionTime != "" {
				conds[i].LastTransitionTime = p.LastTransitionTime
			}
		}
	}
	return conds
}

// needsStatusPatch reports whether status differs from what we would write
// (ignoring lastTransitionTime), avoiding a patch -> watch event -> patch
// loop.
func needsStatusPatch(obj map[string]interface{}, gen int64, conds []v1alpha1.Condition) bool {
	if observedGeneration(obj) != gen {
		return true
	}
	prev := currentConditions(obj)
	if len(prev) != len(conds) {
		return true
	}
	for i := range conds {
		a, b := prev[i], conds[i]
		if a.Type != b.Type || a.Status != b.Status || a.Reason != b.Reason || a.Message != b.Message || a.ObservedGeneration != b.ObservedGeneration {
			return true
		}
	}
	return false
}
