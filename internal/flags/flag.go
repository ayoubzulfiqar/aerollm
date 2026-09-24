// Package flags implements an in-memory feature flag store with
// deterministic percentage rollouts, allow/deny lists and ordered targeting
// rules, plus a JSON HTTP API.
package flags

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// RolloutStrategy defines how a feature flag is rolled out.
type RolloutStrategy string

const (
	RolloutGlobal     RolloutStrategy = "global"
	RolloutPercentage RolloutStrategy = "percentage"
	RolloutAllowList  RolloutStrategy = "allowlist"
	RolloutDenyList   RolloutStrategy = "denylist"
)

// RuleOperator is the comparison applied by a targeting rule.
type RuleOperator string

const (
	OpEquals    RuleOperator = "eq"       // attribute equals one of Values (case-insensitive)
	OpNotEquals RuleOperator = "neq"      // attribute equals none of Values
	OpIn        RuleOperator = "in"       // alias of eq
	OpNotIn     RuleOperator = "not_in"   // alias of neq
	OpPrefix    RuleOperator = "prefix"   // attribute has one of Values as prefix
	OpSuffix    RuleOperator = "suffix"   // attribute has one of Values as suffix
	OpContains  RuleOperator = "contains" // attribute contains one of Values
	OpExists    RuleOperator = "exists"   // attribute is present and non-empty
)

// Rule is a targeting rule. Rules are evaluated in order before the rollout
// strategy; the first matching rule decides the result (Serve).
type Rule struct {
	Attribute string       `json:"attribute"`
	Operator  RuleOperator `json:"operator"`
	Values    []string     `json:"values,omitempty"`
	Serve     bool         `json:"serve"`
}

// FeatureFlag represents a feature flag definition.
//
// Enabled is a kill switch: a disabled flag always evaluates to false.
// Percentage rollouts hash the flag key together with a stable unit
// identifier (BucketBy attribute, or the first non-empty of id, user_id,
// user, key, tenant_id, tenant) so a given unit always gets the same answer.
type FeatureFlag struct {
	Key         string                 `json:"key"`
	Enabled     bool                   `json:"enabled"`
	Strategy    RolloutStrategy        `json:"strategy"`
	Percentage  int                    `json:"percentage"`
	AllowList   []string               `json:"allow_list"`
	DenyList    []string               `json:"deny_list"`
	Description string                 `json:"description"`
	Metadata    map[string]interface{} `json:"metadata"`
	Rules       []Rule                 `json:"rules,omitempty"`
	BucketBy    string                 `json:"bucket_by,omitempty"`
}

// Limits applied to flag definitions and the store.
const (
	MaxFlags          = 10000
	MaxListEntries    = 10000
	MaxRules          = 100
	MaxRuleValues     = 1000
	MaxValueLength    = 256
	MaxDescription    = 1024
	MaxMetadataKeys   = 64
	maxBodyBytes      = 1 << 20
	maxAttributeLen   = 128
	percentageBuckets = 10000
)

var (
	// ErrInvalidFlag is returned (wrapped) when a flag definition fails validation.
	ErrInvalidFlag = errors.New("invalid feature flag")
	// ErrStoreFull is returned when the store already holds MaxFlags flags.
	ErrStoreFull = errors.New("feature flag store is full")
	// ErrNotFound is returned when a flag does not exist.
	ErrNotFound = errors.New("feature flag not found")

	keyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// identityAttributes are consulted, in order, to find the unit identifier
// used for percentage bucketing and allow/deny lists.
var identityAttributes = []string{"id", "user_id", "user", "key", "tenant_id", "tenant"}

// Store stores feature flags with thread-safe access.
type Store struct {
	mu       sync.RWMutex
	flags    map[string]FeatureFlag
	rollouts map[string]RolloutPolicy
}

// NewStore initializes a feature flag store.
func NewStore() *Store {
	return &Store{
		flags:    make(map[string]FeatureFlag),
		rollouts: make(map[string]RolloutPolicy),
	}
}

// Validate normalizes and validates a flag definition. An empty strategy is
// normalized to RolloutGlobal.
func (f *FeatureFlag) Validate() error {
	var errs []error
	if !keyPattern.MatchString(f.Key) {
		errs = append(errs, fmt.Errorf("key must match %s", keyPattern.String()))
	}
	if f.Strategy == "" {
		f.Strategy = RolloutGlobal
	}
	switch f.Strategy {
	case RolloutGlobal, RolloutPercentage, RolloutAllowList, RolloutDenyList:
	default:
		errs = append(errs, fmt.Errorf("unknown strategy %q (want global|percentage|allowlist|denylist)", f.Strategy))
	}
	if f.Percentage < 0 || f.Percentage > 100 {
		errs = append(errs, errors.New("percentage must be between 0 and 100"))
	}
	if err := validateList("allow_list", f.AllowList); err != nil {
		errs = append(errs, err)
	}
	if err := validateList("deny_list", f.DenyList); err != nil {
		errs = append(errs, err)
	}
	if len(f.Description) > MaxDescription {
		errs = append(errs, fmt.Errorf("description exceeds %d bytes", MaxDescription))
	}
	if len(f.Metadata) > MaxMetadataKeys {
		errs = append(errs, fmt.Errorf("metadata exceeds %d keys", MaxMetadataKeys))
	}
	if len(f.BucketBy) > maxAttributeLen {
		errs = append(errs, fmt.Errorf("bucket_by exceeds %d bytes", maxAttributeLen))
	}
	if len(f.Rules) > MaxRules {
		errs = append(errs, fmt.Errorf("at most %d rules allowed", MaxRules))
	}
	for i, r := range f.Rules {
		if err := r.validate(); err != nil {
			errs = append(errs, fmt.Errorf("rules[%d]: %w", i, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalidFlag, errors.Join(errs...))
	}
	return nil
}

func validateList(name string, list []string) error {
	if len(list) > MaxListEntries {
		return fmt.Errorf("%s exceeds %d entries", name, MaxListEntries)
	}
	for _, v := range list {
		if len(v) > MaxValueLength {
			return fmt.Errorf("%s entry exceeds %d bytes", name, MaxValueLength)
		}
	}
	return nil
}

func (r Rule) validate() error {
	if r.Attribute == "" || len(r.Attribute) > maxAttributeLen {
		return fmt.Errorf("attribute is required (max %d bytes)", maxAttributeLen)
	}
	switch r.Operator {
	case OpExists:
	case OpEquals, OpNotEquals, OpIn, OpNotIn, OpPrefix, OpSuffix, OpContains:
		if len(r.Values) == 0 {
			return fmt.Errorf("operator %q requires at least one value", r.Operator)
		}
	default:
		return fmt.Errorf("unknown operator %q", r.Operator)
	}
	if len(r.Values) > MaxRuleValues {
		return fmt.Errorf("at most %d values allowed", MaxRuleValues)
	}
	for _, v := range r.Values {
		if len(v) > MaxValueLength {
			return fmt.Errorf("value exceeds %d bytes", MaxValueLength)
		}
	}
	return nil
}

// Upsert validates and adds or updates a feature flag.
func (s *Store) Upsert(flag FeatureFlag) error {
	if err := flag.Validate(); err != nil {
		return err
	}
	flag = cloneFlag(flag)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.flags[flag.Key]; !exists && len(s.flags) >= MaxFlags {
		return ErrStoreFull
	}
	s.flags[flag.Key] = flag
	return nil
}

// Get retrieves a copy of a feature flag by key.
func (s *Store) Get(key string) (FeatureFlag, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.flags[key]
	if !ok {
		return FeatureFlag{}, false
	}
	return cloneFlag(f), true
}

// Delete removes a flag and any rollout policy for it. It reports whether the
// flag existed.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.flags[key]
	delete(s.flags, key)
	delete(s.rollouts, key)
	return ok
}

// List returns copies of all feature flags sorted by key.
func (s *Store) List() []FeatureFlag {
	s.mu.RLock()
	out := make([]FeatureFlag, 0, len(s.flags))
	for _, f := range s.flags {
		out = append(out, cloneFlag(f))
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// SetRollout stores rollout policy metadata for a key. The policy is
// informational; evaluation is driven by the flag's Strategy/Percentage.
func (s *Store) SetRollout(key string, policy RolloutPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rollouts[key]; !exists && len(s.rollouts) >= MaxFlags {
		return
	}
	s.rollouts[key] = policy
}

// GetRollout retrieves rollout policy for a key.
func (s *Store) GetRollout(key string) (RolloutPolicy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.rollouts[key]
	return p, ok
}

// RolloutPolicy defines rollout weighting.
type RolloutPolicy struct {
	Key       string `json:"key"`
	Weight    int    `json:"weight"`
	CreatedAt string `json:"created_at"`
}

// Evaluate decides if a feature flag is active for a given context.
type Evaluate func(key string, context map[string]string) bool

// Evaluation explains a flag decision.
type Evaluation struct {
	Key     string `json:"key"`
	Enabled bool   `json:"enabled"`
	Reason  string `json:"reason"`
}

// Enabled evaluates a feature flag based on its rules and strategy.
func (s *Store) Enabled(key string, ctx map[string]string) bool {
	return s.Explain(key, ctx).Enabled
}

// Explain evaluates a flag and reports why it resolved the way it did.
func (s *Store) Explain(key string, ctx map[string]string) Evaluation {
	s.mu.RLock()
	f, ok := s.flags[key]
	s.mu.RUnlock()
	if !ok {
		return Evaluation{Key: key, Reason: "flag_not_found"}
	}
	enabled, reason := evaluate(f, ctx)
	return Evaluation{Key: key, Enabled: enabled, Reason: reason}
}

// evaluate is pure: it reads only the (immutable, stored) flag and ctx.
func evaluate(f FeatureFlag, ctx map[string]string) (bool, string) {
	if !f.Enabled {
		return false, "disabled"
	}
	for i, r := range f.Rules {
		if r.matches(ctx) {
			return r.Serve, fmt.Sprintf("rule:%d", i)
		}
	}
	switch f.Strategy {
	case RolloutGlobal, "":
		return true, "global"
	case RolloutPercentage:
		unit := identity(f, ctx)
		if unit == "" {
			// Without a stable unit identifier a deterministic split is
			// impossible; only a full rollout is on.
			return f.Percentage >= 100, "percentage_no_identity"
		}
		return bucket(f.Key, unit) < f.Percentage*(percentageBuckets/100), "percentage"
	case RolloutAllowList:
		return inList(identity(f, ctx), f.AllowList), "allowlist"
	case RolloutDenyList:
		return !inList(identity(f, ctx), f.DenyList), "denylist"
	default:
		return false, "unknown_strategy"
	}
}

func (r Rule) matches(ctx map[string]string) bool {
	v, present := ctx[r.Attribute]
	switch r.Operator {
	case OpExists:
		return present && v != ""
	case OpEquals, OpIn:
		return present && inList(v, r.Values)
	case OpNotEquals, OpNotIn:
		return !inList(v, r.Values)
	case OpPrefix, OpSuffix, OpContains:
		if !present {
			return false
		}
		lv := strings.ToLower(v)
		for _, want := range r.Values {
			lw := strings.ToLower(want)
			switch r.Operator {
			case OpPrefix:
				if strings.HasPrefix(lv, lw) {
					return true
				}
			case OpSuffix:
				if strings.HasSuffix(lv, lw) {
					return true
				}
			default:
				if strings.Contains(lv, lw) {
					return true
				}
			}
		}
	}
	return false
}

// identity returns the unit identifier used for bucketing and lists.
func identity(f FeatureFlag, ctx map[string]string) string {
	if f.BucketBy != "" {
		return ctx[f.BucketBy]
	}
	for _, attr := range identityAttributes {
		if v := ctx[attr]; v != "" {
			return v
		}
	}
	return ""
}

// bucket deterministically maps (flag key, unit) to [0, percentageBuckets).
// Including the flag key decorrelates rollouts of different flags.
func bucket(key, unit string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(unit))
	return int(h.Sum64() % percentageBuckets)
}

func inList(needle string, haystack []string) bool {
	if needle == "" {
		return false
	}
	for _, item := range haystack {
		if strings.EqualFold(item, needle) {
			return true
		}
	}
	return false
}

func cloneFlag(f FeatureFlag) FeatureFlag {
	if f.AllowList != nil {
		f.AllowList = append([]string(nil), f.AllowList...)
	}
	if f.DenyList != nil {
		f.DenyList = append([]string(nil), f.DenyList...)
	}
	if f.Rules != nil {
		rules := make([]Rule, len(f.Rules))
		for i, r := range f.Rules {
			if r.Values != nil {
				r.Values = append([]string(nil), r.Values...)
			}
			rules[i] = r
		}
		f.Rules = rules
	}
	if f.Metadata != nil {
		f.Metadata, _ = deepCopy(f.Metadata).(map[string]interface{})
	}
	return f
}

func deepCopy(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		m := make(map[string]interface{}, len(t))
		for k, val := range t {
			m[k] = deepCopy(val)
		}
		return m
	case []interface{}:
		s := make([]interface{}, len(t))
		for i, val := range t {
			s[i] = deepCopy(val)
		}
		return s
	default:
		return v
	}
}

// flagPatch is the body accepted by PATCH; nil fields are left unchanged.
type flagPatch struct {
	Enabled     *bool                   `json:"enabled"`
	Strategy    *RolloutStrategy        `json:"strategy"`
	Percentage  *int                    `json:"percentage"`
	AllowList   *[]string               `json:"allow_list"`
	DenyList    *[]string               `json:"deny_list"`
	Description *string                 `json:"description"`
	Metadata    *map[string]interface{} `json:"metadata"`
	Rules       *[]Rule                 `json:"rules"`
	BucketBy    *string                 `json:"bucket_by"`
}

func (p flagPatch) apply(f *FeatureFlag) {
	if p.Enabled != nil {
		f.Enabled = *p.Enabled
	}
	if p.Strategy != nil {
		f.Strategy = *p.Strategy
	}
	if p.Percentage != nil {
		f.Percentage = *p.Percentage
	}
	if p.AllowList != nil {
		f.AllowList = *p.AllowList
	}
	if p.DenyList != nil {
		f.DenyList = *p.DenyList
	}
	if p.Description != nil {
		f.Description = *p.Description
	}
	if p.Metadata != nil {
		f.Metadata = *p.Metadata
	}
	if p.Rules != nil {
		f.Rules = *p.Rules
	}
	if p.BucketBy != nil {
		f.BucketBy = *p.BucketBy
	}
}

// WebhookHandler exposes JSON-based feature flag CRUD and evaluation.
//
//	GET    /v1/flags                     list flags
//	GET    /v1/flags/{key} | ?key={key}  get one flag
//	GET    /v1/flags/{key}/evaluate?id=u evaluate with query params as context
//	POST   /v1/flags[/{key}]             create or replace (200)
//	POST   /v1/flags/{key}/evaluate      evaluate with {"context":{...}}
//	PUT    /v1/flags/{key}               replace an existing flag
//	PATCH  /v1/flags/{key}               partially update an existing flag
//	DELETE /v1/flags/{key}               delete (204)
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, action := parsePath(r)
		if action != "" && action != "evaluate" {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		if action == "evaluate" {
			handleEvaluate(w, r, store, key)
			return
		}
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			if key == "" {
				writeJSON(w, http.StatusOK, store.List())
				return
			}
			f, ok := store.Get(key)
			if !ok {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			writeJSON(w, http.StatusOK, f)
		case http.MethodPost, http.MethodPut:
			var f FeatureFlag
			if !decodeBody(w, r, &f) {
				return
			}
			if f.Key == "" {
				f.Key = key
			}
			if key != "" && f.Key != key {
				writeError(w, http.StatusBadRequest, "key in body does not match key in path")
				return
			}
			if r.Method == http.MethodPut {
				if key == "" {
					writeError(w, http.StatusBadRequest, "missing key")
					return
				}
				if _, ok := store.Get(key); !ok {
					writeError(w, http.StatusNotFound, "not found")
					return
				}
			}
			if err := store.Upsert(f); err != nil {
				writeStoreError(w, err)
				return
			}
			stored, _ := store.Get(f.Key)
			writeJSON(w, http.StatusOK, stored)
		case http.MethodPatch:
			if key == "" {
				writeError(w, http.StatusBadRequest, "missing key")
				return
			}
			var p flagPatch
			if !decodeBody(w, r, &p) {
				return
			}
			store.mu.Lock()
			existing, ok := store.flags[key]
			if !ok {
				store.mu.Unlock()
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			updated := cloneFlag(existing)
			p.apply(&updated)
			if err := updated.Validate(); err != nil {
				store.mu.Unlock()
				writeStoreError(w, err)
				return
			}
			store.flags[key] = cloneFlag(updated)
			store.mu.Unlock()
			writeJSON(w, http.StatusOK, updated)
		case http.MethodDelete:
			if key == "" {
				writeError(w, http.StatusBadRequest, "missing key")
				return
			}
			if !store.Delete(key) {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			methodNotAllowed(w, "GET, HEAD, POST, PUT, PATCH, DELETE")
		}
	}
}

func handleEvaluate(w http.ResponseWriter, r *http.Request, store *Store, key string) {
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}
	ctx := map[string]string{}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				ctx[k] = v[0]
			}
		}
	case http.MethodPost:
		var body struct {
			Context map[string]string `json:"context"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		if body.Context != nil {
			ctx = body.Context
		}
	default:
		methodNotAllowed(w, "GET, HEAD, POST")
		return
	}
	if _, ok := store.Get(key); !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, store.Explain(key, ctx))
}

// parsePath extracts the flag key (from ?key= or the path) and an optional
// trailing action segment such as "evaluate".
func parsePath(r *http.Request) (key, action string) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/flags")
	rest = strings.Trim(rest, "/")
	if rest != "" {
		parts := strings.SplitN(rest, "/", 2)
		key = parts[0]
		if len(parts) == 2 {
			action = parts[1]
		}
	}
	if key == "" {
		key = r.URL.Query().Get("key")
	}
	return key, action
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalidFlag):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrStoreFull):
		writeError(w, http.StatusInsufficientStorage, err.Error())
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, "missing body")
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "bad request: invalid JSON")
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "bad request: unexpected data after JSON body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}
