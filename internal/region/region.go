// Package region manages deployment regions, data-residency policies and
// per-region provider route rules, and resolves residency-compliant routes.
//
// Residency semantics: a ResidencyPolicy with Required=true pins a data type
// to its region — requests carrying that data type may only be routed to one
// of the regions named by required policies for it (the union when several
// exist). Non-required policies express a preference. Route rules list the
// providers usable in a region; lower Priority values are tried first.
package region

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Region defines a deployment region.
type Region struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	Primary  bool   `json:"primary"`
}

// ResidencyPolicy defines data residency requirements.
type ResidencyPolicy struct {
	ID       string `json:"id"`
	Region   string `json:"region"`
	DataType string `json:"data_type"`
	Required bool   `json:"required"`
}

// RouteRule defines routing rules by region.
type RouteRule struct {
	ID        string   `json:"id"`
	Region    string   `json:"region"`
	Providers []string `json:"providers"`
	Priority  int      `json:"priority"`
	Enabled   bool     `json:"enabled"`
}

// Decision is the outcome of residency-aware route resolution.
type Decision struct {
	Region            Region   `json:"region"`
	Providers         []string `json:"providers"`
	Rules             []string `json:"rules,omitempty"`
	ResidencyEnforced bool     `json:"residency_enforced"`
	Rerouted          bool     `json:"rerouted,omitempty"`
	Reason            string   `json:"reason"`
}

// Limits.
const (
	MaxRegions        = 256
	MaxPolicies       = 4096
	MaxRules          = 4096
	MaxProvidersPer   = 64
	maxNameLength     = 128
	maxDataTypeLength = 64
	maxEndpointLength = 2048
	maxBodyBytes      = 1 << 20
)

var (
	// ErrNotFound is returned when a referenced object does not exist.
	ErrNotFound = errors.New("not found")
	// ErrInvalid is returned (wrapped) for validation failures.
	ErrInvalid = errors.New("invalid region configuration")
	// ErrInUse is returned when deleting a region still referenced by policies or rules.
	ErrInUse = errors.New("region is referenced by residency policies or route rules")
	// ErrStoreFull is returned when a collection reached its limit.
	ErrStoreFull = errors.New("region store is full")
	// ErrNoRegions is returned by Resolve when no regions are configured.
	ErrNoRegions = errors.New("no regions configured")
	// ErrNoCompliantRoute is returned by Resolve when residency is required
	// but no allowed region has an enabled route rule.
	ErrNoCompliantRoute = errors.New("no residency-compliant route available")
	// ErrPersistence is returned (wrapped) when the durable store rejects a
	// read or write. The in-memory state is left unchanged.
	ErrPersistence = errors.New("region: persistence failed")

	idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// Store manages regions, residency policies, and route rules. It is
// in-memory by default; EnablePersistence adds write-through durability.
type Store struct {
	mu       sync.RWMutex
	regions  map[string]Region
	policies map[string]ResidencyPolicy
	rules    map[string]RouteRule
	ps       persist.Store // nil unless persistence is enabled
}

// NewStore creates a region store.
func NewStore() *Store {
	return &Store{
		regions:  make(map[string]Region),
		policies: make(map[string]ResidencyPolicy),
		rules:    make(map[string]RouteRule),
	}
}

func invalid(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func checkID(id string) error {
	if !idPattern.MatchString(id) {
		return invalid("id must match %s", idPattern.String())
	}
	return nil
}

func validateRegion(r *Region) error {
	if err := checkID(r.ID); err != nil {
		return err
	}
	r.Name = strings.TrimSpace(r.Name)
	if r.Name == "" {
		r.Name = r.ID
	}
	if len(r.Name) > maxNameLength {
		return invalid("name exceeds %d bytes", maxNameLength)
	}
	if r.Endpoint != "" {
		if len(r.Endpoint) > maxEndpointLength {
			return invalid("endpoint too long")
		}
		u, err := url.Parse(r.Endpoint)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return invalid("endpoint must be an absolute http(s) URL")
		}
		if u.User != nil {
			return invalid("endpoint must not contain credentials")
		}
	}
	return nil
}

// UpsertRegion validates and adds or updates a region. A server-generated ID
// is assigned when empty. Marking a region primary demotes any other primary.
func (s *Store) UpsertRegion(region Region) (Region, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if region.ID == "" {
		region.ID = s.freshIDLocked("rg")
	}
	if err := validateRegion(&region); err != nil {
		return Region{}, err
	}
	if _, exists := s.regions[region.ID]; !exists && len(s.regions) >= MaxRegions {
		return Region{}, ErrStoreFull
	}
	// The upserted region is written first, then any demoted primary.
	changed := []Region{region}
	if region.Primary {
		for id, other := range s.regions {
			if id != region.ID && other.Primary {
				other.Primary = false
				changed = append(changed, other)
			}
		}
	}
	if err := s.persistRegionsLocked(changed); err != nil {
		return Region{}, err
	}
	for _, r := range changed {
		s.regions[r.ID] = r
	}
	return region, nil
}

// GetRegion retrieves a region by id.
func (s *Store) GetRegion(id string) (Region, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.regions[id]
	return r, ok
}

// ListRegions returns all regions sorted by ID.
func (s *Store) ListRegions() []Region {
	s.mu.RLock()
	out := make([]Region, 0, len(s.regions))
	for _, r := range s.regions {
		out = append(out, r)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// DeleteRegion removes a region. Regions still referenced by residency
// policies or route rules cannot be deleted (ErrInUse) so residency
// guarantees are never silently dropped.
func (s *Store) DeleteRegion(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.regions[id]; !ok {
		return ErrNotFound
	}
	for _, p := range s.policies {
		if p.Region == id {
			return ErrInUse
		}
	}
	for _, r := range s.rules {
		if r.Region == id {
			return ErrInUse
		}
	}
	if err := s.deleteLocked(BucketRegions, id); err != nil {
		return err
	}
	delete(s.regions, id)
	return nil
}

// UpsertPolicy validates and adds or updates a residency policy. The region
// must exist. Data types are matched case-insensitively.
func (s *Store) UpsertPolicy(policy ResidencyPolicy) (ResidencyPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if policy.ID == "" {
		policy.ID = s.freshIDLocked("rp")
	}
	if err := validatePolicy(&policy, s.regions); err != nil {
		return ResidencyPolicy{}, err
	}
	if _, exists := s.policies[policy.ID]; !exists && len(s.policies) >= MaxPolicies {
		return ResidencyPolicy{}, ErrStoreFull
	}
	if err := s.putLocked(BucketPolicies, policy.ID, policy); err != nil {
		return ResidencyPolicy{}, err
	}
	s.policies[policy.ID] = policy
	return policy, nil
}

// validatePolicy normalizes and validates p against the known regions.
func validatePolicy(p *ResidencyPolicy, regions map[string]Region) error {
	if err := checkID(p.ID); err != nil {
		return err
	}
	p.DataType = strings.ToLower(strings.TrimSpace(p.DataType))
	if p.DataType == "" || len(p.DataType) > maxDataTypeLength {
		return invalid("data_type is required (max %d bytes)", maxDataTypeLength)
	}
	if _, ok := regions[p.Region]; !ok {
		return invalid("unknown region %q", p.Region)
	}
	return nil
}

// GetPolicy retrieves a residency policy by id.
func (s *Store) GetPolicy(id string) (ResidencyPolicy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.policies[id]
	return p, ok
}

// ListPolicies returns all residency policies sorted by ID.
func (s *Store) ListPolicies() []ResidencyPolicy {
	s.mu.RLock()
	out := make([]ResidencyPolicy, 0, len(s.policies))
	for _, p := range s.policies {
		out = append(out, p)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// DeletePolicy removes a residency policy.
func (s *Store) DeletePolicy(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.policies[id]; !ok {
		return ErrNotFound
	}
	if err := s.deleteLocked(BucketPolicies, id); err != nil {
		return err
	}
	delete(s.policies, id)
	return nil
}

// UpsertRule validates and adds or updates a route rule. The region must
// exist; providers are trimmed and de-duplicated.
func (s *Store) UpsertRule(rule RouteRule) (RouteRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rule.ID == "" {
		rule.ID = s.freshIDLocked("rr")
	}
	if err := validateRule(&rule, s.regions); err != nil {
		return RouteRule{}, err
	}
	if _, exists := s.rules[rule.ID]; !exists && len(s.rules) >= MaxRules {
		return RouteRule{}, ErrStoreFull
	}
	if err := s.putLocked(BucketRules, rule.ID, rule); err != nil {
		return RouteRule{}, err
	}
	s.rules[rule.ID] = rule
	return cloneRule(rule), nil
}

// validateRule normalizes and validates r against the known regions.
func validateRule(r *RouteRule, regions map[string]Region) error {
	if err := checkID(r.ID); err != nil {
		return err
	}
	if _, ok := regions[r.Region]; !ok {
		return invalid("unknown region %q", r.Region)
	}
	if r.Priority < 0 {
		return invalid("priority must be >= 0")
	}
	providers, err := cleanProviders(r.Providers)
	if err != nil {
		return err
	}
	r.Providers = providers
	return nil
}

func cleanProviders(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, invalid("providers must not be empty")
	}
	if len(in) > MaxProvidersPer {
		return nil, invalid("at most %d providers per rule", MaxProvidersPer)
	}
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, p := range in {
		p = strings.TrimSpace(p)
		if p == "" || len(p) > maxNameLength {
			return nil, invalid("provider names must be 1-%d bytes", maxNameLength)
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out, nil
}

func cloneRule(r RouteRule) RouteRule {
	r.Providers = append([]string(nil), r.Providers...)
	return r
}

// GetRule retrieves a route rule by id.
func (s *Store) GetRule(id string) (RouteRule, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rules[id]
	if !ok {
		return RouteRule{}, false
	}
	return cloneRule(r), true
}

// ListRules returns all route rules sorted by region, priority and ID.
func (s *Store) ListRules() []RouteRule {
	s.mu.RLock()
	out := make([]RouteRule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, cloneRule(r))
	}
	s.mu.RUnlock()
	sortRules(out)
	return out
}

func sortRules(rules []RouteRule) {
	sort.Slice(rules, func(i, j int) bool {
		a, b := rules[i], rules[j]
		if a.Region != b.Region {
			return a.Region < b.Region
		}
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		return a.ID < b.ID
	})
}

// DeleteRule removes a route rule.
func (s *Store) DeleteRule(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[id]; !ok {
		return ErrNotFound
	}
	if err := s.deleteLocked(BucketRules, id); err != nil {
		return err
	}
	delete(s.rules, id)
	return nil
}

// Resolve picks a region and provider list for a request carrying data of
// dataType, honoring residency policies. preferredRegion (optional) is used
// when it is compliant; otherwise the request is rerouted to a compliant
// region (Decision.Rerouted). When residency is required and no allowed
// region has an enabled rule, ErrNoCompliantRoute is returned — callers must
// fail closed rather than fall back to an arbitrary provider.
func (s *Store) Resolve(dataType, preferredRegion string) (Decision, error) {
	dataType = strings.ToLower(strings.TrimSpace(dataType))
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.regions) == 0 {
		if dataType != "" && s.hasRequiredLocked(dataType) {
			return Decision{}, ErrNoCompliantRoute
		}
		return Decision{}, ErrNoRegions
	}

	required := map[string]bool{}
	var preferredByPolicy []string
	policies := make([]ResidencyPolicy, 0, len(s.policies))
	for _, p := range s.policies {
		policies = append(policies, p)
	}
	sort.Slice(policies, func(i, j int) bool { return policies[i].ID < policies[j].ID })
	for _, p := range policies {
		if dataType == "" || p.DataType != dataType {
			continue
		}
		if _, ok := s.regions[p.Region]; !ok {
			continue
		}
		if p.Required {
			required[p.Region] = true
		} else {
			preferredByPolicy = append(preferredByPolicy, p.Region)
		}
	}
	enforced := len(required) > 0

	allowed := func(id string) bool {
		if _, ok := s.regions[id]; !ok {
			return false
		}
		return !enforced || required[id]
	}

	// Candidate order: caller preference, policy preferences, primary, rest by ID.
	var order []string
	seen := map[string]bool{}
	add := func(id string) {
		if id != "" && !seen[id] && allowed(id) {
			seen[id] = true
			order = append(order, id)
		}
	}
	add(preferredRegion)
	for _, id := range preferredByPolicy {
		add(id)
	}
	ids := make([]string, 0, len(s.regions))
	for id, r := range s.regions {
		if r.Primary {
			add(id)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		add(id)
	}
	if len(order) == 0 {
		return Decision{}, ErrNoCompliantRoute
	}

	for _, id := range order {
		rules := s.enabledRulesLocked(id)
		if len(rules) == 0 {
			continue
		}
		d := Decision{
			Region:            s.regions[id],
			ResidencyEnforced: enforced,
			Rerouted:          preferredRegion != "" && id != preferredRegion,
		}
		seenProv := map[string]bool{}
		for _, r := range rules {
			d.Rules = append(d.Rules, r.ID)
			for _, p := range r.Providers {
				if !seenProv[p] {
					seenProv[p] = true
					d.Providers = append(d.Providers, p)
				}
			}
		}
		switch {
		case enforced:
			d.Reason = fmt.Sprintf("data type %q is pinned by residency policy", dataType)
		case id == preferredRegion:
			d.Reason = "preferred region"
		default:
			d.Reason = "first region with enabled route rules"
		}
		return d, nil
	}
	if enforced {
		return Decision{}, ErrNoCompliantRoute
	}
	return Decision{
		Region:   s.regions[order[0]],
		Rerouted: preferredRegion != "" && order[0] != preferredRegion,
		Reason:   "no enabled route rules; use default providers",
	}, nil
}

func (s *Store) hasRequiredLocked(dataType string) bool {
	for _, p := range s.policies {
		if p.Required && p.DataType == dataType {
			return true
		}
	}
	return false
}

func (s *Store) enabledRulesLocked(regionID string) []RouteRule {
	var out []RouteRule
	for _, r := range s.rules {
		if r.Enabled && r.Region == regionID {
			out = append(out, r)
		}
	}
	sortRules(out)
	return out
}

func (s *Store) freshIDLocked(prefix string) string {
	for {
		id := newID(prefix)
		_, a := s.regions[id]
		_, b := s.policies[id]
		_, c := s.rules[id]
		if !a && !b && !c {
			return id
		}
	}
}

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("region: crypto/rand failed: " + err.Error())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

// collection abstracts the three REST collections served by WebhookHandler.
type collection struct {
	list   func() interface{}
	get    func(id string) (interface{}, bool)
	upsert func(existing []byte, body []byte, id string) (interface{}, error)
	delete func(id string) error
}

func (s *Store) collections() map[string]collection {
	return map[string]collection{
		"regions": {
			list: func() interface{} { return s.ListRegions() },
			get: func(id string) (interface{}, bool) {
				r, ok := s.GetRegion(id)
				return r, ok
			},
			upsert: func(existing, body []byte, id string) (interface{}, error) {
				var r Region
				if err := mergeJSON(&r, existing, body); err != nil {
					return nil, err
				}
				if id != "" {
					if r.ID != "" && r.ID != id {
						return nil, invalid("id in body does not match id in path")
					}
					r.ID = id
				}
				return s.UpsertRegion(r)
			},
			delete: s.DeleteRegion,
		},
		"residency": {
			list: func() interface{} { return s.ListPolicies() },
			get: func(id string) (interface{}, bool) {
				p, ok := s.GetPolicy(id)
				return p, ok
			},
			upsert: func(existing, body []byte, id string) (interface{}, error) {
				var p ResidencyPolicy
				if err := mergeJSON(&p, existing, body); err != nil {
					return nil, err
				}
				if id != "" {
					if p.ID != "" && p.ID != id {
						return nil, invalid("id in body does not match id in path")
					}
					p.ID = id
				}
				return s.UpsertPolicy(p)
			},
			delete: s.DeletePolicy,
		},
		"routes": {
			list: func() interface{} { return s.ListRules() },
			get: func(id string) (interface{}, bool) {
				r, ok := s.GetRule(id)
				return r, ok
			},
			upsert: func(existing, body []byte, id string) (interface{}, error) {
				var r RouteRule
				if err := mergeJSON(&r, existing, body); err != nil {
					return nil, err
				}
				if id != "" {
					if r.ID != "" && r.ID != id {
						return nil, invalid("id in body does not match id in path")
					}
					r.ID = id
				}
				return s.UpsertRule(r)
			},
			delete: s.DeleteRule,
		},
	}
}

// mergeJSON decodes existing (if any) and then body onto dst, so PATCH
// requests only change the fields they mention.
func mergeJSON(dst interface{}, existing, body []byte) error {
	if len(existing) > 0 {
		if err := json.Unmarshal(existing, dst); err != nil {
			return err
		}
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(dst); err != nil {
		return invalid("invalid JSON body")
	}
	if dec.More() {
		return invalid("unexpected data after JSON body")
	}
	return nil
}

// WebhookHandler exposes JSON CRUD for regions, policies, and rules under
// /v1/region/{regions|residency|routes}[/{id}] (ids may also be passed as
// ?id=). POST creates or replaces (200); PUT replaces and PATCH merges into an
// existing object (404 when missing); DELETE returns 204 (409 when a region is
// still referenced). GET /v1/region/routes?resolve=true&data_type=pii&region=eu
// returns the residency-aware routing Decision.
func WebhookHandler(store *Store) http.HandlerFunc {
	cols := store.collections()
	return func(w http.ResponseWriter, r *http.Request) {
		rest, ok := strings.CutPrefix(r.URL.Path, "/v1/region/")
		if !ok {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		parts := strings.Split(strings.Trim(rest, "/"), "/")
		col, known := cols[parts[0]]
		if !known || len(parts) > 2 {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		id := ""
		if len(parts) == 2 {
			id = parts[1]
		}
		if id == "" {
			id = r.URL.Query().Get("id")
		}

		switch r.Method {
		case http.MethodGet, http.MethodHead:
			if parts[0] == "routes" && r.URL.Query().Get("resolve") == "true" {
				q := r.URL.Query()
				d, err := store.Resolve(q.Get("data_type"), q.Get("region"))
				if err != nil {
					writeStoreError(w, err)
					return
				}
				writeJSON(w, http.StatusOK, d)
				return
			}
			if id == "" {
				writeJSON(w, http.StatusOK, col.list())
				return
			}
			v, found := col.get(id)
			if !found {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			writeJSON(w, http.StatusOK, v)
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			body, ok := readBody(w, r)
			if !ok {
				return
			}
			var existing []byte
			if r.Method != http.MethodPost {
				if id == "" {
					writeError(w, http.StatusBadRequest, "missing id")
					return
				}
				cur, found := col.get(id)
				if !found {
					writeError(w, http.StatusNotFound, "not found")
					return
				}
				if r.Method == http.MethodPatch {
					existing, _ = json.Marshal(cur)
				}
			}
			v, err := col.upsert(existing, body, id)
			if err != nil {
				writeStoreError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, v)
		case http.MethodDelete:
			if id == "" {
				writeError(w, http.StatusBadRequest, "missing id")
				return
			}
			if err := col.delete(id); err != nil {
				writeStoreError(w, err)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			methodNotAllowed(w, "GET, HEAD, POST, PUT, PATCH, DELETE")
		}
	}
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, "missing body")
		return nil, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeError(w, http.StatusBadRequest, "bad request")
		}
		return nil, false
	}
	return body, true
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrInUse):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrStoreFull):
		writeError(w, http.StatusInsufficientStorage, err.Error())
	case errors.Is(err, ErrNoRegions), errors.Is(err, ErrNoCompliantRoute):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrPersistence):
		writeError(w, http.StatusInternalServerError, "persistence failure")
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
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
