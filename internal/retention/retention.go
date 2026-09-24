// Package retention stores data-retention policies and enforces them.
//
// A policy applies to a named resource and bounds it by age (TTL) and/or
// size (MaxItems). Enforcement happens against Targets registered per
// resource: Sweep (or the periodic Run loop) deletes items older than the
// TTL and then trims the oldest items beyond MaxItems. When several policies
// name the same resource the strictest bounds win. Resources without a
// registered target are reported as unenforced rather than silently skipped.
//
// JSON: "ttl" is expressed in hours (a number, fractional allowed) for
// backward compatibility with existing clients; a Go duration string such as
// "36h" or "90m" (or "7d") is also accepted, as is "ttl_duration".
// Responses include both "ttl" (hours) and "ttl_duration".
package retention

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// RetentionPolicy defines data retention rules.
type RetentionPolicy struct {
	ID        string        `json:"id"`
	Resource  string        `json:"resource"`
	TTL       time.Duration `json:"ttl"`
	MaxItems  int           `json:"max_items"`
	CreatedAt time.Time     `json:"created_at"`
}

// Limits.
const (
	MaxTTL          = 10 * 365 * 24 * time.Hour
	MaxItemsLimit   = 1_000_000_000
	MaxPolicies     = 1000
	maxBodyBytes    = 1 << 20
	minSweepSpacing = time.Second
)

var (
	// ErrInvalid is returned (wrapped) for invalid policies.
	ErrInvalid = errors.New("invalid retention policy")
	// ErrNotFound is returned when a policy does not exist.
	ErrNotFound = errors.New("retention policy not found")
	// ErrStoreFull is returned when MaxPolicies is reached.
	ErrStoreFull = errors.New("retention store is full")
	// ErrPersistence is returned (wrapped) when the durable store rejects a
	// read or write. The in-memory state is left unchanged.
	ErrPersistence = errors.New("retention: persistence failed")

	idPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	resourcePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
)

type policyJSON struct {
	ID          string    `json:"id"`
	Resource    string    `json:"resource"`
	TTL         float64   `json:"ttl"`
	TTLDuration string    `json:"ttl_duration"`
	MaxItems    int       `json:"max_items"`
	CreatedAt   time.Time `json:"created_at"`
}

// MarshalJSON renders TTL as hours ("ttl") plus a duration string ("ttl_duration").
func (p RetentionPolicy) MarshalJSON() ([]byte, error) {
	return json.Marshal(policyJSON{
		ID:          p.ID,
		Resource:    p.Resource,
		TTL:         p.TTL.Hours(),
		TTLDuration: p.TTL.String(),
		MaxItems:    p.MaxItems,
		CreatedAt:   p.CreatedAt,
	})
}

// UnmarshalJSON accepts "ttl" as hours (number) or a duration string, and
// "ttl_duration" as a duration string. Only fields present in the input are
// overwritten, which lets PATCH merge into an existing policy.
func (p *RetentionPolicy) UnmarshalJSON(data []byte) error {
	var raw struct {
		ID          *string          `json:"id"`
		Resource    *string          `json:"resource"`
		TTL         *json.RawMessage `json:"ttl"`
		TTLDuration *string          `json:"ttl_duration"`
		MaxItems    *int             `json:"max_items"`
		CreatedAt   *time.Time       `json:"created_at"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.ID != nil {
		p.ID = *raw.ID
	}
	if raw.Resource != nil {
		p.Resource = *raw.Resource
	}
	if raw.MaxItems != nil {
		p.MaxItems = *raw.MaxItems
	}
	if raw.CreatedAt != nil {
		p.CreatedAt = *raw.CreatedAt
	}
	switch {
	case raw.TTLDuration != nil:
		d, err := ParseTTL(*raw.TTLDuration)
		if err != nil {
			return err
		}
		p.TTL = d
	case raw.TTL != nil:
		d, err := parseTTLValue(*raw.TTL)
		if err != nil {
			return err
		}
		p.TTL = d
	}
	return nil
}

func parseTTLValue(raw json.RawMessage) (time.Duration, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return 0, err
		}
		return ParseTTL(s)
	}
	if bytes.Equal(raw, []byte("null")) {
		return 0, nil
	}
	var hours float64
	if err := json.Unmarshal(raw, &hours); err != nil {
		return 0, fmt.Errorf("ttl must be a number of hours or a duration string")
	}
	return hoursToDuration(hours)
}

func hoursToDuration(hours float64) (time.Duration, error) {
	if math.IsNaN(hours) || math.IsInf(hours, 0) || hours < 0 || hours > MaxTTL.Hours() {
		return 0, fmt.Errorf("ttl must be between 0 and %.0f hours", MaxTTL.Hours())
	}
	return time.Duration(hours * float64(time.Hour)), nil
}

// ParseTTL parses a Go duration ("36h", "90m") or a day count ("7d"), or a
// bare number interpreted as hours.
func ParseTTL(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid ttl %q", s)
		}
		return hoursToDuration(n * 24)
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		return hoursToDuration(n)
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid ttl %q", s)
	}
	if d < 0 || d > MaxTTL {
		return 0, fmt.Errorf("ttl must be between 0 and %s", MaxTTL)
	}
	return d, nil
}

// Validate checks a policy.
func (p *RetentionPolicy) Validate() error {
	var errs []error
	if !idPattern.MatchString(p.ID) {
		errs = append(errs, fmt.Errorf("id must match %s", idPattern.String()))
	}
	if !resourcePattern.MatchString(p.Resource) {
		errs = append(errs, fmt.Errorf("resource is required and must match %s", resourcePattern.String()))
	}
	if p.TTL < 0 || p.TTL > MaxTTL {
		errs = append(errs, fmt.Errorf("ttl must be between 0 and %s", MaxTTL))
	}
	if p.MaxItems < 0 || p.MaxItems > MaxItemsLimit {
		errs = append(errs, fmt.Errorf("max_items must be between 0 and %d", MaxItemsLimit))
	}
	if p.TTL == 0 && p.MaxItems == 0 {
		errs = append(errs, errors.New("at least one of ttl or max_items must be positive"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalid, errors.Join(errs...))
	}
	return nil
}

// Item is a retained record as seen by the enforcer.
type Item struct {
	Key       string
	CreatedAt time.Time
}

// Target is a data set a policy can be enforced against.
type Target interface {
	// Items lists the current items.
	Items(ctx context.Context) ([]Item, error)
	// Delete removes the given keys.
	Delete(ctx context.Context, keys []string) error
}

// TargetFuncs adapts two functions to the Target interface.
type TargetFuncs struct {
	ItemsFunc  func(ctx context.Context) ([]Item, error)
	DeleteFunc func(ctx context.Context, keys []string) error
}

// Items implements Target.
func (t TargetFuncs) Items(ctx context.Context) ([]Item, error) {
	if t.ItemsFunc == nil {
		return nil, errors.New("retention: ItemsFunc is nil")
	}
	return t.ItemsFunc(ctx)
}

// Delete implements Target.
func (t TargetFuncs) Delete(ctx context.Context, keys []string) error {
	if t.DeleteFunc == nil {
		return errors.New("retention: DeleteFunc is nil")
	}
	return t.DeleteFunc(ctx, keys)
}

// MemoryTarget is a thread-safe in-memory Target, useful for simple
// in-process collections and tests.
type MemoryTarget struct {
	mu    sync.Mutex
	items map[string]time.Time
}

// NewMemoryTarget creates an empty MemoryTarget.
func NewMemoryTarget() *MemoryTarget {
	return &MemoryTarget{items: make(map[string]time.Time)}
}

// Put records an item.
func (m *MemoryTarget) Put(key string, createdAt time.Time) {
	m.mu.Lock()
	m.items[key] = createdAt
	m.mu.Unlock()
}

// Has reports whether key is present.
func (m *MemoryTarget) Has(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.items[key]
	return ok
}

// Len returns the number of items.
func (m *MemoryTarget) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.items)
}

// Items implements Target.
func (m *MemoryTarget) Items(context.Context) ([]Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Item, 0, len(m.items))
	for k, t := range m.items {
		out = append(out, Item{Key: k, CreatedAt: t})
	}
	return out, nil
}

// Delete implements Target.
func (m *MemoryTarget) Delete(_ context.Context, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.items, k)
	}
	return nil
}

// RetentionStore manages retention policies and their enforcement. It is
// in-memory by default; EnablePersistence adds write-through durability for
// policies (targets are runtime wiring and are never persisted).
type RetentionStore struct {
	mu       sync.RWMutex
	policies map[string]RetentionPolicy
	targets  map[string]Target
	now      func() time.Time
	ps       persist.Store // nil unless persistence is enabled

	sweepMu sync.Mutex
}

// NewRetentionStore creates a retention store.
func NewRetentionStore() *RetentionStore {
	return &RetentionStore{
		policies: make(map[string]RetentionPolicy),
		targets:  make(map[string]Target),
		now:      time.Now,
	}
}

// Upsert validates and adds or updates a policy, generating an ID when empty
// and preserving CreatedAt of an existing policy. It returns the stored copy.
func (s *RetentionStore) Upsert(policy RetentionPolicy) (RetentionPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if policy.ID == "" {
		for {
			policy.ID = newID("ret")
			if _, taken := s.policies[policy.ID]; !taken {
				break
			}
		}
	}
	if err := policy.Validate(); err != nil {
		return RetentionPolicy{}, err
	}
	existing, exists := s.policies[policy.ID]
	if !exists && len(s.policies) >= MaxPolicies {
		return RetentionPolicy{}, ErrStoreFull
	}
	if exists {
		policy.CreatedAt = existing.CreatedAt
	} else {
		policy.CreatedAt = s.now()
	}
	if err := s.putLocked(policy); err != nil {
		return RetentionPolicy{}, err
	}
	s.policies[policy.ID] = policy
	return policy, nil
}

// Get retrieves a policy by id.
func (s *RetentionStore) Get(id string) (RetentionPolicy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.policies[id]
	return p, ok
}

// List returns all policies sorted by ID.
func (s *RetentionStore) List() []RetentionPolicy {
	s.mu.RLock()
	out := make([]RetentionPolicy, 0, len(s.policies))
	for _, p := range s.policies {
		out = append(out, p)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Delete removes a policy and reports whether it was removed. When
// persistence is enabled and the durable delete fails, the policy is kept,
// the failure is logged and false is returned; use Remove to observe the
// error.
func (s *RetentionStore) Delete(id string) bool {
	err := s.Remove(id)
	if err != nil && !errors.Is(err, ErrNotFound) {
		slog.Error("retention: delete policy failed", "id", id, "error", err)
	}
	return err == nil
}

// Remove deletes a policy. It returns ErrNotFound when the policy does not
// exist and an error wrapping ErrPersistence (leaving the policy in place)
// when the durable delete fails.
func (s *RetentionStore) Remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.policies[id]; !ok {
		return ErrNotFound
	}
	if s.ps != nil {
		if err := s.ps.Delete(BucketPolicies, id); err != nil {
			return fmt.Errorf("%w: delete %s/%s: %w", ErrPersistence, BucketPolicies, id, err)
		}
	}
	delete(s.policies, id)
	return nil
}

// RegisterTarget attaches the data set that policies for resource are
// enforced against. A nil target unregisters.
func (s *RetentionStore) RegisterTarget(resource string, t Target) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t == nil {
		delete(s.targets, resource)
		return
	}
	s.targets[resource] = t
}

// Effective returns the strictest bounds across all policies for resource
// (smallest positive TTL and MaxItems). ok is false when no policy applies.
func (s *RetentionStore) Effective(resource string) (ttl time.Duration, maxItems int, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.effectiveLocked(resource)
}

func (s *RetentionStore) effectiveLocked(resource string) (ttl time.Duration, maxItems int, ok bool) {
	for _, p := range s.policies {
		if p.Resource != resource {
			continue
		}
		ok = true
		if p.TTL > 0 && (ttl == 0 || p.TTL < ttl) {
			ttl = p.TTL
		}
		if p.MaxItems > 0 && (maxItems == 0 || p.MaxItems < maxItems) {
			maxItems = p.MaxItems
		}
	}
	return ttl, maxItems, ok
}

// Plan returns the keys that must be deleted from items to satisfy the
// policies for resource at time now: first everything older than the TTL,
// then the oldest items beyond MaxItems. Items with an unknown (zero)
// CreatedAt never expire by TTL but are trimmed first by MaxItems.
func (s *RetentionStore) Plan(resource string, items []Item, now time.Time) (expired, trimmed []string) {
	ttl, maxItems, ok := s.Effective(resource)
	if !ok {
		return nil, nil
	}
	return plan(items, ttl, maxItems, now)
}

func plan(items []Item, ttl time.Duration, maxItems int, now time.Time) (expired, trimmed []string) {
	kept := make([]Item, 0, len(items))
	cutoff := now.Add(-ttl)
	for _, it := range items {
		if ttl > 0 && !it.CreatedAt.IsZero() && it.CreatedAt.Before(cutoff) {
			expired = append(expired, it.Key)
			continue
		}
		kept = append(kept, it)
	}
	if maxItems > 0 && len(kept) > maxItems {
		sort.Slice(kept, func(i, j int) bool {
			if !kept[i].CreatedAt.Equal(kept[j].CreatedAt) {
				return kept[i].CreatedAt.Before(kept[j].CreatedAt)
			}
			return kept[i].Key < kept[j].Key
		})
		for _, it := range kept[:len(kept)-maxItems] {
			trimmed = append(trimmed, it.Key)
		}
	}
	return expired, trimmed
}

// ResourceSweep reports the enforcement outcome for one resource.
type ResourceSweep struct {
	Resource   string `json:"resource"`
	Enforced   bool   `json:"enforced"`
	Scanned    int    `json:"scanned"`
	Expired    int    `json:"expired"`
	Trimmed    int    `json:"trimmed"`
	Error      string `json:"error,omitempty"`
	MaxItems   int    `json:"max_items,omitempty"`
	TTLSeconds int64  `json:"ttl_seconds,omitempty"`
}

// SweepReport summarizes one enforcement pass.
type SweepReport struct {
	StartedAt time.Time       `json:"started_at"`
	Resources []ResourceSweep `json:"resources"`
}

// Sweep enforces every policy once against its registered target. Sweeps do
// not overlap. The returned error joins per-resource failures.
func (s *RetentionStore) Sweep(ctx context.Context) (SweepReport, error) {
	s.sweepMu.Lock()
	defer s.sweepMu.Unlock()

	now := s.now()
	type job struct {
		resource string
		ttl      time.Duration
		max      int
		target   Target
	}
	s.mu.RLock()
	seen := map[string]bool{}
	var jobs []job
	for _, p := range s.policies {
		if seen[p.Resource] {
			continue
		}
		seen[p.Resource] = true
		ttl, max, _ := s.effectiveLocked(p.Resource)
		jobs = append(jobs, job{p.Resource, ttl, max, s.targets[p.Resource]})
	}
	s.mu.RUnlock()
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].resource < jobs[j].resource })

	report := SweepReport{StartedAt: now}
	var errs []error
	for _, j := range jobs {
		rs := ResourceSweep{Resource: j.resource, MaxItems: j.max, TTLSeconds: int64(j.ttl / time.Second)}
		if err := ctx.Err(); err != nil {
			rs.Error = err.Error()
			report.Resources = append(report.Resources, rs)
			errs = append(errs, err)
			continue
		}
		if j.target == nil {
			rs.Error = "no target registered for resource; policy not enforced"
			report.Resources = append(report.Resources, rs)
			continue
		}
		rs.Enforced = true
		items, err := j.target.Items(ctx)
		if err != nil {
			rs.Error = err.Error()
			errs = append(errs, fmt.Errorf("retention %s: list: %w", j.resource, err))
			report.Resources = append(report.Resources, rs)
			continue
		}
		rs.Scanned = len(items)
		expired, trimmed := plan(items, j.ttl, j.max, now)
		if doomed := append(expired, trimmed...); len(doomed) > 0 {
			if err := j.target.Delete(ctx, doomed); err != nil {
				rs.Error = err.Error()
				errs = append(errs, fmt.Errorf("retention %s: delete: %w", j.resource, err))
				report.Resources = append(report.Resources, rs)
				continue
			}
		}
		rs.Expired, rs.Trimmed = len(expired), len(trimmed)
		report.Resources = append(report.Resources, rs)
	}
	return report, errors.Join(errs...)
}

// Run sweeps every interval until ctx is cancelled. onSweep (optional)
// receives each report and error.
func (s *RetentionStore) Run(ctx context.Context, interval time.Duration, onSweep func(SweepReport, error)) {
	if interval < minSweepSpacing {
		interval = minSweepSpacing
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rep, err := s.Sweep(ctx)
			if onSweep != nil {
				onSweep(rep, err)
			}
		}
	}
}

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("retention: crypto/rand failed: " + err.Error())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

// WebhookHandler exposes JSON CRUD for retention policies.
//
//	GET    /v1/retention[/{id}] | ?id=   list / get
//	POST   /v1/retention                 create or replace (200)
//	POST   /v1/retention?sweep=true      run an enforcement sweep now
//	PUT    /v1/retention/{id} | ?id=     replace existing (404 when missing)
//	PATCH  /v1/retention/{id} | ?id=     merge into existing
//	DELETE /v1/retention/{id} | ?id=     delete (204)
func WebhookHandler(store *RetentionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest, ok := strings.CutPrefix(r.URL.Path, "/v1/retention")
		if !ok || (rest != "" && !strings.HasPrefix(rest, "/")) || strings.Contains(strings.Trim(rest, "/"), "/") {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		id := strings.Trim(rest, "/")
		if id == "" {
			id = r.URL.Query().Get("id")
		}
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			if id == "" {
				writeJSON(w, http.StatusOK, store.List())
				return
			}
			p, found := store.Get(id)
			if !found {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			writeJSON(w, http.StatusOK, p)
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			if r.Method == http.MethodPost && r.URL.Query().Get("sweep") == "true" {
				rep, err := store.Sweep(r.Context())
				if err != nil {
					writeJSON(w, http.StatusOK, struct {
						SweepReport
						Error string `json:"error"`
					}{rep, "one or more resources failed; see per-resource errors"})
					return
				}
				writeJSON(w, http.StatusOK, rep)
				return
			}
			body, ok := readBody(w, r)
			if !ok {
				return
			}
			var p RetentionPolicy
			if r.Method != http.MethodPost {
				if id == "" {
					writeError(w, http.StatusBadRequest, "missing id")
					return
				}
				existing, found := store.Get(id)
				if !found {
					writeError(w, http.StatusNotFound, "not found")
					return
				}
				if r.Method == http.MethodPatch {
					p = existing
				}
			}
			if err := decodeStrict(body, &p); err != nil {
				writeError(w, http.StatusBadRequest, "bad request: "+err.Error())
				return
			}
			if r.Method != http.MethodPost {
				if p.ID != "" && p.ID != id {
					writeError(w, http.StatusBadRequest, "id in body does not match id in path")
					return
				}
				p.ID = id
			}
			stored, err := store.Upsert(p)
			if err != nil {
				switch {
				case errors.Is(err, ErrInvalid):
					writeError(w, http.StatusBadRequest, err.Error())
				case errors.Is(err, ErrStoreFull):
					writeError(w, http.StatusInsufficientStorage, err.Error())
				case errors.Is(err, ErrPersistence):
					writeError(w, http.StatusInternalServerError, "persistence failure")
				default:
					writeError(w, http.StatusInternalServerError, "internal error")
				}
				return
			}
			writeJSON(w, http.StatusOK, stored)
		case http.MethodDelete:
			if id == "" {
				writeError(w, http.StatusBadRequest, "missing id")
				return
			}
			if err := store.Remove(id); err != nil {
				switch {
				case errors.Is(err, ErrNotFound):
					writeError(w, http.StatusNotFound, "not found")
				case errors.Is(err, ErrPersistence):
					writeError(w, http.StatusInternalServerError, "persistence failure")
				default:
					writeError(w, http.StatusInternalServerError, "internal error")
				}
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			methodNotAllowed(w, "GET, HEAD, POST, PUT, PATCH, DELETE")
		}
	}
}

func decodeStrict(body []byte, dst interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(dst); err != nil {
		var syn *json.SyntaxError
		var typ *json.UnmarshalTypeError
		switch {
		case errors.Is(err, io.EOF):
			return errors.New("empty body")
		case errors.As(err, &syn), errors.As(err, &typ), errors.Is(err, io.ErrUnexpectedEOF):
			return errors.New("invalid JSON")
		default:
			return err // validation error from RetentionPolicy.UnmarshalJSON
		}
	}
	if dec.More() {
		return errors.New("unexpected data after JSON body")
	}
	return nil
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
