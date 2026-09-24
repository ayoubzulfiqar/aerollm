// Package incident tracks operational incidents with an enforced lifecycle
// state machine and exposes a JSON HTTP API.
//
// Lifecycle:
//
//	open -> acknowledged | investigating | resolved | closed
//	acknowledged -> investigating | resolved | closed
//	investigating -> resolved | closed
//	resolved -> closed | open (reopen)
//	closed (terminal)
//
// Transitions to the current status are idempotent no-ops. Timestamps
// (acknowledged_at, resolved_at, closed_at, updated_at) are server managed.
package incident

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Severity levels for incidents.
type Severity string

const (
	SeverityLow      Severity = "low"
	SeverityMedium   Severity = "medium"
	SeverityHigh     Severity = "high"
	SeverityCritical Severity = "critical"
)

// Status tracks incident lifecycle.
type Status string

const (
	StatusOpen          Status = "open"
	StatusAcknowledged  Status = "acknowledged"
	StatusInvestigating Status = "investigating"
	StatusResolved      Status = "resolved"
	StatusClosed        Status = "closed"
)

// Incident represents an operational incident.
type Incident struct {
	ID             string    `json:"id"`
	Title          string    `json:"title"`
	Description    string    `json:"description"`
	Severity       Severity  `json:"severity"`
	Status         Status    `json:"status"`
	Source         string    `json:"source"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	AcknowledgedAt time.Time `json:"acknowledged_at,omitzero"`
	ResolvedAt     time.Time `json:"resolved_at,omitzero"`
	ClosedAt       time.Time `json:"closed_at,omitzero"`
}

// Limits.
const (
	DefaultMaxIncidents = 10000
	MaxTitleLength      = 256
	MaxDescription      = 8192
	MaxSourceLength     = 128
	maxBodyBytes        = 1 << 20
)

var (
	// ErrNotFound is returned when an incident does not exist.
	ErrNotFound = errors.New("incident not found")
	// ErrInvalid is returned (wrapped) when an incident fails validation.
	ErrInvalid = errors.New("invalid incident")
	// ErrInvalidTransition is returned (wrapped) for disallowed status changes.
	ErrInvalidTransition = errors.New("invalid status transition")
	// ErrConflict is returned when creating an incident whose ID already exists.
	ErrConflict = errors.New("incident already exists")
	// ErrStoreFull is returned when the store is full and nothing can be evicted.
	ErrStoreFull = errors.New("incident store is full")

	idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

var transitions = map[Status][]Status{
	StatusOpen:          {StatusAcknowledged, StatusInvestigating, StatusResolved, StatusClosed},
	StatusAcknowledged:  {StatusInvestigating, StatusResolved, StatusClosed},
	StatusInvestigating: {StatusResolved, StatusClosed},
	StatusResolved:      {StatusClosed, StatusOpen},
	StatusClosed:        {},
}

// ValidSeverity reports whether s is a known severity.
func ValidSeverity(s Severity) bool {
	switch s {
	case SeverityLow, SeverityMedium, SeverityHigh, SeverityCritical:
		return true
	}
	return false
}

// ValidStatus reports whether s is a known status.
func ValidStatus(s Status) bool {
	_, ok := transitions[s]
	return ok
}

// CanTransition reports whether an incident may move from one status to another.
func CanTransition(from, to Status) bool {
	if !ValidStatus(from) || !ValidStatus(to) {
		return false
	}
	if from == to {
		return true
	}
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Event describes a change to an incident, delivered to the store's hook.
type Event struct {
	Type     string   `json:"type"` // created | updated | transitioned | deleted
	From     Status   `json:"from,omitempty"`
	Incident Incident `json:"incident"`
}

// Store manages incidents in memory.
type Store struct {
	mu        sync.RWMutex
	incidents map[string]Incident
	max       int
	now       func() time.Time
	hook      func(Event)
}

// NewStore creates an incident store holding at most DefaultMaxIncidents.
func NewStore() *Store {
	return NewStoreWithLimit(DefaultMaxIncidents)
}

// NewStoreWithLimit creates an incident store holding at most max incidents
// (<= 0 means DefaultMaxIncidents). When full, the oldest closed or resolved
// incident is evicted; if none exist, creation fails with ErrStoreFull.
func NewStoreWithLimit(max int) *Store {
	if max <= 0 {
		max = DefaultMaxIncidents
	}
	return &Store{incidents: make(map[string]Incident), max: max, now: time.Now}
}

// SetHook registers fn to be called (synchronously, outside the store lock)
// after every change. fn must not block; spawn a goroutine for slow work such
// as sending notifications. Pass nil to remove the hook.
func (s *Store) SetHook(fn func(Event)) {
	s.mu.Lock()
	s.hook = fn
	s.mu.Unlock()
}

func (s *Store) emit(e Event) {
	s.mu.RLock()
	hook := s.hook
	s.mu.RUnlock()
	if hook != nil {
		hook(e)
	}
}

func normalize(inc *Incident) error {
	var errs []error
	inc.Title = strings.TrimSpace(inc.Title)
	if inc.Title == "" {
		errs = append(errs, errors.New("title is required"))
	} else if len(inc.Title) > MaxTitleLength {
		errs = append(errs, fmt.Errorf("title exceeds %d bytes", MaxTitleLength))
	}
	if len(inc.Description) > MaxDescription {
		errs = append(errs, fmt.Errorf("description exceeds %d bytes", MaxDescription))
	}
	if len(inc.Source) > MaxSourceLength {
		errs = append(errs, fmt.Errorf("source exceeds %d bytes", MaxSourceLength))
	}
	if inc.Severity == "" {
		inc.Severity = SeverityMedium
	}
	if !ValidSeverity(inc.Severity) {
		errs = append(errs, fmt.Errorf("unknown severity %q (want low|medium|high|critical)", inc.Severity))
	}
	if inc.Status != "" && !ValidStatus(inc.Status) {
		errs = append(errs, fmt.Errorf("unknown status %q (want open|acknowledged|investigating|resolved|closed)", inc.Status))
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalid, errors.Join(errs...))
	}
	return nil
}

// stampStatus sets lifecycle timestamps for entering status to at time t.
func stampStatus(inc *Incident, to Status, t time.Time) {
	switch to {
	case StatusAcknowledged, StatusInvestigating:
		if inc.AcknowledgedAt.IsZero() {
			inc.AcknowledgedAt = t
		}
	case StatusResolved:
		inc.ResolvedAt = t
	case StatusClosed:
		if inc.ResolvedAt.IsZero() {
			inc.ResolvedAt = t
		}
		inc.ClosedAt = t
	case StatusOpen:
		// Reopened: clear resolution timestamps.
		inc.ResolvedAt = time.Time{}
		inc.ClosedAt = time.Time{}
	}
	inc.Status = to
}

// Create validates and stores a new incident, returning the stored copy
// (with server-generated ID and timestamps).
func (s *Store) Create(incident Incident) (Incident, error) {
	if err := normalize(&incident); err != nil {
		return Incident{}, err
	}
	if incident.ID != "" && !idPattern.MatchString(incident.ID) {
		return Incident{}, fmt.Errorf("%w: id must match %s", ErrInvalid, idPattern.String())
	}
	status := incident.Status
	if status == "" {
		status = StatusOpen
	}
	s.mu.Lock()
	now := s.now()
	if incident.ID == "" {
		for {
			incident.ID = newID("inc")
			if _, taken := s.incidents[incident.ID]; !taken {
				break
			}
		}
	} else if _, exists := s.incidents[incident.ID]; exists {
		s.mu.Unlock()
		return Incident{}, ErrConflict
	}
	if len(s.incidents) >= s.max && !s.evictLocked() {
		s.mu.Unlock()
		return Incident{}, ErrStoreFull
	}
	incident.CreatedAt = now
	incident.UpdatedAt = now
	incident.AcknowledgedAt, incident.ResolvedAt, incident.ClosedAt = time.Time{}, time.Time{}, time.Time{}
	stampStatus(&incident, status, now)
	s.incidents[incident.ID] = incident
	s.mu.Unlock()
	s.emit(Event{Type: "created", Incident: incident})
	return incident, nil
}

// evictLocked removes the least recently updated closed/resolved incident.
func (s *Store) evictLocked() bool {
	var victim string
	var oldest time.Time
	for id, inc := range s.incidents {
		if inc.Status != StatusClosed && inc.Status != StatusResolved {
			continue
		}
		if victim == "" || inc.UpdatedAt.Before(oldest) {
			victim, oldest = id, inc.UpdatedAt
		}
	}
	if victim == "" {
		return false
	}
	delete(s.incidents, victim)
	return true
}

// Get retrieves an incident by id.
func (s *Store) Get(id string) (Incident, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inc, ok := s.incidents[id]
	return inc, ok
}

// List returns all incidents ordered by creation time (oldest first).
func (s *Store) List() []Incident {
	return s.Filter("", "")
}

// Filter returns incidents matching the given status and severity (empty
// values match everything), ordered by creation time.
func (s *Store) Filter(status Status, severity Severity) []Incident {
	s.mu.RLock()
	out := make([]Incident, 0, len(s.incidents))
	for _, inc := range s.incidents {
		if status != "" && inc.Status != status {
			continue
		}
		if severity != "" && inc.Severity != severity {
			continue
		}
		out = append(out, inc)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Update replaces the mutable fields (title, description, severity, source)
// of an existing incident and, when updated.Status is non-empty, performs a
// validated status transition. ID and timestamps are server managed.
func (s *Store) Update(id string, updated Incident) (Incident, error) {
	title, desc, sev, src, status := updated.Title, updated.Description, updated.Severity, updated.Source, updated.Status
	return s.Patch(id, Patch{Title: &title, Description: &desc, Severity: &sev, Source: &src, Status: optionalStatus(status)})
}

func optionalStatus(s Status) *Status {
	if s == "" {
		return nil
	}
	return &s
}

// Patch holds optional field updates; nil fields are left unchanged.
type Patch struct {
	Title       *string   `json:"title"`
	Description *string   `json:"description"`
	Severity    *Severity `json:"severity"`
	Source      *string   `json:"source"`
	Status      *Status   `json:"status"`
}

// Patch applies a partial update with lifecycle validation.
func (s *Store) Patch(id string, p Patch) (Incident, error) {
	s.mu.Lock()
	existing, ok := s.incidents[id]
	if !ok {
		s.mu.Unlock()
		return Incident{}, ErrNotFound
	}
	next := existing
	if p.Title != nil {
		next.Title = *p.Title
	}
	if p.Description != nil {
		next.Description = *p.Description
	}
	if p.Severity != nil {
		next.Severity = *p.Severity
	}
	if p.Source != nil {
		next.Source = *p.Source
	}
	next.Status = existing.Status
	if p.Status != nil && *p.Status != "" {
		next.Status = *p.Status
	}
	if err := normalize(&next); err != nil {
		s.mu.Unlock()
		return Incident{}, err
	}
	from := existing.Status
	if !CanTransition(from, next.Status) {
		s.mu.Unlock()
		return Incident{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, next.Status)
	}
	now := s.now()
	if next.Status != from {
		stampStatus(&next, next.Status, now)
	}
	next.UpdatedAt = now
	s.incidents[id] = next
	s.mu.Unlock()
	ev := Event{Type: "updated", Incident: next}
	if next.Status != from {
		ev.Type, ev.From = "transitioned", from
	}
	s.emit(ev)
	return next, nil
}

// Transition moves an incident to a new status if the lifecycle allows it.
func (s *Store) Transition(id string, to Status) (Incident, error) {
	if !ValidStatus(to) {
		return Incident{}, fmt.Errorf("%w: unknown status %q", ErrInvalid, to)
	}
	return s.Patch(id, Patch{Status: &to})
}

// Resolve marks an incident as resolved. It returns false if the incident
// does not exist or cannot be resolved (e.g. it is already closed).
func (s *Store) Resolve(id string) bool {
	_, err := s.Transition(id, StatusResolved)
	return err == nil
}

// Delete removes an incident and reports whether it existed.
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	inc, ok := s.incidents[id]
	delete(s.incidents, id)
	s.mu.Unlock()
	if ok {
		s.emit(Event{Type: "deleted", Incident: inc})
	}
	return ok
}

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("incident: crypto/rand failed: " + err.Error())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

// actionStatus maps URL actions to target statuses.
var actionStatus = map[string]Status{
	"ack":         StatusAcknowledged,
	"acknowledge": StatusAcknowledged,
	"investigate": StatusInvestigating,
	"resolve":     StatusResolved,
	"close":       StatusClosed,
	"reopen":      StatusOpen,
}

// WebhookHandler exposes JSON CRUD for incidents.
//
//	GET    /v1/incidents[?status=&severity=]      list
//	GET    /v1/incidents/{id} | ?id=              get
//	POST   /v1/incidents                          create (201)
//	POST   /v1/incidents?resolve=true&id={id}     resolve (legacy)
//	POST   /v1/incidents/{id}/{ack|resolve|close|reopen|investigate}
//	PUT    /v1/incidents/{id} | ?id=              replace mutable fields
//	PATCH  /v1/incidents/{id} | ?id=              partial update / transition
//	DELETE /v1/incidents/{id} | ?id=              delete (204)
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, action, ok := parsePath(r)
		if !ok {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			if action != "" {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			if id == "" {
				q := r.URL.Query()
				writeJSON(w, http.StatusOK, store.Filter(Status(q.Get("status")), Severity(q.Get("severity"))))
				return
			}
			inc, found := store.Get(id)
			if !found {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			writeJSON(w, http.StatusOK, inc)
		case http.MethodPost:
			if r.URL.Query().Get("resolve") == "true" {
				if id == "" {
					writeError(w, http.StatusBadRequest, "missing id")
					return
				}
				if _, err := store.Transition(id, StatusResolved); err != nil {
					writeStoreError(w, err)
					return
				}
				writeJSON(w, http.StatusOK, map[string]string{"status": "resolved"})
				return
			}
			if action != "" {
				to, known := actionStatus[action]
				if !known || id == "" {
					writeError(w, http.StatusNotFound, "not found")
					return
				}
				inc, err := store.Transition(id, to)
				if err != nil {
					writeStoreError(w, err)
					return
				}
				writeJSON(w, http.StatusOK, inc)
				return
			}
			if id != "" && r.URL.Query().Get("id") == "" {
				// POST to /v1/incidents/{id} is not a create.
				methodNotAllowed(w, "GET, HEAD, PUT, PATCH, DELETE")
				return
			}
			var inc Incident
			if !decodeBody(w, r, &inc) {
				return
			}
			created, err := store.Create(inc)
			if err != nil {
				writeStoreError(w, err)
				return
			}
			writeJSON(w, http.StatusCreated, created)
		case http.MethodPut, http.MethodPatch:
			if action != "" {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			if id == "" {
				writeError(w, http.StatusBadRequest, "missing id")
				return
			}
			var (
				updated Incident
				err     error
			)
			if r.Method == http.MethodPut {
				var inc Incident
				if !decodeBody(w, r, &inc) {
					return
				}
				updated, err = store.Update(id, inc)
			} else {
				var p Patch
				if !decodeBody(w, r, &p) {
					return
				}
				updated, err = store.Patch(id, p)
			}
			if err != nil {
				writeStoreError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, updated)
		case http.MethodDelete:
			if action != "" {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			if id == "" {
				writeError(w, http.StatusBadRequest, "missing id")
				return
			}
			if !store.Delete(id) {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			methodNotAllowed(w, "GET, HEAD, POST, PUT, PATCH, DELETE")
		}
	}
}

// parsePath returns the incident id (path segment or ?id=) and an optional
// action segment. ok is false for paths outside /v1/incidents.
func parsePath(r *http.Request) (id, action string, ok bool) {
	rest, found := strings.CutPrefix(r.URL.Path, "/v1/incidents")
	if found && rest != "" && !strings.HasPrefix(rest, "/") {
		return "", "", false
	}
	rest = strings.Trim(rest, "/")
	if rest != "" {
		parts := strings.Split(rest, "/")
		if len(parts) > 2 {
			return "", "", false
		}
		id = parts[0]
		if len(parts) == 2 {
			action = parts[1]
		}
	}
	if id == "" {
		id = r.URL.Query().Get("id")
	}
	return id, action, true
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrInvalidTransition):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrStoreFull):
		writeError(w, http.StatusInsufficientStorage, err.Error())
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
