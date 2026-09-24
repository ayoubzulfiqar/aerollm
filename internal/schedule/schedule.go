// Package schedule stores automation tasks and computes when they are due.
//
// A Store on its own only validates tasks and computes their next activation
// time (next_run); it never executes anything. To actually execute tasks,
// create a Runner with an Executor and call Runner.Run with a cancellable
// context.
//
// Three task types are supported:
//   - cron: a 5-field cron expression (see ParseCronInLocation), evaluated in
//     the task's timezone (IANA name, default UTC);
//   - interval: a Go duration such as "30s" or "@every 5m" (fixed rate,
//     missed slots are skipped);
//   - onetime: an RFC 3339 timestamp in schedule, or run_at when schedule is
//     empty; it must be in the future when created or rescheduled.
package schedule

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// TaskType defines scheduled task types.
type TaskType string

const (
	TaskCron     TaskType = "cron"
	TaskInterval TaskType = "interval"
	TaskOneTime  TaskType = "onetime"
)

// TaskStatus tracks task lifecycle.
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
)

// Limits applied to tasks.
const (
	// MaxTasks bounds the number of tasks a Store holds.
	MaxTasks        = 10000
	maxNameLen      = 256
	maxPayloadBytes = 64 << 10
	maxTimezoneLen  = 64
	maxLastErrorLen = 1024
)

// ScheduledTask represents an automation task.
type ScheduledTask struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Type      TaskType   `json:"type"`
	Schedule  string     `json:"schedule"`
	Payload   string     `json:"payload"`
	Status    TaskStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	RunAt     time.Time  `json:"run_at"`
	// Timezone is the IANA location cron expressions are evaluated in
	// (default UTC).
	Timezone string `json:"timezone,omitempty"`
	// NextRun is the next activation time; zero when the task will not run
	// again (e.g. a completed one-time task).
	NextRun   time.Time `json:"next_run,omitzero"`
	LastRunAt time.Time `json:"last_run_at,omitzero"`
	LastError string    `json:"last_error,omitempty"`
	Runs      int64     `json:"runs,omitempty"`
}

// TaskUpdate is a partial update; nil fields are left unchanged.
type TaskUpdate struct {
	Name     *string     `json:"name,omitempty"`
	Type     *TaskType   `json:"type,omitempty"`
	Schedule *string     `json:"schedule,omitempty"`
	Payload  *string     `json:"payload,omitempty"`
	Timezone *string     `json:"timezone,omitempty"`
	Status   *TaskStatus `json:"status,omitempty"`
	RunAt    *time.Time  `json:"run_at,omitempty"`
}

// Errors returned by the Store.
var (
	ErrNotFound  = errors.New("schedule: task not found")
	ErrConflict  = errors.New("schedule: task already exists")
	ErrStoreFull = errors.New("schedule: task limit reached")
)

// ValidationError reports an invalid task definition.
type ValidationError struct {
	Msg string
}

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, args ...any) error {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// plan is the compiled form of a task's schedule.
type plan struct {
	typ      TaskType
	cron     *CronSchedule
	interval time.Duration
	runAt    time.Time
}

// first returns the first activation at or after creation time now.
func (p plan) first(now time.Time) time.Time {
	switch {
	case p.interval > 0:
		return now.Truncate(time.Second).Add(p.interval)
	case p.cron != nil:
		return p.cron.Next(now)
	default:
		return p.runAt
	}
}

// afterRun returns the activation following a run that was scheduled for
// `scheduled` and completed at `completed`. Missed activations are skipped so
// a task that was late runs once, not once per missed slot.
func (p plan) afterRun(scheduled, completed time.Time) time.Time {
	ref := scheduled
	if completed.After(ref) {
		ref = completed
	}
	switch {
	case p.interval > 0:
		next := scheduled.Add(p.interval)
		if !next.After(ref) {
			k := ref.Sub(scheduled)/p.interval + 1
			next = scheduled.Add(k * p.interval)
		}
		return next
	case p.cron != nil:
		return p.cron.Next(ref)
	default:
		return time.Time{}
	}
}

type entry struct {
	task     ScheduledTask
	plan     plan
	rev      uint64
	inflight bool
}

// Store manages scheduled tasks in memory. It is safe for concurrent use.
type Store struct {
	mu       sync.RWMutex
	tasks    map[string]*entry
	now      func() time.Time
	maxTasks int
}

// NewStore creates a task store.
func NewStore() *Store {
	return &Store{tasks: make(map[string]*entry), now: time.Now, maxTasks: MaxTasks}
}

func validStatus(s TaskStatus) bool {
	switch s {
	case TaskPending, TaskRunning, TaskCompleted, TaskFailed:
		return true
	}
	return false
}

func newTaskID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "task_" + hex.EncodeToString(b[:])
}

// validate normalizes t in place and compiles its schedule. It does not check
// whether one-time tasks are in the future; callers do that.
func validate(t *ScheduledTask) (plan, error) {
	if t.ID != "" && !idPattern.MatchString(t.ID) {
		return plan{}, invalid("invalid id: must match %s", idPattern.String())
	}
	t.Name = strings.TrimSpace(t.Name)
	if t.Name == "" {
		return plan{}, invalid("name is required")
	}
	if len(t.Name) > maxNameLen {
		return plan{}, invalid("name must be at most %d bytes", maxNameLen)
	}
	if len(t.Payload) > maxPayloadBytes {
		return plan{}, invalid("payload must be at most %d bytes", maxPayloadBytes)
	}
	if t.Status != "" && !validStatus(t.Status) {
		return plan{}, invalid("invalid status %q: must be one of pending, running, completed, failed", t.Status)
	}
	if t.Type == "" {
		t.Type = TaskCron
	}
	t.Schedule = strings.TrimSpace(t.Schedule)
	if len(t.Schedule) > maxCronExprLen {
		return plan{}, invalid("schedule must be at most %d characters", maxCronExprLen)
	}
	t.Timezone = strings.TrimSpace(t.Timezone)
	loc := time.UTC
	if t.Timezone != "" {
		if len(t.Timezone) > maxTimezoneLen || strings.EqualFold(t.Timezone, "Local") {
			return plan{}, invalid("invalid timezone %q", t.Timezone)
		}
		l, err := time.LoadLocation(t.Timezone)
		if err != nil {
			return plan{}, invalid("invalid timezone %q", t.Timezone)
		}
		loc = l
	}

	p := plan{typ: t.Type}
	switch t.Type {
	case TaskCron:
		if t.Schedule == "" {
			return plan{}, invalid("schedule is required for cron tasks")
		}
		cs, err := ParseCronInLocation(t.Schedule, loc)
		if err != nil {
			return plan{}, invalid("invalid schedule: %v", err)
		}
		if cs.Interval() > 0 {
			p.interval = cs.Interval()
		} else {
			p.cron = cs
		}
		t.RunAt = time.Time{}
	case TaskInterval:
		d, err := ParseInterval(t.Schedule)
		if err != nil {
			return plan{}, invalid("invalid schedule: %v", err)
		}
		p.interval = d
		t.RunAt = time.Time{}
	case TaskOneTime:
		runAt := t.RunAt
		if t.Schedule != "" {
			ts, err := time.Parse(time.RFC3339, t.Schedule)
			if err != nil {
				return plan{}, invalid("invalid schedule: one-time tasks need an RFC 3339 timestamp")
			}
			if !runAt.IsZero() && !runAt.Equal(ts) {
				return plan{}, invalid("schedule and run_at disagree")
			}
			runAt = ts
		}
		if runAt.IsZero() {
			return plan{}, invalid("one-time tasks need a schedule timestamp or run_at")
		}
		t.RunAt = runAt
		t.Schedule = runAt.Format(time.RFC3339)
		p.runAt = runAt
	default:
		return plan{}, invalid("invalid type %q: must be one of cron, interval, onetime", t.Type)
	}
	return p, nil
}

// initialNext computes the first activation for a new or rescheduled task.
func initialNext(t ScheduledTask, p plan, now time.Time) (time.Time, error) {
	next := p.first(now)
	if next.IsZero() {
		return time.Time{}, invalid("schedule never fires")
	}
	if t.Type == TaskOneTime && !next.After(now) {
		return time.Time{}, invalid("run time %s is in the past", next.Format(time.RFC3339))
	}
	return next, nil
}

// Create adds a new task. It fails with ErrConflict if a task with the same
// ID exists. The ID is generated when empty.
func (s *Store) Create(task ScheduledTask) (ScheduledTask, error) {
	return s.put(task, false)
}

// Upsert adds or replaces a task and returns the stored copy. Replacing a
// task keeps its creation time and run history.
func (s *Store) Upsert(task ScheduledTask) (ScheduledTask, error) {
	return s.put(task, true)
}

func (s *Store) put(task ScheduledTask, replace bool) (ScheduledTask, error) {
	p, err := validate(&task)
	if err != nil {
		return ScheduledTask{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()

	var existing *entry
	if task.ID != "" {
		existing = s.tasks[task.ID]
		if existing != nil && !replace {
			return ScheduledTask{}, ErrConflict
		}
	}
	if existing == nil && len(s.tasks) >= s.maxTasks {
		return ScheduledTask{}, ErrStoreFull
	}

	if existing != nil {
		prev := existing.task
		task.CreatedAt = prev.CreatedAt
		task.LastRunAt = prev.LastRunAt
		task.LastError = prev.LastError
		task.Runs = prev.Runs
		if task.Status == "" {
			task.Status = prev.Status
		}
		if task.Type == TaskOneTime && prev.Type == TaskOneTime && task.RunAt.Equal(prev.RunAt) {
			// Same one-time slot: keep whatever the runner decided.
			task.NextRun = prev.NextRun
		} else {
			next, err := initialNext(task, p, now)
			if err != nil {
				return ScheduledTask{}, err
			}
			task.NextRun = next
		}
		existing.task = task
		existing.plan = p
		existing.rev++
		return task, nil
	}

	next, err := initialNext(task, p, now)
	if err != nil {
		return ScheduledTask{}, err
	}
	task.NextRun = next
	if task.ID == "" {
		for {
			task.ID = newTaskID()
			if _, taken := s.tasks[task.ID]; !taken {
				break
			}
		}
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = now
	}
	if task.Status == "" {
		task.Status = TaskPending
	}
	s.tasks[task.ID] = &entry{task: task, plan: p}
	return task, nil
}

// Update applies a partial update to a task, re-validating it and
// recomputing its next run when schedule-related fields change.
func (s *Store) Update(id string, u TaskUpdate) (ScheduledTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.tasks[id]
	if !ok {
		return ScheduledTask{}, ErrNotFound
	}
	t := e.task
	rescheduled := false
	if u.Name != nil {
		t.Name = *u.Name
	}
	if u.Payload != nil {
		t.Payload = *u.Payload
	}
	if u.Status != nil {
		if !validStatus(*u.Status) {
			return ScheduledTask{}, invalid("invalid status %q: must be one of pending, running, completed, failed", *u.Status)
		}
		t.Status = *u.Status
	}
	if u.Type != nil && *u.Type != t.Type {
		t.Type = *u.Type
		rescheduled = true
	}
	if u.Timezone != nil && *u.Timezone != t.Timezone {
		t.Timezone = *u.Timezone
		rescheduled = true
	}
	if u.Schedule != nil {
		if strings.TrimSpace(*u.Schedule) != t.Schedule {
			rescheduled = true
		}
		t.Schedule = *u.Schedule
	}
	if u.RunAt != nil {
		if !u.RunAt.Equal(t.RunAt) {
			rescheduled = true
		}
		t.RunAt = *u.RunAt
		if u.Schedule == nil && t.Type == TaskOneTime {
			// run_at alone reschedules a one-time task.
			t.Schedule = ""
		}
	}
	if rescheduled && t.Type != TaskOneTime && u.RunAt == nil {
		t.RunAt = time.Time{}
	}

	p, err := validate(&t)
	if err != nil {
		return ScheduledTask{}, err
	}
	if rescheduled {
		now := s.now()
		if t.Type == TaskOneTime && e.task.Type == TaskOneTime && t.RunAt.Equal(e.task.RunAt) {
			t.NextRun = e.task.NextRun
		} else {
			next, err := initialNext(t, p, now)
			if err != nil {
				return ScheduledTask{}, err
			}
			t.NextRun = next
			if t.Type == TaskOneTime && u.Status == nil && (t.Status == TaskCompleted || t.Status == TaskFailed) {
				t.Status = TaskPending
			}
		}
	}
	e.task = t
	e.plan = p
	e.rev++
	return t, nil
}

// Get retrieves a task by id.
func (s *Store) Get(id string) (ScheduledTask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.tasks[id]
	if !ok {
		return ScheduledTask{}, false
	}
	return e.task, true
}

// List returns all tasks ordered by creation time, then ID.
func (s *Store) List() []ScheduledTask {
	s.mu.RLock()
	out := make([]ScheduledTask, 0, len(s.tasks))
	for _, e := range s.tasks {
		out = append(out, e.task)
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

// Delete removes a task. It reports whether the task existed. A run that is
// in flight finishes, but its result is discarded.
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[id]; !ok {
		return false
	}
	delete(s.tasks, id)
	return true
}

// UpdateStatus updates task status. Unknown ids and invalid statuses are
// ignored; use SetStatus to observe errors.
func (s *Store) UpdateStatus(id string, status TaskStatus) {
	_ = s.SetStatus(id, status)
}

// SetStatus updates task status, returning ErrNotFound for unknown ids and a
// ValidationError for unknown statuses.
func (s *Store) SetStatus(id string, status TaskStatus) error {
	if !validStatus(status) {
		return invalid("invalid status %q", status)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.tasks[id]
	if !ok {
		return ErrNotFound
	}
	e.task.Status = status
	return nil
}

// claim is a task handed to the runner.
type claim struct {
	entry     *entry
	task      ScheduledTask
	rev       uint64
	scheduled time.Time
}

// claimDue marks up to max due tasks as in flight and returns them, earliest
// first. Tasks already in flight are never claimed twice.
func (s *Store) claimDue(now time.Time, max int) []claim {
	if max <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []*entry
	for _, e := range s.tasks {
		if e.inflight || e.task.NextRun.IsZero() || e.task.NextRun.After(now) {
			continue
		}
		due = append(due, e)
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].task.NextRun.Equal(due[j].task.NextRun) {
			return due[i].task.NextRun.Before(due[j].task.NextRun)
		}
		return due[i].task.ID < due[j].task.ID
	})
	if len(due) > max {
		due = due[:max]
	}
	claims := make([]claim, 0, len(due))
	for _, e := range due {
		e.inflight = true
		e.task.Status = TaskRunning
		claims = append(claims, claim{entry: e, task: e.task, rev: e.rev, scheduled: e.task.NextRun})
	}
	return claims
}

// finishRun records the outcome of a claimed run and schedules the next one.
func (s *Store) finishRun(c claim, started, completed time.Time, runErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.tasks[c.task.ID]
	if !ok || e != c.entry {
		return // deleted (or replaced by a new task with the same id) meanwhile
	}
	e.inflight = false
	e.task.Runs++
	e.task.LastRunAt = started
	if runErr != nil {
		e.task.Status = TaskFailed
		e.task.LastError = truncateUTF8(runErr.Error(), maxLastErrorLen)
	} else {
		e.task.Status = TaskCompleted
		e.task.LastError = ""
	}
	if e.rev == c.rev {
		e.task.NextRun = e.plan.afterRun(c.scheduled, completed)
	}
	// Otherwise the task was rescheduled during the run and NextRun already
	// reflects the new schedule.
}

// nextDue returns the earliest NextRun among tasks not in flight, and
// whether any such task is already due at now.
func (s *Store) nextDue(now time.Time) (earliest time.Time, dueNow bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.tasks {
		if e.inflight || e.task.NextRun.IsZero() {
			continue
		}
		if earliest.IsZero() || e.task.NextRun.Before(earliest) {
			earliest = e.task.NextRun
		}
	}
	return earliest, !earliest.IsZero() && !earliest.After(now)
}

func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}
