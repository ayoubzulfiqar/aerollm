package schedule

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// TaskType defines scheduled task types.
type TaskType string

const (
	TaskCron       TaskType = "cron"
	TaskInterval   TaskType = "interval"
	TaskOneTime    TaskType = "onetime"
)

// TaskStatus tracks task lifecycle.
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
)

// ScheduledTask represents an automation task.
type ScheduledTask struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Type      TaskType  `json:"type"`
	Schedule  string    `json:"schedule"`
	Payload   string    `json:"payload"`
	Status    TaskStatus `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	RunAt     time.Time `json:"run_at"`
}

// Store manages scheduled tasks in memory.
type Store struct {
	mu     sync.RWMutex
	tasks  map[string]ScheduledTask
}

// NewStore creates a task store.
func NewStore() *Store {
	return &Store{tasks: make(map[string]ScheduledTask)}
}

// Upsert adds or updates a task.
func (s *Store) Upsert(task ScheduledTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task.ID == "" {
		task.ID = "task_" + time.Now().Format("20060102150405")
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	if task.Status == "" {
		task.Status = TaskPending
	}
	s.tasks[task.ID] = task
}

// Get retrieves a task by id.
func (s *Store) Get(id string) (ScheduledTask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, ok := s.tasks[id]
	return task, ok
}

// List returns all tasks.
func (s *Store) List() []ScheduledTask {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ScheduledTask, 0, len(s.tasks))
	for _, task := range s.tasks {
		out = append(out, task)
	}
	return out
}

// UpdateStatus updates task status.
func (s *Store) UpdateStatus(id string, status TaskStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, ok := s.tasks[id]; ok {
		task.Status = status
		s.tasks[id] = task
	}
}

// WebhookHandler exposes JSON CRUD for scheduled tasks.
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var task ScheduledTask
			if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			store.Upsert(task)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(task)
		case http.MethodGet:
			id := r.URL.Query().Get("id")
			if id == "" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.List())
				return
			}
			if task, ok := store.Get(id); ok {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(task)
				return
			}
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		case http.MethodPut:
			id := r.URL.Query().Get("id")
			if id == "" {
				http.Error(w, `{"error":"missing id"}`, http.StatusBadRequest)
				return
			}
			var update struct {
				Status TaskStatus `json:"status"`
			}
			if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			store.UpdateStatus(id, update.Status)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}
