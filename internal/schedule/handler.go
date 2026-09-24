package schedule

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	basePath     = "/v1/schedule"
	maxBodyBytes = 1 << 20
)

var allowedMethods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}

// WebhookHandler exposes REST CRUD for scheduled tasks:
//
//	GET    /v1/schedule                list (optional ?status= and ?type= filters)
//	GET    /v1/schedule?id=ID          fetch one (also /v1/schedule/ID)
//	POST   /v1/schedule                create (201)
//	PUT    /v1/schedule?id=ID          partial update (PATCH is equivalent)
//	DELETE /v1/schedule?id=ID          delete (204)
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if store == nil {
			writeError(w, http.StatusInternalServerError, "schedule store not configured")
			return
		}
		id, status, msg := requestID(r)
		if status != 0 {
			writeError(w, status, msg)
			return
		}
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			if id == "" {
				writeJSON(w, http.StatusOK, filterTasks(store.List(), r))
				return
			}
			task, ok := store.Get(id)
			if !ok {
				writeError(w, http.StatusNotFound, "not found")
				return
			}
			writeJSON(w, http.StatusOK, task)
		case http.MethodPost:
			if id != "" {
				methodNotAllowed(w, http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPatch, http.MethodDelete)
				return
			}
			var task ScheduledTask
			if !decodeBody(w, r, &task) {
				return
			}
			if task.Status != "" && task.Status != TaskPending {
				writeError(w, http.StatusBadRequest, "status must be pending (or omitted) when creating a task")
				return
			}
			// Server-owned fields.
			task.CreatedAt = time.Time{}
			task.NextRun = time.Time{}
			task.LastRunAt = time.Time{}
			task.LastError = ""
			task.Runs = 0
			created, err := store.Create(task)
			if err != nil {
				writeStoreError(w, err)
				return
			}
			writeJSON(w, http.StatusCreated, created)
		case http.MethodPut, http.MethodPatch:
			if id == "" {
				writeError(w, http.StatusBadRequest, "missing id")
				return
			}
			var u TaskUpdate
			if !decodeBody(w, r, &u) {
				return
			}
			updated, err := store.Update(id, u)
			if err != nil {
				writeStoreError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, updated)
		case http.MethodDelete:
			if id == "" {
				writeError(w, http.StatusBadRequest, "missing id")
				return
			}
			if err := store.Remove(id); err != nil {
				writeStoreError(w, err)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			methodNotAllowed(w, allowedMethods...)
		}
	}
}

// requestID extracts the task id from ?id= or a /v1/schedule/{id} path.
// A non-zero status signals an error response.
func requestID(r *http.Request) (id string, status int, msg string) {
	q := r.URL.Query().Get("id")
	var p string
	if rest, ok := strings.CutPrefix(r.URL.Path, basePath+"/"); ok {
		p = strings.TrimSuffix(rest, "/")
		if strings.Contains(p, "/") {
			return "", http.StatusNotFound, "not found"
		}
	}
	switch {
	case q != "" && p != "" && q != p:
		return "", http.StatusBadRequest, "conflicting ids in path and query"
	case p != "":
		return p, 0, ""
	default:
		return q, 0, ""
	}
}

func filterTasks(tasks []ScheduledTask, r *http.Request) []ScheduledTask {
	status := TaskStatus(r.URL.Query().Get("status"))
	typ := TaskType(r.URL.Query().Get("type"))
	if status == "" && typ == "" {
		return tasks
	}
	out := tasks[:0]
	for _, t := range tasks {
		if (status == "" || t.Status == status) && (typ == "" || t.Type == typ) {
			out = append(out, t)
		}
	}
	return out
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, "missing body")
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		case errors.Is(err, io.EOF):
			writeError(w, http.StatusBadRequest, "missing body")
		default:
			writeError(w, http.StatusBadRequest, "invalid JSON body")
		}
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeError(w, http.StatusBadRequest, "unexpected data after JSON body")
		}
		return false
	}
	return true
}

func writeStoreError(w http.ResponseWriter, err error) {
	var verr *ValidationError
	switch {
	case errors.As(err, &verr):
		writeError(w, http.StatusBadRequest, verr.Msg)
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, ErrConflict):
		writeError(w, http.StatusConflict, "task already exists")
	case errors.Is(err, ErrStoreFull):
		writeError(w, http.StatusInsufficientStorage, "task limit reached")
	case errors.Is(err, ErrPersistence):
		writeError(w, http.StatusInternalServerError, "persistence failure")
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func methodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}
