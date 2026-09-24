package notification

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

const (
	channelsPath      = "/v1/notification/channels"
	subscriptionsPath = "/v1/notification/subscriptions"
	maxBodyBytes      = 1 << 20
)

// WebhookHandler exposes JSON CRUD for channels and subscriptions:
//
//	GET    /v1/notification/channels            list (secrets redacted)
//	GET    /v1/notification/channels?id=ID      get (also /channels/ID)
//	POST   /v1/notification/channels            create or upsert (200)
//	PUT    /v1/notification/channels?id=ID      replace
//	PATCH  /v1/notification/channels?id=ID      partial update
//	DELETE /v1/notification/channels?id=ID      delete (cascades subscriptions)
//
// and the same verbs for /v1/notification/subscriptions.
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if id, ok := matchCollection(r.URL.Path, channelsPath); ok {
			id, err := resolveID(r, id)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			handleChannels(store, w, r, id)
			return
		}
		if id, ok := matchCollection(r.URL.Path, subscriptionsPath); ok {
			id, err := resolveID(r, id)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			handleSubscriptions(store, w, r, id)
			return
		}
		writeError(w, http.StatusNotFound, "not found")
	}
}

// matchCollection reports whether path addresses the collection at prefix and
// returns the id path segment, if any.
func matchCollection(path, prefix string) (string, bool) {
	if path == prefix || path == prefix+"/" {
		return "", true
	}
	if !strings.HasPrefix(path, prefix+"/") {
		return "", false
	}
	rest := strings.TrimSuffix(path[len(prefix)+1:], "/")
	if rest == "" || strings.Contains(rest, "/") {
		return "", false
	}
	return rest, true
}

func resolveID(r *http.Request, pathID string) (string, error) {
	q := r.URL.Query().Get("id")
	if q != "" && pathID != "" && q != pathID {
		return "", errors.New("id in path and query differ")
	}
	if q != "" {
		return q, nil
	}
	return pathID, nil
}

func handleChannels(store *Store, w http.ResponseWriter, r *http.Request, id string) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if id == "" {
			list := store.ListChannels()
			for i := range list {
				list[i] = list[i].Redacted()
			}
			writeJSON(w, http.StatusOK, list)
			return
		}
		ch, ok := store.GetChannel(id)
		if !ok {
			writeError(w, http.StatusNotFound, "channel not found")
			return
		}
		writeJSON(w, http.StatusOK, ch.Redacted())
	case http.MethodPost:
		var ch Channel
		if !decodeJSON(w, r, &ch) {
			return
		}
		if id != "" {
			if ch.ID != "" && ch.ID != id {
				writeError(w, http.StatusBadRequest, "id in body does not match request id")
				return
			}
			ch.ID = id
		}
		stored, err := store.UpsertChannel(ch)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, stored.Redacted())
	case http.MethodPut:
		if id == "" {
			writeError(w, http.StatusBadRequest, "missing id")
			return
		}
		var ch Channel
		if !decodeJSON(w, r, &ch) {
			return
		}
		if ch.ID != "" && ch.ID != id {
			writeError(w, http.StatusBadRequest, "id in body does not match request id")
			return
		}
		stored, err := store.UpdateChannel(id, ch)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, stored.Redacted())
	case http.MethodPatch:
		if id == "" {
			writeError(w, http.StatusBadRequest, "missing id")
			return
		}
		var patch struct {
			Name     *string            `json:"name"`
			Type     *ChannelType       `json:"type"`
			Target   *string            `json:"target"`
			Enabled  *bool              `json:"enabled"`
			Metadata *map[string]string `json:"metadata"`
		}
		if !decodeJSON(w, r, &patch) {
			return
		}
		stored, err := store.ModifyChannel(id, func(c *Channel) error {
			if patch.Name != nil {
				c.Name = *patch.Name
			}
			if patch.Type != nil {
				c.Type = *patch.Type
			}
			if patch.Target != nil {
				c.Target = *patch.Target
			}
			if patch.Enabled != nil {
				c.Enabled = *patch.Enabled
			}
			if patch.Metadata != nil {
				c.Metadata = *patch.Metadata
			}
			return nil
		})
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, stored.Redacted())
	case http.MethodDelete:
		if id == "" {
			writeError(w, http.StatusBadRequest, "missing id")
			return
		}
		if err := store.RemoveChannel(id); err != nil {
			if errors.Is(err, ErrNotFound) {
				writeError(w, http.StatusNotFound, "channel not found")
				return
			}
			writeStoreError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete)
	}
}

func handleSubscriptions(store *Store, w http.ResponseWriter, r *http.Request, id string) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if id == "" {
			writeJSON(w, http.StatusOK, store.ListSubscriptions())
			return
		}
		sub, ok := store.GetSubscription(id)
		if !ok {
			writeError(w, http.StatusNotFound, "subscription not found")
			return
		}
		writeJSON(w, http.StatusOK, sub)
	case http.MethodPost:
		var sub Subscription
		if !decodeJSON(w, r, &sub) {
			return
		}
		if id != "" {
			if sub.ID != "" && sub.ID != id {
				writeError(w, http.StatusBadRequest, "id in body does not match request id")
				return
			}
			sub.ID = id
		}
		stored, err := store.UpsertSubscription(sub)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, stored)
	case http.MethodPut:
		if id == "" {
			writeError(w, http.StatusBadRequest, "missing id")
			return
		}
		var sub Subscription
		if !decodeJSON(w, r, &sub) {
			return
		}
		if sub.ID != "" && sub.ID != id {
			writeError(w, http.StatusBadRequest, "id in body does not match request id")
			return
		}
		stored, err := store.UpdateSubscription(id, sub)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, stored)
	case http.MethodPatch:
		if id == "" {
			writeError(w, http.StatusBadRequest, "missing id")
			return
		}
		var patch struct {
			AlertID   *string `json:"alert_id"`
			ChannelID *string `json:"channel_id"`
			Enabled   *bool   `json:"enabled"`
		}
		if !decodeJSON(w, r, &patch) {
			return
		}
		stored, err := store.ModifySubscription(id, func(s *Subscription) error {
			if patch.AlertID != nil {
				s.AlertID = *patch.AlertID
			}
			if patch.ChannelID != nil {
				s.ChannelID = *patch.ChannelID
			}
			if patch.Enabled != nil {
				s.Enabled = *patch.Enabled
			}
			return nil
		})
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, stored)
	case http.MethodDelete:
		if id == "" {
			writeError(w, http.StatusBadRequest, "missing id")
			return
		}
		if err := store.RemoveSubscription(id); err != nil {
			if errors.Is(err, ErrNotFound) {
				writeError(w, http.StatusNotFound, "subscription not found")
				return
			}
			writeStoreError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete)
	}
}

func writeStoreError(w http.ResponseWriter, err error) {
	var verr *ValidationError
	switch {
	case errors.As(err, &verr):
		writeError(w, http.StatusBadRequest, verr.Error())
	case errors.Is(err, ErrUnknownChannel):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, ErrStoreFull):
		writeError(w, http.StatusInsufficientStorage, err.Error())
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

// decodeJSON decodes exactly one JSON value from a size-capped body. It
// writes the error response itself and returns false on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
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
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "unexpected data after JSON body")
		return false
	}
	return true
}
