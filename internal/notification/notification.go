package notification

import (
	"encoding/json"
	"net/http"
	"sync"
)

// ChannelType defines notification channel types.
type ChannelType string

const (
	ChannelWebhook ChannelType = "webhook"
	ChannelEmail   ChannelType = "email"
	ChannelSlack   ChannelType = "slack"
	ChannelSMS     ChannelType = "sms"
)

// Channel represents a notification destination.
type Channel struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Type      ChannelType       `json:"type"`
	Target    string            `json:"target"`
	Enabled   bool              `json:"enabled"`
	Metadata  map[string]string `json:"metadata"`
}

// Subscription links an alert to a channel.
type Subscription struct {
	ID        string `json:"id"`
	AlertID   string `json:"alert_id"`
	ChannelID string `json:"channel_id"`
	Enabled   bool   `json:"enabled"`
}

// Store manages notifications in memory.
type Store struct {
	mu            sync.RWMutex
	channels      map[string]Channel
	subscriptions map[string]Subscription
}

// NewStore creates a notification store.
func NewStore() *Store {
	return &Store{
		channels:      make(map[string]Channel),
		subscriptions: make(map[string]Subscription),
	}
}

// UpsertChannel adds or updates a channel.
func (s *Store) UpsertChannel(channel Channel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[channel.ID] = channel
}

// GetChannel retrieves a channel by id.
func (s *Store) GetChannel(id string) (Channel, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ch, ok := s.channels[id]
	return ch, ok
}

// ListChannels returns all channels.
func (s *Store) ListChannels() []Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Channel, 0, len(s.channels))
	for _, ch := range s.channels {
		out = append(out, ch)
	}
	return out
}

// UpsertSubscription adds or updates a subscription.
func (s *Store) UpsertSubscription(sub Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[sub.ID] = sub
}

// GetSubscription retrieves a subscription by id.
func (s *Store) GetSubscription(id string) (Subscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.subscriptions[id]
	return sub, ok
}

// ListSubscriptions returns all subscriptions.
func (s *Store) ListSubscriptions() []Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		out = append(out, sub)
	}
	return out
}

// WebhookHandler exposes JSON CRUD for channels and subscriptions.
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/v1/notification/channels" || path == "/v1/notification/channels/":
			switch r.Method {
			case http.MethodPost:
				var ch Channel
				if err := json.NewDecoder(r.Body).Decode(&ch); err != nil {
					http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
					return
				}
				store.UpsertChannel(ch)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(ch)
			case http.MethodGet:
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.ListChannels())
			default:
				http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			}
		case path == "/v1/notification/subscriptions" || path == "/v1/notification/subscriptions/":
			switch r.Method {
			case http.MethodPost:
				var sub Subscription
				if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
					http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
					return
				}
				store.UpsertSubscription(sub)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(sub)
			case http.MethodGet:
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.ListSubscriptions())
			default:
				http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			}
		default:
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		}
	}
}
