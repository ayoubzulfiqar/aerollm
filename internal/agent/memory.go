package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// MemoryProvider is the interface for agent memory backends.
type MemoryProvider interface {
	Remember(ctx context.Context, conversationID string, message models.Message) error
	Recall(ctx context.Context, conversationID string, limit int) ([]models.Message, error)
	Summarize(ctx context.Context, conversationID string) (string, error)
}

// MessageMemory stores conversation history in memory.
type MessageMemory struct {
	mu      sync.RWMutex
	history map[string][]models.Message
}

// NewMessageMemory creates a new in-memory conversation history store.
func NewMessageMemory() *MessageMemory {
	return &MessageMemory{history: make(map[string][]models.Message)}
}

// Remember appends a message to conversation history.
func (m *MessageMemory) Remember(ctx context.Context, conversationID string, message models.Message) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	m.history[conversationID] = append(m.history[conversationID], message)
	return nil
}

// Recall returns recent messages for a conversation.
func (m *MessageMemory) Recall(ctx context.Context, conversationID string, limit int) ([]models.Message, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	history := m.history[conversationID]
	if limit <= 0 || limit > len(history) {
		return history, nil
	}
	return history[len(history)-limit:], nil
}

// Summarize returns a simple summary of the conversation.
func (m *MessageMemory) Summarize(ctx context.Context, conversationID string) (string, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	history := m.history[conversationID]
	if len(history) == 0 {
		return "", nil
	}
	summary := fmt.Sprintf("conversation %s has %d messages", conversationID, len(history))
	return summary, nil
}

// VectorMemory is the interface for long-term vector-backed memory.
type VectorMemory interface {
	Upsert(ctx context.Context, conversationID string, message models.Message) error
	Search(ctx context.Context, conversationID string, query string, limit int) ([]models.Message, error)
}

// InMemoryVectorMemory implements VectorMemory using an in-memory store with simple keyword search.
type InMemoryVectorMemory struct {
	mu      sync.RWMutex
	entries []vectorEntry
}

type vectorEntry struct {
	conversationID string
	message        models.Message
}

// NewInMemoryVectorMemory creates a new in-memory vector search backend.
func NewInMemoryVectorMemory() *InMemoryVectorMemory {
	return &InMemoryVectorMemory{}
}

// Upsert appends a message to the in-memory vector store.
func (v *InMemoryVectorMemory) Upsert(ctx context.Context, conversationID string, message models.Message) error {
	_ = ctx
	v.mu.Lock()
	defer v.mu.Unlock()
	v.entries = append(v.entries, vectorEntry{conversationID: conversationID, message: message})
	return nil
}

// Search returns messages from the conversation that match the query by simple substring matching.
func (v *InMemoryVectorMemory) Search(ctx context.Context, conversationID string, query string, limit int) ([]models.Message, error) {
	_ = ctx
	v.mu.RLock()
	defer v.mu.RUnlock()
	q := strings.ToLower(query)
	var out []models.Message
	for _, e := range v.entries {
		if e.conversationID != conversationID {
			continue
		}
		if e.message.Content != nil && strings.Contains(strings.ToLower(*e.message.Content), q) {
			out = append(out, e.message)
			if len(out) >= limit && limit > 0 {
				break
			}
		}
	}
	return out, nil
}
