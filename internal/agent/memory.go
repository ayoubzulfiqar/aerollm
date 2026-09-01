package agent

import (
	"context"
	"fmt"
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
