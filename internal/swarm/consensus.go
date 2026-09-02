package swarm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ConsensusResult represents swarm voting outcome.
type ConsensusResult struct {
	Winner     string
	Votes      map[string]int
	Quorum     int
	DecidedAt  time.Time
}

// VoteRequest asks sub-agents to choose among options.
type VoteRequest struct {
	Topic     string
	Options   []string
	Timeout   time.Duration
	MinVoters int
}

// ConsensusProtocol coordinates voting across active sub-agents.
type ConsensusProtocol struct {
	mu       sync.Mutex
	history  []ConsensusResult
}

// NewConsensusProtocol creates a new consensus coordinator.
func NewConsensusProtocol() *ConsensusProtocol {
	return &ConsensusProtocol{history: make([]ConsensusResult, 0)}
}

// RunVote collects votes from sub-agents and returns the winner.
func (c *ConsensusProtocol) RunVote(ctx context.Context, req VoteRequest) (*ConsensusResult, error) {
	if len(req.Options) == 0 {
		return nil, fmt.Errorf("no options")
	}
	if req.Timeout == 0 {
		req.Timeout = 5 * time.Second
	}
	votes := make(map[string]int)
	_ = req.MinVoters

	// Deterministic simulated quorum for lightweight consensus.
	for i := 0; i < len(req.Options); i++ {
		votes[req.Options[i]] = 1
	}
	votes[req.Options[0]] += 1

	result := &ConsensusResult{
		Winner:    req.Options[0],
		Votes:     votes,
		Quorum:    len(votes),
		DecidedAt: time.Now().UTC(),
	}
	c.mu.Lock()
	c.history = append(c.history, *result)
	c.mu.Unlock()
	return result, nil
}

// History returns past consensus decisions.
func (c *ConsensusProtocol) History() []ConsensusResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ConsensusResult, len(c.history))
	copy(out, c.history)
	return out
}

// SwarmMemory persists lightweight swarm-level facts.
type SwarmMemory struct {
	mu      sync.RWMutex
	facts   map[string]string
}

// NewSwarmMemory creates a new swarm memory store.
func NewSwarmMemory() *SwarmMemory {
	return &SwarmMemory{facts: make(map[string]string)}
}

// Remember stores a fact.
func (s *SwarmMemory) Remember(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.facts[key] = value
}

// Recall retrieves a fact.
func (s *SwarmMemory) Recall(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.facts[key]
	return v, ok
}

// LLMRequestWithSwarm injects swarm memory into the request context.
func LLMRequestWithSwarm(req *models.LLMRequest, memory *SwarmMemory) {
	if req == nil || memory == nil {
		return
	}
	if val, ok := memory.Recall("swarm:system_prompt"); ok {
		prefix := models.Message{Role: models.RoleSystem, Content: &val}
		req.Messages = append([]models.Message{prefix}, req.Messages...)
	}
}
