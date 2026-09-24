package swarm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ConsensusResult represents swarm voting outcome.
type ConsensusResult struct {
	Winner string
	Votes  map[string]int
	// Quorum is the number of valid votes counted.
	Quorum    int
	DecidedAt time.Time
}

// Voter casts a vote for one of the options on a topic. Implementations are
// typically backed by a sub-agent / LLM call and must honor ctx.
type Voter func(ctx context.Context, topic string, options []string) (string, error)

// VoteRequest asks sub-agents to choose among options.
type VoteRequest struct {
	Topic   string
	Options []string
	// Timeout bounds the whole vote (default 5s). Votes arriving later are
	// discarded.
	Timeout time.Duration
	// MinVoters is the minimum number of valid votes required (default 1).
	MinVoters int
	// Voters cast the votes. At least one is required.
	Voters []Voter
}

var (
	// ErrNoVoters is returned when a vote has no voters.
	ErrNoVoters = errors.New("consensus: no voters")
	// ErrNoQuorum is returned when fewer than MinVoters valid votes were cast.
	ErrNoQuorum = errors.New("consensus: quorum not reached")
)

// ConsensusProtocol coordinates voting across active sub-agents.
type ConsensusProtocol struct {
	mu      sync.Mutex
	history []ConsensusResult
}

// maxHistory bounds the retained decision history.
const maxHistory = 1000

// NewConsensusProtocol creates a new consensus coordinator.
func NewConsensusProtocol() *ConsensusProtocol {
	return &ConsensusProtocol{history: make([]ConsensusResult, 0)}
}

// RunVote collects votes concurrently from req.Voters and returns the option
// with the most valid votes. Votes for unknown options, voter errors, panics
// and votes arriving after the timeout are ignored. Ties are broken by the
// order of req.Options. It fails when there are no options or voters, or when
// fewer than MinVoters valid votes were cast.
func (c *ConsensusProtocol) RunVote(ctx context.Context, req VoteRequest) (*ConsensusResult, error) {
	if len(req.Options) == 0 {
		return nil, fmt.Errorf("no options")
	}
	if len(req.Voters) == 0 {
		return nil, ErrNoVoters
	}
	if req.Timeout <= 0 {
		req.Timeout = 5 * time.Second
	}
	minVoters := req.MinVoters
	if minVoters <= 0 {
		minVoters = 1
	}
	valid := make(map[string]bool, len(req.Options))
	for _, o := range req.Options {
		valid[o] = true
	}
	options := append([]string(nil), req.Options...)

	voteCtx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	ballots := make(chan string, len(req.Voters))
	var wg sync.WaitGroup
	for _, voter := range req.Voters {
		if voter == nil {
			continue
		}
		wg.Add(1)
		go func(v Voter) {
			defer wg.Done()
			defer func() { _ = recover() }()
			choice, err := v(voteCtx, req.Topic, append([]string(nil), options...))
			if err != nil || !valid[choice] {
				return
			}
			ballots <- choice // buffered for every voter: never blocks
		}(voter)
	}
	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	votes := make(map[string]int)
	cast := 0
collect:
	for {
		select {
		case choice := <-ballots:
			votes[choice]++
			cast++
		case <-allDone:
			// Drain ballots delivered before the last voter returned.
			for {
				select {
				case choice := <-ballots:
					votes[choice]++
					cast++
				default:
					break collect
				}
			}
		case <-voteCtx.Done():
			break collect
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cast < minVoters {
		return nil, fmt.Errorf("%w: %d valid votes, need %d", ErrNoQuorum, cast, minVoters)
	}

	winner := ""
	best := -1
	for _, o := range req.Options {
		if votes[o] > best {
			winner, best = o, votes[o]
		}
	}
	result := &ConsensusResult{
		Winner:    winner,
		Votes:     votes,
		Quorum:    cast,
		DecidedAt: time.Now().UTC(),
	}
	c.mu.Lock()
	c.history = append(c.history, *result)
	if over := len(c.history) - maxHistory; over > 0 {
		c.history = append([]ConsensusResult(nil), c.history[over:]...)
	}
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
	mu    sync.RWMutex
	facts map[string]string
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
