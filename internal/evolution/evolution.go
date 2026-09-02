package evolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Config configures the self-evolution engine.
type Config struct {
	Interval         time.Duration
	PatchDir         string
	MaxPending       int
	AutoDeploy       bool
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Interval:   30 * time.Minute,
		PatchDir:   "evolution-patches",
		MaxPending: 10,
		AutoDeploy: false,
	}
}

// ImprovementProposal represents a candidate code or prompt improvement.
type ImprovementProposal struct {
	ID          string
	Type        string
	Description string
	Payload     []byte
	Score       float64
	CreatedAt   time.Time
}

// Engine evaluates candidate improvements and emits safe deployable patches.
type Engine struct {
	mu        sync.Mutex
	cfg       Config
	pending   []ImprovementProposal
	applied   map[string]struct{}
	patchesDir string
}

// NewEngine creates a new self-evolution engine.
func NewEngine(cfg Config) *Engine {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultConfig().Interval
	}
	if cfg.PatchDir == "" {
		cfg.PatchDir = DefaultConfig().PatchDir
	}
	return &Engine{
		cfg:       cfg,
		pending:   make([]ImprovementProposal, 0),
		applied:   make(map[string]struct{}),
		patchesDir: cfg.PatchDir,
	}
}

// Start begins the background evaluation loop.
func (e *Engine) Start(ctx context.Context) {
	if e == nil {
		return
	}
	e.mu.Lock()
	if e.patchesDir == "" {
		e.patchesDir = DefaultConfig().PatchDir
	}
	e.mu.Unlock()
	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = e.evaluate(ctx)
		}
	}
}

func (e *Engine) evaluate(ctx context.Context) error {
	_ = ctx
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.pending) == 0 {
		return nil
	}
	var best *ImprovementProposal
	for i := range e.pending {
		p := &e.pending[i]
		if best == nil || p.Score > best.Score {
			best = p
		}
	}
	if best == nil {
		return nil
	}
	if _, done := e.applied[best.ID]; done {
		return nil
	}
	if err := e.writePatch(best); err != nil {
		return err
	}
	e.applied[best.ID] = struct{}{}
	e.pending = nil
	return nil
}

func (e *Engine) writePatch(proposal *ImprovementProposal) error {
	if e.patchesDir == "" {
		return nil
	}
	if err := os.MkdirAll(e.patchesDir, 0o755); err != nil {
		return err
	}
	name := fmt.Sprintf("evolution-%s-%d.patch", proposal.Type, time.Now().UTC().UnixNano())
	path := filepath.Join(e.patchesDir, name)
	return os.WriteFile(path, proposal.Payload, 0o644)
}

// Submit adds a candidate improvement proposal.
func (e *Engine) Submit(p ImprovementProposal) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if p.ID == "" {
		sum := sha256.Sum256([]byte(p.Description + time.Now().String()))
		p.ID = hex.EncodeToString(sum[:])
	}
	p.CreatedAt = time.Now().UTC()
	if len(e.pending) >= e.cfg.MaxPending {
		e.pending = e.pending[1:]
	}
	e.pending = append(e.pending, p)
}

// Pending returns current candidate proposals.
func (e *Engine) Pending() []ImprovementProposal {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]ImprovementProposal, len(e.pending))
	copy(out, e.pending)
	return out
}
