// Package evolution collects scored improvement proposals and periodically
// emits the best one as an inert proposal file for human review.
//
// Safety contract: the engine NEVER applies, compiles, loads or executes a
// proposal payload. "Applying" a proposal only means writing its payload
// verbatim to a new file under PatchDir (mode 0600) so an operator can
// review it. Config.AutoDeploy is reserved and ignored.
package evolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// MaxPayloadBytes is the maximum size of a proposal payload.
	MaxPayloadBytes = 1 << 20

	// maxTypeLen is the maximum length of a sanitized proposal type.
	maxTypeLen = 32

	// maxAppliedIDs bounds the memory used to remember applied proposals.
	maxAppliedIDs = 10000
)

var (
	// ErrInvalidProposal is returned by Submit for proposals that are
	// rejected (non-finite score, oversize payload, nil engine).
	ErrInvalidProposal = errors.New("evolution: invalid proposal")

	// ErrAlreadyApplied is returned by Submit for a proposal ID that has
	// already been written out.
	ErrAlreadyApplied = errors.New("evolution: proposal already applied")
)

// Config configures the self-evolution engine.
type Config struct {
	Interval   time.Duration
	PatchDir   string
	MaxPending int
	// AutoDeploy is reserved and ignored: the engine only writes proposal
	// files for human review and never deploys anything.
	AutoDeploy bool
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

// Stats summarizes engine activity.
type Stats struct {
	Pending       int
	Applied       int
	LastPatchPath string
	LastError     string
}

// Engine evaluates candidate improvements and emits proposal files.
type Engine struct {
	mu           sync.Mutex
	cfg          Config
	pending      []ImprovementProposal
	applied      map[string]struct{}
	appliedOrder []string
	patchesDir   string
	running      bool
	evaluating   bool
	lastPath     string
	lastErr      error
}

var idSeq atomic.Uint64

// NewEngine creates a new self-evolution engine.
func NewEngine(cfg Config) *Engine {
	def := DefaultConfig()
	if cfg.Interval <= 0 {
		cfg.Interval = def.Interval
	}
	if cfg.PatchDir == "" {
		cfg.PatchDir = def.PatchDir
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = def.MaxPending
	}
	return &Engine{
		cfg:        cfg,
		pending:    make([]ImprovementProposal, 0),
		applied:    make(map[string]struct{}),
		patchesDir: cfg.PatchDir,
	}
}

// Start runs the background evaluation loop until ctx is canceled. Calling
// Start while it is already running is a no-op.
func (e *Engine) Start(ctx context.Context) {
	if e == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	if e.patchesDir == "" {
		e.patchesDir = DefaultConfig().PatchDir
	}
	interval := e.cfg.Interval
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.running = false
		e.mu.Unlock()
	}()
	if interval <= 0 {
		interval = DefaultConfig().Interval
	}

	ticker := time.NewTicker(interval)
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

// evaluate writes the highest scoring not-yet-applied proposal to PatchDir
// and removes it from the pending queue. Other proposals stay pending.
func (e *Engine) evaluate(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	e.mu.Lock()
	if e.evaluating {
		e.mu.Unlock()
		return nil
	}
	// Drop proposals that were already applied and pick the best of the rest
	// (first one wins ties, so selection is deterministic).
	kept := e.pending[:0]
	bestIdx := -1
	for _, p := range e.pending {
		if _, done := e.applied[p.ID]; done {
			continue
		}
		kept = append(kept, p)
		if bestIdx < 0 || p.Score > kept[bestIdx].Score {
			bestIdx = len(kept) - 1
		}
	}
	clear(e.pending[len(kept):])
	e.pending = kept
	if bestIdx < 0 {
		e.mu.Unlock()
		return nil
	}
	best := e.pending[bestIdx]
	dir := e.patchesDir
	e.evaluating = true
	e.mu.Unlock()

	path, err := writeProposal(dir, &best)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.evaluating = false
	if err != nil {
		// Keep the proposal pending so the next tick retries.
		e.lastErr = err
		return err
	}
	e.lastErr = nil
	e.lastPath = path
	e.markAppliedLocked(best.ID)
	for i := range e.pending {
		if e.pending[i].ID == best.ID {
			e.pending = append(e.pending[:i], e.pending[i+1:]...)
			break
		}
	}
	return nil
}

func (e *Engine) markAppliedLocked(id string) {
	if _, ok := e.applied[id]; ok {
		return
	}
	e.applied[id] = struct{}{}
	e.appliedOrder = append(e.appliedOrder, id)
	if len(e.appliedOrder) > maxAppliedIDs {
		oldest := e.appliedOrder[0]
		e.appliedOrder = e.appliedOrder[1:]
		delete(e.applied, oldest)
	}
}

// writeProposal writes the payload to a new file inside dir. The file name
// is built only from sanitized components and the write goes through an
// os.Root so it can never escape dir.
func writeProposal(dir string, p *ImprovementProposal) (string, error) {
	if dir == "" {
		return "", errors.New("evolution: no patch directory configured")
	}
	if len(p.Payload) > MaxPayloadBytes {
		return "", fmt.Errorf("%w: payload exceeds %d bytes", ErrInvalidProposal, MaxPayloadBytes)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("evolution: creating patch dir: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return "", fmt.Errorf("evolution: opening patch dir: %w", err)
	}
	defer root.Close()

	name := fmt.Sprintf("evolution-%s-%d-%s.patch", sanitizeType(p.Type), time.Now().UTC().UnixNano(), shortID(p.ID))
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("evolution: creating proposal file: %w", err)
	}
	if _, err := f.Write(p.Payload); err != nil {
		_ = f.Close()
		_ = root.Remove(name)
		return "", fmt.Errorf("evolution: writing proposal file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = root.Remove(name)
		return "", fmt.Errorf("evolution: closing proposal file: %w", err)
	}
	return filepath.Join(dir, name), nil
}

// sanitizeType maps a proposal type to [a-z0-9_-]{1,32}.
func sanitizeType(t string) string {
	var sb strings.Builder
	for _, r := range strings.ToLower(t) {
		if sb.Len() >= maxTypeLen {
			break
		}
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			sb.WriteRune(r)
		}
	}
	if sb.Len() == 0 {
		return "proposal"
	}
	return sb.String()
}

// shortID returns up to 12 alphanumeric characters of id for file names.
func shortID(id string) string {
	var sb strings.Builder
	for _, r := range id {
		if sb.Len() >= 12 {
			break
		}
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
		}
	}
	if sb.Len() == 0 {
		return "noid"
	}
	return sb.String()
}

// Submit adds a candidate improvement proposal. Proposals with a non-finite
// score or a payload larger than MaxPayloadBytes are rejected, as are IDs
// that were already applied. A pending proposal with the same ID is
// replaced. When the queue is full the oldest pending proposal is dropped.
func (e *Engine) Submit(p ImprovementProposal) error {
	if e == nil {
		return fmt.Errorf("%w: engine is nil", ErrInvalidProposal)
	}
	if math.IsNaN(p.Score) || math.IsInf(p.Score, 0) {
		return fmt.Errorf("%w: score must be finite", ErrInvalidProposal)
	}
	if len(p.Payload) > MaxPayloadBytes {
		return fmt.Errorf("%w: payload exceeds %d bytes", ErrInvalidProposal, MaxPayloadBytes)
	}
	p.Payload = append([]byte(nil), p.Payload...)
	e.mu.Lock()
	defer e.mu.Unlock()
	if p.ID == "" {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d", p.Description, time.Now().UTC().Format(time.RFC3339Nano), idSeq.Add(1))))
		p.ID = hex.EncodeToString(sum[:])
	}
	if _, done := e.applied[p.ID]; done {
		return ErrAlreadyApplied
	}
	p.CreatedAt = time.Now().UTC()
	for i := range e.pending {
		if e.pending[i].ID == p.ID {
			e.pending[i] = p
			return nil
		}
	}
	maxPending := e.cfg.MaxPending
	if maxPending <= 0 {
		maxPending = DefaultConfig().MaxPending
	}
	for len(e.pending) >= maxPending {
		e.pending[0] = ImprovementProposal{}
		e.pending = e.pending[1:]
	}
	e.pending = append(e.pending, p)
	return nil
}

// Pending returns deep copies of the current candidate proposals.
func (e *Engine) Pending() []ImprovementProposal {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]ImprovementProposal, len(e.pending))
	for i, p := range e.pending {
		p.Payload = append([]byte(nil), p.Payload...)
		out[i] = p
	}
	return out
}

// Applied returns the IDs of proposals written out, oldest first (bounded).
func (e *Engine) Applied() []string {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.appliedOrder...)
}

// Stats returns a snapshot of engine activity.
func (e *Engine) Stats() Stats {
	if e == nil {
		return Stats{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s := Stats{Pending: len(e.pending), Applied: len(e.appliedOrder), LastPatchPath: e.lastPath}
	if e.lastErr != nil {
		s.LastError = e.lastErr.Error()
	}
	return s
}
