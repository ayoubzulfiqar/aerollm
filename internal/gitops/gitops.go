package gitops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// PromptTemplate represents a parsed prompt template.
type PromptTemplate struct {
	Version string
	Commit  string
	Path    string
	Payload map[string]interface{}
	Raw     []byte
}

// PromptStore provides access to prompt templates.
type PromptStore interface {
	Get(version string) (*PromptTemplate, error)
	List() ([]string, error)
}

// gitPromptStore watches a git repository for prompt templates.
type gitPromptStore struct {
	repoURL     string
	localPath   string
	branch      string
	interval    time.Duration
	knownCommit string
	mu          sync.RWMutex
}

// NewGitPromptStore creates a new git-backed prompt store.
func NewGitPromptStore(repoURL, localPath, branch string, pollInterval time.Duration) *gitPromptStore {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Minute
	}
	return &gitPromptStore{repoURL: repoURL, localPath: localPath, branch: branch, interval: pollInterval}
}

// Start begins the background polling loop.
func (g *gitPromptStore) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(g.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = g.sync()
			}
		}
	}()
}

// sync clones or pulls the repository and updates the known commit.
func (g *gitPromptStore) sync() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := os.MkdirAll(g.localPath, 0o700); err != nil {
		return err
	}

	if _, err := os.Stat(filepath.Join(g.localPath, ".git")); os.IsNotExist(err) {
		cmd := exec.Command("git", "clone", "--branch", g.branch, "--single-branch", g.repoURL, g.localPath)
		cmd.Dir = g.localPath
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("git clone failed: %s: %w", strings.TrimSpace(string(out)), err)
		}
	} else {
		cmd := exec.Command("git", "pull", "origin", g.branch)
		cmd.Dir = g.localPath
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("git pull failed: %s: %w", strings.TrimSpace(string(out)), err)
		}
	}

	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = g.localPath
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git rev-parse failed: %s: %w", strings.TrimSpace(string(out)), err)
	}
	g.knownCommit = strings.TrimSpace(string(out))
	return nil
}

// Get returns the template for the given version/commit.
func (g *gitPromptStore) Get(version string) (*PromptTemplate, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if version == "" || version == "latest" {
		version = g.knownCommit
	}
	base := filepath.Join(g.localPath, "prompts")
	path := filepath.Join(base, version+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read prompt %s: %w", version, err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("parse prompt %s: %w", version, err)
	}
	return &PromptTemplate{Version: version, Commit: g.knownCommit, Path: path, Payload: payload, Raw: raw}, nil
}

// List returns known prompt versions from the repo.
func (g *gitPromptStore) List() ([]string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	base := filepath.Join(g.localPath, "prompts")
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	var versions []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch filepath.Ext(name) {
		case ".json", ".yaml", ".yml":
			versions = append(versions, strings.TrimSuffix(name, filepath.Ext(name)))
		}
	}
	return versions, nil
}

// Checksum computes sha256 of the raw payload.
func (p *PromptTemplate) Checksum() string {
	sum := sha256.Sum256(p.Raw)
	return hex.EncodeToString(sum[:])
}
