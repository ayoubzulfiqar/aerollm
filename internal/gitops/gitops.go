// Package gitops serves prompt templates from a git repository.
//
// A gitPromptStore keeps a managed mirror of one branch of a repository in
// LocalPath (it is reset hard to the remote branch and cleaned on every
// sync — do not point it at a working copy you care about) and serves the
// JSON files under <LocalPath>/prompts from an in-memory snapshot that is
// swapped atomically after each successful sync. Reads never touch git and
// are never blocked by network operations.
//
// Security properties:
//   - git runs without a shell, with separate arguments, "--" before
//     positional arguments, a timeout, GIT_TERMINAL_PROMPT=0 and
//     GIT_ALLOW_PROTOCOL restricted to https, ssh, git and file (so remote
//     helpers such as ext:: can never run).
//   - the branch and repository URL are validated to prevent option
//     injection ("--upload-pack=...", "-oProxyCommand=...").
//   - template versions are looked up in a map built from a directory scan;
//     user input is never joined into a filesystem path. Symlinks and
//     non-regular files are skipped and reads are confined with os.Root.
//   - credentials embedded in the repository URL are redacted from errors.
package gitops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	// DefaultPollInterval is used when no positive poll interval is given.
	DefaultPollInterval = 2 * time.Minute
	// DefaultGitTimeout bounds every git invocation.
	DefaultGitTimeout = 2 * time.Minute
	// DefaultBranch is used when no branch is configured.
	DefaultBranch = "main"
	// MaxPromptBytes caps the size of a single prompt file.
	MaxPromptBytes = 1 << 20
	// MaxPromptFiles caps the number of prompt files loaded per snapshot.
	MaxPromptFiles = 1000
	// PromptsDir is the directory inside the repository holding templates.
	PromptsDir = "prompts"
	// allowedProtocols is exported to git via GIT_ALLOW_PROTOCOL.
	allowedProtocols = "https:ssh:git:file"
)

// Errors returned by the store.
var (
	ErrInvalidVersion  = errors.New("gitops: invalid prompt version")
	ErrPromptNotFound  = errors.New("gitops: prompt not found")
	ErrInvalidBranch   = errors.New("gitops: invalid branch")
	ErrInvalidRepoURL  = errors.New("gitops: invalid repository URL")
	ErrInvalidPath     = errors.New("gitops: invalid local path")
	ErrTooManyPrompts  = fmt.Errorf("gitops: more than %d prompt files", MaxPromptFiles)
	ErrNoPromptsLoaded = errors.New("gitops: no prompts available")
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

// GitConfig configures a git-backed prompt store.
type GitConfig struct {
	// RepoURL is the remote (https://, ssh://, git://, file:// or scp-like
	// user@host:path). Empty means local mode: LocalPath is served as-is and
	// Sync only reloads it from disk.
	RepoURL string
	// LocalPath is the managed mirror directory.
	LocalPath string
	// Branch to track (default "main").
	Branch string
	// PollInterval between syncs started by Start (default 2m).
	PollInterval time.Duration
	// Timeout for each git invocation (default 2m).
	Timeout time.Duration
	// GitBinary overrides the git executable (default "git" from PATH).
	GitBinary string
}

// GitPromptStore is the exported name of the git-backed prompt store.
type GitPromptStore = gitPromptStore

// gitPromptStore watches a git repository for prompt templates.
type gitPromptStore struct {
	repoURL   string
	localPath string
	branch    string
	interval  time.Duration
	timeout   time.Duration
	gitBin    string
	cfgErr    error

	syncMu sync.Mutex // serialises git operations and snapshot loads

	mu          sync.RWMutex // guards the fields below
	knownCommit string
	snap        *snapshot
	lastSync    time.Time
	lastErr     error
}

type snapshot struct {
	commit    string
	templates map[string]*PromptTemplate
	invalid   map[string]error
	versions  []string // valid versions, natural order
}

// NewGitPromptStore creates a new git-backed prompt store. The signature is
// kept for compatibility: an invalid configuration does not panic, instead
// Validate and every Sync report the validation error.
func NewGitPromptStore(repoURL, localPath, branch string, pollInterval time.Duration) *gitPromptStore {
	g, _ := NewGitPromptStoreFromConfig(GitConfig{
		RepoURL:      repoURL,
		LocalPath:    localPath,
		Branch:       branch,
		PollInterval: pollInterval,
	})
	return g
}

// NewGitPromptStoreFromConfig creates a store and returns the configuration
// validation error, if any, alongside it.
func NewGitPromptStoreFromConfig(cfg GitConfig) (*gitPromptStore, error) {
	g := &gitPromptStore{
		repoURL:  cfg.RepoURL,
		branch:   cfg.Branch,
		interval: cfg.PollInterval,
		timeout:  cfg.Timeout,
		gitBin:   cfg.GitBinary,
	}
	if g.interval <= 0 {
		g.interval = DefaultPollInterval
	}
	if g.timeout <= 0 {
		g.timeout = DefaultGitTimeout
	}
	if g.gitBin == "" {
		g.gitBin = "git"
	}
	if g.branch == "" {
		g.branch = DefaultBranch
	}
	var errs []error
	if cfg.LocalPath == "" {
		errs = append(errs, fmt.Errorf("%w: empty", ErrInvalidPath))
	} else if hasControl(cfg.LocalPath) {
		errs = append(errs, fmt.Errorf("%w: contains control characters", ErrInvalidPath))
	} else if abs, err := filepath.Abs(cfg.LocalPath); err != nil {
		errs = append(errs, fmt.Errorf("%w: %v", ErrInvalidPath, err))
	} else {
		g.localPath = abs
	}
	if err := ValidateBranch(g.branch); err != nil {
		errs = append(errs, err)
	}
	if g.repoURL != "" {
		if err := ValidateRepoURL(g.repoURL); err != nil {
			errs = append(errs, err)
		}
	}
	g.cfgErr = errors.Join(errs...)
	return g, g.cfgErr
}

// Validate reports whether the store configuration is usable.
func (g *gitPromptStore) Validate() error { return g.cfgErr }

var (
	versionPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	// A "<transport>::<address>" remote invokes git-remote-<transport>
	// (e.g. ext:: runs arbitrary commands).
	remoteHelperPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*::`)
	scpLikePattern      = regexp.MustCompile(`^(?:([A-Za-z0-9._-]+)@)?([A-Za-z0-9.-]+|\[[0-9A-Fa-f:.]+\]):(.+)$`)
)

// ValidateBranch applies the rules of `git check-ref-format --branch`, plus
// a rejection of leading dashes so the value can never be parsed as an
// option.
func ValidateBranch(b string) error {
	bad := func(why string) error { return fmt.Errorf("%w %q: %s", ErrInvalidBranch, b, why) }
	switch {
	case b == "":
		return bad("empty")
	case len(b) > 255:
		return bad("too long")
	case b == "@":
		return bad(`"@" is not a valid branch`)
	case strings.HasPrefix(b, "-"):
		return bad("must not start with '-'")
	case strings.HasPrefix(b, "/") || strings.HasSuffix(b, "/"):
		return bad("must not start or end with '/'")
	case strings.HasSuffix(b, "."):
		return bad("must not end with '.'")
	case strings.Contains(b, ".."), strings.Contains(b, "//"), strings.Contains(b, "@{"):
		return bad(`must not contain "..", "//" or "@{"`)
	}
	for _, r := range b {
		if r < 0x20 || r == 0x7f || r == ' ' || strings.ContainsRune("~^:?*[\\", r) {
			return bad("contains a forbidden character")
		}
	}
	for _, comp := range strings.Split(b, "/") {
		if strings.HasPrefix(comp, ".") || strings.HasSuffix(comp, ".lock") {
			return bad(`path components must not start with '.' or end with ".lock"`)
		}
	}
	return nil
}

// ValidateRepoURL accepts https://, ssh://, git:// and file:// URLs and
// scp-like "user@host:path" remotes, and rejects anything git could
// interpret as an option or a remote helper invocation.
func ValidateRepoURL(raw string) error {
	bad := func(why string) error { return fmt.Errorf("%w: %s", ErrInvalidRepoURL, why) }
	switch {
	case raw == "":
		return bad("empty")
	case len(raw) > 2048:
		return bad("too long")
	case hasControl(raw) || strings.ContainsAny(raw, " \t"):
		return bad("contains whitespace or control characters")
	case strings.HasPrefix(raw, "-"):
		return bad("must not start with '-'")
	case remoteHelperPattern.MatchString(raw):
		return bad("remote helper (<transport>::) syntax is not allowed")
	}
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return bad("unparseable URL")
		}
		switch u.Scheme {
		case "https", "ssh", "git":
			host := u.Hostname()
			if host == "" {
				return bad("missing host")
			}
			if strings.HasPrefix(host, "-") || (u.User != nil && strings.HasPrefix(u.User.Username(), "-")) {
				return bad("host and user must not start with '-'")
			}
		case "file":
			if u.Host != "" && u.Host != "localhost" {
				return bad("file URLs must not name a remote host")
			}
			if !strings.HasPrefix(u.Path, "/") {
				return bad("file URLs need an absolute path")
			}
		default:
			return bad(fmt.Sprintf("scheme %q is not allowed (use https, ssh, git or file)", u.Scheme))
		}
		return nil
	}
	m := scpLikePattern.FindStringSubmatch(raw)
	if m == nil {
		return bad("expected an https/ssh/git/file URL or user@host:path")
	}
	if strings.HasPrefix(m[1], "-") || strings.HasPrefix(m[2], "-") {
		return bad("host and user must not start with '-'")
	}
	return nil
}

// ValidateVersion checks a prompt version name.
func ValidateVersion(v string) error {
	if !versionPattern.MatchString(v) || v == "." || v == ".." {
		return fmt.Errorf("%w: %q", ErrInvalidVersion, v)
	}
	return nil
}

// Start begins the background polling loop: one sync immediately, then one
// per poll interval until ctx is cancelled. Errors are recorded and
// available through LastSyncError.
func (g *gitPromptStore) Start(ctx context.Context) {
	go func() {
		_ = g.Sync(ctx)
		ticker := time.NewTicker(g.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = g.Sync(ctx)
			}
		}
	}()
}

// LastSync returns the time of the last successful sync.
func (g *gitPromptStore) LastSync() time.Time {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.lastSync
}

// LastSyncError returns the error of the most recent sync (nil on success).
func (g *gitPromptStore) LastSyncError() error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.lastErr
}

// Commit returns the HEAD commit of the last successful sync.
func (g *gitPromptStore) Commit() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.knownCommit
}

// sync is kept for callers of the original unexported API.
func (g *gitPromptStore) sync() error { return g.Sync(context.Background()) }

// Sync clones or updates the mirror, then atomically swaps in a new prompt
// snapshot. On failure the previous snapshot keeps being served.
func (g *gitPromptStore) Sync(ctx context.Context) error {
	err := g.doSync(ctx)
	g.mu.Lock()
	g.lastErr = err
	if err == nil {
		g.lastSync = time.Now()
	}
	g.mu.Unlock()
	return err
}

func (g *gitPromptStore) doSync(ctx context.Context) error {
	if g.cfgErr != nil {
		return g.cfgErr
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g.syncMu.Lock()
	defer g.syncMu.Unlock()

	commit := ""
	if g.repoURL != "" {
		var err error
		if commit, err = g.updateMirror(ctx); err != nil {
			return err
		}
	}
	snap, err := loadSnapshot(g.localPath, commit)
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.snap = snap
	if commit != "" {
		g.knownCommit = commit
	}
	g.mu.Unlock()
	return nil
}

// updateMirror clones the branch if needed, otherwise fetches it and resets
// the mirror to it. Returns the resulting HEAD commit.
func (g *gitPromptStore) updateMirror(ctx context.Context) (string, error) {
	if err := os.MkdirAll(g.localPath, 0o700); err != nil {
		return "", fmt.Errorf("gitops: create %s: %w", g.localPath, err)
	}
	remoteRef := "refs/remotes/origin/" + g.branch
	if _, err := os.Lstat(filepath.Join(g.localPath, ".git")); errors.Is(err, os.ErrNotExist) {
		if _, err := g.git(ctx, "", "clone", "--branch", g.branch, "--single-branch", "--no-tags", "--", g.repoURL, g.localPath); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", fmt.Errorf("gitops: stat checkout: %w", err)
	} else {
		// Fetch from the configured URL directly so a changed RepoURL takes
		// effect even though the checkout's origin still names the old one.
		if _, err := g.git(ctx, g.localPath, "fetch", "--no-tags", "--", g.repoURL, "+refs/heads/"+g.branch+":"+remoteRef); err != nil {
			return "", err
		}
		if _, err := g.git(ctx, g.localPath, "reset", "--hard", remoteRef, "--"); err != nil {
			return "", err
		}
		if _, err := g.git(ctx, g.localPath, "clean", "-ffdx"); err != nil {
			return "", err
		}
	}
	out, err := g.git(ctx, g.localPath, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", err
	}
	commit := strings.TrimSpace(out)
	if len(commit) < 40 || strings.Trim(commit, "0123456789abcdef") != "" {
		return "", fmt.Errorf("gitops: unexpected rev-parse output %q", commit)
	}
	return commit, nil
}

// git runs one git command with a timeout, no shell and a locked-down
// environment. Output in errors is truncated and has credentials redacted.
func (g *gitPromptStore) git(ctx context.Context, dir string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	full := append([]string{"-c", "protocol.ext.allow=never", "-c", "core.fsmonitor=false"}, args...)
	cmd := exec.CommandContext(ctx, g.gitBin, full...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ALLOW_PROTOCOL="+allowedProtocols,
		"GIT_ASKPASS=",
		"SSH_ASKPASS=",
	)
	if os.Getenv("GIT_SSH_COMMAND") == "" {
		// Never block on interactive ssh prompts (passwords, host keys).
		cmd.Env = append(cmd.Env, "GIT_SSH_COMMAND=ssh -o BatchMode=yes")
	}
	cmd.WaitDelay = 5 * time.Second
	var buf bytes.Buffer
	cmd.Stdout = &limitedWriter{w: &buf, n: 64 << 10}
	cmd.Stderr = cmd.Stdout
	err := cmd.Run()
	out := buf.String()
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%w (timeout %s)", ctx.Err(), g.timeout)
		}
		msg := strings.TrimSpace(out)
		if len(msg) > 1024 {
			msg = msg[:1024] + "..."
		}
		return "", fmt.Errorf("gitops: git %s failed: %s: %s", args[0], g.redact(msg), g.redact(err.Error()))
	}
	return out, nil
}

var userinfoPattern = regexp.MustCompile(`://[^/@\s]+@`)

// redact strips credentials embedded in the repository URL from s.
func (g *gitPromptStore) redact(s string) string {
	if u, err := url.Parse(g.repoURL); err == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok && pw != "" {
			s = strings.ReplaceAll(s, pw, "***")
		}
		if name := u.User.Username(); name != "" {
			s = strings.ReplaceAll(s, name+"@", "***@")
		}
	}
	return userinfoPattern.ReplaceAllString(s, "://***@")
}

type limitedWriter struct {
	w io.Writer
	n int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if l.n <= 0 {
		return len(p), nil
	}
	chunk := p
	if len(chunk) > l.n {
		chunk = chunk[:l.n]
	}
	l.n -= len(chunk)
	if _, err := l.w.Write(chunk); err != nil {
		return 0, err
	}
	return len(p), nil
}

// loadSnapshot reads <localPath>/prompts/*.json into memory. Reads are
// confined to localPath with os.Root; symlinks, non-regular files and
// oversized files are skipped.
func loadSnapshot(localPath, commit string) (*snapshot, error) {
	root, err := os.OpenRoot(localPath)
	if err != nil {
		return nil, fmt.Errorf("gitops: open %s: %w", localPath, err)
	}
	defer root.Close()
	fi, err := root.Lstat(PromptsDir)
	if err != nil {
		return nil, fmt.Errorf("gitops: %s directory: %w", PromptsDir, err)
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return nil, fmt.Errorf("gitops: %s must be a real directory, not a symlink or file", PromptsDir)
	}
	dir, err := root.Open(PromptsDir)
	if err != nil {
		return nil, fmt.Errorf("gitops: open %s: %w", PromptsDir, err)
	}
	entries, err := dir.ReadDir(-1)
	dir.Close()
	if err != nil {
		return nil, fmt.Errorf("gitops: read %s: %w", PromptsDir, err)
	}
	snap := &snapshot{
		commit:    commit,
		templates: make(map[string]*PromptTemplate),
		invalid:   make(map[string]error),
	}
	count := 0
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) != ".json" || !e.Type().IsRegular() {
			continue // skips directories, symlinks, devices, non-JSON
		}
		version := strings.TrimSuffix(name, ".json")
		if ValidateVersion(version) != nil {
			continue
		}
		count++
		if count > MaxPromptFiles {
			return nil, ErrTooManyPrompts
		}
		rel := PromptsDir + "/" + name
		raw, err := readRegular(root, rel)
		if err != nil {
			snap.invalid[version] = err
			continue
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
			snap.invalid[version] = fmt.Errorf("gitops: prompt %s is not a JSON object", version)
			continue
		}
		snap.templates[version] = &PromptTemplate{
			Version: version,
			Commit:  commit,
			Path:    filepath.Join(localPath, PromptsDir, name),
			Payload: payload,
			Raw:     raw,
		}
		snap.versions = append(snap.versions, version)
	}
	sort.Slice(snap.versions, func(i, j int) bool { return naturalLess(snap.versions[i], snap.versions[j]) })
	return snap, nil
}

// readRegular reads a regular file of at most MaxPromptBytes, refusing
// anything that was swapped for a different file after the directory scan.
func readRegular(root *os.Root, rel string) ([]byte, error) {
	before, err := root.Lstat(rel)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, fmt.Errorf("gitops: %s is not a regular file", rel)
	}
	if before.Size() > MaxPromptBytes {
		return nil, fmt.Errorf("gitops: %s exceeds %d bytes", rel, MaxPromptBytes)
	}
	f, err := root.Open(rel)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, after) {
		return nil, fmt.Errorf("gitops: %s changed while reading", rel)
	}
	raw, err := io.ReadAll(io.LimitReader(f, MaxPromptBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxPromptBytes {
		return nil, fmt.Errorf("gitops: %s exceeds %d bytes", rel, MaxPromptBytes)
	}
	return raw, nil
}

// naturalLess orders strings so that embedded numbers compare numerically
// ("v2" < "v10", "1.2.9" < "1.2.10").
func naturalLess(a, b string) bool {
	for a != "" && b != "" {
		ca, ra := chunk(a)
		cb, rb := chunk(b)
		if ca != cb {
			da, db := isDigit(ca[0]), isDigit(cb[0])
			switch {
			case da && db:
				na, nb := strings.TrimLeft(ca, "0"), strings.TrimLeft(cb, "0")
				if len(na) != len(nb) {
					return len(na) < len(nb)
				}
				if na != nb {
					return na < nb
				}
				return len(ca) < len(cb)
			default:
				return ca < cb
			}
		}
		a, b = ra, rb
	}
	return len(a) < len(b)
}

func chunk(s string) (string, string) {
	d := isDigit(s[0])
	i := 1
	for i < len(s) && isDigit(s[i]) == d {
		i++
	}
	return s[:i], s[i:]
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func hasControl(s string) bool {
	for _, r := range s {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}

// current returns the active snapshot, lazily loading it from disk when no
// sync has happened yet (so an existing checkout works without git).
func (g *gitPromptStore) current() (*snapshot, error) {
	g.mu.RLock()
	snap := g.snap
	g.mu.RUnlock()
	if snap != nil {
		return snap, nil
	}
	if g.localPath == "" {
		return nil, g.cfgErr
	}
	g.syncMu.Lock()
	defer g.syncMu.Unlock()
	g.mu.RLock()
	snap, commit := g.snap, g.knownCommit
	g.mu.RUnlock()
	if snap != nil {
		return snap, nil
	}
	snap, err := loadSnapshot(g.localPath, commit)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.snap == nil {
		g.snap = snap
	}
	snap = g.snap
	g.mu.Unlock()
	return snap, nil
}

// Get returns the template for the given version. "" and "latest" resolve
// to latest.json when present, otherwise to the highest version in natural
// order. The returned template is a private copy.
func (g *gitPromptStore) Get(version string) (*PromptTemplate, error) {
	if version != "" && version != "latest" {
		if err := ValidateVersion(version); err != nil {
			return nil, err
		}
	}
	snap, err := g.current()
	if err != nil {
		return nil, err
	}
	if version == "" || version == "latest" {
		if _, ok := snap.templates["latest"]; ok {
			version = "latest"
		} else if n := len(snap.versions); n > 0 {
			version = snap.versions[n-1]
		} else {
			return nil, ErrNoPromptsLoaded
		}
	}
	t, ok := snap.templates[version]
	if !ok {
		if err := snap.invalid[version]; err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", ErrPromptNotFound, version)
	}
	cp := *t
	cp.Raw = append([]byte(nil), t.Raw...)
	if err := json.Unmarshal(cp.Raw, &cp.Payload); err != nil {
		return nil, fmt.Errorf("gitops: parse prompt %s: %w", version, err)
	}
	return &cp, nil
}

// List returns the loadable prompt versions in natural order.
func (g *gitPromptStore) List() ([]string, error) {
	snap, err := g.current()
	if err != nil {
		return nil, err
	}
	return append([]string(nil), snap.versions...), nil
}

// Checksum computes sha256 of the raw payload.
func (p *PromptTemplate) Checksum() string {
	sum := sha256.Sum256(p.Raw)
	return hex.EncodeToString(sum[:])
}
