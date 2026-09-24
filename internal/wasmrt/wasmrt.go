// Package wasmrt runs untrusted WebAssembly modules as sandboxed pure
// functions on top of wazero (a pure-Go, zero-dependency engine).
//
// # Execution model
//
// A module is a WASI preview1 "command": it exports "_start" (and normally
// "memory") and may import only functions from "wasi_snapshot_preview1".
// Each Run instantiates a fresh module instance, writes the input to its
// stdin, calls "_start", and returns what it wrote to stdout. Exit code 0
// (or returning from "_start") is success; any other exit code is an
// *ExitError carrying the captured, size-capped stderr. Go programs built
// with GOOS=wasip1 GOARCH=wasm, Rust wasm32-wasip1 and wasi-sdk C programs
// all fit this model.
//
// # Security properties
//
//   - No filesystem: no directories are pre-opened, so path_open and friends
//     fail. No sockets: no listeners are configured. No host imports other
//     than WASI preview1 are linked; modules importing anything else are
//     rejected at compile time.
//   - No ambient environment: argv is ["module"] and the environment is empty
//     unless RunOptions supplies explicit values.
//   - Deterministic clocks and randomness by default: the wall and monotonic
//     clocks are wazero's fake clocks (they advance 1ms per read), sleeps
//     return immediately and random_get is a fixed-seed deterministic source.
//     This keeps modules reproducible pure functions and denies them
//     high-resolution timers useful for side-channel attacks. Config.AllowClock
//     and Config.AllowRandom opt into the real clock (with sleeps bounded by
//     the run deadline) and crypto/rand respectively.
//   - Memory: every linear memory is capped at Config.MaxMemoryPages (64 KiB
//     pages); RunOptions.MaxMemoryPages can lower the cap per run. Growth past
//     the cap fails inside the guest (memory.grow returns -1) and a run that
//     then fails is reported as ErrMemoryLimit. wazero additionally enforces a
//     hard ceiling of 2x the cap; a single grow request beyond that ceiling
//     is also refused but surfaces as the guest's own failure (e.g. ErrTrap).
//   - CPU: every run has a wall-clock deadline enforced by wazero's
//     WithCloseOnContextDone, which interrupts even tight loops. Close and
//     caller cancellation interrupt runs the same way.
//   - Sizes: module bytes, stdin, stdout and stderr are all capped. Exceeding
//     the stdout cap aborts the run with ErrOutputTooLarge; stderr is
//     silently truncated.
//   - Concurrency: at most Config.MaxConcurrent compilations/runs execute at
//     once; callers wait at most Config.QueueTimeout for a slot (ErrBusy).
//   - Isolation: each run gets a fresh instance with its own memory, and the
//     guest runs with a context that carries none of the caller's values, so
//     caller contexts cannot smuggle wazero experimental hooks (listeners,
//     socket configs, allocators) into a guest. Host-side panics are
//     recovered and reported as ErrPanic.
//
// Compiled modules are cached per Runtime in a bounded LRU keyed by the
// SHA-256 of the module bytes. All Runtimes in a process share wazero's
// in-memory compilation engine, which reference-counts machine code by
// module identity (bytes + termination checks), so the same module used by
// several Runtimes is compiled once and freed when the last one releases it.
package wasmrt

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
	"golang.org/x/sync/singleflight"
)

// PageSize is the size of a WebAssembly linear-memory page.
const PageSize = 64 << 10

// Defaults applied by New for zero Config fields.
const (
	DefaultMaxModuleBytes = 32 << 20
	DefaultMaxMemoryPages = 1024 // 64 MiB
	DefaultTimeout        = 5 * time.Second
	DefaultMaxInputBytes  = 1 << 20
	DefaultMaxOutputBytes = 1 << 20
	DefaultMaxStderrBytes = 16 << 10
	DefaultCacheSize      = 32
	DefaultQueueTimeout   = time.Second

	// maxPages is the WebAssembly (32-bit) limit on linear-memory pages.
	maxPages = 65536
	// maxErrorStderr bounds how much stderr is quoted in ExitError.Error.
	maxErrorStderr = 512
	// wasiModule is the only import namespace a guest may use.
	wasiModule = wasi_snapshot_preview1.ModuleName
	// defaultArgv0 is the program name guests see when no args are given.
	defaultArgv0 = "module"
)

// Errors returned by the runtime. Use errors.Is to test for them.
var (
	ErrClosed         = errors.New("wasmrt: runtime closed")
	ErrInvalidModule  = errors.New("wasmrt: invalid module")
	ErrModuleTooLarge = errors.New("wasmrt: module too large")
	ErrInputTooLarge  = errors.New("wasmrt: input too large")
	ErrOutputTooLarge = errors.New("wasmrt: output too large")
	ErrTimeout        = errors.New("wasmrt: execution timed out")
	ErrMemoryLimit    = errors.New("wasmrt: memory limit exceeded")
	ErrBusy           = errors.New("wasmrt: too many concurrent executions")
	ErrTrap           = errors.New("wasmrt: module trapped")
	ErrPanic          = errors.New("wasmrt: runtime panic")
	ErrInvalidOptions = errors.New("wasmrt: invalid run options")
)

// sharedCompilation is the process-wide wazero compilation cache. Every
// Runtime uses identical engine settings (core features V2, termination
// checks on), which is what makes sharing it sound.
var sharedCompilation = wazero.NewCompilationCache()

// wasmMagic is the preamble of a WebAssembly binary module, version 1.
var wasmMagic = []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}

// CheckHeader performs the cheap structural check done before compiling:
// the bytes must start with the WebAssembly magic and binary version 1
// (component-model binaries and text format are rejected).
func CheckHeader(wasm []byte) error {
	if len(wasm) < len(wasmMagic) || !bytes.Equal(wasm[:len(wasmMagic)], wasmMagic) {
		return fmt.Errorf("%w: not a WebAssembly v1 binary (bad magic/version)", ErrInvalidModule)
	}
	return nil
}

// Config configures a Runtime. Zero fields take the Default* values.
type Config struct {
	// MaxModuleBytes caps the size of a module binary.
	MaxModuleBytes int
	// MaxMemoryPages caps each linear memory, in 64 KiB pages (<= 65536).
	MaxMemoryPages uint32
	// Timeout is the default wall-clock limit of a run.
	Timeout time.Duration
	// MaxInputBytes caps the stdin payload.
	MaxInputBytes int
	// MaxOutputBytes caps stdout; exceeding it aborts the run.
	MaxOutputBytes int
	// MaxStderrBytes caps captured stderr; the excess is dropped.
	MaxStderrBytes int
	// MaxConcurrent bounds simultaneous compilations and runs
	// (default GOMAXPROCS, minimum 2).
	MaxConcurrent int
	// QueueTimeout bounds the wait for a free concurrency slot.
	QueueTimeout time.Duration
	// CacheSize bounds the number of compiled modules kept in memory.
	CacheSize int
	// AllowClock exposes the real wall/monotonic clocks and real (deadline
	// bounded) sleeps. Off by default: guests see deterministic fake clocks.
	AllowClock bool
	// AllowRandom backs random_get with crypto/rand. Off by default: guests
	// get a deterministic pseudo-random stream.
	AllowRandom bool
	// Interpreter forces wazero's interpreter instead of its compiler. The
	// compiler is used automatically where supported (amd64/arm64).
	Interpreter bool
}

// DefaultConfig returns the configuration New uses for a zero Config.
func DefaultConfig() Config {
	c, _ := Config{}.normalize()
	return c
}

func (c Config) normalize() (Config, error) {
	if c.MaxModuleBytes < 0 || c.Timeout < 0 || c.MaxInputBytes < 0 || c.MaxOutputBytes < 0 ||
		c.MaxStderrBytes < 0 || c.MaxConcurrent < 0 || c.QueueTimeout < 0 || c.CacheSize < 0 {
		return c, errors.New("wasmrt: negative config value")
	}
	if c.MaxMemoryPages > maxPages {
		return c, fmt.Errorf("wasmrt: MaxMemoryPages %d exceeds %d", c.MaxMemoryPages, maxPages)
	}
	if c.MaxModuleBytes == 0 {
		c.MaxModuleBytes = DefaultMaxModuleBytes
	}
	if c.MaxMemoryPages == 0 {
		c.MaxMemoryPages = DefaultMaxMemoryPages
	}
	if c.Timeout == 0 {
		c.Timeout = DefaultTimeout
	}
	if c.MaxInputBytes == 0 {
		c.MaxInputBytes = DefaultMaxInputBytes
	}
	if c.MaxOutputBytes == 0 {
		c.MaxOutputBytes = DefaultMaxOutputBytes
	}
	if c.MaxStderrBytes == 0 {
		c.MaxStderrBytes = DefaultMaxStderrBytes
	}
	if c.MaxConcurrent == 0 {
		c.MaxConcurrent = max(2, runtime.GOMAXPROCS(0))
	}
	if c.QueueTimeout == 0 {
		c.QueueTimeout = DefaultQueueTimeout
	}
	if c.CacheSize == 0 {
		c.CacheSize = DefaultCacheSize
	}
	return c, nil
}

// RunOptions tunes a single run. The zero value uses the runtime defaults.
type RunOptions struct {
	// Timeout replaces Config.Timeout for this run when > 0.
	Timeout time.Duration
	// MaxMemoryPages lowers the memory cap for this run when > 0. It can
	// never raise the cap above Config.MaxMemoryPages.
	MaxMemoryPages uint32
	// Args is the full argv the guest sees (default ["module"]). Entries
	// must be non-empty and free of NUL bytes.
	Args []string
	// Env is the guest environment (default empty). Keys must be non-empty
	// and free of '=' and NUL; values must be free of NUL.
	Env map[string]string
}

func (o RunOptions) validate() error {
	for _, a := range o.Args {
		if a == "" || strings.IndexByte(a, 0) >= 0 {
			return fmt.Errorf("%w: empty or NUL-containing argument", ErrInvalidOptions)
		}
	}
	for k, v := range o.Env {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.IndexByte(v, 0) >= 0 {
			return fmt.Errorf("%w: invalid environment entry %q", ErrInvalidOptions, k)
		}
	}
	if o.Timeout < 0 {
		return fmt.Errorf("%w: negative timeout", ErrInvalidOptions)
	}
	return nil
}

// Result describes a finished run. It is also returned, partially filled,
// alongside errors that occur after the guest started (e.g. *ExitError),
// so callers can inspect stderr.
type Result struct {
	Stdout          []byte
	Stderr          []byte // capped at Config.MaxStderrBytes
	StderrTruncated bool
	ExitCode        uint32
	Duration        time.Duration
	PeakMemoryBytes uint64
}

// ExitError reports a guest that exited with a non-zero code.
type ExitError struct {
	Code   uint32
	Stderr string // captured stderr, capped at Config.MaxStderrBytes
}

func (e *ExitError) Error() string {
	msg := fmt.Sprintf("wasmrt: module exited with code %d", e.Code)
	if s := sanitize(e.Stderr, maxErrorStderr); s != "" {
		msg += ": " + s
	}
	return msg
}

// SanitizeText makes untrusted guest text safe to embed in an error message
// or log line: control characters are dropped (newlines and tabs become
// spaces), invalid UTF-8 is replaced, runs of spaces are collapsed and the
// result is truncated to at most limit bytes (plus a "..." marker).
func SanitizeText(s string, limit int) string { return sanitize(s, limit) }

func sanitize(s string, limit int) string {
	var b strings.Builder
	space := false
	for _, r := range strings.ToValidUTF8(s, "�") {
		if r == '\n' || r == '\r' || r == '\t' || r == ' ' {
			space = true
			continue
		}
		if !unicode.IsPrint(r) {
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		if b.Len()+utf8.RuneLen(r) > limit {
			b.WriteString("...")
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

// Module is a validated, compiled module handle returned by Compile. It
// keeps a private copy of the bytes so it can be recompiled transparently
// if its compiled form is evicted from the cache. Modules are immutable and
// safe for concurrent use, and can be run on any Runtime.
type Module struct {
	key  [32]byte
	wasm []byte
}

// Hash returns the hex SHA-256 of the module bytes (the cache key).
func (m *Module) Hash() string { return hex.EncodeToString(m.key[:]) }

// Size returns the module size in bytes.
func (m *Module) Size() int { return len(m.wasm) }

// Stats is a snapshot of runtime counters.
type Stats struct {
	CachedModules int
	CacheHits     uint64
	CacheMisses   uint64
	InFlight      int // operations started (including those queued for a slot)
	Active        int // operations holding a concurrency slot
}

// Runtime compiles and runs untrusted modules. It is safe for concurrent
// use. Close releases it.
type Runtime struct {
	cfg       Config
	wz        wazero.Runtime
	wasiFuncs map[string]api.FunctionDefinition
	wasi      wazero.CompiledModule
	slots     chan struct{}
	closing   context.Context
	stopAll   context.CancelFunc
	sf        singleflight.Group

	mu       sync.Mutex
	closed   bool
	inFlight sync.WaitGroup
	running  int
	cache    map[[32]byte]*cacheEntry
	lruHead  *cacheEntry // most recently used
	lruTail  *cacheEntry // least recently used
	hits     uint64
	misses   uint64
}

type cacheEntry struct {
	key        [32]byte
	cm         wazero.CompiledModule
	refs       int
	evicted    bool
	prev, next *cacheEntry
}

// New creates a runtime. Zero Config fields take their defaults.
func New(cfg Config) (*Runtime, error) {
	cfg, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	// wazero enforces a hard per-memory ceiling at decode/grow time. It is
	// set above the configured cap (the per-run allocator enforces the real
	// cap) so that growth attempts past the cap reach the allocator and can
	// be reported precisely as ErrMemoryLimit; the ceiling bounds memory
	// even if the allocator were bypassed.
	backstop := uint32(min(uint64(maxPages), 2*uint64(cfg.MaxMemoryPages)))
	rc := wazero.NewRuntimeConfig()
	if cfg.Interpreter {
		rc = wazero.NewRuntimeConfigInterpreter()
	}
	rc = rc.WithCoreFeatures(api.CoreFeaturesV2).
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(backstop).
		WithDebugInfoEnabled(false).
		WithCustomSections(false).
		WithCompilationCache(sharedCompilation)

	bg := context.Background()
	wz := wazero.NewRuntimeWithConfig(bg, rc)
	wasiCompiled, err := wasi_snapshot_preview1.NewBuilder(wz).Compile(bg)
	if err != nil {
		_ = wz.Close(bg)
		return nil, fmt.Errorf("wasmrt: compile WASI host module: %w", err)
	}
	if _, err := wz.InstantiateModule(bg, wasiCompiled, wazero.NewModuleConfig()); err != nil {
		_ = wasiCompiled.Close(bg)
		_ = wz.Close(bg)
		return nil, fmt.Errorf("wasmrt: instantiate WASI host module: %w", err)
	}
	closing, stop := context.WithCancel(bg)
	return &Runtime{
		cfg:       cfg,
		wz:        wz,
		wasiFuncs: wasiCompiled.ExportedFunctions(),
		wasi:      wasiCompiled,
		slots:     make(chan struct{}, cfg.MaxConcurrent),
		closing:   closing,
		stopAll:   stop,
		cache:     make(map[[32]byte]*cacheEntry),
	}, nil
}

// Config returns the effective (defaulted) configuration.
func (r *Runtime) Config() Config { return r.cfg }

// Stats returns a snapshot of the runtime counters.
func (r *Runtime) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{CachedModules: len(r.cache), CacheHits: r.hits, CacheMisses: r.misses, InFlight: r.running, Active: len(r.slots)}
}

// Close interrupts in-flight runs (they fail with ErrClosed), waits for
// them to unwind and releases all compiled modules. It is idempotent.
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	r.stopAll()
	r.inFlight.Wait()

	r.mu.Lock()
	entries := make([]*cacheEntry, 0, len(r.cache))
	for _, e := range r.cache {
		entries = append(entries, e)
	}
	r.cache = map[[32]byte]*cacheEntry{}
	r.lruHead, r.lruTail = nil, nil
	r.mu.Unlock()
	// Compiled modules must be released explicitly: with a shared
	// compilation cache, closing the runtime does not drop engine references.
	bg := context.Background()
	for _, e := range entries {
		_ = e.cm.Close(bg)
	}
	_ = r.wasi.Close(bg)
	return r.wz.Close(bg)
}

// begin registers an in-flight operation, failing if the runtime is closed.
func (r *Runtime) begin() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	r.inFlight.Add(1)
	r.running++
	return nil
}

func (r *Runtime) end() {
	r.mu.Lock()
	r.running--
	r.mu.Unlock()
	r.inFlight.Done()
}

// acquireSlot waits for a concurrency slot.
func (r *Runtime) acquireSlot(ctx context.Context) (func(), error) {
	release := func() { <-r.slots }
	select {
	case r.slots <- struct{}{}:
		return release, nil
	default:
	}
	t := time.NewTimer(r.cfg.QueueTimeout)
	defer t.Stop()
	select {
	case r.slots <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.closing.Done():
		return nil, ErrClosed
	case <-t.C:
		return nil, ErrBusy
	}
}

func (r *Runtime) checkSize(wasm []byte) error {
	if len(wasm) == 0 {
		return fmt.Errorf("%w: empty module", ErrInvalidModule)
	}
	if len(wasm) > r.cfg.MaxModuleBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrModuleTooLarge, len(wasm), r.cfg.MaxModuleBytes)
	}
	return CheckHeader(wasm)
}

// Compile validates and compiles wasm (or fetches it from the cache) and
// returns a reusable handle. The bytes are copied. Validation covers the
// header, size, full WebAssembly validation, imports (only
// wasi_snapshot_preview1 functions that exist) and a "_start" export with
// signature () -> ().
func (r *Runtime) Compile(ctx context.Context, wasm []byte) (m *Module, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.checkSize(wasm); err != nil {
		return nil, err
	}
	if err := r.begin(); err != nil {
		return nil, err
	}
	defer r.end()
	defer recoverPanic(&err)

	release, err := r.acquireSlot(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	m = &Module{key: sha256.Sum256(wasm), wasm: bytes.Clone(wasm)}
	e, err := r.acquire(m.key, m.wasm)
	if err != nil {
		return nil, err
	}
	r.releaseEntry(e)
	return m, nil
}

// Validate reports whether wasm would be accepted by Compile. A valid
// module is left in the compiled-module cache.
func (r *Runtime) Validate(ctx context.Context, wasm []byte) error {
	_, err := r.Compile(ctx, wasm)
	return err
}

// Run compiles wasm (cached by SHA-256) and runs it with input on stdin
// using default RunOptions. The caller must not modify wasm during the call.
func (r *Runtime) Run(ctx context.Context, wasm []byte, input []byte) (Result, error) {
	if err := r.checkSize(wasm); err != nil {
		return Result{}, err
	}
	// Only the hash is taken here; the bytes are copied if (and only if)
	// they have to be compiled, since compiled modules may alias them.
	m := &Module{key: sha256.Sum256(wasm), wasm: wasm}
	return r.run(ctx, m, input, RunOptions{}, false)
}

// RunModule runs a compiled module with input on stdin.
func (r *Runtime) RunModule(ctx context.Context, m *Module, input []byte, opts RunOptions) (Result, error) {
	if m == nil || len(m.wasm) == 0 {
		return Result{}, fmt.Errorf("%w: nil module", ErrInvalidModule)
	}
	if len(m.wasm) > r.cfg.MaxModuleBytes {
		return Result{}, fmt.Errorf("%w: %d bytes exceeds %d", ErrModuleTooLarge, len(m.wasm), r.cfg.MaxModuleBytes)
	}
	return r.run(ctx, m, input, opts, true)
}

func (r *Runtime) run(ctx context.Context, m *Module, input []byte, opts RunOptions, owned bool) (res Result, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(input) > r.cfg.MaxInputBytes {
		return res, fmt.Errorf("%w: %d bytes exceeds %d", ErrInputTooLarge, len(input), r.cfg.MaxInputBytes)
	}
	if err := opts.validate(); err != nil {
		return res, err
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	if err := r.begin(); err != nil {
		return res, err
	}
	defer r.end()
	defer recoverPanic(&err)

	release, err := r.acquireSlot(ctx)
	if err != nil {
		return res, err
	}
	defer release()

	e, err := r.acquireFor(m, owned)
	if err != nil {
		return res, err
	}
	defer r.releaseEntry(e)
	return r.exec(ctx, e, input, opts)
}

// acquireFor fetches m's compiled form. Unless owned, m.wasm belongs to the
// caller and is cloned before compiling, because compiled modules may alias
// the bytes they were compiled from.
func (r *Runtime) acquireFor(m *Module, owned bool) (*cacheEntry, error) {
	if owned {
		return r.acquire(m.key, m.wasm)
	}
	if e := r.lookup(m.key); e != nil {
		return e, nil
	}
	return r.acquire(m.key, bytes.Clone(m.wasm))
}

func recoverPanic(err *error) {
	if p := recover(); p != nil {
		*err = fmt.Errorf("%w: %v", ErrPanic, p)
	}
}

// exec instantiates and runs one compiled module.
func (r *Runtime) exec(parent context.Context, e *cacheEntry, input []byte, opts RunOptions) (Result, error) {
	timeout := r.cfg.Timeout
	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}
	limitPages := r.cfg.MaxMemoryPages
	if opts.MaxMemoryPages > 0 && opts.MaxMemoryPages < limitPages {
		limitPages = opts.MaxMemoryPages
	}

	// The guest context deliberately descends from Background, not from the
	// caller, so no caller-supplied context values (wazero reads several
	// experimental hooks from context values) reach the guest. Caller
	// cancellation and runtime Close are propagated explicitly.
	cctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	stopParent := context.AfterFunc(parent, func() { cancel(parent.Err()) })
	defer stopParent()
	stopClosing := context.AfterFunc(r.closing, func() { cancel(ErrClosed) })
	defer stopClosing()
	runCtx, cancelTimeout := context.WithTimeoutCause(cctx, timeout, ErrTimeout)
	defer cancelTimeout()

	mem := &memTracker{limit: uint64(limitPages) * PageSize}
	runCtx = withAllocator(runCtx, mem)

	stdout := &cappedWriter{limit: r.cfg.MaxOutputBytes, onOverflow: func() { cancel(ErrOutputTooLarge) }}
	stderr := &cappedWriter{limit: r.cfg.MaxStderrBytes, truncate: true}
	mc := r.moduleConfig(runCtx, input, stdout, stderr, opts)

	start := time.Now()
	res := Result{}
	finish := func() {
		res.Duration = time.Since(start)
		res.Stdout = stdout.bytes()
		res.Stderr = stderr.bytes()
		res.StderrTruncated = stderr.overflowed()
		res.PeakMemoryBytes = mem.peakBytes()
	}

	mod, err := r.wz.InstantiateModule(runCtx, e.cm, mc)
	if err != nil {
		finish()
		return res, r.classify(runCtx, mem, stdout, stderr, fmt.Errorf("instantiate: %w", err))
	}
	defer mod.Close(context.Background())
	if mem.exceeded() {
		finish()
		return res, fmt.Errorf("%w: initial memory exceeds %d pages", ErrMemoryLimit, limitPages)
	}
	fn := mod.ExportedFunction("_start")
	if fn == nil {
		finish()
		return res, fmt.Errorf("%w: missing _start export", ErrInvalidModule)
	}
	_, err = fn.Call(runCtx)
	finish()
	var exitErr *sys.ExitError
	if err == nil || (errors.As(err, &exitErr) && exitErr.ExitCode() == 0 && runCtx.Err() == nil) {
		if stdout.overflowed() {
			return res, fmt.Errorf("%w: exceeds %d bytes", ErrOutputTooLarge, r.cfg.MaxOutputBytes)
		}
		return res, nil
	}
	if exitErr != nil {
		res.ExitCode = exitErr.ExitCode()
	}
	return res, r.classify(runCtx, mem, stdout, stderr, err)
}

// classify maps a failed instantiation or call to the package errors.
func (r *Runtime) classify(runCtx context.Context, mem *memTracker, stdout, stderr *cappedWriter, err error) error {
	if stdout.overflowed() {
		return fmt.Errorf("%w: exceeds %d bytes", ErrOutputTooLarge, r.cfg.MaxOutputBytes)
	}
	if runCtx.Err() != nil {
		cause := context.Cause(runCtx)
		switch {
		case errors.Is(cause, ErrTimeout), errors.Is(cause, ErrClosed), errors.Is(cause, ErrOutputTooLarge):
			return cause
		case errors.Is(cause, context.Canceled), errors.Is(cause, context.DeadlineExceeded):
			return cause // caller cancellation or caller deadline
		}
	}
	var exitErr *sys.ExitError
	if errors.As(err, &exitErr) {
		ee := &ExitError{Code: exitErr.ExitCode(), Stderr: string(stderr.bytes())}
		if mem.exceeded() {
			return fmt.Errorf("%w: %w", ErrMemoryLimit, ee)
		}
		return ee
	}
	if mem.exceeded() {
		return fmt.Errorf("%w: %s", ErrMemoryLimit, sanitize(err.Error(), maxErrorStderr))
	}
	return fmt.Errorf("%w: %s", ErrTrap, sanitize(err.Error(), maxErrorStderr))
}

func (r *Runtime) moduleConfig(runCtx context.Context, input []byte, stdout, stderr *cappedWriter, opts RunOptions) wazero.ModuleConfig {
	args := opts.Args
	if len(args) == 0 {
		args = []string{defaultArgv0}
	}
	mc := wazero.NewModuleConfig().
		WithName("").         // anonymous: the same module may run concurrently
		WithStartFunctions(). // "_start" is called explicitly
		WithStdin(bytes.NewReader(input)).
		WithStdout(stdout).
		WithStderr(stderr).
		WithArgs(args...)
	keys := make([]string, 0, len(opts.Env))
	for k := range opts.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		mc = mc.WithEnv(k, opts.Env[k])
	}
	if r.cfg.AllowClock {
		mc = mc.WithSysWalltime().WithSysNanotime().WithNanosleep(func(ns int64) {
			if ns <= 0 {
				return
			}
			t := time.NewTimer(time.Duration(ns))
			defer t.Stop()
			select {
			case <-t.C:
			case <-runCtx.Done():
			}
		})
	}
	if r.cfg.AllowRandom {
		mc = mc.WithRandSource(rand.Reader)
	}
	return mc
}

// lookup returns a referenced cache entry for key, or nil.
func (r *Runtime) lookup(key [32]byte) *cacheEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.cache[key]; ok {
		r.hits++
		e.refs++
		r.touch(e)
		return e
	}
	return nil
}

// acquire returns a referenced cache entry for key, compiling wasm (which
// must not be modified afterwards) on a miss. Concurrent misses for the
// same key share one compilation.
func (r *Runtime) acquire(key [32]byte, wasm []byte) (*cacheEntry, error) {
	for attempt := 0; attempt < 3; attempt++ {
		if e := r.lookup(key); e != nil {
			return e, nil
		}
		v, err, _ := r.sf.Do(string(key[:]), func() (interface{}, error) {
			return r.compileAndInsert(key, wasm)
		})
		if err != nil {
			return nil, err
		}
		e := v.(*cacheEntry)
		r.mu.Lock()
		if !e.evicted {
			e.refs++
			r.touch(e)
			r.mu.Unlock()
			return e, nil
		}
		r.mu.Unlock()
		// Evicted by a concurrent insert before we could reference it.
	}
	return nil, fmt.Errorf("%w: compiled-module cache is thrashing; increase CacheSize", ErrBusy)
}

func (r *Runtime) compileAndInsert(key [32]byte, wasm []byte) (*cacheEntry, error) {
	r.mu.Lock()
	if e, ok := r.cache[key]; ok { // inserted while we waited
		r.mu.Unlock()
		return e, nil
	}
	r.misses++
	r.mu.Unlock()

	// A clean context: compilation also consults context values.
	// Close waits for in-flight operations, so wz cannot close under us.
	cm, err := r.wz.CompileModule(context.Background(), wasm)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidModule, sanitize(err.Error(), maxErrorStderr))
	}
	if err := r.checkCompiled(cm); err != nil {
		_ = cm.Close(context.Background())
		return nil, err
	}

	e := &cacheEntry{key: key, cm: cm}
	var evict []*cacheEntry
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = cm.Close(context.Background())
		return nil, ErrClosed
	}
	r.cache[key] = e
	r.pushFront(e)
	for len(r.cache) > r.cfg.CacheSize && r.lruTail != nil && r.lruTail != e {
		old := r.lruTail
		r.unlink(old)
		delete(r.cache, old.key)
		old.evicted = true
		if old.refs == 0 {
			evict = append(evict, old)
		}
	}
	r.mu.Unlock()
	for _, old := range evict {
		_ = old.cm.Close(context.Background())
	}
	return e, nil
}

// checkCompiled enforces the import/export contract.
func (r *Runtime) checkCompiled(cm wazero.CompiledModule) error {
	for _, f := range cm.ImportedFunctions() {
		mod, name, _ := f.Import()
		if mod != wasiModule {
			return fmt.Errorf("%w: imports %q from %q; only %s is available",
				ErrInvalidModule, sanitize(name, 64), sanitize(mod, 64), wasiModule)
		}
		def, ok := r.wasiFuncs[name]
		if !ok {
			return fmt.Errorf("%w: imports unknown WASI function %q", ErrInvalidModule, sanitize(name, 64))
		}
		if !sameTypes(def.ParamTypes(), f.ParamTypes()) || !sameTypes(def.ResultTypes(), f.ResultTypes()) {
			return fmt.Errorf("%w: WASI function %q imported with the wrong signature", ErrInvalidModule, name)
		}
	}
	if len(cm.ImportedMemories()) > 0 {
		return fmt.Errorf("%w: modules may not import memory", ErrInvalidModule)
	}
	start, ok := cm.ExportedFunctions()["_start"]
	if !ok {
		return fmt.Errorf("%w: missing _start export (only WASI commands are supported)", ErrInvalidModule)
	}
	if len(start.ParamTypes()) != 0 || len(start.ResultTypes()) != 0 {
		return fmt.Errorf("%w: _start must have signature () -> ()", ErrInvalidModule)
	}
	for _, m := range cm.ExportedMemories() {
		if m.Min() > r.cfg.MaxMemoryPages {
			return fmt.Errorf("%w: %w: initial memory of %d pages exceeds %d",
				ErrInvalidModule, ErrMemoryLimit, m.Min(), r.cfg.MaxMemoryPages)
		}
	}
	return nil
}

func sameTypes(a, b []api.ValueType) bool {
	return bytes.Equal(a, b)
}

func (r *Runtime) releaseEntry(e *cacheEntry) {
	if e == nil {
		return
	}
	r.mu.Lock()
	e.refs--
	closeIt := e.evicted && e.refs == 0
	r.mu.Unlock()
	if closeIt {
		_ = e.cm.Close(context.Background())
	}
}

// LRU list helpers; r.mu must be held.
func (r *Runtime) pushFront(e *cacheEntry) {
	e.prev, e.next = nil, r.lruHead
	if r.lruHead != nil {
		r.lruHead.prev = e
	}
	r.lruHead = e
	if r.lruTail == nil {
		r.lruTail = e
	}
}

func (r *Runtime) unlink(e *cacheEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else if r.lruHead == e {
		r.lruHead = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else if r.lruTail == e {
		r.lruTail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (r *Runtime) touch(e *cacheEntry) {
	if r.lruHead == e {
		return
	}
	r.unlink(e)
	r.pushFront(e)
}
