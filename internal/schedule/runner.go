package schedule

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Executor executes one run of a task. The context is cancelled when the
// per-task timeout elapses or the Runner is stopped.
type Executor func(ctx context.Context, task ScheduledTask) error

// Runner defaults.
const (
	DefaultMaxConcurrent = 4
	DefaultTaskTimeout   = 5 * time.Minute
	DefaultPollInterval  = time.Second
	maxRunnerConcurrency = 1024
)

// RunnerOptions configures a Runner. Zero values select the defaults.
type RunnerOptions struct {
	// MaxConcurrent bounds how many tasks execute at once (default 4).
	MaxConcurrent int
	// TaskTimeout bounds a single execution (default 5m).
	TaskTimeout time.Duration
	// PollInterval is the maximum time between due-task scans (default 1s).
	PollInterval time.Duration
	// Now overrides the clock (default: the store's clock).
	Now func() time.Time
}

// ErrRunnerActive is returned when Run is called on a Runner that is already
// running.
var ErrRunnerActive = errors.New("schedule: runner is already running")

// Runner executes due tasks from a Store. A task never has two runs in flight
// at the same time; a task that missed several activations (e.g. while the
// process was down or a previous run overran) runs once and is then
// rescheduled to its next future activation.
type Runner struct {
	store  *Store
	exec   Executor
	opts   RunnerOptions
	active atomic.Bool
	wake   chan struct{}
}

// NewRunner creates a Runner. It returns an error when store or exec is nil
// or the options are invalid.
func NewRunner(store *Store, exec Executor, opts RunnerOptions) (*Runner, error) {
	if store == nil {
		return nil, errors.New("schedule: runner requires a store")
	}
	if exec == nil {
		return nil, errors.New("schedule: runner requires an executor")
	}
	if opts.MaxConcurrent < 0 || opts.MaxConcurrent > maxRunnerConcurrency {
		return nil, fmt.Errorf("schedule: MaxConcurrent must be between 1 and %d", maxRunnerConcurrency)
	}
	if opts.TaskTimeout < 0 || opts.PollInterval < 0 {
		return nil, errors.New("schedule: timeouts must not be negative")
	}
	if opts.MaxConcurrent == 0 {
		opts.MaxConcurrent = DefaultMaxConcurrent
	}
	if opts.TaskTimeout == 0 {
		opts.TaskTimeout = DefaultTaskTimeout
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = DefaultPollInterval
	}
	if opts.Now == nil {
		opts.Now = store.now
	}
	return &Runner{store: store, exec: exec, opts: opts, wake: make(chan struct{}, 1)}, nil
}

// Run executes due tasks until ctx is cancelled. It returns nil after ctx is
// cancelled and all in-flight executions (which observe the cancellation)
// have finished.
func (r *Runner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("schedule: nil context")
	}
	if !r.active.CompareAndSwap(false, true) {
		return ErrRunnerActive
	}
	defer r.active.Store(false)

	sem := make(chan struct{}, r.opts.MaxConcurrent)
	var wg sync.WaitGroup
	defer wg.Wait()

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		case <-r.wake:
		}
		if ctx.Err() != nil {
			return nil
		}
		r.dispatch(ctx, sem, &wg)
		timer.Reset(r.nextWait(sem))
	}
}

func (r *Runner) dispatch(ctx context.Context, sem chan struct{}, wg *sync.WaitGroup) {
	// Only this goroutine acquires slots, so free can only grow concurrently.
	free := cap(sem) - len(sem)
	for _, c := range r.store.claimDue(r.opts.Now(), free) {
		sem <- struct{}{}
		wg.Add(1)
		go func(c claim) {
			defer wg.Done()
			defer func() {
				<-sem
				r.signal()
			}()
			r.execute(ctx, c)
		}(c)
	}
}

func (r *Runner) execute(ctx context.Context, c claim) {
	started := r.opts.Now()
	tctx, cancel := context.WithTimeout(ctx, r.opts.TaskTimeout)
	err := r.safeExec(tctx, c.task)
	cancel()
	r.store.finishRun(c, started, r.opts.Now(), err)
}

func (r *Runner) safeExec(ctx context.Context, task ScheduledTask) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("task panicked: %v", rec)
		}
	}()
	return r.exec(ctx, task)
}

func (r *Runner) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// nextWait returns how long to sleep before the next scan.
func (r *Runner) nextWait(sem chan struct{}) time.Duration {
	poll := r.opts.PollInterval
	now := r.opts.Now()
	earliest, dueNow := r.store.nextDue(now)
	switch {
	case earliest.IsZero():
		return poll
	case dueNow:
		if len(sem) == cap(sem) {
			// Saturated: a finishing run wakes the loop.
			return poll
		}
		return time.Millisecond
	}
	if d := earliest.Sub(now); d < poll {
		return d
	}
	return poll
}
