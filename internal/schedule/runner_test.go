package schedule

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClockStore(start time.Time) (*Store, *fakeClock) {
	clk := &fakeClock{t: start}
	s := NewStore()
	s.now = clk.Now
	return s, clk
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func startRunner(t *testing.T, s *Store, exec Executor, opts RunnerOptions) (cancel func()) {
	t.Helper()
	if opts.PollInterval == 0 {
		opts.PollInterval = 2 * time.Millisecond
	}
	r, err := NewRunner(s, exec, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	cancel = func() {
		once.Do(func() {
			stop()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("runner did not stop")
			}
		})
	}
	t.Cleanup(cancel)
	return cancel
}

func TestNewRunnerValidation(t *testing.T) {
	s := NewStore()
	noop := func(context.Context, ScheduledTask) error { return nil }
	if _, err := NewRunner(nil, noop, RunnerOptions{}); err == nil {
		t.Fatal("expected error for nil store")
	}
	if _, err := NewRunner(s, nil, RunnerOptions{}); err == nil {
		t.Fatal("expected error for nil executor")
	}
	if _, err := NewRunner(s, noop, RunnerOptions{MaxConcurrent: -1}); err == nil {
		t.Fatal("expected error for negative concurrency")
	}
	if _, err := NewRunner(s, noop, RunnerOptions{TaskTimeout: -time.Second}); err == nil {
		t.Fatal("expected error for negative timeout")
	}
}

func TestRunnerRejectsConcurrentRun(t *testing.T) {
	s := NewStore()
	r, err := NewRunner(s, func(context.Context, ScheduledTask) error { return nil }, RunnerOptions{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitFor(t, "runner start", r.active.Load)
	if err := r.Run(ctx); !errors.Is(err, ErrRunnerActive) {
		t.Fatalf("expected ErrRunnerActive, got %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRunsDueTasks(t *testing.T) {
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newClockStore(start)
	task, err := s.Create(ScheduledTask{Name: "tick", Type: TaskInterval, Schedule: "10s", Payload: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	var runs atomic.Int32
	var gotPayload atomic.Value
	startRunner(t, s, func(ctx context.Context, tk ScheduledTask) error {
		gotPayload.Store(tk.Payload)
		runs.Add(1)
		return nil
	}, RunnerOptions{})

	time.Sleep(20 * time.Millisecond)
	if runs.Load() != 0 {
		t.Fatal("task ran before it was due")
	}
	clk.Advance(10 * time.Second)
	waitFor(t, "first run", func() bool {
		cur, _ := s.Get(task.ID)
		return cur.Runs == 1 && cur.Status == TaskCompleted
	})
	cur, _ := s.Get(task.ID)
	if !cur.NextRun.Equal(start.Add(20*time.Second)) || !cur.LastRunAt.Equal(start.Add(10*time.Second)) || cur.LastError != "" {
		t.Fatalf("unexpected task after run: %+v", cur)
	}
	if gotPayload.Load() != "hello" {
		t.Fatalf("executor got payload %v", gotPayload.Load())
	}
	clk.Advance(10 * time.Second)
	waitFor(t, "second run", func() bool { return runs.Load() == 2 })
}

func TestRunnerNoOverlapAndSkipsMissedRuns(t *testing.T) {
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newClockStore(start)
	task, err := s.Create(ScheduledTask{Name: "slow", Type: TaskInterval, Schedule: "1s"})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var inflight, maxInflight, runs atomic.Int32
	startRunner(t, s, func(ctx context.Context, tk ScheduledTask) error {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		runs.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}, RunnerOptions{MaxConcurrent: 4})

	clk.Advance(time.Second)
	waitFor(t, "run to start", func() bool { return inflight.Load() == 1 })
	// Many activations pass while the run is still in flight.
	for i := 0; i < 10; i++ {
		clk.Advance(time.Second)
		time.Sleep(3 * time.Millisecond)
	}
	if cur, _ := s.Get(task.ID); cur.Status != TaskRunning {
		t.Fatalf("expected running status, got %s", cur.Status)
	}
	close(release)
	waitFor(t, "run to finish", func() bool {
		cur, _ := s.Get(task.ID)
		return cur.Runs >= 1 && cur.Status != TaskRunning
	})
	cur, _ := s.Get(task.ID)
	if !cur.NextRun.After(clk.Now()) {
		t.Fatalf("missed activations must be skipped: next %s, now %s", cur.NextRun, clk.Now())
	}
	if maxInflight.Load() != 1 {
		t.Fatalf("task overlapped: max in flight %d", maxInflight.Load())
	}
	if runs.Load() != 1 {
		t.Fatalf("missed activations must collapse into one run, got %d", runs.Load())
	}
}

func TestRunnerOneTimeRunsOnce(t *testing.T) {
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newClockStore(start)
	task, err := s.Create(ScheduledTask{Name: "once", Type: TaskOneTime, RunAt: start.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	var runs atomic.Int32
	startRunner(t, s, func(context.Context, ScheduledTask) error {
		runs.Add(1)
		return nil
	}, RunnerOptions{})
	clk.Advance(2 * time.Minute)
	waitFor(t, "one-time run", func() bool {
		cur, _ := s.Get(task.ID)
		return cur.Status == TaskCompleted
	})
	clk.Advance(time.Hour)
	time.Sleep(20 * time.Millisecond)
	cur, _ := s.Get(task.ID)
	if runs.Load() != 1 || cur.Runs != 1 || !cur.NextRun.IsZero() {
		t.Fatalf("one-time task must run exactly once: runs=%d task=%+v", runs.Load(), cur)
	}
}

func TestRunnerFailureAndPanic(t *testing.T) {
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newClockStore(start)
	fail, _ := s.Create(ScheduledTask{Name: "fail", Type: TaskOneTime, RunAt: start.Add(time.Second)})
	boom, _ := s.Create(ScheduledTask{Name: "boom", Type: TaskOneTime, RunAt: start.Add(time.Second)})
	startRunner(t, s, func(_ context.Context, tk ScheduledTask) error {
		if tk.Name == "boom" {
			panic("kaboom")
		}
		return errors.New("disk full")
	}, RunnerOptions{})
	clk.Advance(2 * time.Second)
	waitFor(t, "failures recorded", func() bool {
		a, _ := s.Get(fail.ID)
		b, _ := s.Get(boom.ID)
		return a.Status == TaskFailed && b.Status == TaskFailed
	})
	a, _ := s.Get(fail.ID)
	b, _ := s.Get(boom.ID)
	if a.LastError != "disk full" || !strings.Contains(b.LastError, "kaboom") {
		t.Fatalf("unexpected errors: %q / %q", a.LastError, b.LastError)
	}
}

func TestRunnerTimeoutAndCancellation(t *testing.T) {
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newClockStore(start)
	slow, _ := s.Create(ScheduledTask{Name: "slow", Type: TaskOneTime, RunAt: start.Add(time.Second)})
	started := make(chan struct{})
	finished := make(chan struct{})
	cancel := startRunner(t, s, func(ctx context.Context, tk ScheduledTask) error {
		close(started)
		defer close(finished)
		<-ctx.Done()
		return ctx.Err()
	}, RunnerOptions{TaskTimeout: time.Hour})
	clk.Advance(2 * time.Second)
	<-started
	cancel() // must wait for the in-flight run to observe cancellation
	select {
	case <-finished:
	default:
		t.Fatal("Run returned before the in-flight execution finished")
	}
	cur, _ := s.Get(slow.ID)
	if cur.Status != TaskFailed || !strings.Contains(cur.LastError, "context canceled") {
		t.Fatalf("unexpected task after cancellation: %+v", cur)
	}

	// Per-task timeout.
	s2, clk2 := newClockStore(start)
	to, _ := s2.Create(ScheduledTask{Name: "to", Type: TaskOneTime, RunAt: start.Add(time.Second)})
	startRunner(t, s2, func(ctx context.Context, tk ScheduledTask) error {
		<-ctx.Done()
		return ctx.Err()
	}, RunnerOptions{TaskTimeout: 20 * time.Millisecond})
	clk2.Advance(2 * time.Second)
	waitFor(t, "timeout", func() bool {
		cur, _ := s2.Get(to.ID)
		return cur.Status == TaskFailed && strings.Contains(cur.LastError, "deadline exceeded")
	})
}

func TestRunnerBoundedConcurrency(t *testing.T) {
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newClockStore(start)
	for i := 0; i < 6; i++ {
		if _, err := s.Create(ScheduledTask{Name: "t", Type: TaskOneTime, RunAt: start.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	release := make(chan struct{})
	var inflight, maxInflight, runs atomic.Int32
	startRunner(t, s, func(ctx context.Context, _ ScheduledTask) error {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		runs.Add(1)
		<-release
		return nil
	}, RunnerOptions{MaxConcurrent: 2})
	clk.Advance(2 * time.Second)
	waitFor(t, "two runs in flight", func() bool { return inflight.Load() == 2 })
	time.Sleep(20 * time.Millisecond)
	if maxInflight.Load() != 2 {
		t.Fatalf("expected at most 2 concurrent runs, got %d", maxInflight.Load())
	}
	close(release)
	waitFor(t, "all runs", func() bool { return runs.Load() == 6 })
	if maxInflight.Load() > 2 {
		t.Fatalf("concurrency bound exceeded: %d", maxInflight.Load())
	}
}

func TestRunnerDeletedDuringRun(t *testing.T) {
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newClockStore(start)
	task, _ := s.Create(ScheduledTask{Name: "gone", Type: TaskInterval, Schedule: "1s"})
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	startRunner(t, s, func(ctx context.Context, _ ScheduledTask) error {
		started <- struct{}{}
		<-release
		return nil
	}, RunnerOptions{})
	clk.Advance(time.Second)
	<-started
	if !s.Delete(task.ID) {
		t.Fatal("delete failed")
	}
	close(release)
	time.Sleep(20 * time.Millisecond)
	if _, ok := s.Get(task.ID); ok {
		t.Fatal("finished run must not resurrect a deleted task")
	}
}
