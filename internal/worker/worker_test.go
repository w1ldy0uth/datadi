package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/w1ldy0uth/datadi/internal/task"
)

func TestBackoffDuration(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 500 * time.Millisecond},
		{2, 1 * time.Second},
		{3, 2 * time.Second},
		{4, 4 * time.Second},
		{5, 8 * time.Second},
		{6, 16 * time.Second},
		{7, 30 * time.Second}, // would be 32s uncapped, clamped to the 30s max
		{10, 30 * time.Second},
	}

	for _, c := range cases {
		if got := backoffDuration(c.attempt); got != c.want {
			t.Errorf("backoffDuration(%d) = %s, want %s", c.attempt, got, c.want)
		}
	}
}

// fakeDispatcher lets each test control what Dispatch returns.
type fakeDispatcher struct {
	fn func(ctx context.Context, name string, payload []byte) error
}

func (f *fakeDispatcher) Dispatch(ctx context.Context, name string, payload []byte) error {
	return f.fn(ctx, name, payload)
}

type fakeRequeuer struct {
	mu       sync.Mutex
	enqueued []*task.Task
}

func (f *fakeRequeuer) Enqueue(t *task.Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueued = append(f.enqueued, t)
}

func (f *fakeRequeuer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.enqueued)
}

type fakeDeadLetterer struct {
	mu      sync.Mutex
	entries []struct {
		task   *task.Task
		reason error
	}
}

func (f *fakeDeadLetterer) Add(t *task.Task, reason error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, struct {
		task   *task.Task
		reason error
	}{t, reason})
}

func (f *fakeDeadLetterer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entries)
}

func (f *fakeDeadLetterer) lastReason() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.entries) == 0 {
		return nil
	}
	return f.entries[len(f.entries)-1].reason
}

type fakePersister struct {
	mu          sync.Mutex
	saved       []task.Status
	deadLetters int
}

func (f *fakePersister) Save(_ context.Context, t *task.Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved = append(f.saved, t.Status)
	return nil
}

func (f *fakePersister) SaveDeadLetter(_ context.Context, _ *task.Task, _ error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deadLetters++
	return nil
}

func (f *fakePersister) savedStatuses() []task.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]task.Status(nil), f.saved...)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestProcess_Success(t *testing.T) {
	d := &fakeDispatcher{fn: func(context.Context, string, []byte) error { return nil }}
	rq := &fakeRequeuer{}
	dl := &fakeDeadLetterer{}
	w := New(0, d, rq, dl, nil)

	tk := &task.Task{ID: "t1", MaxRetries: 3}
	w.process(context.Background(), tk)

	if tk.Status != task.StatusDone {
		t.Errorf("task status = %q, want %q", tk.Status, task.StatusDone)
	}
	if dl.count() != 0 || rq.count() != 0 {
		t.Fatal("successful task should not be dead-lettered or requeued")
	}
}

func TestProcess_PermanentError_DeadLettersImmediately(t *testing.T) {
	cause := errors.New("bad payload")
	d := &fakeDispatcher{fn: func(context.Context, string, []byte) error { return task.Permanent(cause) }}
	rq := &fakeRequeuer{}
	dl := &fakeDeadLetterer{}
	w := New(0, d, rq, dl, nil)

	tk := &task.Task{ID: "t1", MaxRetries: 3}
	w.process(context.Background(), tk)

	if tk.Status != task.StatusFailed {
		t.Errorf("task status = %q, want %q", tk.Status, task.StatusFailed)
	}
	if tk.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0 (permanent errors should not consume retries)", tk.RetryCount)
	}
	if dl.count() != 1 {
		t.Fatalf("dead-letter count = %d, want 1", dl.count())
	}
	if rq.count() != 0 {
		t.Fatal("permanently failed task should not be requeued")
	}
}

func TestProcess_RetriesExhausted_DeadLetters(t *testing.T) {
	cause := errors.New("transient")
	d := &fakeDispatcher{fn: func(context.Context, string, []byte) error { return cause }}
	rq := &fakeRequeuer{}
	dl := &fakeDeadLetterer{}
	w := New(0, d, rq, dl, nil)

	// MaxRetries 0 means the very first failure exhausts retries.
	tk := &task.Task{ID: "t1", MaxRetries: 0}
	w.process(context.Background(), tk)

	if tk.Status != task.StatusFailed {
		t.Errorf("task status = %q, want %q", tk.Status, task.StatusFailed)
	}
	if dl.count() != 1 {
		t.Fatalf("dead-letter count = %d, want 1", dl.count())
	}
}

func TestProcess_TransientError_RequeuesAfterBackoff(t *testing.T) {
	cause := errors.New("transient")
	d := &fakeDispatcher{fn: func(context.Context, string, []byte) error { return cause }}
	rq := &fakeRequeuer{}
	dl := &fakeDeadLetterer{}
	w := New(0, d, rq, dl, nil)

	tk := &task.Task{ID: "t1", MaxRetries: 3}
	w.process(context.Background(), tk)

	if tk.Status != task.StatusPending {
		t.Errorf("task status = %q, want %q", tk.Status, task.StatusPending)
	}

	waitFor(t, 2*time.Second, func() bool { return rq.count() == 1 })
	w.Wait()

	if dl.count() != 0 {
		t.Fatal("task requeued within retry budget should not be dead-lettered")
	}
}

func TestProcess_ShutdownDuringBackoff_DeadLetters(t *testing.T) {
	cause := errors.New("transient")
	d := &fakeDispatcher{fn: func(context.Context, string, []byte) error { return cause }}
	rq := &fakeRequeuer{}
	dl := &fakeDeadLetterer{}
	w := New(0, d, rq, dl, nil)

	ctx, cancel := context.WithCancel(context.Background())
	tk := &task.Task{ID: "t1", MaxRetries: 3}
	w.process(ctx, tk)
	cancel()

	w.Wait()

	if rq.count() != 0 {
		t.Fatal("task canceled mid-backoff should not be requeued")
	}
	if dl.count() != 1 {
		t.Fatalf("dead-letter count = %d, want 1", dl.count())
	}
	if got := dl.lastReason(); got == nil || !errors.Is(got, cause) {
		t.Errorf("dead-letter reason = %v, want it to wrap %v", got, cause)
	}
}

func TestProcess_PerTaskTimeout_TreatedAsRetryableFailure(t *testing.T) {
	d := &fakeDispatcher{fn: func(ctx context.Context, _ string, _ []byte) error {
		<-ctx.Done() // block until the per-task timeout fires
		return ctx.Err()
	}}
	rq := &fakeRequeuer{}
	dl := &fakeDeadLetterer{}
	w := New(0, d, rq, dl, nil)

	tk := &task.Task{ID: "t1", MaxRetries: 3, Timeout: 10 * time.Millisecond}
	w.process(context.Background(), tk)

	// A per-task deadline expiring should be treated like any other dispatch
	// error (retried with backoff), not like a shutdown cancellation.
	if tk.Status != task.StatusPending {
		t.Errorf("task status = %q, want %q", tk.Status, task.StatusPending)
	}
	if tk.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", tk.RetryCount)
	}

	waitFor(t, 2*time.Second, func() bool { return rq.count() == 1 })
	w.Wait()

	if dl.count() != 0 {
		t.Fatal("task retried within budget should not be dead-lettered")
	}
}

func TestProcess_PersistsStateTransitions(t *testing.T) {
	d := &fakeDispatcher{fn: func(context.Context, string, []byte) error { return nil }}
	rq := &fakeRequeuer{}
	dl := &fakeDeadLetterer{}
	ps := &fakePersister{}
	w := New(0, d, rq, dl, ps)

	tk := &task.Task{ID: "t1", MaxRetries: 3}
	w.process(context.Background(), tk)

	got := ps.savedStatuses()
	want := []task.Status{task.StatusRunning, task.StatusDone}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("persisted statuses = %v, want %v", got, want)
	}
}

func TestProcess_PersistsDeadLetterOnPermanentError(t *testing.T) {
	d := &fakeDispatcher{fn: func(context.Context, string, []byte) error { return task.Permanent(errors.New("bad")) }}
	rq := &fakeRequeuer{}
	dl := &fakeDeadLetterer{}
	ps := &fakePersister{}
	w := New(0, d, rq, dl, ps)

	tk := &task.Task{ID: "t1", MaxRetries: 3}
	w.process(context.Background(), tk)

	if ps.deadLetters != 1 {
		t.Fatalf("persisted dead-letter count = %d, want 1", ps.deadLetters)
	}
}

func TestProcess_DispatchCanceled_NotCountedAsFailure(t *testing.T) {
	d := &fakeDispatcher{fn: func(context.Context, string, []byte) error { return context.Canceled }}
	rq := &fakeRequeuer{}
	dl := &fakeDeadLetterer{}
	w := New(0, d, rq, dl, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tk := &task.Task{ID: "t1", MaxRetries: 3}
	w.process(ctx, tk)

	if tk.Status != task.StatusPending {
		t.Errorf("task status = %q, want %q", tk.Status, task.StatusPending)
	}
	if tk.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", tk.RetryCount)
	}
	if dl.count() != 0 || rq.count() != 0 {
		t.Fatal("dispatch canceled due to shutdown should neither dead-letter nor requeue")
	}
}
