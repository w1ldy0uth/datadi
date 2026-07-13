package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/w1ldy0uth/datadi/internal/task"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, name string, payload []byte) error
}

type Requeuer interface {
	Enqueue(t *task.Task)
}

type DeadLetterer interface {
	Add(t *task.Task, reason error)
}

// Persister durably records task state so it survives a process restart. A nil Persister is
// valid and disables persistence, e.g. for tests that don't care about it.
type Persister interface {
	Save(ctx context.Context, t *task.Task) error
	SaveDeadLetter(ctx context.Context, t *task.Task, reason error) error
}

type Worker struct {
	id         int
	dispatcher Dispatcher
	requirer   Requeuer
	deadLetter DeadLetterer
	persister  Persister
	retryWG    sync.WaitGroup
}

func New(id int, dispatcher Dispatcher, requirer Requeuer, deadLetter DeadLetterer, persister Persister) *Worker {
	return &Worker{id: id, dispatcher: dispatcher, requirer: requirer, deadLetter: deadLetter, persister: persister}
}

// persistTimeout bounds save/saveDeadLetter's background context. They deliberately don't use
// the worker's shutdown context, since persistence is exactly what needs to succeed while
// shutting down (e.g. recording a task canceled mid-retry-backoff) and must not be short-
// circuited by the same cancellation that triggered it. But context.Background() alone never
// expires, so a stuck write would block shutdown forever; this timeout caps that risk.
const persistTimeout = 5 * time.Second

func (w *Worker) save(t *task.Task) {
	if w.persister == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	if err := w.persister.Save(ctx, t); err != nil {
		log.Printf("Worker %d: persisting task %s: %v", w.id, t.ID, err)
	}
}

func (w *Worker) saveDeadLetter(t *task.Task, reason error) {
	if w.persister == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	if err := w.persister.SaveDeadLetter(ctx, t, reason); err != nil {
		log.Printf("Worker %d: persisting dead-lettered task %s: %v", w.id, t.ID, err)
	}
}

func (w *Worker) Start(ctx context.Context, tasks <-chan *task.Task) {
	for {
		select {
		case t, ok := <-tasks:
			if !ok {
				return
			}
			w.process(ctx, t)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) process(ctx context.Context, t *task.Task) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Worker %d: task %s panicked: %v", w.id, t.ID, r)
			w.handleFailure(ctx, t, fmt.Errorf("task panicked: %v", r))
		}
	}()

	log.Printf("Worker %d processing task: %s", w.id, t.ID)
	t.Status = task.StatusRunning
	w.save(t)

	dispatchCtx := ctx
	if t.Timeout > 0 {
		var cancel context.CancelFunc
		dispatchCtx, cancel = context.WithTimeout(ctx, t.Timeout)
		defer cancel()
	}

	if err := w.dispatcher.Dispatch(dispatchCtx, t.Name, t.Payload); err != nil {
		if ctx.Err() != nil {
			log.Printf("Worker %d: task %s canceled during dispatch, not counting as a failed attempt", w.id, t.ID)
			t.Status = task.StatusPending
			w.save(t)
			return
		}

		log.Printf("Task %s failed: %v", t.ID, err)

		if task.IsPermanent(err) {
			t.Status = task.StatusFailed
			log.Printf("Worker %d: task %s failed permanently, giving up", w.id, t.ID)
			w.deadLetter.Add(t, err)
			w.saveDeadLetter(t, err)
			return
		}

		w.handleFailure(ctx, t, err)
		return
	}

	t.Status = task.StatusDone
	log.Printf("Worker %d: task %s completed", w.id, t.ID)
	w.save(t)
}

func (w *Worker) handleFailure(ctx context.Context, t *task.Task, cause error) {
	t.RetryCount++

	if t.RetryCount > t.MaxRetries {
		t.Status = task.StatusFailed
		log.Printf("Worker %d: task %s exhausted retries (%d/%d), giving up",
			w.id, t.ID, t.RetryCount, t.MaxRetries)
		w.deadLetter.Add(t, cause)
		w.saveDeadLetter(t, cause)
		return
	}

	t.Status = task.StatusPending
	delay := backoffDuration(t.RetryCount)
	log.Printf("Worker %d: retrying task %s in %s (attempt %d/%d)",
		w.id, t.ID, delay, t.RetryCount, t.MaxRetries)
	w.save(t)

	w.retryWG.Add(1)
	go func() {
		defer w.retryWG.Done()

		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
			w.requirer.Enqueue(t)
		case <-ctx.Done():
			log.Printf("Worker %d: context canceled, dead-lettering task %s instead of retrying", w.id, t.ID)
			t.Status = task.StatusFailed
			reason := fmt.Errorf("shutdown during retry backoff: %w", cause)
			w.deadLetter.Add(t, reason)
			w.saveDeadLetter(t, reason)
		}
	}()
}

// Wait blocks until in-flight retry backoff goroutines have dead-lettered or requeued their task.
func (w *Worker) Wait() {
	w.retryWG.Wait()
}

func backoffDuration(attempt int) time.Duration {
	const (
		base = 500 * time.Millisecond
		max  = 30 * time.Second
	)
	d := base * time.Duration(1<<uint(attempt-1))
	if d > max {
		return max
	}
	return d
}
