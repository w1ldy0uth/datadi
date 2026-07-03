package worker

import (
	"context"
	"fmt"
	"log"
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

type Worker struct {
	id         int
	dispatcher Dispatcher
	requirer   Requeuer
	deadLetter DeadLetterer
}

func New(id int, dispatcher Dispatcher, requirer Requeuer, deadLetter DeadLetterer) *Worker {
	return &Worker{id: id, dispatcher: dispatcher, requirer: requirer, deadLetter: deadLetter}
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

	if err := w.dispatcher.Dispatch(ctx, t.Name, t.Payload); err != nil {
		log.Printf("Task %s failed: %v", t.ID, err)
		w.handleFailure(ctx, t, err)
		return
	}

	t.Status = task.StatusDone
	log.Printf("Worker %d: task %s completed", w.id, t.ID)
}

func (w *Worker) handleFailure(ctx context.Context, t *task.Task, cause error) {
	t.RetryCount++

	if t.RetryCount > t.MaxRetries {
		t.Status = task.StatusFailed
		log.Printf("Worker %d: task %s exhausted retries (%d/%d), giving up",
			w.id, t.ID, t.RetryCount, t.MaxRetries)
		w.deadLetter.Add(t, cause)
		return
	}

	t.Status = task.StatusPending
	delay := backoffDuration(t.RetryCount)
	log.Printf("Worker %d: retrying task %s in %s (attempt %d/%d)",
		w.id, t.ID, delay, t.RetryCount, t.MaxRetries)

	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
			w.requirer.Enqueue(t)
		case <-ctx.Done():
			log.Printf("Worker %d: context canceled, not retrying task %s", w.id, t.ID)
		}
	}()
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
