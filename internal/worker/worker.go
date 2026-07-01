package worker

import (
	"context"
	"log"

	"github.com/w1ldy0uth/datadi/internal/task"
)

type HandlerFunc func(ctx context.Context, t *task.Task) error

type Worker struct {
	id      int
	handler HandlerFunc
}

func New(id int, handler HandlerFunc) *Worker {
	return &Worker{id: id, handler: handler}
}

func (w *Worker) Start(ctx context.Context, tasks <-chan *task.Task) {
	for {
		select {
		case t, ok := <-tasks:
			if !ok {
				return
			}
			log.Printf("Worker %d processing task: %s", w.id, t.ID)
			if err := w.handler(ctx, t); err != nil {
				log.Printf("Task %s failed: %v", t.ID, err)
			}
		case <-ctx.Done():
			return
		}
	}
}
