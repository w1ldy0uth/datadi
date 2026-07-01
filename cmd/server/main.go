package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/w1ldy0uth/datadi/internal/queue"
	"github.com/w1ldy0uth/datadi/internal/task"
	"github.com/w1ldy0uth/datadi/internal/worker"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	q := queue.New(100)
	dlq := queue.NewDeadLetterQueue()

	handler := func(ctx context.Context, t *task.Task) error {
		log.Printf("Executing task %s (%s)", t.ID, t.Name)
		if rand.Intn(2) == 0 { // fail simulation
			return fmt.Errorf("Task %s: simulated error", t.ID)
		}
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	const numWorkers = 3
	var wg sync.WaitGroup

	for i := range numWorkers {
		w := worker.New(i, handler, q, dlq)
		wg.Add(1)
		wg.Go(func() {
			defer wg.Done()
			w.Start(ctx, q.Dequeue())
		})
	}

	go func() {
		for i := range 10 {
			q.Enqueue(&task.Task{
				ID:         fmt.Sprintf("task-%d", i),
				Name:       fmt.Sprintf("Task %d", i),
				Status:     task.StatusPending,
				CreatedAt:  time.Now(),
				MaxRetries: 3,
			})
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down, waiting for workers to finish...")
	for _, w := range dlq.List() {
		log.Printf("Dead letter: task: %s failed permanently: %s", w.Task.ID, w.Reason)
	}
	wg.Wait()
	log.Println("Done")
}
