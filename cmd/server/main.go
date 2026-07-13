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
	"github.com/w1ldy0uth/datadi/internal/registry"
	"github.com/w1ldy0uth/datadi/internal/task"
	"github.com/w1ldy0uth/datadi/internal/worker"
)

const demoTaskName = "demo-task"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	q := queue.New(100)
	dlq := queue.NewDeadLetterQueue()

	reg := registry.New()
	err := reg.Register(demoTaskName, func(ctx context.Context, payload []byte) error {
		log.Printf("Executing task %s", demoTaskName)
		switch rand.Intn(3) {
		case 0: // transient failure, retried with backoff
			return fmt.Errorf("%s: simulated transient error", demoTaskName)
		case 1: // permanent failure, dead-lettered immediately
			return task.Permanent(fmt.Errorf("%s: simulated permanent error", demoTaskName))
		default:
			time.Sleep(500 * time.Millisecond)
			return nil
		}
	})
	if err != nil {
		log.Fatalf("Registering handler: %v", err)
	}

	const numWorkers = 3
	var wg sync.WaitGroup
	workers := make([]*worker.Worker, numWorkers)

	for i := range numWorkers {
		w := worker.New(i, reg, q, dlq)
		workers[i] = w
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
				Name:       demoTaskName,
				Status:     task.StatusPending,
				CreatedAt:  time.Now(),
				MaxRetries: 3,
			})
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down, waiting for workers to finish...")
	wg.Wait()
	for _, w := range workers {
		w.Wait()
	}
	for _, w := range dlq.List() {
		log.Printf("Dead letter: task: %s failed permanently: %s", w.Task.ID, w.Reason)
	}
	log.Println("Done")
}
