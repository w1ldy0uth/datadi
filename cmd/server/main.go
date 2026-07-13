package main

import (
	"context"
	"encoding/json"
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

// demoPayload is the consumer-defined payload type for demoTaskName. Handlers only ever see
// raw bytes, so it's up to the consumer to marshal/unmarshal their own payload struct.
type demoPayload struct {
	Message string `json:"message"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	q := queue.New(100)
	dlq := queue.NewDeadLetterQueue()

	reg := registry.New()
	err := reg.Register(demoTaskName, func(ctx context.Context, payload []byte) error {
		var p demoPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return task.Permanent(fmt.Errorf("%s: invalid payload: %w", demoTaskName, err))
		}

		log.Printf("Executing task %s: %s", demoTaskName, p.Message)
		switch rand.Intn(4) {
		case 0: // transient failure, retried with backoff
			return fmt.Errorf("%s: simulated transient error", demoTaskName)
		case 1: // permanent failure, dead-lettered immediately
			return task.Permanent(fmt.Errorf("%s: simulated permanent error", demoTaskName))
		case 2: // runs past the task's timeout; ctx expires and Dispatch returns ctx.Err()
			select {
			case <-time.After(2 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
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
			payload, err := json.Marshal(demoPayload{Message: fmt.Sprintf("hello from task-%d", i)})
			if err != nil {
				log.Fatalf("Marshaling demo payload: %v", err)
			}
			q.Enqueue(&task.Task{
				ID:         fmt.Sprintf("task-%d", i),
				Name:       demoTaskName,
				Payload:    payload,
				Status:     task.StatusPending,
				CreatedAt:  time.Now(),
				MaxRetries: 3,
				Timeout:    1 * time.Second,
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
