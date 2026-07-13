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
	"github.com/w1ldy0uth/datadi/internal/store"
	"github.com/w1ldy0uth/datadi/internal/task"
	"github.com/w1ldy0uth/datadi/internal/worker"
)

const (
	demoTaskName = "demo-task"
	dbPath       = "datadi.db"
)

// demoPayload is the consumer-defined payload type for demoTaskName. Handlers only ever see
// raw bytes, so it's up to the consumer to marshal/unmarshal their own payload struct.
type demoPayload struct {
	Message string `json:"message"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("Opening store: %v", err)
	}
	defer st.Close()

	q := queue.New(100)
	dlq := queue.NewDeadLetterQueue()

	if recovered, err := st.LoadPendingAndRunning(ctx); err != nil {
		log.Fatalf("Loading persisted tasks: %v", err)
	} else {
		for _, t := range recovered {
			log.Printf("Recovered task %s from a previous run (status was %s), re-enqueuing", t.ID, t.Status)
			q.Enqueue(t)
		}
	}
	if entries, err := st.LoadDeadLetters(ctx); err != nil {
		log.Fatalf("Loading persisted dead letters: %v", err)
	} else {
		for _, entry := range entries {
			dlq.AddEntry(entry)
		}
	}

	reg := registry.New()
	err = reg.Register(demoTaskName, func(ctx context.Context, payload []byte) error {
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
		w := worker.New(i, reg, q, dlq, st)
		workers[i] = w
		wg.Add(1)
		wg.Go(func() {
			defer wg.Done()
			w.Start(ctx, q.Dequeue())
		})
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		runID := time.Now().UnixNano()
		for i := range 10 {
			select {
			case <-ctx.Done():
				return
			default:
			}

			payload, err := json.Marshal(demoPayload{Message: fmt.Sprintf("hello from task-%d", i)})
			if err != nil {
				log.Fatalf("Marshaling demo payload: %v", err)
			}
			t := &task.Task{
				ID:         fmt.Sprintf("task-%d-%d", runID, i),
				Name:       demoTaskName,
				Payload:    payload,
				Status:     task.StatusPending,
				CreatedAt:  time.Now(),
				MaxRetries: 3,
				Timeout:    1 * time.Second,
			}
			// Uses context.Background(), not ctx: this write must not be short-circuited by the
			// same shutdown signal the select above is checking, mirroring worker.save's rationale.
			if err := st.Save(context.Background(), t); err != nil {
				log.Printf("Persisting new task %s: %v", t.ID, err)
			}
			q.Enqueue(t)
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
