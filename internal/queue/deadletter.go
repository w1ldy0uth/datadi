package queue

import (
	"sync"
	"time"

	"github.com/w1ldy0uth/datadi/internal/task"
)

type DeadLetterEntry struct {
	Task     *task.Task
	Reason   string
	FailedAt time.Time
}

type DeadLetterQueue struct {
	mu      sync.Mutex
	entries []DeadLetterEntry
}

func NewDeadLetterQueue() *DeadLetterQueue {
	return &DeadLetterQueue{}
}

func (d *DeadLetterQueue) Add(t *task.Task, reason error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	reasonStr := ""
	if reason != nil {
		reasonStr = reason.Error()
	}

	d.entries = append(d.entries, DeadLetterEntry{
		Task:     t,
		Reason:   reasonStr,
		FailedAt: time.Now(),
	})
}

func (d *DeadLetterQueue) List() []DeadLetterEntry {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]DeadLetterEntry, len(d.entries))
	copy(out, d.entries)
	return out
}

func (d *DeadLetterQueue) Requeue(id string, target Requeuer) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, entry := range d.entries {
		if entry.Task.ID == id {
			entry.Task.RetryCount = 0
			entry.Task.Status = task.StatusPending
			target.Enqueue(entry.Task)
			d.entries = append(d.entries[:i], d.entries[i+1:]...)
			return true
		}
	}
	return false
}

type Requeuer interface {
	Enqueue(t *task.Task)
}
