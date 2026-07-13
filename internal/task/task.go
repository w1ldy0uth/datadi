package task

import "time"

type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

type Task struct {
	ID         string
	Name       string
	Payload    []byte
	Status     Status
	CreatedAt  time.Time
	RetryCount int
	MaxRetries int
	// Timeout bounds a single dispatch attempt via context.WithTimeout. Zero means no per-task deadline.
	Timeout time.Duration
}
