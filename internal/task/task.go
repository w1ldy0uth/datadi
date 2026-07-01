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
}
