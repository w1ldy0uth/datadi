package queue

import "github.com/w1ldy0uth/datadi/internal/task"

type Queue struct {
	ch chan *task.Task
}

func New(size int) *Queue {
	return &Queue{ch: make(chan *task.Task, size)}
}

func (q *Queue) Enqueue(t *task.Task) {
	q.ch <- t
}

func (q *Queue) Dequeue() <-chan *task.Task {
	return q.ch
}
