package queue

import (
	"errors"
	"testing"

	"github.com/w1ldy0uth/datadi/internal/task"
)

func TestDeadLetterQueue_AddAndList(t *testing.T) {
	dlq := NewDeadLetterQueue()
	tk := &task.Task{ID: "t1", Status: task.StatusFailed}

	dlq.Add(tk, errors.New("boom"))

	entries := dlq.List()
	if len(entries) != 1 {
		t.Fatalf("List() returned %d entries, want 1", len(entries))
	}
	if entries[0].Task.ID != "t1" {
		t.Errorf("entry task ID = %q, want %q", entries[0].Task.ID, "t1")
	}
	if entries[0].Reason != "boom" {
		t.Errorf("entry reason = %q, want %q", entries[0].Reason, "boom")
	}
}

func TestDeadLetterQueue_List_ReturnsDefensiveSliceCopy(t *testing.T) {
	dlq := NewDeadLetterQueue()
	dlq.Add(&task.Task{ID: "t1"}, errors.New("boom"))

	entries := dlq.List()
	entries[0] = DeadLetterEntry{Task: &task.Task{ID: "replaced"}}

	if dlq.List()[0].Task.ID != "t1" {
		t.Fatal("mutating the returned slice's entries affected the dead-letter queue's internal state")
	}
}

type fakeRequeuer struct {
	enqueued []*task.Task
}

func (f *fakeRequeuer) Enqueue(t *task.Task) {
	f.enqueued = append(f.enqueued, t)
}

func TestDeadLetterQueue_Requeue_MovesEntryBack(t *testing.T) {
	dlq := NewDeadLetterQueue()
	tk := &task.Task{ID: "t1", Status: task.StatusFailed, RetryCount: 3}
	dlq.Add(tk, errors.New("boom"))

	target := &fakeRequeuer{}
	ok := dlq.Requeue("t1", target)
	if !ok {
		t.Fatal("Requeue returned false for a known ID")
	}

	if len(target.enqueued) != 1 || target.enqueued[0].ID != "t1" {
		t.Fatalf("expected task t1 to be enqueued on the target, got %+v", target.enqueued)
	}
	if tk.Status != task.StatusPending {
		t.Errorf("requeued task status = %q, want %q", tk.Status, task.StatusPending)
	}
	if tk.RetryCount != 0 {
		t.Errorf("requeued task RetryCount = %d, want 0", tk.RetryCount)
	}
	if len(dlq.List()) != 0 {
		t.Fatal("requeued task was not removed from the dead-letter queue")
	}
}

func TestDeadLetterQueue_Requeue_UnknownID(t *testing.T) {
	dlq := NewDeadLetterQueue()
	dlq.Add(&task.Task{ID: "t1"}, errors.New("boom"))

	target := &fakeRequeuer{}
	ok := dlq.Requeue("does-not-exist", target)
	if ok {
		t.Fatal("Requeue returned true for an unknown ID")
	}
	if len(target.enqueued) != 0 {
		t.Fatal("Requeue enqueued a task for an unknown ID")
	}
	if len(dlq.List()) != 1 {
		t.Fatal("Requeue with an unknown ID mutated the dead-letter queue")
	}
}
