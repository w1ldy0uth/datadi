package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/w1ldy0uth/datadi/internal/task"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpen_AppliesMigrations(t *testing.T) {
	s := openTestStore(t)

	var count int
	if err := s.db.QueryRow(`SELECT COUNT(1) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("querying schema_migrations: %v", err)
	}
	if count == 0 {
		t.Fatal("expected at least one migration to be recorded")
	}
}

func TestOpen_IsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open (re-applying migrations) should not fail: %v", err)
	}
	s2.Close()
}

func TestSave_And_LoadPendingAndRunning(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	pending := &task.Task{ID: "p1", Name: "demo", Status: task.StatusPending, CreatedAt: time.Now(), MaxRetries: 3, Timeout: 5 * time.Second}
	running := &task.Task{ID: "r1", Name: "demo", Status: task.StatusRunning, CreatedAt: time.Now(), MaxRetries: 3}
	done := &task.Task{ID: "d1", Name: "demo", Status: task.StatusDone, CreatedAt: time.Now(), MaxRetries: 3}

	for _, tk := range []*task.Task{pending, running, done} {
		if err := s.Save(ctx, tk); err != nil {
			t.Fatalf("Save(%s): %v", tk.ID, err)
		}
	}

	recovered, err := s.LoadPendingAndRunning(ctx)
	if err != nil {
		t.Fatalf("LoadPendingAndRunning: %v", err)
	}

	byID := map[string]*task.Task{}
	for _, tk := range recovered {
		byID[tk.ID] = tk
	}

	if len(byID) != 2 {
		t.Fatalf("recovered %d tasks, want 2 (pending + running, not done): %+v", len(byID), byID)
	}
	if _, ok := byID["d1"]; ok {
		t.Fatal("a done task should not be recovered")
	}

	// A task that was running when the process died comes back as pending,
	// since its in-flight attempt was lost and needs a fresh dispatch.
	if got := byID["r1"].Status; got != task.StatusPending {
		t.Errorf("recovered running task status = %q, want %q", got, task.StatusPending)
	}
	if got := byID["p1"].Timeout; got != 5*time.Second {
		t.Errorf("recovered task Timeout = %s, want %s", got, 5*time.Second)
	}
}

func TestSave_Upserts(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	tk := &task.Task{ID: "t1", Name: "demo", Status: task.StatusPending, CreatedAt: time.Now(), MaxRetries: 3}
	if err := s.Save(ctx, tk); err != nil {
		t.Fatalf("first Save: %v", err)
	}

	tk.Status = task.StatusRunning
	tk.RetryCount = 2
	if err := s.Save(ctx, tk); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	recovered, err := s.LoadPendingAndRunning(ctx)
	if err != nil {
		t.Fatalf("LoadPendingAndRunning: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("expected exactly one row after upsert, got %d", len(recovered))
	}
	if recovered[0].RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2 (Save should overwrite, not duplicate)", recovered[0].RetryCount)
	}
}

func TestSaveDeadLetter_And_LoadDeadLetters(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	tk := &task.Task{ID: "t1", Name: "demo", Status: task.StatusFailed, CreatedAt: time.Now(), MaxRetries: 3, RetryCount: 4}
	if err := s.SaveDeadLetter(ctx, tk, errors.New("boom")); err != nil {
		t.Fatalf("SaveDeadLetter: %v", err)
	}

	entries, err := s.LoadDeadLetters(ctx)
	if err != nil {
		t.Fatalf("LoadDeadLetters: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("loaded %d dead letters, want 1", len(entries))
	}
	if entries[0].Task.ID != "t1" {
		t.Errorf("dead letter task ID = %q, want %q", entries[0].Task.ID, "t1")
	}
	if entries[0].Reason != "boom" {
		t.Errorf("dead letter reason = %q, want %q", entries[0].Reason, "boom")
	}

	// A dead-lettered task should not also show up as recoverable pending/running work.
	recovered, err := s.LoadPendingAndRunning(ctx)
	if err != nil {
		t.Fatalf("LoadPendingAndRunning: %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("expected no pending/running tasks, got %d", len(recovered))
	}
}
