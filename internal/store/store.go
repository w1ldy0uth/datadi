// Package store persists task state to SQLite so it survives a process restart.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/w1ldy0uth/datadi/internal/queue"
	"github.com/w1ldy0uth/datadi/internal/task"
)

type Store struct {
	db *sql.DB
}

// Open opens (creating if necessary) the SQLite database at path and applies any pending migrations.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: opening %s: %w", path, err)
	}
	// SQLite only supports one writer at a time; a single connection avoids
	// SQLITE_BUSY errors from concurrent workers writing through this Store.
	db.SetMaxOpenConns(1)

	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// Save upserts a task's current state, e.g. after it moves to running, done, or pending-for-retry.
func (s *Store) Save(ctx context.Context, t *task.Task) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tasks (id, name, payload, status, created_at, updated_at, retry_count, max_retries, timeout_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name=excluded.name, payload=excluded.payload, status=excluded.status,
			updated_at=excluded.updated_at, retry_count=excluded.retry_count,
			max_retries=excluded.max_retries, timeout_ns=excluded.timeout_ns
	`, t.ID, t.Name, t.Payload, string(t.Status), t.CreatedAt, time.Now(), t.RetryCount, t.MaxRetries, int64(t.Timeout))
	if err != nil {
		return fmt.Errorf("store: saving task %s: %w", t.ID, err)
	}
	return nil
}

// SaveDeadLetter upserts a task as permanently failed, along with why.
func (s *Store) SaveDeadLetter(ctx context.Context, t *task.Task, reason error) error {
	reasonStr := ""
	if reason != nil {
		reasonStr = reason.Error()
	}
	now := time.Now()

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tasks (id, name, payload, status, created_at, updated_at, retry_count, max_retries, timeout_ns, dead_letter_reason, dead_letter_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status=excluded.status, updated_at=excluded.updated_at, retry_count=excluded.retry_count,
			dead_letter_reason=excluded.dead_letter_reason, dead_letter_at=excluded.dead_letter_at
	`, t.ID, t.Name, t.Payload, string(task.StatusFailed), t.CreatedAt, now, t.RetryCount, t.MaxRetries, int64(t.Timeout), reasonStr, now)
	if err != nil {
		return fmt.Errorf("store: dead-lettering task %s: %w", t.ID, err)
	}
	return nil
}

// LoadPendingAndRunning returns tasks that were pending or still in flight when the process last
// exited, so the caller can requeue them. Running tasks are reported back as pending: the worker
// that had them never got to report success or failure, so their in-flight attempt is treated as
// lost and eligible for a fresh dispatch.
func (s *Store) LoadPendingAndRunning(ctx context.Context) ([]*task.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, payload, status, created_at, retry_count, max_retries, timeout_ns
		FROM tasks
		WHERE status IN (?, ?)
	`, string(task.StatusPending), string(task.StatusRunning))
	if err != nil {
		return nil, fmt.Errorf("store: loading pending/running tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*task.Task
	for rows.Next() {
		var (
			t         task.Task
			status    string
			timeoutNs int64
		)
		if err := rows.Scan(&t.ID, &t.Name, &t.Payload, &status, &t.CreatedAt, &t.RetryCount, &t.MaxRetries, &timeoutNs); err != nil {
			return nil, fmt.Errorf("store: scanning task row: %w", err)
		}
		t.Status = task.StatusPending
		t.Timeout = time.Duration(timeoutNs)
		tasks = append(tasks, &t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating pending/running tasks: %w", err)
	}
	return tasks, nil
}

// LoadDeadLetters returns previously persisted dead-letter entries, so the in-memory
// DeadLetterQueue reflects prior runs after a restart.
func (s *Store) LoadDeadLetters(ctx context.Context) ([]queue.DeadLetterEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, payload, created_at, retry_count, max_retries, timeout_ns, dead_letter_reason, dead_letter_at
		FROM tasks
		WHERE status = ? AND dead_letter_reason IS NOT NULL
	`, string(task.StatusFailed))
	if err != nil {
		return nil, fmt.Errorf("store: loading dead letters: %w", err)
	}
	defer rows.Close()

	var entries []queue.DeadLetterEntry
	for rows.Next() {
		var (
			t         task.Task
			timeoutNs int64
			reason    string
			failedAt  time.Time
		)
		if err := rows.Scan(&t.ID, &t.Name, &t.Payload, &t.CreatedAt, &t.RetryCount, &t.MaxRetries, &timeoutNs, &reason, &failedAt); err != nil {
			return nil, fmt.Errorf("store: scanning dead letter row: %w", err)
		}
		t.Status = task.StatusFailed
		t.Timeout = time.Duration(timeoutNs)
		entries = append(entries, queue.DeadLetterEntry{Task: &t, Reason: reason, FailedAt: failedAt})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating dead letters: %w", err)
	}
	return entries, nil
}
