CREATE TABLE IF NOT EXISTS tasks (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    payload            BLOB,
    status             TEXT NOT NULL,
    created_at         DATETIME NOT NULL,
    updated_at         DATETIME NOT NULL,
    retry_count        INTEGER NOT NULL DEFAULT 0,
    max_retries        INTEGER NOT NULL DEFAULT 0,
    timeout_ns         INTEGER NOT NULL DEFAULT 0,
    dead_letter_reason TEXT,
    dead_letter_at     DATETIME
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
