package store

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"time"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrate applies any embedded migrations not yet recorded in schema_migrations, in filename
// order. Each new schema change ships as an additional numbered .sql file rather than editing
// an existing one, so already-deployed databases only ever move forward.
func migrate(db *sql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at DATETIME NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("store: creating schema_migrations table: %w", err)
	}

	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("store: reading migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	for _, entry := range entries {
		version := entry.Name()

		var applied int
		if err := db.QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = ?`, version).Scan(&applied); err != nil {
			return fmt.Errorf("store: checking migration %s: %w", version, err)
		}
		if applied > 0 {
			continue
		}

		script, err := migrationFS.ReadFile("migrations/" + version)
		if err != nil {
			return fmt.Errorf("store: reading migration %s: %w", version, err)
		}

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("store: starting transaction for migration %s: %w", version, err)
		}
		if _, err := tx.Exec(string(script)); err != nil {
			tx.Rollback()
			return fmt.Errorf("store: applying migration %s: %w", version, err)
		}
		if _, err := tx.Exec(`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`, version, time.Now()); err != nil {
			tx.Rollback()
			return fmt.Errorf("store: recording migration %s: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: committing migration %s: %w", version, err)
		}
	}

	return nil
}
