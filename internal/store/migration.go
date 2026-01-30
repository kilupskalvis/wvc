package store

import "fmt"

const currentSchemaVersion = 2

// RunMigrations applies any pending database migrations.
func (s *Store) RunMigrations() error {
	version, err := s.getSchemaVersion()
	if err != nil {
		return err
	}

	if version < 2 {
		if err := s.migrateToV2(); err != nil {
			return fmt.Errorf("migration to v2 failed: %w", err)
		}
	}

	return nil
}

// getSchemaVersion returns the current database schema version
func (s *Store) getSchemaVersion() (int, error) {
	var version int
	err := s.db.QueryRow("SELECT version FROM wvc_schema_version ORDER BY version DESC LIMIT 1").Scan(&version)
	if err != nil {
		return 0, nil
	}
	return version, nil
}

// migrateToV2 adds stash tables
func (s *Store) migrateToV2() error {
	migration := `
	CREATE TABLE IF NOT EXISTS stashes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		message TEXT NOT NULL,
		branch_name TEXT NOT NULL DEFAULT '',
		commit_id TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS stash_changes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		stash_id INTEGER NOT NULL,
		class_name TEXT NOT NULL,
		object_id TEXT NOT NULL,
		change_type TEXT NOT NULL,
		object_data JSON,
		previous_data JSON,
		was_staged BOOLEAN NOT NULL DEFAULT FALSE,
		vector_hash TEXT,
		previous_vector_hash TEXT,
		FOREIGN KEY (stash_id) REFERENCES stashes(id)
	);

	CREATE INDEX IF NOT EXISTS idx_stash_changes_stash ON stash_changes(stash_id);
	`

	if _, err := s.db.Exec(migration); err != nil {
		return err
	}

	_, err := s.db.Exec("INSERT OR REPLACE INTO wvc_schema_version (version) VALUES (?)", 2)
	return err
}
