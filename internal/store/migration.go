package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

const currentSchemaVersion = 5

// RunMigrations applies any pending database migrations
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

	if version < 3 {
		if err := s.migrateToV3(); err != nil {
			return fmt.Errorf("migration to v3 failed: %w", err)
		}
	}

	if version < 4 {
		if err := s.migrateToV4(); err != nil {
			return fmt.Errorf("migration to v4 failed: %w", err)
		}
	}

	if version < 5 {
		if err := s.migrateToV5(); err != nil {
			return fmt.Errorf("migration to v5 failed: %w", err)
		}
	}

	return nil
}

// getSchemaVersion returns the current schema version, 1 if not set
func (s *Store) getSchemaVersion() (int, error) {
	// Check if version table exists
	var tableName string
	err := s.db.QueryRow(`
		SELECT name FROM sqlite_master
		WHERE type='table' AND name='wvc_schema_version'
	`).Scan(&tableName)

	if err == sql.ErrNoRows {
		// Table doesn't exist, this is v1
		return 1, nil
	}
	if err != nil {
		return 0, err
	}

	// Get version
	var version int
	err = s.db.QueryRow("SELECT COALESCE(MAX(version), 1) FROM wvc_schema_version").Scan(&version)
	if err != nil {
		return 1, nil
	}

	return version, nil
}

// migrateToV2 adds staging and incremental detection tables
func (s *Store) migrateToV2() error {
	migrations := []string{
		// Create version tracking table
		`CREATE TABLE IF NOT EXISTS wvc_schema_version (
			version INTEGER PRIMARY KEY
		)`,

		// Create staging area table
		`CREATE TABLE IF NOT EXISTS staged_changes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			class_name TEXT NOT NULL,
			object_id TEXT NOT NULL,
			change_type TEXT NOT NULL,
			object_data JSON,
			previous_data JSON,
			staged_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(class_name, object_id)
		)`,

		// Create scan metadata table for incremental detection
		`CREATE TABLE IF NOT EXISTS scan_metadata (
			class_name TEXT PRIMARY KEY,
			last_scan_time INTEGER NOT NULL,
			last_scan_count INTEGER NOT NULL,
			scan_high_watermark INTEGER DEFAULT 0
		)`,

		// Create index on staged_changes
		`CREATE INDEX IF NOT EXISTS idx_staged_class ON staged_changes(class_name)`,
	}

	for _, migration := range migrations {
		if _, err := s.db.Exec(migration); err != nil {
			return err
		}
	}

	// Add last_update_time column to known_objects if it doesn't exist
	// SQLite doesn't have IF NOT EXISTS for ALTER TABLE, so we check first
	var colCount int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM pragma_table_info('known_objects')
		WHERE name='last_update_time'
	`).Scan(&colCount)
	if err == nil && colCount == 0 {
		_, err = s.db.Exec(`ALTER TABLE known_objects ADD COLUMN last_update_time INTEGER DEFAULT 0`)
		if err != nil {
			// Column might already exist in some edge cases, ignore error
		}
	}

	// Initialize scan_metadata for existing classes
	_, err = s.db.Exec(`
		INSERT OR IGNORE INTO scan_metadata (class_name, last_scan_time, last_scan_count, scan_high_watermark)
		SELECT DISTINCT class_name, 0, 0, 0 FROM known_objects
	`)
	if err != nil {
		return err
	}

	// Record migration version
	_, err = s.db.Exec("INSERT OR REPLACE INTO wvc_schema_version (version) VALUES (?)", 2)
	return err
}

// migrateToV3 adds vector blob storage and vector hash tracking
func (s *Store) migrateToV3() error {
	migrations := []string{
		// Create vector blobs table for content-addressable vector storage
		`CREATE TABLE IF NOT EXISTS vector_blobs (
			hash TEXT PRIMARY KEY,
			data BLOB NOT NULL,
			dimensions INTEGER NOT NULL,
			ref_count INTEGER DEFAULT 1
		)`,

		// Create index on ref_count for cleanup operations
		`CREATE INDEX IF NOT EXISTS idx_vector_blobs_refcount ON vector_blobs(ref_count)`,
	}

	for _, migration := range migrations {
		if _, err := s.db.Exec(migration); err != nil {
			return err
		}
	}

	// Add vector_hash column to known_objects if it doesn't exist
	if !s.columnExists("known_objects", "vector_hash") {
		_, err := s.db.Exec(`ALTER TABLE known_objects ADD COLUMN vector_hash TEXT`)
		if err != nil {
			// Column might already exist, ignore error
		}
	}

	// Add vector_hash columns to operations if they don't exist
	if !s.columnExists("operations", "vector_hash") {
		_, _ = s.db.Exec(`ALTER TABLE operations ADD COLUMN vector_hash TEXT`)
	}
	if !s.columnExists("operations", "previous_vector_hash") {
		_, _ = s.db.Exec(`ALTER TABLE operations ADD COLUMN previous_vector_hash TEXT`)
	}

	// Add vector_hash columns to staged_changes if they don't exist
	if !s.columnExists("staged_changes", "vector_hash") {
		_, _ = s.db.Exec(`ALTER TABLE staged_changes ADD COLUMN vector_hash TEXT`)
	}
	if !s.columnExists("staged_changes", "previous_vector_hash") {
		_, _ = s.db.Exec(`ALTER TABLE staged_changes ADD COLUMN previous_vector_hash TEXT`)
	}

	// Migrate existing vectors from known_objects JSON to vector_blobs
	// This extracts vectors from object_data and stores them in the new table
	if err := s.migrateExistingVectors(); err != nil {
		return fmt.Errorf("failed to migrate existing vectors: %w", err)
	}

	// Record migration version
	_, err := s.db.Exec("INSERT OR REPLACE INTO wvc_schema_version (version) VALUES (?)", 3)
	return err
}

// columnExists checks if a column exists in a table
func (s *Store) columnExists(table, column string) bool {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM pragma_table_info(?)
		WHERE name = ?
	`, table, column).Scan(&count)
	return err == nil && count > 0
}

// migrateToV4 adds schema_hash column to schema_versions table
func (s *Store) migrateToV4() error {
	// Add schema_hash column to schema_versions if it doesn't exist
	if !s.columnExists("schema_versions", "schema_hash") {
		_, err := s.db.Exec(`ALTER TABLE schema_versions ADD COLUMN schema_hash TEXT`)
		if err != nil {
			// Column might already exist in some edge cases, ignore error
		}
	}

	// Record migration version
	_, err := s.db.Exec("INSERT OR REPLACE INTO wvc_schema_version (version) VALUES (?)", 4)
	return err
}

// migrateToV5 adds branches table and HEAD_BRANCH tracking
func (s *Store) migrateToV5() error {
	migrations := []string{
		// Create branches table
		`CREATE TABLE IF NOT EXISTS branches (
			name TEXT PRIMARY KEY,
			commit_id TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (commit_id) REFERENCES commits(id)
		)`,

		// Create index on commit_id for reverse lookups
		`CREATE INDEX IF NOT EXISTS idx_branches_commit ON branches(commit_id)`,
	}

	for _, migration := range migrations {
		if _, err := s.db.Exec(migration); err != nil {
			return err
		}
	}

	// Create default "main" branch pointing to current HEAD (if any commits exist)
	head, err := s.GetHEAD()
	if err != nil {
		return err
	}

	// Only create the main branch if we have commits
	if head != "" {
		_, err = s.db.Exec(
			`INSERT OR IGNORE INTO branches (name, commit_id) VALUES ('main', ?)`,
			head,
		)
		if err != nil {
			return err
		}

		// Set HEAD_BRANCH to main
		if err := s.SetValue("HEAD_BRANCH", "main"); err != nil {
			return err
		}
	}

	// Record migration version
	_, err = s.db.Exec("INSERT OR REPLACE INTO wvc_schema_version (version) VALUES (?)", 5)
	return err
}

// migrateExistingVectors extracts vectors from existing known_objects and stores them in vector_blobs
func (s *Store) migrateExistingVectors() error {
	rows, err := s.db.Query(`
		SELECT class_name, object_id, object_data FROM known_objects
		WHERE vector_hash IS NULL
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var className, objectID string
		var objectData []byte

		if err := rows.Scan(&className, &objectID, &objectData); err != nil {
			continue
		}

		// Parse the object to extract vector
		var obj struct {
			Vector interface{} `json:"vector"`
		}
		if err := json.Unmarshal(objectData, &obj); err != nil {
			continue
		}

		if obj.Vector == nil {
			continue
		}

		// Convert vector to bytes and store
		vectorBytes, dims, err := VectorToBytes(obj.Vector)
		if err != nil || len(vectorBytes) == 0 {
			continue
		}

		// Save vector blob and get hash
		hash, err := s.SaveVectorBlob(vectorBytes, dims)
		if err != nil {
			continue
		}

		// Update known_objects with vector_hash
		_, _ = s.db.Exec(`
			UPDATE known_objects SET vector_hash = ?
			WHERE class_name = ? AND object_id = ?
		`, hash, className, objectID)
	}

	return rows.Err()
}
