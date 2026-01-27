// Package store provides SQLite-based persistence for WVC.
// It manages operations, commits, and known object state.
package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Store represents the SQLite database store
type Store struct {
	db *sql.DB
}

// New creates a new store connection
func New(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	s := &Store{db: db}
	return s, nil
}

// Close closes the database connection
func (s *Store) Close() error {
	return s.db.Close()
}

// Initialize creates the database schema
func (s *Store) Initialize() error {
	schema := `
	-- Operations log (append-only)
	CREATE TABLE IF NOT EXISTS operations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		operation_type TEXT NOT NULL,
		class_name TEXT NOT NULL,
		object_id TEXT NOT NULL,
		object_data JSON,
		previous_data JSON,
		commit_id TEXT,
		reverted BOOLEAN DEFAULT FALSE,
		vector_hash TEXT,
		previous_vector_hash TEXT
	);

	-- Commits
	CREATE TABLE IF NOT EXISTS commits (
		id TEXT PRIMARY KEY,
		parent_id TEXT,
		message TEXT NOT NULL,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		operation_count INTEGER,
		FOREIGN KEY (parent_id) REFERENCES commits(id)
	);

	-- Schema versions (Weaviate schema tracking)
	CREATE TABLE IF NOT EXISTS schema_versions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		schema_json JSON NOT NULL,
		schema_hash TEXT,
		commit_id TEXT,
		FOREIGN KEY (commit_id) REFERENCES commits(id)
	);

	-- Config (HEAD pointer, etc.)
	CREATE TABLE IF NOT EXISTS kv (
		key TEXT PRIMARY KEY,
		value TEXT
	);

	-- Known objects (last known state for diff detection)
	CREATE TABLE IF NOT EXISTS known_objects (
		class_name TEXT NOT NULL,
		object_id TEXT NOT NULL,
		object_hash TEXT NOT NULL,
		object_data JSON NOT NULL,
		last_update_time INTEGER DEFAULT 0,
		vector_hash TEXT,
		PRIMARY KEY (class_name, object_id)
	);

	-- Staging area for selective commits
	CREATE TABLE IF NOT EXISTS staged_changes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		class_name TEXT NOT NULL,
		object_id TEXT NOT NULL,
		change_type TEXT NOT NULL,
		object_data JSON,
		previous_data JSON,
		staged_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		vector_hash TEXT,
		previous_vector_hash TEXT,
		UNIQUE(class_name, object_id)
	);

	-- Scan metadata for incremental detection
	CREATE TABLE IF NOT EXISTS scan_metadata (
		class_name TEXT PRIMARY KEY,
		last_scan_time INTEGER NOT NULL,
		last_scan_count INTEGER NOT NULL,
		scan_high_watermark INTEGER DEFAULT 0
	);

	-- WVC schema version tracking
	CREATE TABLE IF NOT EXISTS wvc_schema_version (
		version INTEGER PRIMARY KEY
	);

	-- Vector blobs (content-addressable storage for vectors)
	CREATE TABLE IF NOT EXISTS vector_blobs (
		hash TEXT PRIMARY KEY,
		data BLOB NOT NULL,
		dimensions INTEGER NOT NULL,
		ref_count INTEGER DEFAULT 1
	);

	-- Branches (named references to commits)
	CREATE TABLE IF NOT EXISTS branches (
		name TEXT PRIMARY KEY,
		commit_id TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (commit_id) REFERENCES commits(id)
	);

	-- Indexes
	CREATE INDEX IF NOT EXISTS idx_vector_blobs_refcount ON vector_blobs(ref_count);
	CREATE INDEX IF NOT EXISTS idx_operations_commit ON operations(commit_id);
	CREATE INDEX IF NOT EXISTS idx_operations_object ON operations(class_name, object_id);
	CREATE INDEX IF NOT EXISTS idx_staged_class ON staged_changes(class_name);
	CREATE INDEX IF NOT EXISTS idx_branches_commit ON branches(commit_id);
	`

	_, err := s.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Mark as current schema version
	_, err = s.db.Exec("INSERT OR REPLACE INTO wvc_schema_version (version) VALUES (?)", currentSchemaVersion)
	if err != nil {
		return fmt.Errorf("failed to set schema version: %w", err)
	}

	return nil
}

// DB returns the underlying database connection for advanced queries
func (s *Store) DB() *sql.DB {
	return s.db
}

// GetValue gets a value from the key-value store
func (s *Store) GetValue(key string) (string, error) {
	var value string
	err := s.db.QueryRow("SELECT value FROM kv WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// SetValue sets a value in the key-value store
func (s *Store) SetValue(key, value string) error {
	_, err := s.db.Exec(
		"INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
		key, value, value,
	)
	return err
}

// parseTimestamp parses a timestamp string from SQLite in various formats
func parseTimestamp(s string) time.Time {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999+07:00",
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05.999999+07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05+07:00",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
