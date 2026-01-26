package store

import (
	"database/sql"
	"encoding/json"

	"github.com/kilupskalvis/wvc/internal/models"
)

// SaveSchemaVersion stores a schema snapshot
func (s *Store) SaveSchemaVersion(schemaJSON []byte, schemaHash string) (int64, error) {
	result, err := s.db.Exec(`
		INSERT INTO schema_versions (schema_json, schema_hash)
		VALUES (?, ?)
	`, schemaJSON, schemaHash)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetLatestSchemaVersion returns the most recent committed schema version
func (s *Store) GetLatestSchemaVersion() (*models.SchemaVersion, error) {
	var sv models.SchemaVersion
	var commitID sql.NullString
	var schemaHash sql.NullString
	var timestamp string

	err := s.db.QueryRow(`
		SELECT id, timestamp, schema_json, schema_hash, commit_id
		FROM schema_versions
		WHERE commit_id IS NOT NULL
		ORDER BY id DESC
		LIMIT 1
	`).Scan(&sv.ID, &timestamp, &sv.SchemaJSON, &schemaHash, &commitID)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	sv.Timestamp = timestamp
	if commitID.Valid {
		sv.CommitID = commitID.String
	}
	if schemaHash.Valid {
		sv.SchemaHash = schemaHash.String
	}

	return &sv, nil
}

// GetSchemaVersionByCommit returns the schema at a specific commit
func (s *Store) GetSchemaVersionByCommit(commitID string) (*models.SchemaVersion, error) {
	var sv models.SchemaVersion
	var nullCommitID sql.NullString
	var schemaHash sql.NullString
	var timestamp string

	err := s.db.QueryRow(`
		SELECT id, timestamp, schema_json, schema_hash, commit_id
		FROM schema_versions
		WHERE commit_id = ?
	`, commitID).Scan(&sv.ID, &timestamp, &sv.SchemaJSON, &schemaHash, &nullCommitID)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	sv.Timestamp = timestamp
	if nullCommitID.Valid {
		sv.CommitID = nullCommitID.String
	}
	if schemaHash.Valid {
		sv.SchemaHash = schemaHash.String
	}

	return &sv, nil
}

// MarkSchemaVersionCommitted associates a schema version with a commit
func (s *Store) MarkSchemaVersionCommitted(schemaVersionID int64, commitID string) error {
	_, err := s.db.Exec(`
		UPDATE schema_versions
		SET commit_id = ?
		WHERE id = ?
	`, commitID, schemaVersionID)
	return err
}

// CommitHasSchemaChange checks if a commit has associated schema changes
func (s *Store) CommitHasSchemaChange(commitID string) (bool, error) {
	// Get schema for this commit
	currentSchema, err := s.GetSchemaVersionByCommit(commitID)
	if err != nil {
		return false, err
	}
	if currentSchema == nil {
		return false, nil
	}

	// Get parent commit
	var parentID sql.NullString
	err = s.db.QueryRow(`SELECT parent_id FROM commits WHERE id = ?`, commitID).Scan(&parentID)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	if !parentID.Valid || parentID.String == "" {
		// First commit - has schema change if there's any schema
		var schema models.WeaviateSchema
		if err := json.Unmarshal(currentSchema.SchemaJSON, &schema); err != nil {
			return false, nil
		}
		return len(schema.Classes) > 0, nil
	}

	// Get parent schema
	parentSchema, err := s.GetSchemaVersionByCommit(parentID.String)
	if err != nil {
		return false, err
	}
	if parentSchema == nil {
		// No parent schema means this commit added schema
		return true, nil
	}

	// Compare hashes
	return currentSchema.SchemaHash != parentSchema.SchemaHash, nil
}

// GetPreviousCommitSchema gets the schema from the commit before the given commit
func (s *Store) GetPreviousCommitSchema(commitID string) (*models.SchemaVersion, error) {
	// Get parent commit ID
	var parentID sql.NullString
	err := s.db.QueryRow(`SELECT parent_id FROM commits WHERE id = ?`, commitID).Scan(&parentID)
	if err != nil {
		return nil, err
	}

	if !parentID.Valid || parentID.String == "" {
		return nil, nil
	}

	return s.GetSchemaVersionByCommit(parentID.String)
}
