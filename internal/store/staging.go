package store

import (
	"database/sql"
	"time"
)

// StagedChange represents a staged change in the staging area
type StagedChange struct {
	ID                 int64
	ClassName          string
	ObjectID           string
	ChangeType         string // "insert", "update", "delete"
	ObjectData         []byte
	PreviousData       []byte
	StagedAt           time.Time
	VectorHash         string
	PreviousVectorHash string
}

// AddStagedChange adds or updates a staged change
func (s *Store) AddStagedChange(change *StagedChange) error {
	_, err := s.db.Exec(`
		INSERT INTO staged_changes (class_name, object_id, change_type, object_data, previous_data, staged_at, vector_hash, previous_vector_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(class_name, object_id) DO UPDATE SET
			change_type = excluded.change_type,
			object_data = excluded.object_data,
			previous_data = excluded.previous_data,
			staged_at = excluded.staged_at,
			vector_hash = excluded.vector_hash,
			previous_vector_hash = excluded.previous_vector_hash
	`, change.ClassName, change.ObjectID, change.ChangeType, change.ObjectData, change.PreviousData, time.Now(),
		change.VectorHash, change.PreviousVectorHash)
	return err
}

// RemoveStagedChange removes a staged change
func (s *Store) RemoveStagedChange(className, objectID string) error {
	_, err := s.db.Exec(`
		DELETE FROM staged_changes WHERE class_name = ? AND object_id = ?
	`, className, objectID)
	return err
}

// RemoveStagedChangesByClass removes all staged changes for a class
func (s *Store) RemoveStagedChangesByClass(className string) error {
	_, err := s.db.Exec(`DELETE FROM staged_changes WHERE class_name = ?`, className)
	return err
}

// ClearStagedChanges removes all staged changes
func (s *Store) ClearStagedChanges() error {
	_, err := s.db.Exec("DELETE FROM staged_changes")
	return err
}

// GetStagedChange retrieves a specific staged change
func (s *Store) GetStagedChange(className, objectID string) (*StagedChange, error) {
	var change StagedChange
	var stagedAt string
	var vectorHash, previousVectorHash *string

	err := s.db.QueryRow(`
		SELECT id, class_name, object_id, change_type, object_data, previous_data, staged_at, vector_hash, previous_vector_hash
		FROM staged_changes WHERE class_name = ? AND object_id = ?
	`, className, objectID).Scan(
		&change.ID, &change.ClassName, &change.ObjectID, &change.ChangeType,
		&change.ObjectData, &change.PreviousData, &stagedAt, &vectorHash, &previousVectorHash,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	change.StagedAt = parseTimestamp(stagedAt)
	if vectorHash != nil {
		change.VectorHash = *vectorHash
	}
	if previousVectorHash != nil {
		change.PreviousVectorHash = *previousVectorHash
	}
	return &change, nil
}

// GetAllStagedChanges retrieves all staged changes
func (s *Store) GetAllStagedChanges() ([]*StagedChange, error) {
	rows, err := s.db.Query(`
		SELECT id, class_name, object_id, change_type, object_data, previous_data, staged_at, vector_hash, previous_vector_hash
		FROM staged_changes ORDER BY staged_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanStagedChanges(rows)
}

// GetStagedChangesByClass retrieves staged changes for a specific class
func (s *Store) GetStagedChangesByClass(className string) ([]*StagedChange, error) {
	rows, err := s.db.Query(`
		SELECT id, class_name, object_id, change_type, object_data, previous_data, staged_at, vector_hash, previous_vector_hash
		FROM staged_changes WHERE class_name = ? ORDER BY staged_at ASC
	`, className)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanStagedChanges(rows)
}

// GetStagedChangesCount returns the count of staged changes
func (s *Store) GetStagedChangesCount() (int, error) {
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM staged_changes").Scan(&count)
	return count, err
}

// scanStagedChanges scans rows into StagedChange structs
func scanStagedChanges(rows *sql.Rows) ([]*StagedChange, error) {
	var changes []*StagedChange

	for rows.Next() {
		var change StagedChange
		var stagedAt string
		var vectorHash, previousVectorHash *string

		err := rows.Scan(
			&change.ID, &change.ClassName, &change.ObjectID, &change.ChangeType,
			&change.ObjectData, &change.PreviousData, &stagedAt, &vectorHash, &previousVectorHash,
		)
		if err != nil {
			return nil, err
		}

		change.StagedAt = parseTimestamp(stagedAt)
		if vectorHash != nil {
			change.VectorHash = *vectorHash
		}
		if previousVectorHash != nil {
			change.PreviousVectorHash = *previousVectorHash
		}
		changes = append(changes, &change)
	}

	return changes, nil
}
