package store

import (
	"database/sql"
	"fmt"

	"github.com/kilupskalvis/wvc/internal/models"
)

// CreateStash inserts a new stash entry and returns its ID
func (s *Store) CreateStash(message, branchName, commitID string) (int64, error) {
	result, err := s.db.Exec(`
		INSERT INTO stashes (message, branch_name, commit_id) VALUES (?, ?, ?)
	`, message, branchName, commitID)
	if err != nil {
		return 0, fmt.Errorf("failed to create stash: %w", err)
	}
	return result.LastInsertId()
}

// CreateStashChange inserts a stash change record
func (s *Store) CreateStashChange(change *models.StashChange) error {
	_, err := s.db.Exec(`
		INSERT INTO stash_changes (stash_id, class_name, object_id, change_type, object_data, previous_data, was_staged, vector_hash, previous_vector_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, change.StashID, change.ClassName, change.ObjectID, change.ChangeType,
		change.ObjectData, change.PreviousData, change.WasStaged,
		change.VectorHash, change.PreviousVectorHash)
	return err
}

// ListStashes returns all stashes ordered newest first (stash@{0} = highest id)
func (s *Store) ListStashes() ([]*models.Stash, error) {
	rows, err := s.db.Query(`
		SELECT id, message, branch_name, commit_id, created_at
		FROM stashes ORDER BY id DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stashes []*models.Stash
	for rows.Next() {
		var stash models.Stash
		var createdAt string
		if err := rows.Scan(&stash.ID, &stash.Message, &stash.BranchName, &stash.CommitID, &createdAt); err != nil {
			return nil, err
		}
		stash.CreatedAt = parseTimestamp(createdAt)
		stashes = append(stashes, &stash)
	}
	return stashes, rows.Err()
}

// GetStashByIndex returns the stash at the given index (0 = newest)
func (s *Store) GetStashByIndex(index int) (*models.Stash, error) {
	var stash models.Stash
	var createdAt string

	err := s.db.QueryRow(`
		SELECT id, message, branch_name, commit_id, created_at
		FROM stashes ORDER BY id DESC LIMIT 1 OFFSET ?
	`, index).Scan(&stash.ID, &stash.Message, &stash.BranchName, &stash.CommitID, &createdAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	stash.CreatedAt = parseTimestamp(createdAt)
	return &stash, nil
}

// GetStashChanges returns all changes for a given stash ID
func (s *Store) GetStashChanges(stashID int64) ([]*models.StashChange, error) {
	rows, err := s.db.Query(`
		SELECT id, stash_id, class_name, object_id, change_type, object_data, previous_data, was_staged, vector_hash, previous_vector_hash
		FROM stash_changes WHERE stash_id = ?
	`, stashID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []*models.StashChange
	for rows.Next() {
		var sc models.StashChange
		var vectorHash, previousVectorHash *string

		if err := rows.Scan(&sc.ID, &sc.StashID, &sc.ClassName, &sc.ObjectID,
			&sc.ChangeType, &sc.ObjectData, &sc.PreviousData, &sc.WasStaged,
			&vectorHash, &previousVectorHash); err != nil {
			return nil, err
		}

		if vectorHash != nil {
			sc.VectorHash = *vectorHash
		}
		if previousVectorHash != nil {
			sc.PreviousVectorHash = *previousVectorHash
		}
		changes = append(changes, &sc)
	}
	return changes, rows.Err()
}

// DeleteStash removes a stash and its changes
func (s *Store) DeleteStash(stashID int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM stash_changes WHERE stash_id = ?", stashID); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM stashes WHERE id = ?", stashID); err != nil {
		return err
	}

	return tx.Commit()
}

// DeleteAllStashes removes all stash entries and their changes
func (s *Store) DeleteAllStashes() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM stash_changes"); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM stashes"); err != nil {
		return err
	}

	return tx.Commit()
}

// GetStashCount returns the total number of stashes
func (s *Store) GetStashCount() (int, error) {
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM stashes").Scan(&count)
	return count, err
}
