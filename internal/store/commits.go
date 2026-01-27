package store

import (
	"database/sql"

	"github.com/kilupskalvis/wvc/internal/models"
)

// CreateCommit creates a new commit
func (s *Store) CreateCommit(commit *models.Commit) error {
	_, err := s.db.Exec(`
		INSERT INTO commits (id, parent_id, merge_parent_id, message, timestamp, operation_count)
		VALUES (?, ?, ?, ?, ?, ?)`,
		commit.ID, commit.ParentID,
		sql.NullString{String: commit.MergeParentID, Valid: commit.MergeParentID != ""},
		commit.Message, commit.Timestamp, commit.OperationCount,
	)
	return err
}

// GetCommit retrieves a commit by ID
func (s *Store) GetCommit(id string) (*models.Commit, error) {
	var commit models.Commit
	var parentID sql.NullString
	var mergeParentID sql.NullString
	var timestamp string

	err := s.db.QueryRow(`
		SELECT id, parent_id, merge_parent_id, message, timestamp, operation_count
		FROM commits WHERE id = ?`, id).Scan(
		&commit.ID, &parentID, &mergeParentID, &commit.Message, &timestamp, &commit.OperationCount,
	)
	if err != nil {
		return nil, err
	}

	commit.Timestamp = parseTimestamp(timestamp)
	if parentID.Valid {
		commit.ParentID = parentID.String
	}
	if mergeParentID.Valid {
		commit.MergeParentID = mergeParentID.String
	}

	return &commit, nil
}

// GetCommitByShortID retrieves a commit by short ID prefix
func (s *Store) GetCommitByShortID(shortID string) (*models.Commit, error) {
	var commit models.Commit
	var parentID sql.NullString
	var mergeParentID sql.NullString
	var timestamp string

	err := s.db.QueryRow(`
		SELECT id, parent_id, merge_parent_id, message, timestamp, operation_count
		FROM commits WHERE id LIKE ?`, shortID+"%").Scan(
		&commit.ID, &parentID, &mergeParentID, &commit.Message, &timestamp, &commit.OperationCount,
	)
	if err != nil {
		return nil, err
	}

	commit.Timestamp = parseTimestamp(timestamp)
	if parentID.Valid {
		commit.ParentID = parentID.String
	}
	if mergeParentID.Valid {
		commit.MergeParentID = mergeParentID.String
	}

	return &commit, nil
}

// GetHEAD returns the current HEAD commit ID
func (s *Store) GetHEAD() (string, error) {
	return s.GetValue("HEAD")
}

// SetHEAD sets the current HEAD commit ID
func (s *Store) SetHEAD(commitID string) error {
	return s.SetValue("HEAD", commitID)
}

// GetCommitLog returns all commits in reverse chronological order
func (s *Store) GetCommitLog(limit int) ([]*models.Commit, error) {
	query := `
		SELECT id, parent_id, merge_parent_id, message, timestamp, operation_count
		FROM commits
		ORDER BY timestamp DESC`

	if limit > 0 {
		query += " LIMIT ?"
	}

	var rows *sql.Rows
	var err error
	if limit > 0 {
		rows, err = s.db.Query(query, limit)
	} else {
		rows, err = s.db.Query(query)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var commits []*models.Commit
	for rows.Next() {
		var commit models.Commit
		var parentID sql.NullString
		var mergeParentID sql.NullString
		var timestamp string

		err := rows.Scan(&commit.ID, &parentID, &mergeParentID, &commit.Message, &timestamp, &commit.OperationCount)
		if err != nil {
			return nil, err
		}

		commit.Timestamp = parseTimestamp(timestamp)
		if parentID.Valid {
			commit.ParentID = parentID.String
		}
		if mergeParentID.Valid {
			commit.MergeParentID = mergeParentID.String
		}

		commits = append(commits, &commit)
	}

	return commits, nil
}

// GetAllAncestors returns all ancestor commit IDs via BFS, handling merge commits
func (s *Store) GetAllAncestors(commitID string) (map[string]bool, error) {
	ancestors := make(map[string]bool)
	queue := []string{commitID}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current == "" || ancestors[current] {
			continue
		}
		ancestors[current] = true

		commit, err := s.GetCommit(current)
		if err != nil {
			continue
		}

		if commit.ParentID != "" {
			queue = append(queue, commit.ParentID)
		}
		if commit.MergeParentID != "" {
			queue = append(queue, commit.MergeParentID)
		}
	}

	return ancestors, nil
}
