package store

import (
	"database/sql"

	"github.com/kilupskalvis/wvc/internal/models"
)

// CreateCommit creates a new commit
func (s *Store) CreateCommit(commit *models.Commit) error {
	_, err := s.db.Exec(`
		INSERT INTO commits (id, parent_id, message, timestamp, operation_count)
		VALUES (?, ?, ?, ?, ?)`,
		commit.ID, commit.ParentID, commit.Message, commit.Timestamp, commit.OperationCount,
	)
	return err
}

// GetCommit retrieves a commit by ID
func (s *Store) GetCommit(id string) (*models.Commit, error) {
	var commit models.Commit
	var parentID sql.NullString
	var timestamp string

	err := s.db.QueryRow(`
		SELECT id, parent_id, message, timestamp, operation_count
		FROM commits WHERE id = ?`, id).Scan(
		&commit.ID, &parentID, &commit.Message, &timestamp, &commit.OperationCount,
	)
	if err != nil {
		return nil, err
	}

	commit.Timestamp = parseTimestamp(timestamp)
	if parentID.Valid {
		commit.ParentID = parentID.String
	}

	return &commit, nil
}

// GetCommitByShortID retrieves a commit by short ID prefix
func (s *Store) GetCommitByShortID(shortID string) (*models.Commit, error) {
	var commit models.Commit
	var parentID sql.NullString
	var timestamp string

	err := s.db.QueryRow(`
		SELECT id, parent_id, message, timestamp, operation_count
		FROM commits WHERE id LIKE ?`, shortID+"%").Scan(
		&commit.ID, &parentID, &commit.Message, &timestamp, &commit.OperationCount,
	)
	if err != nil {
		return nil, err
	}

	commit.Timestamp = parseTimestamp(timestamp)
	if parentID.Valid {
		commit.ParentID = parentID.String
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
		SELECT id, parent_id, message, timestamp, operation_count
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
		var timestamp string

		err := rows.Scan(&commit.ID, &parentID, &commit.Message, &timestamp, &commit.OperationCount)
		if err != nil {
			return nil, err
		}

		commit.Timestamp = parseTimestamp(timestamp)
		if parentID.Valid {
			commit.ParentID = parentID.String
		}

		commits = append(commits, &commit)
	}

	return commits, nil
}

// GetCommitCount returns the total number of commits
func (s *Store) GetCommitCount() (int, error) {
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM commits").Scan(&count)
	return count, err
}
