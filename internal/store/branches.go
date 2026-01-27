package store

import (
	"database/sql"
	"fmt"

	"github.com/kilupskalvis/wvc/internal/models"
)

// CreateBranch creates a new branch pointing to a commit
func (s *Store) CreateBranch(name, commitID string) error {
	_, err := s.db.Exec(
		`INSERT INTO branches (name, commit_id) VALUES (?, ?)`,
		name, commitID,
	)
	return err
}

// GetBranch retrieves a branch by name
func (s *Store) GetBranch(name string) (*models.Branch, error) {
	var branch models.Branch
	var createdAt string
	err := s.db.QueryRow(
		`SELECT name, commit_id, created_at FROM branches WHERE name = ?`,
		name,
	).Scan(&branch.Name, &branch.CommitID, &createdAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	branch.CreatedAt = parseTimestamp(createdAt)
	return &branch, nil
}

// ListBranches returns all branches
func (s *Store) ListBranches() ([]*models.Branch, error) {
	rows, err := s.db.Query(
		`SELECT name, commit_id, created_at FROM branches ORDER BY name`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var branches []*models.Branch
	for rows.Next() {
		var branch models.Branch
		var createdAt string
		if err := rows.Scan(&branch.Name, &branch.CommitID, &createdAt); err != nil {
			return nil, err
		}
		branch.CreatedAt = parseTimestamp(createdAt)
		branches = append(branches, &branch)
	}
	return branches, rows.Err()
}

// UpdateBranch updates a branch to point to a new commit
func (s *Store) UpdateBranch(name, commitID string) error {
	result, err := s.db.Exec(
		`UPDATE branches SET commit_id = ? WHERE name = ?`,
		commitID, name,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("branch '%s' not found", name)
	}
	return nil
}

// DeleteBranch removes a branch
func (s *Store) DeleteBranch(name string) error {
	result, err := s.db.Exec(`DELETE FROM branches WHERE name = ?`, name)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("branch '%s' not found", name)
	}
	return nil
}

// GetCurrentBranch returns the current branch name (empty if detached)
func (s *Store) GetCurrentBranch() (string, error) {
	return s.GetValue("HEAD_BRANCH")
}

// SetCurrentBranch sets the current branch (empty string for detached)
func (s *Store) SetCurrentBranch(name string) error {
	return s.SetValue("HEAD_BRANCH", name)
}

// BranchExists checks if a branch exists
func (s *Store) BranchExists(name string) (bool, error) {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM branches WHERE name = ?`,
		name,
	).Scan(&count)
	return count > 0, err
}

// GetHeadState returns the complete HEAD state
func (s *Store) GetHeadState() (*models.HeadState, error) {
	commitID, err := s.GetHEAD()
	if err != nil {
		return nil, err
	}
	branchName, err := s.GetCurrentBranch()
	if err != nil {
		return nil, err
	}
	return &models.HeadState{
		CommitID:   commitID,
		BranchName: branchName,
		IsDetached: branchName == "",
	}, nil
}
