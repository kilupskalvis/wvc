package store

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	bolt "go.etcd.io/bbolt"
)

const headBranchKey = "HEAD_BRANCH"

// CreateBranch stores a new branch with the given name and commit ID.
func (s *Store) CreateBranch(name, commitID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketBranches)
		if bucket == nil {
			return fmt.Errorf("branches bucket not found")
		}

		branch := &models.Branch{
			Name:      name,
			CommitID:  commitID,
			CreatedAt: time.Now(),
		}

		data, err := json.Marshal(branch)
		if err != nil {
			return fmt.Errorf("marshal branch: %w", err)
		}

		return bucket.Put([]byte(name), data)
	})
}

// GetBranch retrieves a branch by name. Returns (nil, nil) if not found.
func (s *Store) GetBranch(name string) (*models.Branch, error) {
	var branch *models.Branch

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketBranches)
		if bucket == nil {
			return nil
		}

		data := bucket.Get([]byte(name))
		if data == nil {
			return nil
		}

		branch = &models.Branch{}
		return json.Unmarshal(data, branch)
	})

	if err != nil {
		return nil, err
	}

	return branch, nil
}

// ListBranches returns all branches sorted by name.
func (s *Store) ListBranches() ([]*models.Branch, error) {
	var branches []*models.Branch

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketBranches)
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(k, v []byte) error {
			var branch models.Branch
			if err := json.Unmarshal(v, &branch); err != nil {
				return fmt.Errorf("unmarshal branch: %w", err)
			}
			branches = append(branches, &branch)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	sort.Slice(branches, func(i, j int) bool {
		return branches[i].Name < branches[j].Name
	})

	return branches, nil
}

// UpdateBranch updates an existing branch's commit ID.
func (s *Store) UpdateBranch(name, commitID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketBranches)
		if bucket == nil {
			return fmt.Errorf("branches bucket not found")
		}

		data := bucket.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("branch not found: %s", name)
		}

		var branch models.Branch
		if err := json.Unmarshal(data, &branch); err != nil {
			return fmt.Errorf("unmarshal branch: %w", err)
		}

		branch.CommitID = commitID

		updatedData, err := json.Marshal(branch)
		if err != nil {
			return fmt.Errorf("marshal branch: %w", err)
		}

		return bucket.Put([]byte(name), updatedData)
	})
}

// DeleteBranch removes a branch by name.
func (s *Store) DeleteBranch(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketBranches)
		if bucket == nil {
			return fmt.Errorf("branches bucket not found")
		}

		data := bucket.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("branch not found: %s", name)
		}

		return bucket.Delete([]byte(name))
	})
}

// GetCurrentBranch retrieves the current HEAD branch name from the kv bucket.
// Returns ("", nil) if no branch is set.
func (s *Store) GetCurrentBranch() (string, error) {
	var branchName string

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketKV)
		if bucket == nil {
			return nil
		}

		data := bucket.Get([]byte(headBranchKey))
		if data != nil {
			branchName = string(data)
		}
		return nil
	})

	if err != nil {
		return "", err
	}

	return branchName, nil
}

// SetCurrentBranch sets the current HEAD branch name in the kv bucket.
func (s *Store) SetCurrentBranch(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketKV)
		if bucket == nil {
			return fmt.Errorf("kv bucket not found")
		}

		return bucket.Put([]byte(headBranchKey), []byte(name))
	})
}

// BranchExists checks if a branch with the given name exists.
func (s *Store) BranchExists(name string) (bool, error) {
	var exists bool

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketBranches)
		if bucket == nil {
			return nil
		}

		data := bucket.Get([]byte(name))
		exists = data != nil
		return nil
	})

	if err != nil {
		return false, err
	}

	return exists, nil
}
