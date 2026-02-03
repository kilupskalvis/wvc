package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/kilupskalvis/wvc/internal/models"
	bolt "go.etcd.io/bbolt"
)

// CreateCommit stores a new commit.
func (s *Store) CreateCommit(commit *models.Commit) error {
	data, err := json.Marshal(commit)
	if err != nil {
		return fmt.Errorf("marshal commit: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommits)
		if b == nil {
			return fmt.Errorf("commits bucket not found")
		}
		return b.Put([]byte(commit.ID), data)
	})
}

// GetCommit retrieves a commit by its full ID.
func (s *Store) GetCommit(id string) (*models.Commit, error) {
	var commit models.Commit
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommits)
		if b == nil {
			return fmt.Errorf("commits bucket not found")
		}
		v := b.Get([]byte(id))
		if v == nil {
			return fmt.Errorf("commit not found: %s", id)
		}
		return json.Unmarshal(v, &commit)
	})
	if err != nil {
		return nil, err
	}
	return &commit, nil
}

// GetCommitByShortID retrieves a commit by a prefix of its ID.
func (s *Store) GetCommitByShortID(shortID string) (*models.Commit, error) {
	var commit models.Commit
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketCommits).Cursor()
		prefix := []byte(shortID)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if found {
				return fmt.Errorf("ambiguous short commit ID: %s", shortID)
			}
			if err := json.Unmarshal(v, &commit); err != nil {
				return fmt.Errorf("unmarshal commit: %w", err)
			}
			found = true
		}
		if !found {
			return fmt.Errorf("commit not found: %s", shortID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &commit, nil
}

// GetHEAD returns the current HEAD commit ID.
func (s *Store) GetHEAD() (string, error) {
	return s.GetValue("HEAD")
}

// SetHEAD sets the current HEAD commit ID.
func (s *Store) SetHEAD(commitID string) error {
	return s.SetValue("HEAD", commitID)
}

// GetCommitLog returns commits in reverse chronological order.
// If limit is 0, all commits are returned.
func (s *Store) GetCommitLog(limit int) ([]*models.Commit, error) {
	var commits []*models.Commit
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommits)
		return b.ForEach(func(k, v []byte) error {
			var c models.Commit
			if err := json.Unmarshal(v, &c); err != nil {
				return err
			}
			commits = append(commits, &c)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Sort by timestamp descending (bbolt keys are sorted lexicographically by commit ID,
	// but we need chronological order)
	sortCommitsByTimestamp(commits)

	if limit > 0 && len(commits) > limit {
		commits = commits[:limit]
	}
	return commits, nil
}

// sortCommitsByTimestamp sorts commits newest-first.
func sortCommitsByTimestamp(commits []*models.Commit) {
	sort.Slice(commits, func(i, j int) bool {
		return commits[i].Timestamp.After(commits[j].Timestamp)
	})
}

// GetAllAncestors returns all ancestor commit IDs via BFS, handling merge commits.
func (s *Store) GetAllAncestors(commitID string) (map[string]bool, error) {
	ancestors := make(map[string]bool)

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommits)
		if b == nil {
			return fmt.Errorf("commits bucket not found")
		}

		queue := []string{commitID}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]

			if current == "" || ancestors[current] {
				continue
			}

			v := b.Get([]byte(current))
			if v == nil {
				continue
			}

			var commit models.Commit
			if err := json.Unmarshal(v, &commit); err != nil {
				return fmt.Errorf("unmarshal commit %s: %w", current, err)
			}

			ancestors[current] = true

			if commit.ParentID != "" {
				queue = append(queue, commit.ParentID)
			}
			if commit.MergeParentID != "" {
				queue = append(queue, commit.MergeParentID)
			}
		}
		return nil
	})

	return ancestors, err
}

// HasCommit checks whether a commit exists.
func (s *Store) HasCommit(id string) (bool, error) {
	var exists bool
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommits)
		if b == nil {
			return nil
		}
		exists = b.Get([]byte(id)) != nil
		return nil
	})
	return exists, err
}
