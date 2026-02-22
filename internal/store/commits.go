package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

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
		b := tx.Bucket(bucketCommits)
		if b == nil {
			return fmt.Errorf("commits bucket not found (database not initialized?)")
		}
		c := b.Cursor()
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
		if b == nil {
			return fmt.Errorf("commits bucket not found (database not initialized?)")
		}
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

// FinalizeCommit atomically performs the entire commit workflow in a single bbolt
// transaction: re-keys uncommitted operations under the commit ID, stores the commit,
// sets HEAD, and updates (or creates) the branch pointer. This prevents partial state
// if the process crashes mid-commit.
func (s *Store) FinalizeCommit(commit *models.Commit, branchName string, branchExists bool) (int64, error) {
	var count int64
	commitData, err := json.Marshal(commit)
	if err != nil {
		return 0, fmt.Errorf("marshal commit: %w", err)
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		opBucket := tx.Bucket(bucketOperations)
		if opBucket == nil {
			return fmt.Errorf("operations bucket not found (database not initialized?)")
		}
		commitBucket := tx.Bucket(bucketCommits)
		if commitBucket == nil {
			return fmt.Errorf("commits bucket not found (database not initialized?)")
		}
		kvBucket := tx.Bucket(bucketKV)
		if kvBucket == nil {
			return fmt.Errorf("kv bucket not found (database not initialized?)")
		}

		// 1. Mark uncommitted operations as committed under this commit ID
		var keys [][]byte
		c := opBucket.Cursor()
		prefix := []byte(uncommittedPrefix)
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keys = append(keys, keyCopy)
		}

		for seq, oldKey := range keys {
			v := opBucket.Get(oldKey)
			if v == nil {
				continue
			}
			var op models.Operation
			if err := json.Unmarshal(v, &op); err != nil {
				return fmt.Errorf("unmarshal operation: %w", err)
			}
			op.CommitID = commit.ID
			op.Seq = seq
			newData, err := json.Marshal(&op)
			if err != nil {
				return fmt.Errorf("marshal operation: %w", err)
			}
			if err := opBucket.Put(operationKey(commit.ID, seq), newData); err != nil {
				return err
			}
			if err := opBucket.Delete(oldKey); err != nil {
				return err
			}
			count++
		}

		// 2. Store commit
		if err := commitBucket.Put([]byte(commit.ID), commitData); err != nil {
			return fmt.Errorf("store commit: %w", err)
		}

		// 3. Set HEAD
		if err := kvBucket.Put([]byte("HEAD"), []byte(commit.ID)); err != nil {
			return fmt.Errorf("set HEAD: %w", err)
		}

		// 4. Update or create branch
		if branchName != "" {
			branchBucket := tx.Bucket(bucketBranches)
			if branchBucket == nil {
				return fmt.Errorf("branches bucket not found (database not initialized?)")
			}

			if branchExists {
				data := branchBucket.Get([]byte(branchName))
				if data == nil {
					return fmt.Errorf("branch not found: %s", branchName)
				}
				var branch models.Branch
				if err := json.Unmarshal(data, &branch); err != nil {
					return fmt.Errorf("unmarshal branch: %w", err)
				}
				branch.CommitID = commit.ID
				updatedData, err := json.Marshal(branch)
				if err != nil {
					return fmt.Errorf("marshal branch: %w", err)
				}
				if err := branchBucket.Put([]byte(branchName), updatedData); err != nil {
					return err
				}
			} else {
				branch := &models.Branch{
					Name:      branchName,
					CommitID:  commit.ID,
					CreatedAt: time.Now(),
				}
				branchData, err := json.Marshal(branch)
				if err != nil {
					return fmt.Errorf("marshal branch: %w", err)
				}
				if err := branchBucket.Put([]byte(branchName), branchData); err != nil {
					return err
				}
			}
		}

		return nil
	})
	return count, err
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
