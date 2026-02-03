package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

var (
	counterNextStashID = []byte("next_stash_id")
)

// CreateStash creates a new stash entry with an auto-assigned ID.
func (s *Store) CreateStash(message, branchName, commitID string) (int64, error) {
	var stashID int64

	err := s.db.Update(func(tx *bolt.Tx) error {
		stashBucket := tx.Bucket(bucketStashes)
		if stashBucket == nil {
			return fmt.Errorf("stashes bucket not found")
		}

		counterBucket := tx.Bucket(bucketCounters)
		if counterBucket == nil {
			return fmt.Errorf("counters bucket not found")
		}

		// Get next stash ID
		nextIDBytes := counterBucket.Get(counterNextStashID)
		if nextIDBytes == nil {
			stashID = 1
		} else {
			nextID, err := strconv.ParseInt(string(nextIDBytes), 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse next stash ID: %w", err)
			}
			stashID = nextID
		}

		// Create stash
		stash := &models.Stash{
			ID:         stashID,
			Message:    message,
			BranchName: branchName,
			CommitID:   commitID,
			CreatedAt:  time.Now(),
		}

		stashData, err := json.Marshal(stash)
		if err != nil {
			return fmt.Errorf("failed to marshal stash: %w", err)
		}

		// Store stash with zero-padded key
		key := []byte(fmt.Sprintf("%08d", stashID))
		if err := stashBucket.Put(key, stashData); err != nil {
			return fmt.Errorf("failed to store stash: %w", err)
		}

		// Increment next stash ID
		nextStashID := stashID + 1
		if err := counterBucket.Put(counterNextStashID, []byte(strconv.FormatInt(nextStashID, 10))); err != nil {
			return fmt.Errorf("failed to update next stash ID: %w", err)
		}

		// Increment stash count
		countBytes := counterBucket.Get(counterStashCount)
		var count int64
		if countBytes != nil {
			count, err = strconv.ParseInt(string(countBytes), 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse stash count: %w", err)
			}
		}
		count++
		if err := counterBucket.Put(counterStashCount, []byte(strconv.FormatInt(count, 10))); err != nil {
			return fmt.Errorf("failed to update stash count: %w", err)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return stashID, nil
}

// CreateStashChange stores a stash change entry.
func (s *Store) CreateStashChange(change *models.StashChange) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		changeBucket := tx.Bucket(bucketStashChanges)
		if changeBucket == nil {
			return fmt.Errorf("stash_changes bucket not found")
		}

		// Count existing changes for this stash to determine sequence number
		prefix := fmt.Sprintf("%08d:", change.StashID)
		seq := 0
		c := changeBucket.Cursor()
		for k, _ := c.Seek([]byte(prefix)); k != nil && bytes.HasPrefix(k, []byte(prefix)); k, _ = c.Next() {
			seq++
		}

		// Set the ID to the sequence number
		change.ID = int64(seq)

		changeData, err := json.Marshal(change)
		if err != nil {
			return fmt.Errorf("failed to marshal stash change: %w", err)
		}

		// Store with composite key: stash_id:seq
		key := []byte(fmt.Sprintf("%08d:%08d", change.StashID, seq))
		if err := changeBucket.Put(key, changeData); err != nil {
			return fmt.Errorf("failed to store stash change: %w", err)
		}

		return nil
	})
}

// ListStashes returns all stashes ordered newest first.
func (s *Store) ListStashes() ([]*models.Stash, error) {
	var stashes []*models.Stash

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketStashes)
		if bucket == nil {
			return nil
		}

		c := bucket.Cursor()

		// Iterate in reverse order (newest first)
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			var stash models.Stash
			if err := json.Unmarshal(v, &stash); err != nil {
				return fmt.Errorf("failed to unmarshal stash: %w", err)
			}
			stashes = append(stashes, &stash)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return stashes, nil
}

// GetStashByIndex returns a stash by its index (0 = newest).
// Returns nil, nil if not found.
func (s *Store) GetStashByIndex(index int) (*models.Stash, error) {
	var stash *models.Stash

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketStashes)
		if bucket == nil {
			return nil
		}

		c := bucket.Cursor()
		currentIndex := 0

		// Iterate in reverse order (newest first)
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			if currentIndex == index {
				var s models.Stash
				if err := json.Unmarshal(v, &s); err != nil {
					return fmt.Errorf("failed to unmarshal stash: %w", err)
				}
				stash = &s
				return nil
			}
			currentIndex++
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return stash, nil
}

// GetStashChanges returns all changes for a given stash ID.
func (s *Store) GetStashChanges(stashID int64) ([]*models.StashChange, error) {
	var changes []*models.StashChange

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketStashChanges)
		if bucket == nil {
			return nil
		}

		prefix := []byte(fmt.Sprintf("%08d:", stashID))
		c := bucket.Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var change models.StashChange
			if err := json.Unmarshal(v, &change); err != nil {
				return fmt.Errorf("failed to unmarshal stash change: %w", err)
			}
			changes = append(changes, &change)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return changes, nil
}

// DeleteStash deletes a stash and all its associated changes.
func (s *Store) DeleteStash(stashID int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		stashBucket := tx.Bucket(bucketStashes)
		if stashBucket == nil {
			return fmt.Errorf("stashes bucket not found")
		}

		changeBucket := tx.Bucket(bucketStashChanges)
		counterBucket := tx.Bucket(bucketCounters)
		if counterBucket == nil {
			return fmt.Errorf("counters bucket not found")
		}

		// Delete stash
		key := []byte(fmt.Sprintf("%08d", stashID))
		if err := stashBucket.Delete(key); err != nil {
			return fmt.Errorf("failed to delete stash: %w", err)
		}

		// Delete all associated changes
		if changeBucket != nil {
			prefix := []byte(fmt.Sprintf("%08d:", stashID))
			c := changeBucket.Cursor()

			var keysToDelete [][]byte
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				keysToDelete = append(keysToDelete, append([]byte(nil), k...))
			}

			for _, k := range keysToDelete {
				if err := changeBucket.Delete(k); err != nil {
					return fmt.Errorf("failed to delete stash change: %w", err)
				}
			}
		}

		// Decrement stash count
		countBytes := counterBucket.Get(counterStashCount)
		if countBytes != nil {
			count, err := strconv.ParseInt(string(countBytes), 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse stash count: %w", err)
			}
			if count > 0 {
				count--
				if err := counterBucket.Put(counterStashCount, []byte(strconv.FormatInt(count, 10))); err != nil {
					return fmt.Errorf("failed to update stash count: %w", err)
				}
			}
		}

		return nil
	})
}

// DeleteAllStashes clears all stashes and their changes, and resets counters.
func (s *Store) DeleteAllStashes() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		// Delete stashes bucket
		if err := tx.DeleteBucket(bucketStashes); err != nil && err != berrors.ErrBucketNotFound {
			return fmt.Errorf("failed to delete stashes bucket: %w", err)
		}

		// Delete stash changes bucket
		if err := tx.DeleteBucket(bucketStashChanges); err != nil && err != berrors.ErrBucketNotFound {
			return fmt.Errorf("failed to delete stash_changes bucket: %w", err)
		}

		// Recreate buckets
		if _, err := tx.CreateBucket(bucketStashes); err != nil {
			return fmt.Errorf("failed to recreate stashes bucket: %w", err)
		}
		if _, err := tx.CreateBucket(bucketStashChanges); err != nil {
			return fmt.Errorf("failed to recreate stash_changes bucket: %w", err)
		}

		// Reset counters
		counterBucket := tx.Bucket(bucketCounters)
		if counterBucket == nil {
			return fmt.Errorf("counters bucket not found")
		}

		if err := counterBucket.Put(counterStashCount, []byte("0")); err != nil {
			return fmt.Errorf("failed to reset stash count: %w", err)
		}

		if err := counterBucket.Put(counterNextStashID, []byte("1")); err != nil {
			return fmt.Errorf("failed to reset next stash ID: %w", err)
		}

		return nil
	})
}

// GetStashCount returns the current number of stashes.
func (s *Store) GetStashCount() (int, error) {
	var count int

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketCounters)
		if bucket == nil {
			return nil
		}

		countBytes := bucket.Get(counterStashCount)
		if countBytes == nil {
			return nil
		}

		c, err := strconv.Atoi(string(countBytes))
		if err != nil {
			return fmt.Errorf("failed to parse stash count: %w", err)
		}
		count = c

		return nil
	})

	if err != nil {
		return 0, err
	}

	return count, nil
}
