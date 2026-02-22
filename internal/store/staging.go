package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
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

// AddStagedChange adds or updates a staged change in the store.
// Uses key format: {class_name}:{object_id}
func (s *Store) AddStagedChange(change *StagedChange) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		// Get or create the staged changes bucket
		bucket, err := tx.CreateBucketIfNotExists(bucketStagedChanges)
		if err != nil {
			return fmt.Errorf("failed to create staged changes bucket: %w", err)
		}

		// Create the key
		key := []byte(change.ClassName + ":" + change.ObjectID)

		// Check if this is a new entry (for counter management)
		isNew := bucket.Get(key) == nil

		// Serialize the change
		data, err := json.Marshal(change)
		if err != nil {
			return fmt.Errorf("failed to marshal staged change: %w", err)
		}

		// Store the change
		if err := bucket.Put(key, data); err != nil {
			return fmt.Errorf("failed to store staged change: %w", err)
		}

		// Update counter only if this is a new entry
		if isNew {
			if err := s.incrementStagedCount(tx); err != nil {
				return fmt.Errorf("failed to increment staged count: %w", err)
			}
		}

		return nil
	})
}

// RemoveStagedChange removes a staged change by class name and object ID.
func (s *Store) RemoveStagedChange(className, objectID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketStagedChanges)
		if bucket == nil {
			return nil // No staged changes exist
		}

		key := []byte(className + ":" + objectID)

		// Check if the entry exists
		if bucket.Get(key) == nil {
			return nil // Entry doesn't exist, nothing to do
		}

		// Delete the entry
		if err := bucket.Delete(key); err != nil {
			return fmt.Errorf("failed to delete staged change: %w", err)
		}

		// Decrement the counter
		if err := s.decrementStagedCount(tx); err != nil {
			return fmt.Errorf("failed to decrement staged count: %w", err)
		}

		return nil
	})
}

// RemoveStagedChangesByClass removes all staged changes for a given class.
func (s *Store) RemoveStagedChangesByClass(className string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketStagedChanges)
		if bucket == nil {
			return nil // No staged changes exist
		}

		prefix := []byte(className + ":")
		keysToDelete := [][]byte{}

		// Collect all keys matching the prefix
		cursor := bucket.Cursor()
		for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			// Copy the key since it's only valid during the transaction
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keysToDelete = append(keysToDelete, keyCopy)
		}

		// Delete all collected keys
		for _, key := range keysToDelete {
			if err := bucket.Delete(key); err != nil {
				return fmt.Errorf("failed to delete staged change: %w", err)
			}
		}

		// Update the counter
		if len(keysToDelete) > 0 {
			if err := s.adjustStagedCount(tx, -len(keysToDelete)); err != nil {
				return fmt.Errorf("failed to adjust staged count: %w", err)
			}
		}

		return nil
	})
}

// ClearStagedChanges removes all staged changes from the store.
func (s *Store) ClearStagedChanges() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		// Delete the staged changes bucket
		if err := tx.DeleteBucket(bucketStagedChanges); err != nil && err != berrors.ErrBucketNotFound {
			return fmt.Errorf("failed to delete staged changes bucket: %w", err)
		}

		// Reset the counter to 0
		if err := s.resetStagedCount(tx); err != nil {
			return fmt.Errorf("failed to reset staged count: %w", err)
		}

		// Recreate the bucket
		if _, err := tx.CreateBucketIfNotExists(bucketStagedChanges); err != nil {
			return fmt.Errorf("recreate staged changes bucket: %w", err)
		}

		return nil
	})
}

// GetStagedChange retrieves a specific staged change by class name and object ID.
// Returns (nil, nil) if not found.
func (s *Store) GetStagedChange(className, objectID string) (*StagedChange, error) {
	var change *StagedChange

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketStagedChanges)
		if bucket == nil {
			return nil // No staged changes exist
		}

		key := []byte(className + ":" + objectID)
		data := bucket.Get(key)
		if data == nil {
			return nil // Not found
		}

		change = &StagedChange{}
		if err := json.Unmarshal(data, change); err != nil {
			return fmt.Errorf("failed to unmarshal staged change: %w", err)
		}

		return nil
	})

	return change, err
}

// GetAllStagedChanges retrieves all staged changes, sorted by StagedAt.
func (s *Store) GetAllStagedChanges() ([]*StagedChange, error) {
	var changes []*StagedChange

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketStagedChanges)
		if bucket == nil {
			return nil // No staged changes exist
		}

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			change := &StagedChange{}
			if err := json.Unmarshal(v, change); err != nil {
				return fmt.Errorf("failed to unmarshal staged change: %w", err)
			}
			changes = append(changes, change)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort by StagedAt
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].StagedAt.Before(changes[j].StagedAt)
	})

	return changes, nil
}

// GetStagedChangesByClass retrieves all staged changes for a given class.
func (s *Store) GetStagedChangesByClass(className string) ([]*StagedChange, error) {
	var changes []*StagedChange

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketStagedChanges)
		if bucket == nil {
			return nil // No staged changes exist
		}

		prefix := []byte(className + ":")
		cursor := bucket.Cursor()

		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			change := &StagedChange{}
			if err := json.Unmarshal(v, change); err != nil {
				return fmt.Errorf("failed to unmarshal staged change: %w", err)
			}
			changes = append(changes, change)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort by StagedAt
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].StagedAt.Before(changes[j].StagedAt)
	})

	return changes, nil
}

// GetStagedChangesCount returns the total number of staged changes.
func (s *Store) GetStagedChangesCount() (int, error) {
	var count int

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketCounters)
		if bucket == nil {
			count = 0
			return nil
		}

		data := bucket.Get(counterStagedCount)
		if data == nil {
			count = 0
			return nil
		}

		var err error
		count, err = strconv.Atoi(string(data))
		if err != nil {
			return fmt.Errorf("failed to parse staged count: %w", err)
		}

		return nil
	})

	return count, err
}

// incrementStagedCount increments the staged changes counter by 1.
func (s *Store) incrementStagedCount(tx *bolt.Tx) error {
	return s.adjustStagedCount(tx, 1)
}

// decrementStagedCount decrements the staged changes counter by 1.
func (s *Store) decrementStagedCount(tx *bolt.Tx) error {
	return s.adjustStagedCount(tx, -1)
}

// adjustStagedCount adjusts the staged changes counter by the given delta.
func (s *Store) adjustStagedCount(tx *bolt.Tx, delta int) error {
	bucket, err := tx.CreateBucketIfNotExists(bucketCounters)
	if err != nil {
		return fmt.Errorf("failed to create counters bucket: %w", err)
	}

	// Get current count
	var currentCount int
	data := bucket.Get(counterStagedCount)
	if data != nil {
		currentCount, err = strconv.Atoi(string(data))
		if err != nil {
			return fmt.Errorf("failed to parse staged count: %w", err)
		}
	}

	// Update count
	newCount := currentCount + delta
	if newCount < 0 {
		newCount = 0
	}

	// Store new count
	if err := bucket.Put(counterStagedCount, []byte(strconv.Itoa(newCount))); err != nil {
		return fmt.Errorf("failed to update staged count: %w", err)
	}

	return nil
}

// resetStagedCount resets the staged changes counter to 0.
func (s *Store) resetStagedCount(tx *bolt.Tx) error {
	bucket, err := tx.CreateBucketIfNotExists(bucketCounters)
	if err != nil {
		return fmt.Errorf("failed to create counters bucket: %w", err)
	}

	if err := bucket.Put(counterStagedCount, []byte("0")); err != nil {
		return fmt.Errorf("failed to reset staged count: %w", err)
	}

	return nil
}
