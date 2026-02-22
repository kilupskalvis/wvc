package store

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// MarkShallowCommit marks a commit as a shallow boundary.
// Shallow commits indicate where the local history was truncated during a shallow fetch.
func (s *Store) MarkShallowCommit(commitID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketShallowCommit)
		if b == nil {
			return fmt.Errorf("shallow_commits bucket not found")
		}
		return b.Put([]byte(commitID), []byte{})
	})
}

// IsShallowCommit checks whether a commit is a shallow boundary.
func (s *Store) IsShallowCommit(commitID string) (bool, error) {
	var shallow bool
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketShallowCommit)
		if b == nil {
			return nil
		}
		shallow = b.Get([]byte(commitID)) != nil
		return nil
	})
	return shallow, err
}

// ListShallowCommits returns all shallow commit IDs.
func (s *Store) ListShallowCommits() ([]string, error) {
	var ids []string
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketShallowCommit)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, _ []byte) error {
			ids = append(ids, string(k))
			return nil
		})
	})
	return ids, err
}

// RemoveShallowCommit removes a commit from the shallow set.
func (s *Store) RemoveShallowCommit(commitID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketShallowCommit)
		if b == nil {
			return nil
		}
		return b.Delete([]byte(commitID))
	})
}
