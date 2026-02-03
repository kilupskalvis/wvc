// Package store provides bbolt-based persistence for WVC.
// It manages operations, commits, branches, staging, stashes, and known object state
// using a single embedded bbolt database file.
package store

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Bucket names used by the client store.
var (
	bucketCommits       = []byte("commits")
	bucketOperations    = []byte("operations")
	bucketBranches      = []byte("branches")
	bucketSchemaVers    = []byte("schema_versions")
	bucketSchemaIndex   = []byte("schema_index") // maps commit_id -> schema key for lookup
	bucketVectorBlobs   = []byte("vector_blobs")
	bucketKV            = []byte("kv")
	bucketKnownObjects  = []byte("known_objects")
	bucketStagedChanges = []byte("staged_changes")
	bucketStashes       = []byte("stashes")
	bucketStashChanges  = []byte("stash_changes")
	bucketScanMetadata  = []byte("scan_metadata")
	bucketCounters      = []byte("counters")
	bucketRemotes       = []byte("remotes")
	bucketRemoteBranch  = []byte("remote_branches")
	bucketShallowCommit = []byte("shallow_commits")
)

// Counter key names.
var (
	counterStagedCount = []byte("staged_count")
	counterStashCount  = []byte("stash_count")
)

// Store represents the bbolt database store.
type Store struct {
	db *bolt.DB
}

// New opens or creates a bbolt database at the given path.
func New(dbPath string) (*Store, error) {
	dir := filepath.Dir(dbPath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	return &Store{db: db}, nil
}

// Close closes the database.
func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Initialize creates all required buckets.
func (s *Store) Initialize() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		buckets := [][]byte{
			bucketCommits,
			bucketOperations,
			bucketBranches,
			bucketSchemaVers,
			bucketSchemaIndex,
			bucketVectorBlobs,
			bucketKV,
			bucketKnownObjects,
			bucketStagedChanges,
			bucketStashes,
			bucketStashChanges,
			bucketScanMetadata,
			bucketCounters,
			bucketRemotes,
			bucketRemoteBranch,
			bucketShallowCommit,
		}
		for _, name := range buckets {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("create bucket %s: %w", name, err)
			}
		}
		return nil
	})
}

// GetValue gets a value from the key-value bucket.
func (s *Store) GetValue(key string) (string, error) {
	var val string
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketKV)
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if v != nil {
			val = string(v)
		}
		return nil
	})
	return val, err
}

// SetValue sets a value in the key-value bucket.
func (s *Store) SetValue(key, value string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketKV)
		if b == nil {
			return fmt.Errorf("kv bucket not found")
		}
		return b.Put([]byte(key), []byte(value))
	})
}

// RunMigrations is a no-op for bbolt — schema is implicit in bucket structure.
func (s *Store) RunMigrations() error {
	return nil
}
