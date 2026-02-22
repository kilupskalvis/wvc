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

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open database %s (is another wvc process running?): %w", dbPath, err)
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

		kvBucket := tx.Bucket(bucketKV)
		if kvBucket.Get([]byte("schema_version")) == nil {
			if err := kvBucket.Put([]byte("schema_version"), []byte("1")); err != nil {
				return fmt.Errorf("set schema version: %w", err)
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

// RunMigrations checks the schema version and applies any needed migrations.
func (s *Store) RunMigrations() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		kvBucket := tx.Bucket(bucketKV)
		if kvBucket == nil {
			return nil // not initialized yet
		}
		versionBytes := kvBucket.Get([]byte("schema_version"))
		if versionBytes == nil {
			// Pre-migration database, set to version 1
			return kvBucket.Put([]byte("schema_version"), []byte("1"))
		}
		// Current version is 1, no migrations needed yet
		return nil
	})
}
