package store

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/remote"
	bolt "go.etcd.io/bbolt"
)

// InsertCommitBundle atomically inserts a commit, its operations, and optional schema
// snapshot from a remote bundle into the local store. This is used during pull/fetch
// to store downloaded data. The operation is idempotent — if the commit already exists,
// no changes are made.
func (s *Store) InsertCommitBundle(bundle *remote.CommitBundle) error {
	if bundle == nil || bundle.Commit == nil {
		return fmt.Errorf("invalid commit bundle: nil commit")
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		commitBucket := tx.Bucket(bucketCommits)
		opBucket := tx.Bucket(bucketOperations)

		// Idempotent: skip if commit already exists
		if commitBucket.Get([]byte(bundle.Commit.ID)) != nil {
			return nil
		}

		// Store commit
		commitData, err := json.Marshal(bundle.Commit)
		if err != nil {
			return fmt.Errorf("marshal commit: %w", err)
		}
		if err := commitBucket.Put([]byte(bundle.Commit.ID), commitData); err != nil {
			return fmt.Errorf("store commit: %w", err)
		}

		// Store operations
		for i, op := range bundle.Operations {
			op.CommitID = bundle.Commit.ID
			op.Seq = i

			opData, err := json.Marshal(op)
			if err != nil {
				return fmt.Errorf("marshal operation %d: %w", i, err)
			}
			key := operationKey(bundle.Commit.ID, i)
			if err := opBucket.Put(key, opData); err != nil {
				return fmt.Errorf("store operation %d: %w", i, err)
			}
		}

		// Store schema snapshot if present
		if bundle.Schema != nil {
			if err := insertBundleSchema(tx, bundle.Commit.ID, bundle.Schema); err != nil {
				return fmt.Errorf("store schema: %w", err)
			}
		}

		return nil
	})
}

// insertBundleSchema stores a schema snapshot from a remote bundle.
func insertBundleSchema(tx *bolt.Tx, commitID string, schema *remote.SchemaSnapshot) error {
	countersBucket := tx.Bucket(bucketCounters)
	schemasBucket := tx.Bucket(bucketSchemaVers)
	indexBucket := tx.Bucket(bucketSchemaIndex)

	if countersBucket == nil || schemasBucket == nil || indexBucket == nil {
		return fmt.Errorf("required buckets not found")
	}

	// Check if schema already exists for this commit
	indexKey := []byte(fmt.Sprintf("commit:%s", commitID))
	if indexBucket.Get(indexKey) != nil {
		return nil // Already exists
	}

	// Get next schema ID
	counterKey := []byte("next_schema_id")
	var schemaID int64 = 1
	if counterVal := countersBucket.Get(counterKey); counterVal != nil {
		parsed, err := strconv.ParseInt(string(counterVal), 10, 64)
		if err != nil {
			return fmt.Errorf("parse schema counter: %w", err)
		}
		schemaID = parsed
	}

	sv := models.SchemaVersion{
		ID:         schemaID,
		SchemaJSON: schema.SchemaJSON,
		SchemaHash: schema.SchemaHash,
		CommitID:   commitID,
	}

	svData, err := json.Marshal(sv)
	if err != nil {
		return fmt.Errorf("marshal schema version: %w", err)
	}

	schemaKey := []byte(fmt.Sprintf("%08d", schemaID))
	if err := schemasBucket.Put(schemaKey, svData); err != nil {
		return fmt.Errorf("store schema version: %w", err)
	}

	// Update index
	if err := indexBucket.Put(indexKey, schemaKey); err != nil {
		return fmt.Errorf("store schema index: %w", err)
	}

	// Update counter
	nextID := schemaID + 1
	return countersBucket.Put(counterKey, []byte(strconv.FormatInt(nextID, 10)))
}
