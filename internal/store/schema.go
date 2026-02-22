package store

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	bolt "go.etcd.io/bbolt"
)

// SaveSchemaVersion saves a new schema version with auto-incrementing ID
func (s *Store) SaveSchemaVersion(schemaJSON []byte, schemaHash string) (int64, error) {
	var schemaID int64

	err := s.db.Update(func(tx *bolt.Tx) error {
		countersBucket := tx.Bucket(bucketCounters)
		if countersBucket == nil {
			return fmt.Errorf("counters bucket not found")
		}

		schemasBucket := tx.Bucket(bucketSchemaVers)
		if schemasBucket == nil {
			return fmt.Errorf("schema_versions bucket not found")
		}

		// Get next schema ID
		counterKey := []byte("next_schema_id")
		counterVal := countersBucket.Get(counterKey)
		if counterVal == nil {
			schemaID = 1
		} else {
			counter, err := strconv.ParseInt(string(counterVal), 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse schema counter: %w", err)
			}
			schemaID = counter
		}

		// Create schema version
		schemaVersion := models.SchemaVersion{
			ID:         schemaID,
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			SchemaJSON: schemaJSON,
			SchemaHash: schemaHash,
			CommitID:   "", // Not committed yet
		}

		// Serialize and store
		versionJSON, err := json.Marshal(schemaVersion)
		if err != nil {
			return fmt.Errorf("failed to marshal schema version: %w", err)
		}

		key := []byte(fmt.Sprintf("%08d", schemaID))
		if err := schemasBucket.Put(key, versionJSON); err != nil {
			return fmt.Errorf("failed to store schema version: %w", err)
		}

		// Update counter
		nextID := schemaID + 1
		if err := countersBucket.Put(counterKey, []byte(strconv.FormatInt(nextID, 10))); err != nil {
			return fmt.Errorf("failed to update schema counter: %w", err)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return schemaID, nil
}

// GetLatestSchemaVersion returns the latest committed schema version
func (s *Store) GetLatestSchemaVersion() (*models.SchemaVersion, error) {
	var latestSchema *models.SchemaVersion

	err := s.db.View(func(tx *bolt.Tx) error {
		schemasBucket := tx.Bucket(bucketSchemaVers)
		if schemasBucket == nil {
			return fmt.Errorf("schema_versions bucket not found")
		}

		// Iterate in reverse to find the latest committed schema
		cursor := schemasBucket.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var schemaVersion models.SchemaVersion
			if err := json.Unmarshal(v, &schemaVersion); err != nil {
				return fmt.Errorf("failed to unmarshal schema version: %w", err)
			}

			// Check if this schema version is committed
			if schemaVersion.CommitID != "" {
				latestSchema = &schemaVersion
				return nil
			}
		}

		// No committed schema found
		return nil
	})

	if err != nil {
		return nil, err
	}

	return latestSchema, nil
}

// GetSchemaVersionByCommit retrieves a schema version by commit ID
func (s *Store) GetSchemaVersionByCommit(commitID string) (*models.SchemaVersion, error) {
	var schemaVersion *models.SchemaVersion

	err := s.db.View(func(tx *bolt.Tx) error {
		indexBucket := tx.Bucket(bucketSchemaIndex)
		if indexBucket == nil {
			return fmt.Errorf("schema_index bucket not found")
		}

		schemasBucket := tx.Bucket(bucketSchemaVers)
		if schemasBucket == nil {
			return fmt.Errorf("schema_versions bucket not found")
		}

		// Look up schema version key by commit ID
		indexKey := []byte(fmt.Sprintf("commit:%s", commitID))
		schemaKey := indexBucket.Get(indexKey)
		if schemaKey == nil {
			// Not found
			return nil
		}

		// Get the schema version
		schemaJSON := schemasBucket.Get(schemaKey)
		if schemaJSON == nil {
			return fmt.Errorf("schema version not found for key %s", string(schemaKey))
		}

		var sv models.SchemaVersion
		if err := json.Unmarshal(schemaJSON, &sv); err != nil {
			return fmt.Errorf("failed to unmarshal schema version: %w", err)
		}

		schemaVersion = &sv
		return nil
	})

	if err != nil {
		return nil, err
	}

	return schemaVersion, nil
}

// MarkSchemaVersionCommitted marks a schema version as committed and adds index entry
func (s *Store) MarkSchemaVersionCommitted(schemaVersionID int64, commitID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		schemasBucket := tx.Bucket(bucketSchemaVers)
		if schemasBucket == nil {
			return fmt.Errorf("schema_versions bucket not found")
		}

		indexBucket := tx.Bucket(bucketSchemaIndex)
		if indexBucket == nil {
			return fmt.Errorf("schema_index bucket not found")
		}

		// Get the schema version
		key := []byte(fmt.Sprintf("%08d", schemaVersionID))
		schemaJSON := schemasBucket.Get(key)
		if schemaJSON == nil {
			return fmt.Errorf("schema version %d not found", schemaVersionID)
		}

		var schemaVersion models.SchemaVersion
		if err := json.Unmarshal(schemaJSON, &schemaVersion); err != nil {
			return fmt.Errorf("failed to unmarshal schema version: %w", err)
		}

		// Update commit ID
		schemaVersion.CommitID = commitID

		// Serialize and store
		updatedJSON, err := json.Marshal(schemaVersion)
		if err != nil {
			return fmt.Errorf("failed to marshal updated schema version: %w", err)
		}

		if err := schemasBucket.Put(key, updatedJSON); err != nil {
			return fmt.Errorf("failed to update schema version: %w", err)
		}

		// Add index entry
		indexKey := []byte(fmt.Sprintf("commit:%s", commitID))
		if err := indexBucket.Put(indexKey, key); err != nil {
			return fmt.Errorf("failed to create schema index entry: %w", err)
		}

		return nil
	})
}

// CommitHasSchemaChange checks if a commit has a schema change compared to its parent
func (s *Store) CommitHasSchemaChange(commitID string) (bool, error) {
	var hasChange bool

	err := s.db.View(func(tx *bolt.Tx) error {
		commitsBucket := tx.Bucket(bucketCommits)
		if commitsBucket == nil {
			return fmt.Errorf("commits bucket not found")
		}

		indexBucket := tx.Bucket(bucketSchemaIndex)
		if indexBucket == nil {
			return fmt.Errorf("schema_index bucket not found")
		}

		schemasBucket := tx.Bucket(bucketSchemaVers)
		if schemasBucket == nil {
			return fmt.Errorf("schema_versions bucket not found")
		}

		// Get the commit
		commitJSON := commitsBucket.Get([]byte(commitID))
		if commitJSON == nil {
			return fmt.Errorf("commit %s not found", commitID)
		}

		var commit models.Commit
		if err := json.Unmarshal(commitJSON, &commit); err != nil {
			return fmt.Errorf("failed to unmarshal commit: %w", err)
		}

		// Get current commit's schema
		currentSchemaKey := indexBucket.Get([]byte(fmt.Sprintf("commit:%s", commitID)))
		if currentSchemaKey == nil {
			// No schema for this commit
			hasChange = false
			return nil
		}

		currentSchemaJSON := schemasBucket.Get(currentSchemaKey)
		if currentSchemaJSON == nil {
			return fmt.Errorf("schema version not found for commit %s", commitID)
		}

		var currentSchema models.SchemaVersion
		if err := json.Unmarshal(currentSchemaJSON, &currentSchema); err != nil {
			return fmt.Errorf("failed to unmarshal current schema: %w", err)
		}

		// If no parent, this is the first commit
		if commit.ParentID == "" {
			// Compare with empty schema
			var emptySchema models.WeaviateSchema
			emptyJSON, _ := json.Marshal(emptySchema)

			// If current schema is different from empty, it's a change
			hasChange = string(currentSchema.SchemaJSON) != string(emptyJSON)
			return nil
		}

		// Get parent's schema
		parentSchemaKey := indexBucket.Get([]byte(fmt.Sprintf("commit:%s", commit.ParentID)))
		if parentSchemaKey == nil {
			// Parent has no schema, so current schema is a change
			hasChange = true
			return nil
		}

		parentSchemaJSON := schemasBucket.Get(parentSchemaKey)
		if parentSchemaJSON == nil {
			return fmt.Errorf("schema version not found for parent commit %s", commit.ParentID)
		}

		var parentSchema models.SchemaVersion
		if err := json.Unmarshal(parentSchemaJSON, &parentSchema); err != nil {
			return fmt.Errorf("failed to unmarshal parent schema: %w", err)
		}

		// Compare schema hashes
		hasChange = currentSchema.SchemaHash != parentSchema.SchemaHash
		return nil
	})

	if err != nil {
		return false, err
	}

	return hasChange, nil
}

// GetPreviousCommitSchema retrieves the schema from the parent commit
func (s *Store) GetPreviousCommitSchema(commitID string) (*models.SchemaVersion, error) {
	var parentSchema *models.SchemaVersion

	err := s.db.View(func(tx *bolt.Tx) error {
		commitsBucket := tx.Bucket(bucketCommits)
		if commitsBucket == nil {
			return fmt.Errorf("commits bucket not found")
		}

		indexBucket := tx.Bucket(bucketSchemaIndex)
		if indexBucket == nil {
			return fmt.Errorf("schema_index bucket not found")
		}

		schemasBucket := tx.Bucket(bucketSchemaVers)
		if schemasBucket == nil {
			return fmt.Errorf("schema_versions bucket not found")
		}

		// Get the commit
		commitJSON := commitsBucket.Get([]byte(commitID))
		if commitJSON == nil {
			return fmt.Errorf("commit %s not found", commitID)
		}

		var commit models.Commit
		if err := json.Unmarshal(commitJSON, &commit); err != nil {
			return fmt.Errorf("failed to unmarshal commit: %w", err)
		}

		// If no parent, return nil
		if commit.ParentID == "" {
			return nil
		}

		// Get parent's schema
		parentSchemaKey := indexBucket.Get([]byte(fmt.Sprintf("commit:%s", commit.ParentID)))
		if parentSchemaKey == nil {
			// Parent has no schema
			return nil
		}

		parentSchemaJSON := schemasBucket.Get(parentSchemaKey)
		if parentSchemaJSON == nil {
			return fmt.Errorf("schema version not found for parent commit %s", commit.ParentID)
		}

		var ps models.SchemaVersion
		if err := json.Unmarshal(parentSchemaJSON, &ps); err != nil {
			return fmt.Errorf("failed to unmarshal parent schema: %w", err)
		}

		parentSchema = &ps
		return nil
	})

	if err != nil {
		return nil, err
	}

	return parentSchema, nil
}
