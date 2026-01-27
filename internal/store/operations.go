package store

import (
	"encoding/json"

	"github.com/kilupskalvis/wvc/internal/models"
)

// RecordOperation records a new operation in the log
func (s *Store) RecordOperation(op *models.Operation) error {
	_, err := s.db.Exec(`
		INSERT INTO operations (timestamp, operation_type, class_name, object_id, object_data, previous_data, commit_id, reverted, vector_hash, previous_vector_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		op.Timestamp, op.Type, op.ClassName, op.ObjectID, op.ObjectData, op.PreviousData, op.CommitID, op.Reverted,
		op.VectorHash, op.PreviousVectorHash,
	)
	return err
}

// GetUncommittedOperations returns all operations that haven't been committed yet
func (s *Store) GetUncommittedOperations() ([]*models.Operation, error) {
	rows, err := s.db.Query(`
		SELECT id, timestamp, operation_type, class_name, object_id, object_data, previous_data, commit_id, reverted, vector_hash, previous_vector_hash
		FROM operations
		WHERE commit_id IS NULL OR commit_id = ''
		ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanOperations(rows)
}

// GetOperationsByCommit returns all operations for a specific commit
func (s *Store) GetOperationsByCommit(commitID string) ([]*models.Operation, error) {
	rows, err := s.db.Query(`
		SELECT id, timestamp, operation_type, class_name, object_id, object_data, previous_data, commit_id, reverted, vector_hash, previous_vector_hash
		FROM operations
		WHERE commit_id = ?
		ORDER BY id ASC`, commitID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanOperations(rows)
}

// MarkOperationsCommitted marks operations as belonging to a commit
func (s *Store) MarkOperationsCommitted(commitID string) (int64, error) {
	result, err := s.db.Exec(`
		UPDATE operations SET commit_id = ?
		WHERE commit_id IS NULL OR commit_id = ''`, commitID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// MarkOperationsReverted marks operations as reverted
func (s *Store) MarkOperationsReverted(operationIDs []int64) error {
	for _, id := range operationIDs {
		_, err := s.db.Exec("UPDATE operations SET reverted = TRUE WHERE id = ?", id)
		if err != nil {
			return err
		}
	}
	return nil
}

// scanOperations scans rows into Operation structs
func scanOperations(rows interface {
	Next() bool
	Scan(...interface{}) error
}) ([]*models.Operation, error) {
	var operations []*models.Operation

	for rows.Next() {
		var op models.Operation
		var timestamp string
		var commitID *string
		var vectorHash *string
		var previousVectorHash *string

		err := rows.Scan(
			&op.ID, &timestamp, &op.Type, &op.ClassName, &op.ObjectID,
			&op.ObjectData, &op.PreviousData, &commitID, &op.Reverted,
			&vectorHash, &previousVectorHash,
		)
		if err != nil {
			return nil, err
		}

		op.Timestamp = parseTimestamp(timestamp)
		if commitID != nil {
			op.CommitID = *commitID
		}
		if vectorHash != nil {
			op.VectorHash = *vectorHash
		}
		if previousVectorHash != nil {
			op.PreviousVectorHash = *previousVectorHash
		}

		operations = append(operations, &op)
	}

	return operations, nil
}

// SaveKnownObject saves or updates a known object state
func (s *Store) SaveKnownObject(className, objectID, hash string, data []byte) error {
	_, err := s.db.Exec(`
		INSERT INTO known_objects (class_name, object_id, object_hash, object_data)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(class_name, object_id) DO UPDATE SET object_hash = ?, object_data = ?`,
		className, objectID, hash, data, hash, data,
	)
	return err
}

// GetKnownObject retrieves a known object state
func (s *Store) GetKnownObject(className, objectID string) (string, []byte, error) {
	var hash string
	var data []byte
	err := s.db.QueryRow(
		"SELECT object_hash, object_data FROM known_objects WHERE class_name = ? AND object_id = ?",
		className, objectID,
	).Scan(&hash, &data)
	return hash, data, err
}

// GetAllKnownObjects retrieves all known objects
func (s *Store) GetAllKnownObjects() (map[string]*models.WeaviateObject, error) {
	rows, err := s.db.Query("SELECT class_name, object_id, object_data FROM known_objects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	objects := make(map[string]*models.WeaviateObject)
	for rows.Next() {
		var className, objectID string
		var data []byte

		if err := rows.Scan(&className, &objectID, &data); err != nil {
			return nil, err
		}

		var obj models.WeaviateObject
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}

		key := models.ObjectKey(className, objectID)
		objects[key] = &obj
	}

	return objects, nil
}

// DeleteKnownObject removes a known object
func (s *Store) DeleteKnownObject(className, objectID string) error {
	_, err := s.db.Exec(
		"DELETE FROM known_objects WHERE class_name = ? AND object_id = ?",
		className, objectID,
	)
	return err
}

// ClearKnownObjects removes all known objects (used during revert)
func (s *Store) ClearKnownObjects() error {
	_, err := s.db.Exec("DELETE FROM known_objects")
	return err
}

// SaveKnownObjectWithVector saves or updates a known object state including vector hash
func (s *Store) SaveKnownObjectWithVector(className, objectID, objectHash, vectorHash string, data []byte) error {
	_, err := s.db.Exec(`
		INSERT INTO known_objects (class_name, object_id, object_hash, vector_hash, object_data)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(class_name, object_id) DO UPDATE SET
			object_hash = excluded.object_hash,
			vector_hash = excluded.vector_hash,
			object_data = excluded.object_data`,
		className, objectID, objectHash, vectorHash, data,
	)
	return err
}

// GetAllKnownObjectsWithHashes retrieves all known objects with their hashes
func (s *Store) GetAllKnownObjectsWithHashes() (map[string]*models.KnownObjectInfo, error) {
	rows, err := s.db.Query("SELECT class_name, object_id, object_hash, vector_hash, object_data FROM known_objects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	objects := make(map[string]*models.KnownObjectInfo)
	for rows.Next() {
		var className, objectID string
		var objectHash *string
		var vectorHash *string
		var data []byte

		if err := rows.Scan(&className, &objectID, &objectHash, &vectorHash, &data); err != nil {
			return nil, err
		}

		var obj models.WeaviateObject
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}

		info := &models.KnownObjectInfo{Object: &obj}
		if objectHash != nil {
			info.ObjectHash = *objectHash
		}
		if vectorHash != nil {
			info.VectorHash = *vectorHash
		}

		key := models.ObjectKey(className, objectID)
		objects[key] = info
	}

	return objects, nil
}
