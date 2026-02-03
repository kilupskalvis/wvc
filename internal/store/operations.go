package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kilupskalvis/wvc/internal/models"
	bolt "go.etcd.io/bbolt"
)

// operationKey builds the bbolt key for an operation: "{commit_id}:{seq:04d}".
func operationKey(commitID string, seq int) []byte {
	return []byte(fmt.Sprintf("%s:%04d", commitID, seq))
}

// uncommittedPrefix is the key prefix for operations not yet assigned to a commit.
const uncommittedPrefix = "_uncommitted:"

// uncommittedKey builds a key for an uncommitted operation using a sequence counter.
func uncommittedKey(seq int) []byte {
	return []byte(fmt.Sprintf("%s%08d", uncommittedPrefix, seq))
}

// RecordOperation records a new operation in the log.
// If CommitID is empty, the operation is stored as uncommitted.
func (s *Store) RecordOperation(op *models.Operation) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOperations)

		if op.CommitID == "" {
			// Store as uncommitted — assign next sequence number
			seq := nextUncommittedSeq(b)
			op.Seq = seq
			data, err := json.Marshal(op)
			if err != nil {
				return fmt.Errorf("marshal operation: %w", err)
			}
			return b.Put(uncommittedKey(seq), data)
		}

		// Committed operation — use commit_id:seq key
		data, err := json.Marshal(op)
		if err != nil {
			return fmt.Errorf("marshal operation: %w", err)
		}
		return b.Put(operationKey(op.CommitID, op.Seq), data)
	})
}

// nextUncommittedSeq scans for the highest uncommitted sequence and returns the next one.
func nextUncommittedSeq(b *bolt.Bucket) int {
	c := b.Cursor()
	prefix := []byte(uncommittedPrefix)
	maxSeq := -1

	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, []byte(uncommittedPrefix)); k, _ = c.Next() {
		var seq int
		if _, err := fmt.Sscanf(string(k[len(uncommittedPrefix):]), "%d", &seq); err == nil {
			if seq > maxSeq {
				maxSeq = seq
			}
		}
	}
	return maxSeq + 1
}

// GetUncommittedOperations returns all operations not yet committed.
func (s *Store) GetUncommittedOperations() ([]*models.Operation, error) {
	var ops []*models.Operation
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOperations)
		c := b.Cursor()
		prefix := []byte(uncommittedPrefix)

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, []byte(uncommittedPrefix)); k, v = c.Next() {
			var op models.Operation
			if err := json.Unmarshal(v, &op); err != nil {
				return fmt.Errorf("unmarshal operation: %w", err)
			}
			ops = append(ops, &op)
		}
		return nil
	})
	return ops, err
}

// GetOperationsByCommit returns all operations for a specific commit, ordered by seq.
func (s *Store) GetOperationsByCommit(commitID string) ([]*models.Operation, error) {
	var ops []*models.Operation
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOperations)
		c := b.Cursor()
		prefix := []byte(commitID + ":")

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var op models.Operation
			if err := json.Unmarshal(v, &op); err != nil {
				return fmt.Errorf("unmarshal operation: %w", err)
			}
			ops = append(ops, &op)
		}
		return nil
	})
	return ops, err
}

// MarkOperationsCommitted moves uncommitted operations to their commit, assigning
// sequential (commitID, seq) keys. Returns the number of operations committed.
func (s *Store) MarkOperationsCommitted(commitID string) (int64, error) {
	var count int64
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOperations)

		// Collect uncommitted operation keys
		var keys [][]byte
		c := b.Cursor()
		prefix := []byte(uncommittedPrefix)
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, []byte(uncommittedPrefix)); k, _ = c.Next() {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keys = append(keys, keyCopy)
		}

		// Re-key each operation under commit_id:seq
		for seq, oldKey := range keys {
			v := b.Get(oldKey)
			if v == nil {
				continue
			}

			var op models.Operation
			if err := json.Unmarshal(v, &op); err != nil {
				return fmt.Errorf("unmarshal operation: %w", err)
			}

			op.CommitID = commitID
			op.Seq = seq

			newData, err := json.Marshal(&op)
			if err != nil {
				return fmt.Errorf("marshal operation: %w", err)
			}

			if err := b.Put(operationKey(commitID, seq), newData); err != nil {
				return err
			}
			if err := b.Delete(oldKey); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}

// MarkOperationsReverted marks operations within their commit as reverted.
func (s *Store) MarkOperationsReverted(commitID string, seqs []int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOperations)
		for _, seq := range seqs {
			key := operationKey(commitID, seq)
			v := b.Get(key)
			if v == nil {
				continue
			}
			var op models.Operation
			if err := json.Unmarshal(v, &op); err != nil {
				return fmt.Errorf("unmarshal operation: %w", err)
			}
			op.Reverted = true
			data, err := json.Marshal(&op)
			if err != nil {
				return fmt.Errorf("marshal operation: %w", err)
			}
			if err := b.Put(key, data); err != nil {
				return err
			}
		}
		return nil
	})
}

// SaveKnownObject saves or updates a known object state.
func (s *Store) SaveKnownObject(className, objectID, hash string, data []byte) error {
	return s.SaveKnownObjectWithVector(className, objectID, hash, "", data)
}

// GetKnownObject retrieves a known object's hash and data.
func (s *Store) GetKnownObject(className, objectID string) (string, []byte, error) {
	key := className + ":" + objectID
	var info knownObjectRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketKnownObjects).Get([]byte(key))
		if v == nil {
			return fmt.Errorf("known object not found: %s/%s", className, objectID)
		}
		return json.Unmarshal(v, &info)
	})
	if err != nil {
		return "", nil, err
	}
	return info.ObjectHash, info.ObjectData, nil
}

// knownObjectRecord is the internal representation stored in bbolt.
type knownObjectRecord struct {
	ObjectHash string `json:"object_hash"`
	VectorHash string `json:"vector_hash,omitempty"`
	ObjectData []byte `json:"object_data"`
}

// GetAllKnownObjects retrieves all known objects.
func (s *Store) GetAllKnownObjects() (map[string]*models.WeaviateObject, error) {
	objects := make(map[string]*models.WeaviateObject)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketKnownObjects).ForEach(func(k, v []byte) error {
			var rec knownObjectRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			var obj models.WeaviateObject
			if err := json.Unmarshal(rec.ObjectData, &obj); err != nil {
				return err
			}
			// Key format is "class:objectID", convert to "class/objectID"
			parts := strings.SplitN(string(k), ":", 2)
			if len(parts) == 2 {
				objects[models.ObjectKey(parts[0], parts[1])] = &obj
			}
			return nil
		})
	})
	return objects, err
}

// GetAllKnownObjectsWithHashes retrieves all known objects with their hashes.
func (s *Store) GetAllKnownObjectsWithHashes() (map[string]*models.KnownObjectInfo, error) {
	objects := make(map[string]*models.KnownObjectInfo)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketKnownObjects).ForEach(func(k, v []byte) error {
			var rec knownObjectRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			var obj models.WeaviateObject
			if err := json.Unmarshal(rec.ObjectData, &obj); err != nil {
				return err
			}
			parts := strings.SplitN(string(k), ":", 2)
			if len(parts) == 2 {
				objects[models.ObjectKey(parts[0], parts[1])] = &models.KnownObjectInfo{
					Object:     &obj,
					ObjectHash: rec.ObjectHash,
					VectorHash: rec.VectorHash,
				}
			}
			return nil
		})
	})
	return objects, err
}

// DeleteKnownObject removes a known object.
func (s *Store) DeleteKnownObject(className, objectID string) error {
	key := className + ":" + objectID
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketKnownObjects).Delete([]byte(key))
	})
}

// ClearKnownObjects removes all known objects.
func (s *Store) ClearKnownObjects() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucketKnownObjects); err != nil {
			return err
		}
		_, err := tx.CreateBucket(bucketKnownObjects)
		return err
	})
}

// SaveKnownObjectWithVector saves or updates a known object state including vector hash.
func (s *Store) SaveKnownObjectWithVector(className, objectID, objectHash, vectorHash string, data []byte) error {
	key := className + ":" + objectID
	rec := knownObjectRecord{
		ObjectHash: objectHash,
		VectorHash: vectorHash,
		ObjectData: data,
	}
	encoded, err := json.Marshal(&rec)
	if err != nil {
		return fmt.Errorf("marshal known object: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketKnownObjects).Put([]byte(key), encoded)
	})
}
