// Package store provides bbolt-based persistence for WVC.
package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	bolt "go.etcd.io/bbolt"

	"github.com/kilupskalvis/wvc/internal/models"
)

var (
	ErrVectorNotFound = errors.New("vector blob not found")
	ErrInvalidVector  = errors.New("invalid vector format")
)

// vectorBlobRecord stores vector data with reference counting
type vectorBlobRecord struct {
	Data       []byte `json:"data"`
	Dimensions int    `json:"dimensions"`
	RefCount   int    `json:"ref_count"`
}

// VectorToBytes converts a vector (interface{}) to raw binary float32 bytes (little-endian).
// Returns the bytes, dimension count, and any error.
func VectorToBytes(v interface{}) ([]byte, int, error) {
	if v == nil {
		return nil, 0, nil
	}

	var floats []float32

	switch vec := v.(type) {
	case []float32:
		floats = vec
	case []float64:
		floats = make([]float32, len(vec))
		for i, f := range vec {
			floats[i] = float32(f)
		}
	case []interface{}:
		floats = make([]float32, len(vec))
		for i, val := range vec {
			switch n := val.(type) {
			case float64:
				floats[i] = float32(n)
			case float32:
				floats[i] = n
			case int:
				floats[i] = float32(n)
			case int64:
				floats[i] = float32(n)
			default:
				return nil, 0, fmt.Errorf("%w: unsupported element type %T at index %d", ErrInvalidVector, val, i)
			}
		}
	default:
		return nil, 0, fmt.Errorf("%w: unsupported vector type %T", ErrInvalidVector, v)
	}

	if len(floats) == 0 {
		return nil, 0, nil
	}

	// Convert to little-endian binary
	buf := make([]byte, len(floats)*4)
	for i, f := range floats {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}

	return buf, len(floats), nil
}

// BytesToVector converts raw binary bytes back to []float32.
func BytesToVector(data []byte, dimensions int) ([]float32, error) {
	if len(data) == 0 {
		return nil, nil
	}

	expectedLen := dimensions * 4
	if len(data) != expectedLen {
		return nil, fmt.Errorf("%w: expected %d bytes for %d dimensions, got %d",
			ErrInvalidVector, expectedLen, dimensions, len(data))
	}

	floats := make([]float32, dimensions)
	for i := 0; i < dimensions; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4:])
		floats[i] = math.Float32frombits(bits)
	}

	return floats, nil
}

// HashVector computes SHA256 of vector bytes and returns hex-encoded string.
func HashVector(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// VectorFromObject extracts and converts the vector from a WeaviateObject.
// Returns the binary bytes, dimension count, and any error.
func VectorFromObject(obj *models.WeaviateObject) ([]byte, int, error) {
	if obj == nil || obj.Vector == nil {
		return nil, 0, nil
	}
	return VectorToBytes(obj.Vector)
}

// SaveVectorBlob stores a vector blob in the database.
// If a blob with the same hash already exists, increments ref_count instead.
// Returns the hash of the stored blob.
func (s *Store) SaveVectorBlob(data []byte, dimensions int) (string, error) {
	if len(data) == 0 {
		return "", nil
	}

	hash := HashVector(data)

	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketVectorBlobs)
		if err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}

		key := []byte(hash)
		existing := bucket.Get(key)

		if existing != nil {
			// Increment ref count on duplicate
			var record vectorBlobRecord
			if err := json.Unmarshal(existing, &record); err != nil {
				return fmt.Errorf("unmarshal existing record: %w", err)
			}
			record.RefCount++
			encoded, err := json.Marshal(record)
			if err != nil {
				return fmt.Errorf("marshal record: %w", err)
			}
			return bucket.Put(key, encoded)
		}

		// Create new record
		record := vectorBlobRecord{
			Data:       data,
			Dimensions: dimensions,
			RefCount:   1,
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshal record: %w", err)
		}
		return bucket.Put(key, encoded)
	})

	if err != nil {
		return "", fmt.Errorf("failed to save vector blob: %w", err)
	}

	return hash, nil
}

// GetVectorBlob retrieves vector bytes by hash.
// Returns the binary data, dimensions, and any error.
func (s *Store) GetVectorBlob(hash string) ([]byte, int, error) {
	if hash == "" {
		return nil, 0, nil
	}

	var data []byte
	var dimensions int

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketVectorBlobs)
		if bucket == nil {
			return ErrVectorNotFound
		}

		value := bucket.Get([]byte(hash))
		if value == nil {
			return ErrVectorNotFound
		}

		var record vectorBlobRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return fmt.Errorf("unmarshal record: %w", err)
		}

		data = record.Data
		dimensions = record.Dimensions
		return nil
	})

	if err != nil {
		if errors.Is(err, ErrVectorNotFound) {
			return nil, 0, ErrVectorNotFound
		}
		return nil, 0, fmt.Errorf("failed to get vector blob: %w", err)
	}

	return data, dimensions, nil
}

// IncrementVectorRefCount increments the reference count for a vector blob.
func (s *Store) IncrementVectorRefCount(hash string) error {
	if hash == "" {
		return nil
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketVectorBlobs)
		if bucket == nil {
			return ErrVectorNotFound
		}

		key := []byte(hash)
		value := bucket.Get(key)
		if value == nil {
			return ErrVectorNotFound
		}

		var record vectorBlobRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return fmt.Errorf("unmarshal record: %w", err)
		}

		record.RefCount++
		encoded, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshal record: %w", err)
		}

		return bucket.Put(key, encoded)
	})
}

// DecrementVectorRefCount decrements the reference count for a vector blob.
// Returns true if the blob was deleted (ref_count reached 0).
func (s *Store) DecrementVectorRefCount(hash string) (bool, error) {
	if hash == "" {
		return false, nil
	}

	var deleted bool

	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketVectorBlobs)
		if bucket == nil {
			return ErrVectorNotFound
		}

		key := []byte(hash)
		value := bucket.Get(key)
		if value == nil {
			return ErrVectorNotFound
		}

		var record vectorBlobRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return fmt.Errorf("unmarshal record: %w", err)
		}

		record.RefCount--
		if record.RefCount <= 0 {
			deleted = true
			return bucket.Delete(key)
		}

		encoded, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshal record: %w", err)
		}

		return bucket.Put(key, encoded)
	})

	if err != nil {
		return false, fmt.Errorf("failed to decrement vector ref count: %w", err)
	}

	return deleted, nil
}
