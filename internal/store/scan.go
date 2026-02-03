package store

import (
	"bytes"
	"encoding/json"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ScanMetadata represents the scan state for a class
type ScanMetadata struct {
	ClassName         string
	LastScanTime      time.Time
	LastScanCount     int
	ScanHighWatermark int64 // Highest lastUpdateTimeUnix seen
}

// GetScanMetadata retrieves scan metadata for a class, returns nil if not found
func (s *Store) GetScanMetadata(className string) (*ScanMetadata, error) {
	var metadata *ScanMetadata

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketScanMetadata)
		if bucket == nil {
			return nil
		}

		data := bucket.Get([]byte(className))
		if data == nil {
			return nil
		}

		metadata = &ScanMetadata{}
		return json.Unmarshal(data, metadata)
	})

	if err != nil {
		return nil, err
	}

	return metadata, nil
}

// GetKnownObjectCount counts known objects with matching class prefix
func (s *Store) GetKnownObjectCount(className string) (int, error) {
	count := 0
	prefix := []byte(className + ":")

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketKnownObjects)
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			count++
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return count, nil
}
