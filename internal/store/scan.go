package store

import (
	"database/sql"
	"time"
)

// ScanMetadata represents the scan state for a class
type ScanMetadata struct {
	ClassName         string
	LastScanTime      time.Time
	LastScanCount     int
	ScanHighWatermark int64 // Highest lastUpdateTimeUnix seen
}

// GetScanMetadata retrieves scan metadata for a class
func (s *Store) GetScanMetadata(className string) (*ScanMetadata, error) {
	var meta ScanMetadata
	var lastScanTime int64

	err := s.db.QueryRow(`
		SELECT class_name, last_scan_time, last_scan_count, scan_high_watermark
		FROM scan_metadata WHERE class_name = ?
	`, className).Scan(&meta.ClassName, &lastScanTime, &meta.LastScanCount, &meta.ScanHighWatermark)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	meta.LastScanTime = time.Unix(lastScanTime, 0)
	return &meta, nil
}

// GetKnownObjectCount returns the count of known objects for a class
func (s *Store) GetKnownObjectCount(className string) (int, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM known_objects WHERE class_name = ?
	`, className).Scan(&count)
	return count, err
}
