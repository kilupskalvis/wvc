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

// GetAllScanMetadata retrieves scan metadata for all classes
func (s *Store) GetAllScanMetadata() (map[string]*ScanMetadata, error) {
	rows, err := s.db.Query(`
		SELECT class_name, last_scan_time, last_scan_count, scan_high_watermark
		FROM scan_metadata
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]*ScanMetadata)
	for rows.Next() {
		var meta ScanMetadata
		var lastScanTime int64

		if err := rows.Scan(&meta.ClassName, &lastScanTime, &meta.LastScanCount, &meta.ScanHighWatermark); err != nil {
			return nil, err
		}

		meta.LastScanTime = time.Unix(lastScanTime, 0)
		result[meta.ClassName] = &meta
	}

	return result, nil
}

// UpdateScanMetadata updates or inserts scan metadata for a class
func (s *Store) UpdateScanMetadata(meta *ScanMetadata) error {
	_, err := s.db.Exec(`
		INSERT INTO scan_metadata (class_name, last_scan_time, last_scan_count, scan_high_watermark)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(class_name) DO UPDATE SET
			last_scan_time = excluded.last_scan_time,
			last_scan_count = excluded.last_scan_count,
			scan_high_watermark = excluded.scan_high_watermark
	`, meta.ClassName, meta.LastScanTime.Unix(), meta.LastScanCount, meta.ScanHighWatermark)
	return err
}

// DeleteScanMetadata removes scan metadata for a class
func (s *Store) DeleteScanMetadata(className string) error {
	_, err := s.db.Exec("DELETE FROM scan_metadata WHERE class_name = ?", className)
	return err
}

// GetKnownObjectIDs returns all known object IDs for a class
func (s *Store) GetKnownObjectIDs(className string) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT object_id FROM known_objects WHERE class_name = ?
	`, className)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

// GetKnownObjectCount returns the count of known objects for a class
func (s *Store) GetKnownObjectCount(className string) (int, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM known_objects WHERE class_name = ?
	`, className).Scan(&count)
	return count, err
}

// SaveKnownObjectWithTimestamp saves a known object with its update timestamp
func (s *Store) SaveKnownObjectWithTimestamp(className, objectID, hash string, data []byte, lastUpdateTime int64) error {
	_, err := s.db.Exec(`
		INSERT INTO known_objects (class_name, object_id, object_hash, object_data, last_update_time)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(class_name, object_id) DO UPDATE SET
			object_hash = excluded.object_hash,
			object_data = excluded.object_data,
			last_update_time = excluded.last_update_time
	`, className, objectID, hash, data, lastUpdateTime)
	return err
}
