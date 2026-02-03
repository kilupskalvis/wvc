package core

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
)

// IncrementalDiffResult contains both staged and unstaged changes
type IncrementalDiffResult struct {
	Staged   *DiffResult
	Unstaged *DiffResult
}

// TotalChanges returns total unstaged changes
func (r *IncrementalDiffResult) TotalUnstagedChanges() int {
	if r.Unstaged == nil {
		return 0
	}
	return r.Unstaged.TotalChanges()
}

// TotalStagedChanges returns total staged changes
func (r *IncrementalDiffResult) TotalStagedChanges() int {
	if r.Staged == nil {
		return 0
	}
	return r.Staged.TotalChanges()
}

// ComputeIncrementalDiff computes diff using incremental detection when possible
func ComputeIncrementalDiff(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface) (*IncrementalDiffResult, error) {
	// Get staged changes directly from the database
	stagedDiff, err := GetStagedDiff(st)
	if err != nil {
		return nil, err
	}

	result := &IncrementalDiffResult{
		Staged: stagedDiff,
		Unstaged: &DiffResult{
			Inserted: make([]*ObjectChange, 0),
			Updated:  make([]*ObjectChange, 0),
			Deleted:  make([]*ObjectChange, 0),
		},
	}

	useCursor := cfg.SupportsCursorPagination()

	// Get all classes
	classes, err := client.GetClasses(ctx)
	if err != nil {
		return nil, err
	}

	// Build map of staged object keys to exclude from unstaged detection
	stagedChanges, err := st.GetAllStagedChanges()
	if err != nil {
		return nil, err
	}
	stagedMap := make(map[string]*store.StagedChange)
	for _, sc := range stagedChanges {
		key := models.ObjectKey(sc.ClassName, sc.ObjectID)
		stagedMap[key] = sc
	}

	// Process each class
	for _, className := range classes {
		if err := processClassIncremental(ctx, st, client, className, useCursor, result, stagedMap); err != nil {
			return nil, err
		}
	}

	// Check for deleted classes (classes that were known but no longer exist)
	knownClasses, err := getKnownClasses(st)
	if err != nil {
		return nil, err
	}

	classSet := make(map[string]bool)
	for _, c := range classes {
		classSet[c] = true
	}

	for _, knownClass := range knownClasses {
		if !classSet[knownClass] {
			// Class was deleted - all its objects are deletions
			if err := processDeletedClass(st, knownClass, result, stagedMap); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// processClassIncremental processes a single class for changes
func processClassIncremental(ctx context.Context, st *store.Store, client weaviate.ClientInterface, className string, useCursor bool, result *IncrementalDiffResult, stagedMap map[string]*store.StagedChange) error {
	// Get scan metadata
	meta, err := st.GetScanMetadata(className)
	if err != nil {
		return err
	}

	// Get current count
	currentCount, err := client.GetClassCount(ctx, className)
	if err != nil {
		// Fall back to full scan if count fails
		return processClassFullScan(ctx, st, client, className, useCursor, result, stagedMap)
	}

	// Get known count
	knownCount, err := st.GetKnownObjectCount(className)
	if err != nil {
		return err
	}

	// Determine if we need full scan or can use incremental
	needFullScan := meta == nil || // No previous scan
		currentCount != knownCount // Count changed (inserts or deletes)

	if needFullScan {
		return processClassFullScan(ctx, st, client, className, useCursor, result, stagedMap)
	}

	// Incremental scan - fetch all but only process those with newer timestamps
	return processClassIncrementalScan(ctx, st, client, className, useCursor, meta.ScanHighWatermark, result, stagedMap)
}

// processClassFullScan does a full comparison for a class
func processClassFullScan(ctx context.Context, st *store.Store, client weaviate.ClientInterface, className string, useCursor bool, result *IncrementalDiffResult, stagedMap map[string]*store.StagedChange) error {
	// Fetch all current objects
	currentObjects, err := client.GetAllObjects(ctx, className, useCursor)
	if err != nil {
		return err
	}

	// Get known objects with hashes for this class
	knownObjects, err := st.GetAllKnownObjectsWithHashes()
	if err != nil {
		return err
	}

	// Build map of current objects
	currentMap := make(map[string]*models.WeaviateObject)
	for _, obj := range currentObjects {
		key := models.ObjectKey(className, obj.ID)
		currentMap[key] = obj
	}

	// Find inserts and updates (only add to Unstaged - staged changes come from DB)
	for key, current := range currentMap {
		knownInfo, exists := knownObjects[key]

		// Skip if already staged
		if stagedMap[key] != nil {
			continue
		}

		// Compute current hashes (both property and vector)
		currentObjHash, currentVecHash := weaviate.HashObjectFull(current)

		if !exists {
			// New object
			change := &ObjectChange{
				ClassName:   current.Class,
				ObjectID:    current.ID,
				CurrentData: current,
				VectorHash:  currentVecHash,
			}
			result.Unstaged.Inserted = append(result.Unstaged.Inserted, change)
		} else {
			// Check if updated (properties or vector)
			propsChanged := currentObjHash != knownInfo.ObjectHash
			vectorChanged := currentVecHash != knownInfo.VectorHash

			if propsChanged || vectorChanged {
				change := &ObjectChange{
					ClassName:          current.Class,
					ObjectID:           current.ID,
					CurrentData:        current,
					PreviousData:       knownInfo.Object,
					VectorHash:         currentVecHash,
					PreviousVectorHash: knownInfo.VectorHash,
					VectorOnly:         !propsChanged && vectorChanged,
				}
				result.Unstaged.Updated = append(result.Unstaged.Updated, change)
			}
		}
	}

	// Find deletes - objects in known but not in current
	for key, knownInfo := range knownObjects {
		if knownInfo.Object.Class != className {
			continue
		}

		// Skip if already staged
		if stagedMap[key] != nil {
			continue
		}

		if _, exists := currentMap[key]; !exists {
			change := &ObjectChange{
				ClassName:          knownInfo.Object.Class,
				ObjectID:           knownInfo.Object.ID,
				PreviousData:       knownInfo.Object,
				PreviousVectorHash: knownInfo.VectorHash,
			}
			result.Unstaged.Deleted = append(result.Unstaged.Deleted, change)
		}
	}

	// Note: Don't update scan_metadata here - only during commit
	return nil
}

// processClassIncrementalScan only checks objects modified since last scan
func processClassIncrementalScan(ctx context.Context, st *store.Store, client weaviate.ClientInterface, className string, useCursor bool, watermark int64, result *IncrementalDiffResult, stagedMap map[string]*store.StagedChange) error {
	// Fetch all objects (we have to, REST API doesn't support timestamp filtering)
	currentObjects, err := client.GetAllObjects(ctx, className, useCursor)
	if err != nil {
		return err
	}

	// Get known objects with hashes
	knownObjects, err := st.GetAllKnownObjectsWithHashes()
	if err != nil {
		return err
	}

	// Only process objects with timestamps > watermark (and not already staged)
	for _, obj := range currentObjects {
		// Skip if not modified since last scan
		if obj.LastUpdateTimeUnix <= watermark {
			continue
		}

		key := models.ObjectKey(className, obj.ID)

		// Skip if already staged
		if stagedMap[key] != nil {
			continue
		}

		// Compute current hashes (both property and vector)
		currentObjHash, currentVecHash := weaviate.HashObjectFull(obj)

		knownInfo, exists := knownObjects[key]

		if !exists {
			// New object
			change := &ObjectChange{
				ClassName:   obj.Class,
				ObjectID:    obj.ID,
				CurrentData: obj,
				VectorHash:  currentVecHash,
			}
			result.Unstaged.Inserted = append(result.Unstaged.Inserted, change)
		} else {
			// Check if actually updated (properties or vector)
			propsChanged := currentObjHash != knownInfo.ObjectHash
			vectorChanged := currentVecHash != knownInfo.VectorHash

			if propsChanged || vectorChanged {
				change := &ObjectChange{
					ClassName:          obj.Class,
					ObjectID:           obj.ID,
					CurrentData:        obj,
					PreviousData:       knownInfo.Object,
					VectorHash:         currentVecHash,
					PreviousVectorHash: knownInfo.VectorHash,
					VectorOnly:         !propsChanged && vectorChanged,
				}
				result.Unstaged.Updated = append(result.Unstaged.Updated, change)
			}
		}
	}

	// Note: Don't update scan_metadata here - only during commit
	return nil
}

// processDeletedClass handles a class that was deleted entirely
func processDeletedClass(st *store.Store, className string, result *IncrementalDiffResult, stagedMap map[string]*store.StagedChange) error {
	knownObjects, err := st.GetAllKnownObjectsWithHashes()
	if err != nil {
		return err
	}

	for key, knownInfo := range knownObjects {
		if knownInfo.Object.Class != className {
			continue
		}

		// Skip if already staged
		if stagedMap[key] != nil {
			continue
		}

		change := &ObjectChange{
			ClassName:          knownInfo.Object.Class,
			ObjectID:           knownInfo.Object.ID,
			PreviousData:       knownInfo.Object,
			PreviousVectorHash: knownInfo.VectorHash,
		}
		result.Unstaged.Deleted = append(result.Unstaged.Deleted, change)
	}

	return nil
}

// getKnownClasses returns all classes that have known objects.
// It scans the known_objects bucket and extracts distinct class names from keys
// formatted as "class:objectID".
func getKnownClasses(st *store.Store) ([]string, error) {
	knownObjects, err := st.GetAllKnownObjectsWithHashes()
	if err != nil {
		return nil, err
	}

	classSet := make(map[string]bool)
	for _, info := range knownObjects {
		if info.Object != nil && info.Object.Class != "" {
			classSet[info.Object.Class] = true
		}
	}

	classes := make([]string, 0, len(classSet))
	for c := range classSet {
		classes = append(classes, c)
	}
	return classes, nil
}

// ConvertToStagedChange converts an ObjectChange to a StagedChange for storing
func ConvertToStagedChange(change *ObjectChange, changeType string) *store.StagedChange {
	var objectData, previousData []byte

	if change.CurrentData != nil {
		objectData, _ = json.Marshal(change.CurrentData)
	}
	if change.PreviousData != nil {
		previousData, _ = json.Marshal(change.PreviousData)
	}

	return &store.StagedChange{
		ClassName:          change.ClassName,
		ObjectID:           change.ObjectID,
		ChangeType:         changeType,
		ObjectData:         objectData,
		PreviousData:       previousData,
		StagedAt:           time.Now(),
		VectorHash:         change.VectorHash,
		PreviousVectorHash: change.PreviousVectorHash,
	}
}
