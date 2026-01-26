// Package core implements the domain logic for WVC including
// diff computation, commit creation, and revert operations.
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

// DiffResult represents the differences between two states
type DiffResult struct {
	Inserted []*ObjectChange
	Updated  []*ObjectChange
	Deleted  []*ObjectChange
}

// ObjectChange represents a change to an object
type ObjectChange struct {
	ClassName          string
	ObjectID           string
	CurrentData        *models.WeaviateObject
	PreviousData       *models.WeaviateObject
	VectorHash         string // Current vector hash
	PreviousVectorHash string // Previous vector hash (for updates)
	VectorOnly         bool   // True if only the vector changed (properties unchanged)
}

// TotalChanges returns the total number of changes
func (d *DiffResult) TotalChanges() int {
	return len(d.Inserted) + len(d.Updated) + len(d.Deleted)
}

// ComputeDiff computes the difference between current Weaviate state and last known state
func ComputeDiff(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface) (*DiffResult, error) {
	result := &DiffResult{
		Inserted: make([]*ObjectChange, 0),
		Updated:  make([]*ObjectChange, 0),
		Deleted:  make([]*ObjectChange, 0),
	}

	// Determine pagination method based on server version
	useCursor := cfg.SupportsCursorPagination()

	// Get current state from Weaviate
	currentObjects, err := client.GetAllObjectsAllClasses(ctx, useCursor)
	if err != nil {
		return nil, err
	}

	// Get last known state from store with hashes
	knownObjects, err := st.GetAllKnownObjectsWithHashes()
	if err != nil {
		return nil, err
	}

	// Find inserted and updated objects
	for key, current := range currentObjects {
		// Compute current hashes
		currentObjHash, currentVecHash := weaviate.HashObjectFull(current)

		known, exists := knownObjects[key]
		if !exists {
			// New object
			result.Inserted = append(result.Inserted, &ObjectChange{
				ClassName:   current.Class,
				ObjectID:    current.ID,
				CurrentData: current,
				VectorHash:  currentVecHash,
			})
		} else {
			// Check if updated (either properties or vector)
			propsChanged := currentObjHash != known.ObjectHash
			vectorChanged := currentVecHash != known.VectorHash

			if propsChanged || vectorChanged {
				result.Updated = append(result.Updated, &ObjectChange{
					ClassName:          current.Class,
					ObjectID:           current.ID,
					CurrentData:        current,
					PreviousData:       known.Object,
					VectorHash:         currentVecHash,
					PreviousVectorHash: known.VectorHash,
					VectorOnly:         !propsChanged && vectorChanged,
				})
			}
		}
	}

	// Find deleted objects
	for key, known := range knownObjects {
		if _, exists := currentObjects[key]; !exists {
			result.Deleted = append(result.Deleted, &ObjectChange{
				ClassName:          known.Object.Class,
				ObjectID:           known.Object.ID,
				PreviousData:       known.Object,
				PreviousVectorHash: known.VectorHash,
			})
		}
	}

	return result, nil
}

// RecordDiffAsOperations records diff changes as operations in the store
func RecordDiffAsOperations(st *store.Store, diff *DiffResult) error {
	now := time.Now()

	// Record inserts
	for _, change := range diff.Inserted {
		data, _ := json.Marshal(change.CurrentData)

		// Store vector blob and get hash
		vectorHash, err := storeVectorFromObject(st, change.CurrentData)
		if err != nil {
			return err
		}

		op := &models.Operation{
			Timestamp:  now,
			Type:       models.OperationInsert,
			ClassName:  change.ClassName,
			ObjectID:   change.ObjectID,
			ObjectData: data,
			VectorHash: vectorHash,
		}
		if err := st.RecordOperation(op); err != nil {
			return err
		}
	}

	// Record updates
	for _, change := range diff.Updated {
		data, _ := json.Marshal(change.CurrentData)
		prevData, _ := json.Marshal(change.PreviousData)

		// Store current vector blob
		vectorHash, err := storeVectorFromObject(st, change.CurrentData)
		if err != nil {
			return err
		}

		// Increment ref count for previous vector (it's already stored)
		// or store it if this is a migration scenario
		previousVectorHash := change.PreviousVectorHash
		if previousVectorHash == "" && change.PreviousData != nil {
			previousVectorHash, _ = storeVectorFromObject(st, change.PreviousData)
		} else if previousVectorHash != "" {
			_ = st.IncrementVectorRefCount(previousVectorHash)
		}

		op := &models.Operation{
			Timestamp:          now,
			Type:               models.OperationUpdate,
			ClassName:          change.ClassName,
			ObjectID:           change.ObjectID,
			ObjectData:         data,
			PreviousData:       prevData,
			VectorHash:         vectorHash,
			PreviousVectorHash: previousVectorHash,
		}
		if err := st.RecordOperation(op); err != nil {
			return err
		}
	}

	// Record deletes
	for _, change := range diff.Deleted {
		prevData, _ := json.Marshal(change.PreviousData)

		// Store previous vector for revert capability
		previousVectorHash := change.PreviousVectorHash
		if previousVectorHash == "" && change.PreviousData != nil {
			previousVectorHash, _ = storeVectorFromObject(st, change.PreviousData)
		} else if previousVectorHash != "" {
			_ = st.IncrementVectorRefCount(previousVectorHash)
		}

		op := &models.Operation{
			Timestamp:          now,
			Type:               models.OperationDelete,
			ClassName:          change.ClassName,
			ObjectID:           change.ObjectID,
			PreviousData:       prevData,
			PreviousVectorHash: previousVectorHash,
		}
		if err := st.RecordOperation(op); err != nil {
			return err
		}
	}

	return nil
}

// storeVectorFromObject extracts vector from object, stores it, and returns hash
func storeVectorFromObject(st *store.Store, obj *models.WeaviateObject) (string, error) {
	if obj == nil || obj.Vector == nil {
		return "", nil
	}

	vectorBytes, dims, err := store.VectorFromObject(obj)
	if err != nil || len(vectorBytes) == 0 {
		return "", nil
	}

	return st.SaveVectorBlob(vectorBytes, dims)
}

// UpdateKnownState updates the known objects state to match current Weaviate state
func UpdateKnownState(ctx context.Context, st *store.Store, client weaviate.ClientInterface, useCursor bool) error {
	// Get current state from Weaviate
	currentObjects, err := client.GetAllObjectsAllClasses(ctx, useCursor)
	if err != nil {
		return err
	}

	// Clear and rebuild known objects
	if err := st.ClearKnownObjects(); err != nil {
		return err
	}

	for _, obj := range currentObjects {
		objectHash, vectorHash := weaviate.HashObjectFull(obj)

		// Store vector blob if present
		if vectorHash != "" {
			vectorBytes, dims, _ := store.VectorFromObject(obj)
			if len(vectorBytes) > 0 {
				storedHash, err := st.SaveVectorBlob(vectorBytes, dims)
				if err == nil {
					vectorHash = storedHash
				}
			}
		}

		data, _ := json.Marshal(obj)
		if err := st.SaveKnownObjectWithVector(obj.Class, obj.ID, objectHash, vectorHash, data); err != nil {
			return err
		}
	}

	return nil
}
