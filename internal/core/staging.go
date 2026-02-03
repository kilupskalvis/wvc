package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
)

// StageAll stages all detected changes
func StageAll(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface) (int, error) {
	diff, err := ComputeIncrementalDiff(ctx, cfg, st, client)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, change := range diff.Unstaged.Inserted {
		if err := st.AddStagedChange(ConvertToStagedChange(change, "insert")); err != nil {
			return count, err
		}
		count++
	}
	for _, change := range diff.Unstaged.Updated {
		if err := st.AddStagedChange(ConvertToStagedChange(change, "update")); err != nil {
			return count, err
		}
		count++
	}
	for _, change := range diff.Unstaged.Deleted {
		if err := st.AddStagedChange(ConvertToStagedChange(change, "delete")); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// StageClass stages all changes for a specific class
func StageClass(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, className string) (int, error) {
	diff, err := ComputeIncrementalDiff(ctx, cfg, st, client)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, change := range diff.Unstaged.Inserted {
		if change.ClassName == className {
			if err := st.AddStagedChange(ConvertToStagedChange(change, "insert")); err != nil {
				return count, err
			}
			count++
		}
	}
	for _, change := range diff.Unstaged.Updated {
		if change.ClassName == className {
			if err := st.AddStagedChange(ConvertToStagedChange(change, "update")); err != nil {
				return count, err
			}
			count++
		}
	}
	for _, change := range diff.Unstaged.Deleted {
		if change.ClassName == className {
			if err := st.AddStagedChange(ConvertToStagedChange(change, "delete")); err != nil {
				return count, err
			}
			count++
		}
	}
	return count, nil
}

// StageObject stages a specific object change
func StageObject(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, className, objectID string) error {
	diff, err := ComputeIncrementalDiff(ctx, cfg, st, client)
	if err != nil {
		return err
	}

	key := models.ObjectKey(className, objectID)
	for _, change := range diff.Unstaged.Inserted {
		if models.ObjectKey(change.ClassName, change.ObjectID) == key {
			return st.AddStagedChange(ConvertToStagedChange(change, "insert"))
		}
	}
	for _, change := range diff.Unstaged.Updated {
		if models.ObjectKey(change.ClassName, change.ObjectID) == key {
			return st.AddStagedChange(ConvertToStagedChange(change, "update"))
		}
	}
	for _, change := range diff.Unstaged.Deleted {
		if models.ObjectKey(change.ClassName, change.ObjectID) == key {
			return st.AddStagedChange(ConvertToStagedChange(change, "delete"))
		}
	}
	return fmt.Errorf("no changes found for %s/%s", className, objectID)
}

// UnstageAll removes all staged changes
func UnstageAll(st *store.Store) error {
	return st.ClearStagedChanges()
}

// UnstageClass removes staged changes for a specific class
func UnstageClass(st *store.Store, className string) error {
	return st.RemoveStagedChangesByClass(className)
}

// UnstageObject removes a staged change for a specific object
func UnstageObject(st *store.Store, className, objectID string) error {
	return st.RemoveStagedChange(className, objectID)
}

// ParseObjectRef parses a reference like "Article/abc123" into class and ID
func ParseObjectRef(ref string) (className, objectID string, err error) {
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	// Just a class name
	return parts[0], "", nil
}

// GetStagedDiff returns only the staged changes as a DiffResult
func GetStagedDiff(st *store.Store) (*DiffResult, error) {
	staged, err := st.GetAllStagedChanges()
	if err != nil {
		return nil, err
	}

	result := &DiffResult{
		Inserted: make([]*ObjectChange, 0),
		Updated:  make([]*ObjectChange, 0),
		Deleted:  make([]*ObjectChange, 0),
	}

	for _, sc := range staged {
		change := &ObjectChange{
			ClassName:          sc.ClassName,
			ObjectID:           sc.ObjectID,
			VectorHash:         sc.VectorHash,
			PreviousVectorHash: sc.PreviousVectorHash,
		}
		if len(sc.ObjectData) > 0 {
			var obj models.WeaviateObject
			if err := json.Unmarshal(sc.ObjectData, &obj); err == nil {
				change.CurrentData = &obj
			}
		}
		if len(sc.PreviousData) > 0 {
			var obj models.WeaviateObject
			if err := json.Unmarshal(sc.PreviousData, &obj); err == nil {
				change.PreviousData = &obj
			}
		}

		switch sc.ChangeType {
		case "insert":
			result.Inserted = append(result.Inserted, change)
		case "update":
			result.Updated = append(result.Updated, change)
		case "delete":
			result.Deleted = append(result.Deleted, change)
		}
	}

	return result, nil
}
