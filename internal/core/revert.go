package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
)

// SchemaRevertWarning represents a warning about schema operations that couldn't be reverted
type SchemaRevertWarning struct {
	Operation    string
	ClassName    string
	PropertyName string
	Reason       string
}

// RevertCommit reverts all operations in a commit
func RevertCommit(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, commitID string) (*models.Commit, error) {
	return RevertCommitWithWarnings(ctx, cfg, st, client, commitID, nil)
}

// RevertCommitWithWarnings reverts a commit and collects schema warnings
func RevertCommitWithWarnings(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, commitID string, warnings *[]SchemaRevertWarning) (*models.Commit, error) {
	// Get the commit
	commit, err := st.GetCommit(commitID)
	if err != nil {
		// Try short ID
		commit, err = st.GetCommitByShortID(commitID)
		if err != nil {
			return nil, fmt.Errorf("commit not found: %s", commitID)
		}
	}

	// Get schema versions for this commit and its parent
	currentSchema, err := st.GetSchemaVersionByCommit(commit.ID)
	if err != nil {
		return nil, err
	}

	parentSchema, _ := st.GetPreviousCommitSchema(commit.ID)

	// Compute schema diff if both exist
	var schemaDiff *SchemaDiffResult
	if currentSchema != nil {
		var prevJSON []byte
		if parentSchema != nil {
			prevJSON = parentSchema.SchemaJSON
		}
		schemaDiff, _ = ComputeSchemaDiffBetweenVersions(currentSchema.SchemaJSON, prevJSON)
	}

	// STEP 1: Before data revert - recreate deleted classes/properties
	// This is needed so data can be restored into them
	if schemaDiff != nil && warnings != nil {
		revertSchemaBeforeData(ctx, client, schemaDiff, warnings)
	}

	// STEP 2: Get and apply data reverse operations
	operations, err := st.GetOperationsByCommit(commit.ID)
	if err != nil {
		return nil, err
	}

	if len(operations) > 0 {
		if err := applyReverseOperations(ctx, st, client, operations); err != nil {
			return nil, err
		}
	}

	// STEP 3: After data revert - delete added classes (now empty)
	if schemaDiff != nil && warnings != nil {
		revertSchemaAfterData(ctx, client, schemaDiff, warnings)
	}

	// Create revert commit
	revertMessage := fmt.Sprintf("Revert: %s", commit.Message)
	now := time.Now()

	// Get uncommitted operations (the reverse ops we just recorded) for content-addressable ID
	uncommittedOps, err := st.GetUncommittedOperations()
	if err != nil {
		return nil, err
	}
	revertCommitID := models.GenerateCommitID(revertMessage, now, commit.ID, uncommittedOps)

	// Capture current schema state for the revert commit
	if err := captureSchemaSnapshot(ctx, st, client, revertCommitID); err != nil {
		// Non-fatal - continue
	}

	parentID, _ := st.GetHEAD()
	revertCommit := &models.Commit{
		ID:             revertCommitID,
		ParentID:       parentID,
		Message:        revertMessage,
		Timestamp:      now,
		OperationCount: len(operations),
	}

	// Atomically: mark operations committed, create commit, set HEAD, update branch
	branchName, _ := st.GetCurrentBranch()
	branchExists := false
	if branchName != "" {
		existing, _ := st.GetBranch(branchName)
		branchExists = existing != nil
	}
	if _, err := st.FinalizeCommit(revertCommit, branchName, branchExists); err != nil {
		return nil, fmt.Errorf("finalize revert commit: %w", err)
	}

	// Update known state
	useCursor := cfg.SupportsCursorPagination()
	if err := UpdateKnownState(ctx, st, client, useCursor); err != nil {
		return nil, err
	}

	return revertCommit, nil
}

// revertSchemaBeforeData recreates deleted classes/properties before data restore
func revertSchemaBeforeData(ctx context.Context, client weaviate.ClientInterface, diff *SchemaDiffResult, warnings *[]SchemaRevertWarning) {
	// Recreate deleted classes (so we can restore data into them)
	for _, change := range diff.ClassesDeleted {
		classData := change.PreviousValue
		if classData == nil {
			continue
		}

		// Convert map to class struct
		classJSON, _ := json.Marshal(classData)
		var class models.WeaviateClass
		if err := json.Unmarshal(classJSON, &class); err != nil {
			*warnings = append(*warnings, SchemaRevertWarning{
				Operation: "recreate_class",
				ClassName: change.ClassName,
				Reason:    fmt.Sprintf("failed to parse class definition: %v", err),
			})
			continue
		}

		if err := client.CreateClass(ctx, &class); err != nil {
			*warnings = append(*warnings, SchemaRevertWarning{
				Operation: "recreate_class",
				ClassName: change.ClassName,
				Reason:    fmt.Sprintf("failed to create class: %v", err),
			})
		}
	}

	// Re-add deleted properties
	for _, change := range diff.PropertiesDeleted {
		propData := change.PreviousValue
		if propData == nil {
			continue
		}

		propJSON, _ := json.Marshal(propData)
		var prop models.WeaviateProperty
		if err := json.Unmarshal(propJSON, &prop); err != nil {
			*warnings = append(*warnings, SchemaRevertWarning{
				Operation:    "add_property",
				ClassName:    change.ClassName,
				PropertyName: change.PropertyName,
				Reason:       fmt.Sprintf("failed to parse property definition: %v", err),
			})
			continue
		}

		if err := client.AddProperty(ctx, change.ClassName, &prop); err != nil {
			*warnings = append(*warnings, SchemaRevertWarning{
				Operation:    "add_property",
				ClassName:    change.ClassName,
				PropertyName: change.PropertyName,
				Reason:       fmt.Sprintf("failed to add property: %v", err),
			})
		}
	}
}

// revertSchemaAfterData deletes added classes after data is removed
func revertSchemaAfterData(ctx context.Context, client weaviate.ClientInterface, diff *SchemaDiffResult, warnings *[]SchemaRevertWarning) {
	// Delete added classes (should now be empty)
	for _, change := range diff.ClassesAdded {
		if err := client.DeleteClass(ctx, change.ClassName); err != nil {
			*warnings = append(*warnings, SchemaRevertWarning{
				Operation: "delete_class",
				ClassName: change.ClassName,
				Reason:    fmt.Sprintf("failed to delete class (may have data): %v", err),
			})
		}
	}

	// Note: Cannot remove properties from Weaviate
	for _, change := range diff.PropertiesAdded {
		*warnings = append(*warnings, SchemaRevertWarning{
			Operation:    "remove_property",
			ClassName:    change.ClassName,
			PropertyName: change.PropertyName,
			Reason:       "Weaviate does not support removing properties",
		})
	}

	// Note: Cannot revert vectorizer changes
	for _, change := range diff.VectorizersChanged {
		*warnings = append(*warnings, SchemaRevertWarning{
			Operation: "change_vectorizer",
			ClassName: change.ClassName,
			Reason:    "Weaviate does not support changing vectorizers (requires class recreation)",
		})
	}

	// Note: Cannot revert property modifications
	for _, change := range diff.PropertiesModified {
		*warnings = append(*warnings, SchemaRevertWarning{
			Operation:    "modify_property",
			ClassName:    change.ClassName,
			PropertyName: change.PropertyName,
			Reason:       "Weaviate does not support modifying property types",
		})
	}
}

// applyReverseOperations applies the reverse of each operation to undo changes
func applyReverseOperations(ctx context.Context, st *store.Store, client weaviate.ClientInterface, operations []*models.Operation) error {
	now := time.Now()

	// Process in reverse order
	for i := len(operations) - 1; i >= 0; i-- {
		op := operations[i]

		switch op.Type {
		case models.OperationInsert:
			// Reverse of insert is delete
			if err := client.DeleteObject(ctx, op.ClassName, op.ObjectID); err != nil {
				return fmt.Errorf("failed to delete object %s/%s: %w", op.ClassName, op.ObjectID, err)
			}
			// Record the reverse operation
			reverseOp := &models.Operation{
				Timestamp:          now,
				Type:               models.OperationDelete,
				ClassName:          op.ClassName,
				ObjectID:           op.ObjectID,
				PreviousData:       op.ObjectData,
				PreviousVectorHash: op.VectorHash, // The inserted vector becomes previous
			}
			if err := st.RecordOperation(reverseOp); err != nil {
				return err
			}

		case models.OperationDelete:
			// Reverse of delete is insert (using previous data)
			var obj models.WeaviateObject
			if err := json.Unmarshal(op.PreviousData, &obj); err != nil {
				return fmt.Errorf("failed to unmarshal previous data: %w", err)
			}

			// Restore exact vector from blob store if available
			if op.PreviousVectorHash != "" {
				vectorBytes, dims, err := st.GetVectorBlob(op.PreviousVectorHash)
				if err == nil && len(vectorBytes) > 0 {
					exactVector, err := store.BytesToVector(vectorBytes, dims)
					if err == nil {
						obj.Vector = exactVector
					}
				}
			}

			if err := client.CreateObject(ctx, &obj); err != nil {
				return fmt.Errorf("failed to recreate object %s/%s: %w", op.ClassName, op.ObjectID, err)
			}
			// Record the reverse operation
			reverseOp := &models.Operation{
				Timestamp:  now,
				Type:       models.OperationInsert,
				ClassName:  op.ClassName,
				ObjectID:   op.ObjectID,
				ObjectData: op.PreviousData,
				VectorHash: op.PreviousVectorHash, // Restore the previous vector hash
			}
			if err := st.RecordOperation(reverseOp); err != nil {
				return err
			}

		case models.OperationUpdate:
			// Reverse of update is update back to previous data
			var obj models.WeaviateObject
			if err := json.Unmarshal(op.PreviousData, &obj); err != nil {
				return fmt.Errorf("failed to unmarshal previous data: %w", err)
			}

			// Restore exact vector from blob store if available
			if op.PreviousVectorHash != "" {
				vectorBytes, dims, err := st.GetVectorBlob(op.PreviousVectorHash)
				if err == nil && len(vectorBytes) > 0 {
					exactVector, err := store.BytesToVector(vectorBytes, dims)
					if err == nil {
						obj.Vector = exactVector
					}
				}
			}

			if err := client.UpdateObject(ctx, &obj); err != nil {
				return fmt.Errorf("failed to restore object %s/%s: %w", op.ClassName, op.ObjectID, err)
			}
			// Record the reverse operation
			reverseOp := &models.Operation{
				Timestamp:          now,
				Type:               models.OperationUpdate,
				ClassName:          op.ClassName,
				ObjectID:           op.ObjectID,
				ObjectData:         op.PreviousData,
				PreviousData:       op.ObjectData,
				VectorHash:         op.PreviousVectorHash, // Previous becomes current
				PreviousVectorHash: op.VectorHash,         // Current becomes previous
			}
			if err := st.RecordOperation(reverseOp); err != nil {
				return err
			}
		}
	}

	// Mark original operations as reverted — all ops share the same commit ID
	if len(operations) > 0 {
		commitID := operations[0].CommitID
		seqs := make([]int, len(operations))
		for i, op := range operations {
			seqs[i] = op.Seq
		}
		return st.MarkOperationsReverted(commitID, seqs)
	}
	return nil
}
