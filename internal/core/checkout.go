package core

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
)

// CheckoutOptions configures checkout behavior
type CheckoutOptions struct {
	Force         bool   // Force checkout even with uncommitted changes
	CreateBranch  bool   // Create new branch (for -b flag)
	NewBranchName string // Name for new branch
}

// CheckoutResult contains the result of a checkout operation
type CheckoutResult struct {
	PreviousCommit string
	TargetCommit   string
	BranchName     string // Empty if detached
	IsDetached     bool
	Warnings       []CheckoutWarning
	ObjectsAdded   int
	ObjectsRemoved int
	ObjectsUpdated int
}

// CheckoutWarning represents a non-fatal issue during checkout
type CheckoutWarning struct {
	Type    string
	Message string
}

// Checkout switches to a branch or commit
func Checkout(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, target string, opts CheckoutOptions) (*CheckoutResult, error) {
	result := &CheckoutResult{Warnings: []CheckoutWarning{}}

	// Step 1: Check for uncommitted changes (unless --force)
	if !opts.Force {
		hasChanges, err := HasUncommittedChanges(ctx, cfg, st, client)
		if err != nil {
			return nil, fmt.Errorf("failed to check for changes: %w", err)
		}
		if hasChanges {
			return nil, fmt.Errorf("you have uncommitted changes; commit them or use --force to discard")
		}
	}

	// Step 2: Resolve target to commit ID and determine if branch
	targetCommitID, branchName, err := resolveCheckoutTarget(st, target, opts)
	if err != nil {
		return nil, err
	}

	// Validate target commit exists
	if targetCommitID == "" {
		return nil, fmt.Errorf("cannot checkout: no commits yet")
	}

	// Step 3: Handle -b flag (create new branch)
	if opts.CreateBranch {
		if opts.NewBranchName == "" {
			return nil, fmt.Errorf("branch name required with -b")
		}
		exists, err := st.BranchExists(opts.NewBranchName)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("branch '%s' already exists", opts.NewBranchName)
		}
		branchName = opts.NewBranchName
	}

	// Step 4: Get current HEAD for result
	currentHead, _ := st.GetHEAD()
	result.PreviousCommit = currentHead
	result.TargetCommit = targetCommitID
	result.BranchName = branchName
	result.IsDetached = branchName == ""

	// Step 5: If same commit and not forcing, just switch branch pointer
	// If forcing, we still need to restore state to discard any uncommitted changes
	if targetCommitID == currentHead && !opts.Force {
		return finishCheckout(st, targetCommitID, branchName, opts.CreateBranch, result)
	}

	// Step 6: Restore Weaviate state to target commit
	warnings, stats, err := restoreStateToCommit(ctx, cfg, st, client, targetCommitID)
	if err != nil {
		return nil, fmt.Errorf("failed to restore state: %w", err)
	}
	result.Warnings = append(result.Warnings, warnings...)
	result.ObjectsAdded = stats.Added
	result.ObjectsRemoved = stats.Removed
	result.ObjectsUpdated = stats.Updated

	// Step 7: Update HEAD and branch pointers
	return finishCheckout(st, targetCommitID, branchName, opts.CreateBranch, result)
}

// resolveCheckoutTarget resolves a target to (commitID, branchName)
// branchName is empty if target is a commit (detached HEAD)
func resolveCheckoutTarget(st *store.Store, target string, opts CheckoutOptions) (string, string, error) {
	// If creating a branch with -b, the target should be current HEAD
	if opts.CreateBranch {
		head, err := st.GetHEAD()
		if err != nil {
			return "", "", err
		}
		return head, "", nil
	}

	// Try as branch name first
	branch, err := st.GetBranch(target)
	if err != nil {
		return "", "", err
	}
	if branch != nil {
		return branch.CommitID, branch.Name, nil
	}

	// Try as full commit ID
	commit, err := st.GetCommit(target)
	if err == nil && commit != nil {
		return commit.ID, "", nil // Detached HEAD
	}

	// Try as short commit ID
	commit, err = st.GetCommitByShortID(target)
	if err != nil {
		return "", "", fmt.Errorf("'%s' is not a branch or commit", target)
	}
	return commit.ID, "", nil // Detached HEAD
}

// HasUncommittedChanges checks if there are any uncommitted changes
func HasUncommittedChanges(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface) (bool, error) {
	// Check staging area
	stagedCount, err := st.GetStagedChangesCount()
	if err != nil {
		return false, err
	}
	if stagedCount > 0 {
		return true, nil
	}

	// Check for unstaged changes
	diff, err := ComputeIncrementalDiff(ctx, cfg, st, client)
	if err != nil {
		return false, err
	}
	if diff.TotalUnstagedChanges() > 0 {
		return true, nil
	}

	// Check schema changes
	schemaDiff, err := ComputeSchemaDiff(ctx, st, client)
	if err != nil {
		return false, err
	}
	return schemaDiff.HasChanges(), nil
}

// StateRestoreStats tracks objects modified during restore
type StateRestoreStats struct {
	Added   int
	Removed int
	Updated int
}

// restoreStateToCommit transforms Weaviate to match the target commit's state
func restoreStateToCommit(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, targetCommitID string) ([]CheckoutWarning, *StateRestoreStats, error) {
	warnings := []CheckoutWarning{}
	stats := &StateRestoreStats{}

	// Get target state: rebuild what objects should exist at targetCommitID
	targetObjects, err := reconstructStateAtCommit(st, targetCommitID)
	if err != nil {
		return warnings, stats, err
	}

	// Get current Weaviate state
	useCursor := cfg.SupportsCursorPagination()
	currentObjectsList, err := client.GetAllObjectsAllClasses(ctx, useCursor)
	if err != nil {
		return warnings, stats, err
	}

	// Convert to map for easier comparison
	currentObjects := make(map[string]*models.WeaviateObject)
	for key, obj := range currentObjectsList {
		currentObjects[key] = obj
	}

	// Handle schema first (before data operations)
	schemaWarnings, err := restoreSchemaToCommit(ctx, st, client, targetCommitID)
	if err != nil {
		// Non-fatal - continue with data restoration
		warnings = append(warnings, CheckoutWarning{
			Type:    "schema",
			Message: fmt.Sprintf("schema restore had issues: %v", err),
		})
	}
	warnings = append(warnings, schemaWarnings...)

	// Compute what needs to change
	toDelete := make(map[string]*models.WeaviateObject)
	toCreate := make(map[string]*objectWithVector)
	toUpdate := make(map[string]*objectWithVector)

	// Objects in current but not in target -> delete
	for key, obj := range currentObjects {
		if _, exists := targetObjects[key]; !exists {
			toDelete[key] = obj
		}
	}

	// Objects in target but not in current -> create
	// Objects in both but different -> update
	for key, targetObj := range targetObjects {
		currentObj, exists := currentObjects[key]
		if !exists {
			toCreate[key] = targetObj
		} else {
			// Compare hashes
			targetHash, _ := weaviate.HashObjectFull(targetObj.Object)
			currentHash, _ := weaviate.HashObjectFull(currentObj)
			if targetHash != currentHash {
				toUpdate[key] = targetObj
			}
		}
	}

	// Apply deletions first
	for _, obj := range toDelete {
		if err := client.DeleteObject(ctx, obj.Class, obj.ID); err != nil {
			warnings = append(warnings, CheckoutWarning{
				Type:    "delete_failed",
				Message: fmt.Sprintf("failed to delete %s/%s: %v", obj.Class, obj.ID, err),
			})
		} else {
			stats.Removed++
		}
	}

	// Apply creations
	for _, objWithVec := range toCreate {
		obj := objWithVec.Object
		// Restore vector from blob store if available
		restoreObjectVector(st, obj, objWithVec.VectorHash)
		if err := client.CreateObject(ctx, obj); err != nil {
			warnings = append(warnings, CheckoutWarning{
				Type:    "create_failed",
				Message: fmt.Sprintf("failed to create %s/%s: %v", obj.Class, obj.ID, err),
			})
		} else {
			stats.Added++
		}
	}

	// Apply updates
	for _, objWithVec := range toUpdate {
		obj := objWithVec.Object
		restoreObjectVector(st, obj, objWithVec.VectorHash)
		if err := client.UpdateObject(ctx, obj); err != nil {
			warnings = append(warnings, CheckoutWarning{
				Type:    "update_failed",
				Message: fmt.Sprintf("failed to update %s/%s: %v", obj.Class, obj.ID, err),
			})
		} else {
			stats.Updated++
		}
	}

	return warnings, stats, nil
}

// objectWithVector holds an object and its vector hash for restoration
type objectWithVector struct {
	Object     *models.WeaviateObject
	VectorHash string
}

// reconstructStateAtCommit rebuilds what objects should exist at a commit
// by walking the operation history from the beginning to the target commit
func reconstructStateAtCommit(st *store.Store, targetCommitID string) (map[string]*objectWithVector, error) {
	objects := make(map[string]*objectWithVector)

	// Get all commits from root to target (inclusive)
	commitPath, err := getCommitPath(st, targetCommitID)
	if err != nil {
		return nil, err
	}

	// Replay operations in order
	for _, commitID := range commitPath {
		ops, err := st.GetOperationsByCommit(commitID)
		if err != nil {
			return nil, err
		}

		for _, op := range ops {
			key := models.ObjectKey(op.ClassName, op.ObjectID)

			switch op.Type {
			case models.OperationInsert:
				var obj models.WeaviateObject
				if err := json.Unmarshal(op.ObjectData, &obj); err == nil {
					objects[key] = &objectWithVector{
						Object:     &obj,
						VectorHash: op.VectorHash,
					}
				}
			case models.OperationUpdate:
				var obj models.WeaviateObject
				if err := json.Unmarshal(op.ObjectData, &obj); err == nil {
					objects[key] = &objectWithVector{
						Object:     &obj,
						VectorHash: op.VectorHash,
					}
				}
			case models.OperationDelete:
				delete(objects, key)
			}
		}
	}

	return objects, nil
}

// getCommitPath returns commits from root to target in order
func getCommitPath(st *store.Store, targetCommitID string) ([]string, error) {
	var path []string
	currentID := targetCommitID

	for currentID != "" {
		path = append([]string{currentID}, path...) // Prepend
		commit, err := st.GetCommit(currentID)
		if err != nil {
			return nil, err
		}
		currentID = commit.ParentID
	}

	return path, nil
}

// restoreSchemaToCommit restores Weaviate schema to match target commit
func restoreSchemaToCommit(ctx context.Context, st *store.Store, client weaviate.ClientInterface, targetCommitID string) ([]CheckoutWarning, error) {
	warnings := []CheckoutWarning{}

	// Get target schema
	targetSchema, err := st.GetSchemaVersionByCommit(targetCommitID)
	if err != nil {
		return warnings, err
	}

	if targetSchema == nil {
		// No schema at target - this is the initial state
		return warnings, nil
	}

	var targetSchemaStruct models.WeaviateSchema
	if err := json.Unmarshal(targetSchema.SchemaJSON, &targetSchemaStruct); err != nil {
		return warnings, err
	}

	// Get current schema
	currentSchema, err := client.GetSchemaTyped(ctx)
	if err != nil {
		return warnings, err
	}

	// Compute diff: what changes needed to go from current to target
	diff := diffSchemas(currentSchema, &targetSchemaStruct)

	// Classes in current but not in target -> delete them
	for _, change := range diff.ClassesDeleted {
		// In the diff, "deleted" means it's in current but not target
		// So we need to delete it to reach target state
		if err := client.DeleteClass(ctx, change.ClassName); err != nil {
			warnings = append(warnings, CheckoutWarning{
				Type:    "schema",
				Message: fmt.Sprintf("failed to delete class %s: %v", change.ClassName, err),
			})
		}
	}

	// Classes in target but not in current -> create them
	for _, change := range diff.ClassesAdded {
		// In the diff, "added" means it's in target but not current
		// So we need to create it to reach target state
		if change.PreviousValue != nil {
			classJSON, _ := json.Marshal(change.PreviousValue)
			var class models.WeaviateClass
			if err := json.Unmarshal(classJSON, &class); err != nil {
				continue
			}
			if err := client.CreateClass(ctx, &class); err != nil {
				warnings = append(warnings, CheckoutWarning{
					Type:    "schema",
					Message: fmt.Sprintf("failed to create class %s: %v", change.ClassName, err),
				})
			}
		}
	}

	// Handle property changes (Weaviate limitations apply)
	for _, change := range diff.PropertiesAdded {
		// Property in target but not current -> add it
		if change.PreviousValue != nil {
			propJSON, _ := json.Marshal(change.PreviousValue)
			var prop models.WeaviateProperty
			if err := json.Unmarshal(propJSON, &prop); err != nil {
				continue
			}
			if err := client.AddProperty(ctx, change.ClassName, &prop); err != nil {
				warnings = append(warnings, CheckoutWarning{
					Type:    "schema",
					Message: fmt.Sprintf("failed to add property %s.%s: %v", change.ClassName, change.PropertyName, err),
				})
			}
		}
	}

	// Properties in current but not in target - Weaviate doesn't support removal
	for _, change := range diff.PropertiesDeleted {
		warnings = append(warnings, CheckoutWarning{
			Type:    "schema",
			Message: fmt.Sprintf("cannot remove property %s.%s (Weaviate limitation)", change.ClassName, change.PropertyName),
		})
	}

	return warnings, nil
}

// restoreObjectVector retrieves the exact vector from blob store and sets it on the object
func restoreObjectVector(st *store.Store, obj *models.WeaviateObject, vectorHash string) {
	if vectorHash == "" {
		return
	}

	vectorBytes, dims, err := st.GetVectorBlob(vectorHash)
	if err != nil || len(vectorBytes) == 0 {
		return
	}

	exactVector, err := store.BytesToVector(vectorBytes, dims)
	if err != nil {
		return
	}

	obj.Vector = exactVector
}

// finishCheckout updates HEAD and branch pointers
func finishCheckout(st *store.Store, commitID, branchName string, createBranch bool, result *CheckoutResult) (*CheckoutResult, error) {
	// Create branch if -b was used
	if createBranch && branchName != "" {
		if err := st.CreateBranch(branchName, commitID); err != nil {
			return nil, fmt.Errorf("failed to create branch: %w", err)
		}
	}

	// Update HEAD
	if err := st.SetHEAD(commitID); err != nil {
		return nil, err
	}

	// Update current branch
	if err := st.SetCurrentBranch(branchName); err != nil {
		return nil, err
	}

	// Rebuild known_objects to match new state
	if err := rebuildKnownObjectsFromCommit(st, commitID); err != nil {
		result.Warnings = append(result.Warnings, CheckoutWarning{
			Type:    "known_state",
			Message: fmt.Sprintf("failed to rebuild known state: %v", err),
		})
	}

	return result, nil
}

// rebuildKnownObjectsFromCommit rebuilds known_objects table from commit history
func rebuildKnownObjectsFromCommit(st *store.Store, commitID string) error {
	// Clear existing known objects
	if err := st.ClearKnownObjects(); err != nil {
		return err
	}

	// Reconstruct state
	objects, err := reconstructStateAtCommit(st, commitID)
	if err != nil {
		return err
	}

	// Save each object to known_objects
	for _, objWithVec := range objects {
		obj := objWithVec.Object
		objectHash, vectorHash := weaviate.HashObjectFull(obj)
		// Use the stored vector hash if we have it
		if objWithVec.VectorHash != "" {
			vectorHash = objWithVec.VectorHash
		}
		data, _ := json.Marshal(obj)
		if err := st.SaveKnownObjectWithVector(obj.Class, obj.ID, objectHash, vectorHash, data); err != nil {
			return err
		}
	}

	return nil
}
