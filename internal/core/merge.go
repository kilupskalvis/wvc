package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
)

// Merge merges a branch into the current branch
func Merge(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, targetBranch string, opts models.MergeOptions) (*models.MergeResult, error) {
	result := &models.MergeResult{Warnings: []string{}}

	// Step 1: Validate we're on a branch
	currentBranch, err := st.GetCurrentBranch()
	if err != nil {
		return nil, err
	}
	if currentBranch == "" {
		return nil, fmt.Errorf("cannot merge: HEAD is detached")
	}

	// Step 2: Check for uncommitted changes
	hasChanges, err := HasUncommittedChanges(ctx, cfg, st, client)
	if err != nil {
		return nil, err
	}
	if hasChanges {
		return nil, fmt.Errorf("cannot merge: you have uncommitted changes")
	}

	// Step 3: Resolve target branch
	targetCommitID, targetBranchName, err := ResolveRef(st, targetBranch)
	if err != nil {
		return nil, fmt.Errorf("branch '%s' not found", targetBranch)
	}
	if targetBranchName == currentBranch {
		return nil, fmt.Errorf("cannot merge branch '%s' into itself", currentBranch)
	}

	// Step 4: Get our HEAD
	ourHead, err := st.GetHEAD()
	if err != nil {
		return nil, err
	}

	// Step 5: Check if already up-to-date
	if ourHead == targetCommitID {
		result.Success = true
		result.Warnings = append(result.Warnings, "Already up to date.")
		return result, nil
	}

	// Step 6: Try fast-forward
	if !opts.NoFastForward {
		canFF, err := canFastForward(st, ourHead, targetCommitID)
		if err != nil {
			return nil, err
		}
		if canFF {
			return performFastForward(ctx, cfg, st, client, currentBranch, targetCommitID, result)
		}
	}

	// Step 7: Find merge base
	mergeBase, err := FindMergeBase(st, ourHead, targetCommitID)
	if err != nil {
		return nil, err
	}
	if mergeBase == "" {
		return nil, fmt.Errorf("cannot merge: no common ancestor found")
	}

	// Step 8: Perform 3-way merge
	return performThreeWayMerge(ctx, cfg, st, client, ourHead, targetCommitID, mergeBase, currentBranch, targetBranch, opts, result)
}

// FindMergeBase finds the lowest common ancestor of two commits
func FindMergeBase(st *store.Store, commitA, commitB string) (string, error) {
	// Get all ancestors of A
	ancestorsA, err := st.GetAllAncestors(commitA)
	if err != nil {
		return "", err
	}

	// BFS from B, looking for first ancestor in A's set
	queue := []string{commitB}
	visited := make(map[string]bool)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current == "" || visited[current] {
			continue
		}
		visited[current] = true

		if ancestorsA[current] {
			return current, nil
		}

		commit, err := st.GetCommit(current)
		if err != nil {
			continue
		}

		if commit.ParentID != "" {
			queue = append(queue, commit.ParentID)
		}
		if commit.MergeParentID != "" {
			queue = append(queue, commit.MergeParentID)
		}
	}

	return "", nil
}

// canFastForward checks if we can fast-forward (our HEAD is ancestor of their HEAD)
func canFastForward(st *store.Store, ourHead, theirHead string) (bool, error) {
	ancestors, err := st.GetAllAncestors(theirHead)
	if err != nil {
		return false, err
	}
	return ancestors[ourHead], nil
}

// performFastForward performs a fast-forward merge
func performFastForward(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, currentBranch, targetCommitID string, result *models.MergeResult) (*models.MergeResult, error) {
	// Use existing checkout logic to restore state
	warnings, stats, err := restoreStateToCommit(ctx, cfg, st, client, targetCommitID)
	if err != nil {
		return nil, fmt.Errorf("failed to fast-forward: %w", err)
	}
	result.Warnings = append(result.Warnings, warningsToStrings(warnings)...)

	// Update HEAD and branch pointer
	if err := st.SetHEAD(targetCommitID); err != nil {
		return nil, err
	}
	if err := st.UpdateBranch(currentBranch, targetCommitID); err != nil {
		return nil, err
	}

	// Rebuild known objects
	if err := rebuildKnownObjectsFromCommit(st, targetCommitID); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("Warning: failed to rebuild known state: %v", err))
	}

	result.Success = true
	result.FastForward = true
	result.ObjectsAdded = stats.Added
	result.ObjectsUpdated = stats.Updated
	result.ObjectsDeleted = stats.Removed

	return result, nil
}

// performThreeWayMerge performs a 3-way merge
func performThreeWayMerge(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, ourHead, theirHead, mergeBase, currentBranch, targetBranch string, opts models.MergeOptions, result *models.MergeResult) (*models.MergeResult, error) {
	// Reconstruct states at all three points
	baseState, err := reconstructStateAtCommit(st, mergeBase)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct base state: %w", err)
	}

	oursState, err := reconstructStateAtCommit(st, ourHead)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct our state: %w", err)
	}

	theirsState, err := reconstructStateAtCommit(st, theirHead)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct their state: %w", err)
	}

	// Detect conflicts
	conflicts := detectObjectConflicts(baseState, oursState, theirsState)

	// Handle conflicts based on strategy
	if len(conflicts) > 0 {
		if opts.Strategy == models.ConflictAbort || opts.Strategy == "" {
			// Abort: return conflicts without merging
			result.Success = false
			result.Conflicts = conflicts
			return result, nil
		}
	}

	// Compute merged state (non-conflicting changes)
	mergedState := computeMergedState(baseState, oursState, theirsState)

	// Resolve conflicts if using --ours or --theirs
	if len(conflicts) > 0 && (opts.Strategy == models.ConflictOurs || opts.Strategy == models.ConflictTheirs) {
		resolved := resolveConflicts(conflicts, opts.Strategy, mergedState)
		result.ResolvedConflicts = resolved
	}

	// Apply merged state to Weaviate
	stats, err := applyMergedState(ctx, st, client, oursState, mergedState)
	if err != nil {
		return nil, err
	}

	// Create merge commit
	message := opts.Message
	if message == "" {
		message = fmt.Sprintf("Merge branch '%s' into %s", targetBranch, currentBranch)
	}

	mergeCommit, err := createMergeCommit(ctx, cfg, st, client, ourHead, theirHead, message, stats)
	if err != nil {
		return nil, err
	}

	// Update branch pointer
	if err := st.UpdateBranch(currentBranch, mergeCommit.ID); err != nil {
		return nil, err
	}

	result.Success = true
	result.FastForward = false
	result.MergeCommit = mergeCommit
	result.ObjectsAdded = stats.Added
	result.ObjectsUpdated = stats.Updated
	result.ObjectsDeleted = stats.Removed

	return result, nil
}

// detectObjectConflicts detects conflicts between three states
func detectObjectConflicts(baseState, oursState, theirsState map[string]*objectWithVector) []*models.MergeConflict {
	var conflicts []*models.MergeConflict

	// Collect all unique keys
	allKeys := make(map[string]bool)
	for k := range baseState {
		allKeys[k] = true
	}
	for k := range oursState {
		allKeys[k] = true
	}
	for k := range theirsState {
		allKeys[k] = true
	}

	for key := range allKeys {
		base := baseState[key]
		ours := oursState[key]
		theirs := theirsState[key]

		baseHash := hashObjWithVec(base)
		oursHash := hashObjWithVec(ours)
		theirsHash := hashObjWithVec(theirs)

		// No conflict if unchanged in at least one branch
		if oursHash == baseHash || theirsHash == baseHash {
			continue
		}

		// If both changed to the same value, no conflict
		if oursHash == theirsHash {
			continue
		}

		// Conflict detected
		conflict := &models.MergeConflict{Key: key}

		// Parse key to get ClassName/ObjectID
		parts := strings.SplitN(key, "/", 2)
		if len(parts) == 2 {
			conflict.ClassName = parts[0]
			conflict.ObjectID = parts[1]
		}

		// Classify conflict type
		if base == nil {
			conflict.Type = models.ConflictAddAdd
		} else if ours == nil {
			conflict.Type = models.ConflictDeleteModify
		} else if theirs == nil {
			conflict.Type = models.ConflictModifyDelete
		} else {
			conflict.Type = models.ConflictModifyModify
		}

		// Set objects
		if base != nil {
			conflict.Base = base.Object
		}
		if ours != nil {
			conflict.Ours = ours.Object
		}
		if theirs != nil {
			conflict.Theirs = theirs.Object
		}

		conflicts = append(conflicts, conflict)
	}

	return conflicts
}

// computeMergedState computes the merged state from three states (excluding conflicts)
func computeMergedState(baseState, oursState, theirsState map[string]*objectWithVector) map[string]*objectWithVector {
	merged := make(map[string]*objectWithVector)

	// Copy our state as starting point
	for k, v := range oursState {
		merged[k] = v
	}

	// Collect all unique keys
	allKeys := make(map[string]bool)
	for k := range baseState {
		allKeys[k] = true
	}
	for k := range theirsState {
		allKeys[k] = true
	}

	for key := range allKeys {
		base := baseState[key]
		theirs := theirsState[key]
		ours := oursState[key]

		baseHash := hashObjWithVec(base)
		theirsHash := hashObjWithVec(theirs)
		oursHash := hashObjWithVec(ours)

		// If they changed and we didn't, take theirs
		if oursHash == baseHash && theirsHash != baseHash {
			if theirs != nil {
				merged[key] = theirs
			} else {
				delete(merged, key)
			}
		}
		// If we changed, keep ours (already in merged)
		// If both changed the same way, keep either (already have ours)
		// Conflicts are handled separately
	}

	return merged
}

// resolveConflicts resolves conflicts using the specified strategy
func resolveConflicts(conflicts []*models.MergeConflict, strategy models.ConflictStrategy, merged map[string]*objectWithVector) int {
	resolved := 0
	for _, c := range conflicts {
		switch strategy {
		case models.ConflictOurs:
			if c.Ours != nil {
				merged[c.Key] = &objectWithVector{Object: c.Ours}
			} else {
				delete(merged, c.Key) // We deleted it
			}
			resolved++
		case models.ConflictTheirs:
			if c.Theirs != nil {
				merged[c.Key] = &objectWithVector{Object: c.Theirs}
			} else {
				delete(merged, c.Key) // They deleted it
			}
			resolved++
		}
	}
	return resolved
}

// applyMergedState applies the merged state to Weaviate
func applyMergedState(ctx context.Context, st *store.Store, client weaviate.ClientInterface, currentState, mergedState map[string]*objectWithVector) (*StateRestoreStats, error) {
	stats := &StateRestoreStats{}
	now := time.Now()

	// Compute what needs to change
	toDelete := make(map[string]*objectWithVector)
	toCreate := make(map[string]*objectWithVector)
	toUpdate := make(map[string]*objectWithVector)

	// Objects in current but not in merged -> delete
	for key, obj := range currentState {
		if _, exists := mergedState[key]; !exists {
			toDelete[key] = obj
		}
	}

	// Objects in merged but not in current -> create
	// Objects in both but different -> update
	for key, mergedObj := range mergedState {
		currentObj, exists := currentState[key]
		if !exists {
			toCreate[key] = mergedObj
		} else {
			currentHash := hashObjWithVec(currentObj)
			mergedHash := hashObjWithVec(mergedObj)
			if currentHash != mergedHash {
				toUpdate[key] = mergedObj
			}
		}
	}

	// Apply deletions
	for key, objWithVec := range toDelete {
		obj := objWithVec.Object
		if err := client.DeleteObject(ctx, obj.Class, obj.ID); err != nil {
			return stats, fmt.Errorf("failed to delete %s: %w", key, err)
		}
		// Record operation
		data, _ := json.Marshal(obj)
		op := &models.Operation{
			Timestamp:    now,
			Type:         models.OperationDelete,
			ClassName:    obj.Class,
			ObjectID:     obj.ID,
			PreviousData: data,
		}
		if err := st.RecordOperation(op); err != nil {
			return stats, err
		}
		stats.Removed++
	}

	// Apply creations
	for key, objWithVec := range toCreate {
		obj := objWithVec.Object
		restoreObjectVector(st, obj, objWithVec.VectorHash)
		if err := client.CreateObject(ctx, obj); err != nil {
			return stats, fmt.Errorf("failed to create %s: %w", key, err)
		}
		// Record operation
		data, _ := json.Marshal(obj)
		op := &models.Operation{
			Timestamp:  now,
			Type:       models.OperationInsert,
			ClassName:  obj.Class,
			ObjectID:   obj.ID,
			ObjectData: data,
			VectorHash: objWithVec.VectorHash,
		}
		if err := st.RecordOperation(op); err != nil {
			return stats, err
		}
		stats.Added++
	}

	// Apply updates
	for key, objWithVec := range toUpdate {
		obj := objWithVec.Object
		restoreObjectVector(st, obj, objWithVec.VectorHash)
		if err := client.UpdateObject(ctx, obj); err != nil {
			return stats, fmt.Errorf("failed to update %s: %w", key, err)
		}
		// Record operation
		currentObj := currentState[key]
		prevData, _ := json.Marshal(currentObj.Object)
		newData, _ := json.Marshal(obj)
		op := &models.Operation{
			Timestamp:          now,
			Type:               models.OperationUpdate,
			ClassName:          obj.Class,
			ObjectID:           obj.ID,
			ObjectData:         newData,
			PreviousData:       prevData,
			VectorHash:         objWithVec.VectorHash,
			PreviousVectorHash: currentObj.VectorHash,
		}
		if err := st.RecordOperation(op); err != nil {
			return stats, err
		}
		stats.Updated++
	}

	return stats, nil
}

// createMergeCommit creates a merge commit with two parents
func createMergeCommit(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, parent1, parent2, message string, stats *StateRestoreStats) (*models.Commit, error) {
	now := time.Now()

	// Get uncommitted operations for content-addressable commit ID
	uncommittedOps, err := st.GetUncommittedOperations()
	if err != nil {
		return nil, err
	}

	// Generate commit ID — for merges, include both parents in the hash
	commitID := generateMergeCommitID(message, now, parent1, parent2, uncommittedOps)

	// Capture schema snapshot
	if err := captureSchemaSnapshot(ctx, st, client, commitID); err != nil {
		// Non-fatal
	}

	commit := &models.Commit{
		ID:             commitID,
		ParentID:       parent1,
		MergeParentID:  parent2,
		Message:        message,
		Timestamp:      now,
		OperationCount: stats.Added + stats.Updated + stats.Removed,
	}

	// Mark operations as committed
	if _, err := st.MarkOperationsCommitted(commitID); err != nil {
		return nil, err
	}

	// Save commit
	if err := st.CreateCommit(commit); err != nil {
		return nil, err
	}

	// Update HEAD
	if err := st.SetHEAD(commitID); err != nil {
		return nil, err
	}

	// Rebuild known objects
	useCursor := cfg.SupportsCursorPagination()
	if err := UpdateKnownState(ctx, st, client, useCursor); err != nil {
		// Non-fatal
	}

	return commit, nil
}

// hashObjWithVec returns a hash for an objectWithVector (or empty string if nil)
func hashObjWithVec(obj *objectWithVector) string {
	if obj == nil || obj.Object == nil {
		return ""
	}
	hash, _ := weaviate.HashObjectFull(obj.Object)
	return hash
}

// generateMergeCommitID generates a content-addressable commit ID for merge commits.
// Includes both parent IDs and the operations Merkle hash.
func generateMergeCommitID(message string, timestamp time.Time, parent1, parent2 string, operations []*models.Operation) string {
	opsHash := computeOperationsHash(operations)
	data := fmt.Sprintf("%s|%s|%s|%s|%s", message, timestamp.Format(time.RFC3339Nano), parent1, parent2, opsHash)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// warningsToStrings converts CheckoutWarnings to strings
func warningsToStrings(warnings []CheckoutWarning) []string {
	result := make([]string, len(warnings))
	for i, w := range warnings {
		result[i] = w.Message
	}
	return result
}
