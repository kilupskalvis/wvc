package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
)

// StashPushOptions configures stash push behavior
type StashPushOptions struct {
	Message string
}

// StashPushResult contains the result of a stash push
type StashPushResult struct {
	StashIndex    int
	Message       string
	StagedCount   int
	UnstagedCount int
	TotalCount    int
	Warnings      []CheckoutWarning
}

// StashApplyOptions configures apply behavior
type StashApplyOptions struct {
	Index   int  // stash@{N}, default 0
	Restage bool // re-stage previously-staged changes (--index flag)
}

// StashApplyResult contains the result of a stash apply
type StashApplyResult struct {
	Message       string
	StagedCount   int
	UnstagedCount int
	Warnings      []CheckoutWarning
}

// StashListEntry is a display-oriented stash entry
type StashListEntry struct {
	Index      int
	Message    string
	BranchName string
	CommitID   string
	CreatedAt  time.Time
}

// StashShowResult contains the summary of changes in a stash
type StashShowResult struct {
	Message         string
	BranchName      string
	CommitID        string
	StagedChanges   []*models.StashChange
	UnstagedChanges []*models.StashChange
}

// ParseStashRef parses a stash reference like "stash@{N}" or "N" to an integer index.
// Returns 0 for empty string.
func ParseStashRef(ref string) (int, error) {
	if ref == "" {
		return 0, nil
	}

	// Handle stash@{N} format
	if strings.HasPrefix(ref, "stash@{") && strings.HasSuffix(ref, "}") {
		numStr := ref[7 : len(ref)-1]
		n, err := strconv.Atoi(numStr)
		if err != nil {
			return 0, fmt.Errorf("invalid stash reference: %s", ref)
		}
		if n < 0 {
			return 0, fmt.Errorf("invalid stash index: %d", n)
		}
		return n, nil
	}

	// Handle bare integer
	n, err := strconv.Atoi(ref)
	if err != nil {
		return 0, fmt.Errorf("invalid stash reference: %s", ref)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid stash index: %d", n)
	}
	return n, nil
}

// StashPush saves all uncommitted changes and restores Weaviate to the last committed state
func StashPush(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, opts StashPushOptions) (*StashPushResult, error) {
	result := &StashPushResult{
		Warnings: []CheckoutWarning{},
	}

	// Get current HEAD and branch
	headCommitID, err := st.GetHEAD()
	if err != nil {
		return nil, fmt.Errorf("failed to get HEAD: %w", err)
	}
	if headCommitID == "" {
		return nil, fmt.Errorf("no commits yet; cannot stash")
	}

	branchName, _ := st.GetCurrentBranch()

	// Get staged changes
	stagedChanges, err := st.GetAllStagedChanges()
	if err != nil {
		return nil, fmt.Errorf("failed to get staged changes: %w", err)
	}

	// Get unstaged changes
	diff, err := ComputeIncrementalDiff(ctx, cfg, st, client)
	if err != nil {
		return nil, fmt.Errorf("failed to compute diff: %w", err)
	}

	unstagedCount := diff.TotalUnstagedChanges()
	if len(stagedChanges) == 0 && unstagedCount == 0 {
		return nil, fmt.Errorf("no local changes to save")
	}

	// Build message
	message := opts.Message
	if message == "" {
		commit, err := st.GetCommit(headCommitID)
		if err != nil || commit == nil {
			message = fmt.Sprintf("WIP on %s", displayBranch(branchName))
		} else {
			message = fmt.Sprintf("WIP on %s: %s %s", displayBranch(branchName), commit.ShortID(), commit.Message)
		}
	}

	// Create stash entry
	stashID, err := st.CreateStash(message, branchName, headCommitID)
	if err != nil {
		return nil, fmt.Errorf("failed to create stash: %w", err)
	}

	// Save staged changes
	for _, sc := range stagedChanges {
		change := &models.StashChange{
			StashID:            stashID,
			ClassName:          sc.ClassName,
			ObjectID:           sc.ObjectID,
			ChangeType:         sc.ChangeType,
			ObjectData:         sc.ObjectData,
			PreviousData:       sc.PreviousData,
			WasStaged:          true,
			VectorHash:         sc.VectorHash,
			PreviousVectorHash: sc.PreviousVectorHash,
		}
		if err := st.CreateStashChange(change); err != nil {
			return nil, fmt.Errorf("failed to save staged change: %w", err)
		}
	}
	result.StagedCount = len(stagedChanges)

	// Save unstaged changes
	for _, oc := range diff.Unstaged.Inserted {
		if err := saveUnstagedStashChange(st, stashID, oc, "insert"); err != nil {
			return nil, err
		}
	}
	for _, oc := range diff.Unstaged.Updated {
		if err := saveUnstagedStashChange(st, stashID, oc, "update"); err != nil {
			return nil, err
		}
	}
	for _, oc := range diff.Unstaged.Deleted {
		if err := saveUnstagedStashChange(st, stashID, oc, "delete"); err != nil {
			return nil, err
		}
	}
	result.UnstagedCount = unstagedCount
	result.TotalCount = result.StagedCount + result.UnstagedCount
	result.Message = message

	// Get stash index (it's the newest, so index 0)
	result.StashIndex = 0

	// Clear staging area
	if err := st.ClearStagedChanges(); err != nil {
		return nil, fmt.Errorf("failed to clear staging: %w", err)
	}

	// Restore Weaviate to HEAD commit state
	warnings, _, err := restoreStateToCommit(ctx, cfg, st, client, headCommitID)
	if err != nil {
		return nil, fmt.Errorf("failed to restore state: %w", err)
	}
	result.Warnings = append(result.Warnings, warnings...)

	// Rebuild known objects
	if err := rebuildKnownObjectsFromCommit(st, headCommitID); err != nil {
		result.Warnings = append(result.Warnings, CheckoutWarning{
			Type:    "known_state",
			Message: fmt.Sprintf("failed to rebuild known state: %v", err),
		})
	}

	return result, nil
}

// StashApply applies a stash without removing it
func StashApply(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, opts StashApplyOptions) (*StashApplyResult, error) {
	result := &StashApplyResult{
		Warnings: []CheckoutWarning{},
	}

	stash, err := st.GetStashByIndex(opts.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to get stash: %w", err)
	}
	if stash == nil {
		return nil, fmt.Errorf("no stash found at index %d", opts.Index)
	}

	result.Message = stash.Message

	changes, err := st.GetStashChanges(stash.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get stash changes: %w", err)
	}

	// Apply each change to Weaviate
	for _, sc := range changes {
		warning := applyStashChange(ctx, st, client, sc)
		if warning != nil {
			result.Warnings = append(result.Warnings, *warning)
			continue
		}

		if sc.WasStaged {
			result.StagedCount++
		} else {
			result.UnstagedCount++
		}
	}

	// Re-stage previously staged changes if requested
	if opts.Restage {
		for _, sc := range changes {
			if !sc.WasStaged {
				continue
			}
			staged := &store.StagedChange{
				ClassName:          sc.ClassName,
				ObjectID:           sc.ObjectID,
				ChangeType:         sc.ChangeType,
				ObjectData:         sc.ObjectData,
				PreviousData:       sc.PreviousData,
				StagedAt:           time.Now(),
				VectorHash:         sc.VectorHash,
				PreviousVectorHash: sc.PreviousVectorHash,
			}
			if err := st.AddStagedChange(staged); err != nil {
				result.Warnings = append(result.Warnings, CheckoutWarning{
					Type:    "restage",
					Message: fmt.Sprintf("failed to re-stage %s/%s: %v", sc.ClassName, sc.ObjectID, err),
				})
			}
		}
	}

	return result, nil
}

// StashPop applies a stash and then removes it
func StashPop(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, opts StashApplyOptions) (*StashApplyResult, error) {
	// Get stash ID before apply (so we can delete it after)
	stash, err := st.GetStashByIndex(opts.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to get stash: %w", err)
	}
	if stash == nil {
		return nil, fmt.Errorf("no stash found at index %d", opts.Index)
	}
	stashID := stash.ID

	result, err := StashApply(ctx, cfg, st, client, opts)
	if err != nil {
		return nil, err
	}

	if err := st.DeleteStash(stashID); err != nil {
		result.Warnings = append(result.Warnings, CheckoutWarning{
			Type:    "drop",
			Message: fmt.Sprintf("applied stash but failed to drop: %v", err),
		})
	}

	return result, nil
}

// StashDrop removes a stash by index, returning its message
func StashDrop(st *store.Store, index int) (string, error) {
	stash, err := st.GetStashByIndex(index)
	if err != nil {
		return "", fmt.Errorf("failed to get stash: %w", err)
	}
	if stash == nil {
		return "", fmt.Errorf("no stash found at index %d", index)
	}

	message := stash.Message
	if err := st.DeleteStash(stash.ID); err != nil {
		return "", fmt.Errorf("failed to drop stash: %w", err)
	}

	return message, nil
}

// StashList returns all stashes with display indices
func StashList(st *store.Store) ([]StashListEntry, error) {
	stashes, err := st.ListStashes()
	if err != nil {
		return nil, err
	}

	entries := make([]StashListEntry, len(stashes))
	for i, s := range stashes {
		entries[i] = StashListEntry{
			Index:      i,
			Message:    s.Message,
			BranchName: s.BranchName,
			CommitID:   s.CommitID,
			CreatedAt:  s.CreatedAt,
		}
	}
	return entries, nil
}

// StashShow returns a detailed view of a stash's changes
func StashShow(st *store.Store, index int) (*StashShowResult, error) {
	stash, err := st.GetStashByIndex(index)
	if err != nil {
		return nil, fmt.Errorf("failed to get stash: %w", err)
	}
	if stash == nil {
		return nil, fmt.Errorf("no stash found at index %d", index)
	}

	changes, err := st.GetStashChanges(stash.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get stash changes: %w", err)
	}

	result := &StashShowResult{
		Message:         stash.Message,
		BranchName:      stash.BranchName,
		CommitID:        stash.CommitID,
		StagedChanges:   make([]*models.StashChange, 0),
		UnstagedChanges: make([]*models.StashChange, 0),
	}

	for _, c := range changes {
		if c.WasStaged {
			result.StagedChanges = append(result.StagedChanges, c)
		} else {
			result.UnstagedChanges = append(result.UnstagedChanges, c)
		}
	}

	return result, nil
}

// StashClear removes all stashes and returns the count removed
func StashClear(st *store.Store) (int, error) {
	count, err := st.GetStashCount()
	if err != nil {
		return 0, err
	}

	if count == 0 {
		return 0, nil
	}

	if err := st.DeleteAllStashes(); err != nil {
		return 0, fmt.Errorf("failed to clear stashes: %w", err)
	}

	return count, nil
}

// saveUnstagedStashChange converts an ObjectChange to a StashChange and saves it
func saveUnstagedStashChange(st *store.Store, stashID int64, oc *ObjectChange, changeType string) error {
	var objectData, previousData []byte
	if oc.CurrentData != nil {
		objectData, _ = json.Marshal(oc.CurrentData)
	}
	if oc.PreviousData != nil {
		previousData, _ = json.Marshal(oc.PreviousData)
	}

	change := &models.StashChange{
		StashID:            stashID,
		ClassName:          oc.ClassName,
		ObjectID:           oc.ObjectID,
		ChangeType:         changeType,
		ObjectData:         objectData,
		PreviousData:       previousData,
		WasStaged:          false,
		VectorHash:         oc.VectorHash,
		PreviousVectorHash: oc.PreviousVectorHash,
	}
	if err := st.CreateStashChange(change); err != nil {
		return fmt.Errorf("failed to save unstaged change: %w", err)
	}
	return nil
}

// applyStashChange applies a single stash change to Weaviate
func applyStashChange(ctx context.Context, st *store.Store, client weaviate.ClientInterface, sc *models.StashChange) *CheckoutWarning {
	switch sc.ChangeType {
	case "insert":
		var obj models.WeaviateObject
		if err := json.Unmarshal(sc.ObjectData, &obj); err != nil {
			return &CheckoutWarning{Type: "apply", Message: fmt.Sprintf("failed to unmarshal %s/%s: %v", sc.ClassName, sc.ObjectID, err)}
		}
		restoreObjectVector(st, &obj, sc.VectorHash)
		if err := client.CreateObject(ctx, &obj); err != nil {
			return &CheckoutWarning{Type: "apply", Message: fmt.Sprintf("failed to create %s/%s: %v", sc.ClassName, sc.ObjectID, err)}
		}
	case "update":
		var obj models.WeaviateObject
		if err := json.Unmarshal(sc.ObjectData, &obj); err != nil {
			return &CheckoutWarning{Type: "apply", Message: fmt.Sprintf("failed to unmarshal %s/%s: %v", sc.ClassName, sc.ObjectID, err)}
		}
		restoreObjectVector(st, &obj, sc.VectorHash)
		if err := client.UpdateObject(ctx, &obj); err != nil {
			return &CheckoutWarning{Type: "apply", Message: fmt.Sprintf("failed to update %s/%s: %v", sc.ClassName, sc.ObjectID, err)}
		}
	case "delete":
		if err := client.DeleteObject(ctx, sc.ClassName, sc.ObjectID); err != nil {
			return &CheckoutWarning{Type: "apply", Message: fmt.Sprintf("failed to delete %s/%s: %v", sc.ClassName, sc.ObjectID, err)}
		}
	}
	return nil
}

// displayBranch returns a display string for the branch (handles detached HEAD)
func displayBranch(branchName string) string {
	if branchName == "" {
		return "(detached)"
	}
	return branchName
}
