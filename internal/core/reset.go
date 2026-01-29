package core

import (
	"context"
	"fmt"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
)

// ResetMode defines how reset affects the system
type ResetMode int

const (
	// ResetModeSoft moves HEAD and auto-stages changes from undone commits (like git reset --soft)
	ResetModeSoft ResetMode = iota
	// ResetModeMixed moves HEAD and clears staging, preserves Weaviate state
	ResetModeMixed
	// ResetModeHard moves HEAD, clears staging, and restores Weaviate state
	ResetModeHard
)

// String returns a human-readable name for the reset mode
func (m ResetMode) String() string {
	switch m {
	case ResetModeSoft:
		return "soft"
	case ResetModeMixed:
		return "mixed"
	case ResetModeHard:
		return "hard"
	default:
		return "unknown"
	}
}

// ResetOptions configures reset behavior
type ResetOptions struct {
	Mode ResetMode
}

// ResetResult contains the result of a reset operation
type ResetResult struct {
	PreviousCommit string
	TargetCommit   string
	BranchName     string // Branch that was moved (empty if detached)
	Mode           ResetMode
	StagedCleared  int // Number of staged changes cleared (for mixed/hard)
	ChangesStaged  int // Number of changes auto-staged (for soft reset)
	// Only populated for hard reset
	ObjectsAdded   int
	ObjectsRemoved int
	ObjectsUpdated int
	Warnings       []CheckoutWarning
}

// ResetToCommit resets HEAD (and current branch) to the target commit
func ResetToCommit(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, target string, opts ResetOptions) (*ResetResult, error) {
	result := &ResetResult{
		Mode:     opts.Mode,
		Warnings: []CheckoutWarning{},
	}

	// Step 1: Resolve target to commit ID
	targetCommitID, _, err := ResolveRef(st, target)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve '%s': %w", target, err)
	}

	// Step 2: Validate target commit exists
	commit, err := st.GetCommit(targetCommitID)
	if err != nil {
		return nil, fmt.Errorf("failed to get commit: %w", err)
	}
	if commit == nil {
		return nil, fmt.Errorf("commit '%s' not found", target)
	}

	// Step 3: Get current state
	previousCommit, _ := st.GetHEAD()
	currentBranch, _ := st.GetCurrentBranch()

	result.PreviousCommit = previousCommit
	result.TargetCommit = targetCommitID
	result.BranchName = currentBranch

	// Step 4: Get staged count before clearing (for reporting)
	stagedCount, _ := st.GetStagedChangesCount()

	// Step 5: Apply reset based on mode
	switch opts.Mode {
	case ResetModeSoft:
		// Only move HEAD and branch pointer
		if err := moveHeadAndBranch(st, targetCommitID, currentBranch); err != nil {
			return nil, err
		}

	case ResetModeMixed:
		// Move HEAD, branch pointer, and clear staging
		if err := moveHeadAndBranch(st, targetCommitID, currentBranch); err != nil {
			return nil, err
		}
		if err := st.ClearStagedChanges(); err != nil {
			return nil, fmt.Errorf("failed to clear staging area: %w", err)
		}
		result.StagedCleared = stagedCount

	case ResetModeHard:
		// Move HEAD, branch pointer, clear staging, and restore Weaviate
		if err := moveHeadAndBranch(st, targetCommitID, currentBranch); err != nil {
			return nil, err
		}
		if err := st.ClearStagedChanges(); err != nil {
			return nil, fmt.Errorf("failed to clear staging area: %w", err)
		}
		result.StagedCleared = stagedCount

		// Restore Weaviate state (reuse checkout logic)
		warnings, stats, err := restoreStateToCommit(ctx, cfg, st, client, targetCommitID)
		if err != nil {
			return nil, fmt.Errorf("failed to restore state: %w", err)
		}
		result.Warnings = warnings
		result.ObjectsAdded = stats.Added
		result.ObjectsRemoved = stats.Removed
		result.ObjectsUpdated = stats.Updated
	}

	// Step 6: Rebuild known_objects table for all modes
	if err := rebuildKnownObjectsFromCommit(st, targetCommitID); err != nil {
		result.Warnings = append(result.Warnings, CheckoutWarning{
			Type:    "known_state",
			Message: fmt.Sprintf("failed to rebuild known state: %v", err),
		})
	}

	// Step 7: For soft reset, auto-stage the differences (like git reset --soft)
	// This makes the changes from "undone" commits appear as staged
	if opts.Mode == ResetModeSoft {
		staged, err := StageAll(ctx, cfg, st, client)
		if err != nil {
			result.Warnings = append(result.Warnings, CheckoutWarning{
				Type:    "staging",
				Message: fmt.Sprintf("failed to auto-stage changes: %v", err),
			})
		}
		result.ChangesStaged = staged
	}

	return result, nil
}

// moveHeadAndBranch updates HEAD and optionally the current branch pointer
func moveHeadAndBranch(st *store.Store, commitID, branchName string) error {
	// Update HEAD
	if err := st.SetHEAD(commitID); err != nil {
		return fmt.Errorf("failed to update HEAD: %w", err)
	}

	// Update branch pointer if on a branch (not detached)
	if branchName != "" {
		if err := st.UpdateBranch(branchName, commitID); err != nil {
			return fmt.Errorf("failed to update branch '%s': %w", branchName, err)
		}
	}

	return nil
}
