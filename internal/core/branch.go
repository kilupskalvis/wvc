package core

import (
	"fmt"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
)

// ListBranches returns all branches with the current branch name
func ListBranches(st *store.Store) ([]*models.Branch, string, error) {
	branches, err := st.ListBranches()
	if err != nil {
		return nil, "", err
	}

	currentBranch, err := st.GetCurrentBranch()
	if err != nil {
		return nil, "", err
	}

	return branches, currentBranch, nil
}

// CreateBranch creates a new branch at the current HEAD or specified commit
func CreateBranch(st *store.Store, name string, startPoint string) error {
	// Validate branch name
	if name == "" {
		return fmt.Errorf("branch name cannot be empty")
	}

	// Check if branch already exists
	exists, err := st.BranchExists(name)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("branch '%s' already exists", name)
	}

	// Resolve start point
	var commitID string
	if startPoint == "" {
		// Use current HEAD
		commitID, err = st.GetHEAD()
		if err != nil {
			return err
		}
		if commitID == "" {
			return fmt.Errorf("cannot create branch: no commits yet")
		}
	} else {
		// Resolve start point (branch or commit)
		commitID, _, err = ResolveRef(st, startPoint)
		if err != nil {
			return err
		}
	}

	return st.CreateBranch(name, commitID)
}

// DeleteBranch deletes a branch
func DeleteBranch(st *store.Store, name string, force bool) error {
	// Cannot delete current branch
	currentBranch, err := st.GetCurrentBranch()
	if err != nil {
		return err
	}
	if name == currentBranch {
		return fmt.Errorf("cannot delete branch '%s' while it is checked out", name)
	}

	// Check if branch exists
	branch, err := st.GetBranch(name)
	if err != nil {
		return err
	}
	if branch == nil {
		return fmt.Errorf("branch '%s' not found", name)
	}

	return st.DeleteBranch(name)
}

// ResolveRef resolves a ref (branch name or commit ID) to a commit ID
// Returns (commitID, branchName, error) where branchName is empty if ref is a commit
func ResolveRef(st *store.Store, ref string) (commitID string, branchName string, err error) {
	// Try as branch first
	branch, err := st.GetBranch(ref)
	if err != nil {
		return "", "", err
	}
	if branch != nil {
		return branch.CommitID, branch.Name, nil
	}

	// Try as full commit ID
	commit, err := st.GetCommit(ref)
	if err == nil && commit != nil {
		return commit.ID, "", nil
	}

	// Try as short commit ID
	commit, err = st.GetCommitByShortID(ref)
	if err != nil {
		return "", "", fmt.Errorf("'%s' is not a valid branch or commit", ref)
	}

	return commit.ID, "", nil
}
