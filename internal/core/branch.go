package core

import (
	"fmt"
	"strconv"
	"strings"

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
// Supports: branch names, full/short commit IDs, HEAD, HEAD~N
func ResolveRef(st *store.Store, ref string) (commitID string, branchName string, err error) {
	// Check for HEAD or HEAD~N pattern first
	if ref == "HEAD" || strings.HasPrefix(ref, "HEAD~") {
		commitID, err := resolveHEADRef(st, ref)
		return commitID, "", err
	}

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

// resolveHEADRef resolves HEAD or HEAD~N to a commit ID
func resolveHEADRef(st *store.Store, ref string) (string, error) {
	head, err := st.GetHEAD()
	if err != nil {
		return "", fmt.Errorf("failed to get HEAD: %w", err)
	}
	if head == "" {
		return "", fmt.Errorf("HEAD not set: no commits yet")
	}

	// If just "HEAD", return current HEAD
	if ref == "HEAD" {
		return head, nil
	}

	// Parse HEAD~N
	nStr := strings.TrimPrefix(ref, "HEAD~")
	n, err := strconv.Atoi(nStr)
	if err != nil {
		return "", fmt.Errorf("invalid ref '%s': expected HEAD~N where N is a number", ref)
	}
	if n < 0 {
		return "", fmt.Errorf("invalid ref '%s': N must be non-negative", ref)
	}
	if n == 0 {
		return head, nil
	}

	// Walk back N commits following primary parent chain
	commitID := head
	for i := 0; i < n; i++ {
		commit, err := st.GetCommit(commitID)
		if err != nil {
			return "", fmt.Errorf("failed to get commit %s: %w", commitID, err)
		}
		if commit == nil {
			return "", fmt.Errorf("cannot resolve %s: commit %s not found", ref, commitID)
		}
		if commit.ParentID == "" {
			return "", fmt.Errorf("cannot resolve %s: reached root commit after %d step(s)", ref, i)
		}
		commitID = commit.ParentID
	}

	return commitID, nil
}
