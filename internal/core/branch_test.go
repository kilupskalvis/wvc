package core

import (
	"os"
	"testing"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStoreForBranches(t *testing.T) (*store.Store, func()) {
	tmpDir, err := os.MkdirTemp("", "wvc-branch-test")
	require.NoError(t, err)

	st, err := store.New(tmpDir + "/test.db")
	require.NoError(t, err)

	err = st.Initialize()
	require.NoError(t, err)

	cleanup := func() {
		st.Close()
		os.RemoveAll(tmpDir)
	}

	return st, cleanup
}

func TestCreateBranch_AtHead(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create a commit first
	commit := &models.Commit{
		ID:             "abc123",
		Message:        "test commit",
		OperationCount: 0,
	}
	err := st.CreateCommit(commit)
	require.NoError(t, err)

	err = st.SetHEAD("abc123")
	require.NoError(t, err)

	// Create branch at HEAD
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Verify branch exists
	branch, err := st.GetBranch("feature")
	require.NoError(t, err)
	assert.Equal(t, "feature", branch.Name)
	assert.Equal(t, "abc123", branch.CommitID)
}

func TestCreateBranch_AtSpecificCommit(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create two commits
	commit1 := &models.Commit{ID: "commit1", Message: "first"}
	commit2 := &models.Commit{ID: "commit2", ParentID: "commit1", Message: "second"}
	require.NoError(t, st.CreateCommit(commit1))
	require.NoError(t, st.CreateCommit(commit2))
	require.NoError(t, st.SetHEAD("commit2"))

	// Create branch at first commit
	err := CreateBranch(st, "old-branch", "commit1")
	require.NoError(t, err)

	// Verify branch points to commit1
	branch, err := st.GetBranch("old-branch")
	require.NoError(t, err)
	assert.Equal(t, "commit1", branch.CommitID)
}

func TestCreateBranch_AlreadyExists(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create a commit and branch
	commit := &models.Commit{ID: "abc123", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, st.SetHEAD("abc123"))
	require.NoError(t, CreateBranch(st, "feature", ""))

	// Try to create same branch again
	err := CreateBranch(st, "feature", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestCreateBranch_NoCommits(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Try to create branch without any commits
	err := CreateBranch(st, "feature", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no commits")
}

func TestDeleteBranch(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit and two branches
	commit := &models.Commit{ID: "abc123", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, st.SetHEAD("abc123"))
	require.NoError(t, CreateBranch(st, "main", ""))
	require.NoError(t, CreateBranch(st, "feature", ""))
	require.NoError(t, st.SetCurrentBranch("main"))

	// Delete feature branch
	err := DeleteBranch(st, "feature", false)
	require.NoError(t, err)

	// Verify branch is gone
	exists, err := st.BranchExists("feature")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestDeleteBranch_CannotDeleteCurrent(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit and branch
	commit := &models.Commit{ID: "abc123", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, st.SetHEAD("abc123"))
	require.NoError(t, CreateBranch(st, "main", ""))
	require.NoError(t, st.SetCurrentBranch("main"))

	// Try to delete current branch
	err := DeleteBranch(st, "main", false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checked out")
}

func TestListBranches(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit and branches
	commit := &models.Commit{ID: "abc123", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, st.SetHEAD("abc123"))
	require.NoError(t, CreateBranch(st, "main", ""))
	require.NoError(t, CreateBranch(st, "feature", ""))
	require.NoError(t, st.SetCurrentBranch("main"))

	// List branches
	branches, currentBranch, err := ListBranches(st)
	require.NoError(t, err)

	assert.Len(t, branches, 2)
	assert.Equal(t, "main", currentBranch)
}

func TestResolveRef_Branch(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit and branch
	commit := &models.Commit{ID: "abc123def456", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, CreateBranch(st, "feature", "abc123def456"))

	// Resolve branch name
	commitID, branchName, err := ResolveRef(st, "feature")
	require.NoError(t, err)
	assert.Equal(t, "abc123def456", commitID)
	assert.Equal(t, "feature", branchName)
}

func TestResolveRef_FullCommitID(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit
	commit := &models.Commit{ID: "abc123def456", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))

	// Resolve full commit ID
	commitID, branchName, err := ResolveRef(st, "abc123def456")
	require.NoError(t, err)
	assert.Equal(t, "abc123def456", commitID)
	assert.Empty(t, branchName) // Not a branch
}

func TestResolveRef_ShortCommitID(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit
	commit := &models.Commit{ID: "abc123def456", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))

	// Resolve short commit ID
	commitID, branchName, err := ResolveRef(st, "abc123")
	require.NoError(t, err)
	assert.Equal(t, "abc123def456", commitID)
	assert.Empty(t, branchName) // Not a branch
}

func TestResolveRef_NotFound(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Try to resolve non-existent ref
	_, _, err := ResolveRef(st, "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid branch or commit")
}
