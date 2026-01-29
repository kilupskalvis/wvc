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

func TestResolveRef_HEAD(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit and set HEAD
	commit := &models.Commit{ID: "abc123def456", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, st.SetHEAD("abc123def456"))

	// Resolve HEAD
	commitID, branchName, err := ResolveRef(st, "HEAD")
	require.NoError(t, err)
	assert.Equal(t, "abc123def456", commitID)
	assert.Empty(t, branchName)
}

func TestResolveRef_HEAD_NoCommits(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Try to resolve HEAD without any commits
	_, _, err := ResolveRef(st, "HEAD")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HEAD not set")
}

func TestResolveRef_HEADTilde1(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create two commits
	commit1 := &models.Commit{ID: "commit1", Message: "first"}
	commit2 := &models.Commit{ID: "commit2", ParentID: "commit1", Message: "second"}
	require.NoError(t, st.CreateCommit(commit1))
	require.NoError(t, st.CreateCommit(commit2))
	require.NoError(t, st.SetHEAD("commit2"))

	// Resolve HEAD~1
	commitID, branchName, err := ResolveRef(st, "HEAD~1")
	require.NoError(t, err)
	assert.Equal(t, "commit1", commitID)
	assert.Empty(t, branchName)
}

func TestResolveRef_HEADTilde3(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create four commits
	commit1 := &models.Commit{ID: "commit1", Message: "first"}
	commit2 := &models.Commit{ID: "commit2", ParentID: "commit1", Message: "second"}
	commit3 := &models.Commit{ID: "commit3", ParentID: "commit2", Message: "third"}
	commit4 := &models.Commit{ID: "commit4", ParentID: "commit3", Message: "fourth"}
	require.NoError(t, st.CreateCommit(commit1))
	require.NoError(t, st.CreateCommit(commit2))
	require.NoError(t, st.CreateCommit(commit3))
	require.NoError(t, st.CreateCommit(commit4))
	require.NoError(t, st.SetHEAD("commit4"))

	// Resolve HEAD~3
	commitID, branchName, err := ResolveRef(st, "HEAD~3")
	require.NoError(t, err)
	assert.Equal(t, "commit1", commitID)
	assert.Empty(t, branchName)
}

func TestResolveRef_HEADTilde0(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit
	commit := &models.Commit{ID: "abc123", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, st.SetHEAD("abc123"))

	// Resolve HEAD~0 (same as HEAD)
	commitID, _, err := ResolveRef(st, "HEAD~0")
	require.NoError(t, err)
	assert.Equal(t, "abc123", commitID)
}

func TestResolveRef_HEADTilde_BeyondRoot(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create single commit (root)
	commit := &models.Commit{ID: "root", Message: "root commit"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, st.SetHEAD("root"))

	// Try to resolve HEAD~1 (beyond root)
	_, _, err := ResolveRef(st, "HEAD~1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reached root commit")
}

func TestResolveRef_HEADTilde_InvalidNumber(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit
	commit := &models.Commit{ID: "abc123", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, st.SetHEAD("abc123"))

	// Try to resolve HEAD~abc (invalid)
	_, _, err := ResolveRef(st, "HEAD~abc")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid ref")
}

func TestResolveRef_HEADTilde_Negative(t *testing.T) {
	st, cleanup := setupTestStoreForBranches(t)
	defer cleanup()

	// Create commit
	commit := &models.Commit{ID: "abc123", Message: "test"}
	require.NoError(t, st.CreateCommit(commit))
	require.NoError(t, st.SetHEAD("abc123"))

	// Try to resolve HEAD~-1 (negative)
	_, _, err := ResolveRef(st, "HEAD~-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid ref")
}
