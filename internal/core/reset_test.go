package core

import (
	"context"
	"testing"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResetSoft_MovesHEADOnly(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create two commits
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "First commit")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	commit2, err := CreateCommit(ctx, cfg, st, client, "Second commit")
	require.NoError(t, err)

	// Verify starting state
	head, _ := st.GetHEAD()
	assert.Equal(t, commit2.ID, head)

	// Act: Soft reset to first commit
	opts := ResetOptions{Mode: ResetModeSoft}
	result, err := ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: HEAD moved
	head, _ = st.GetHEAD()
	assert.Equal(t, commit1.ID, head)
	assert.Equal(t, commit2.ID, result.PreviousCommit)
	assert.Equal(t, commit1.ID, result.TargetCommit)
}

func TestResetSoft_AutoStagesChanges(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit with one object
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "First commit")
	require.NoError(t, err)

	// Add second object and commit
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Second commit")
	require.NoError(t, err)

	// Verify no staged changes before reset
	stagedBefore, _ := st.GetStagedChangesCount()
	assert.Equal(t, 0, stagedBefore)

	// Act: Soft reset to first commit
	// Like git reset --soft, this should auto-stage the changes from undone commits
	opts := ResetOptions{Mode: ResetModeSoft}
	result, err := ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Changes from undone commit are now staged (like git)
	// obj-002 should appear as a staged insert
	stagedAfter, _ := st.GetStagedChangesCount()
	assert.Equal(t, 1, stagedAfter)
	assert.Equal(t, 1, result.ChangesStaged)

	// Verify the staged change is for obj-002
	staged, err := st.GetStagedChange("Article", "obj-002")
	require.NoError(t, err)
	assert.NotNil(t, staged)
	assert.Equal(t, "insert", staged.ChangeType)
}

func TestResetSoft_PreservesWeaviateState(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create two commits
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "First commit")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Second commit")
	require.NoError(t, err)

	// Verify Weaviate has 2 objects
	assert.Len(t, client.Objects, 2)

	// Act: Soft reset to first commit
	opts := ResetOptions{Mode: ResetModeSoft}
	_, err = ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Weaviate still has 2 objects (not restored)
	assert.Len(t, client.Objects, 2)
}

func TestResetMixed_ClearsStagingArea(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Add staged change
	err = st.AddStagedChange(&store.StagedChange{
		ClassName:  "Article",
		ObjectID:   "staged-obj",
		ChangeType: "insert",
	})
	require.NoError(t, err)

	stagedBefore, _ := st.GetStagedChangesCount()
	assert.Equal(t, 1, stagedBefore)

	// Act: Mixed reset (default)
	opts := ResetOptions{Mode: ResetModeMixed}
	result, err := ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Staging area cleared
	stagedAfter, _ := st.GetStagedChangesCount()
	assert.Equal(t, 0, stagedAfter)
	assert.Equal(t, 1, result.StagedCleared)
}

func TestResetMixed_PreservesWeaviateState(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create two commits
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "First")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Second")
	require.NoError(t, err)

	// Verify Weaviate has 2 objects
	assert.Len(t, client.Objects, 2)

	// Act: Mixed reset to first commit
	opts := ResetOptions{Mode: ResetModeMixed}
	_, err = ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Weaviate still has 2 objects
	assert.Len(t, client.Objects, 2)
}

func TestResetHard_RestoresWeaviateState(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create two commits
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "First commit")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Second commit")
	require.NoError(t, err)

	// Verify Weaviate has 2 objects
	assert.Len(t, client.Objects, 2)

	// Act: Hard reset to first commit
	opts := ResetOptions{Mode: ResetModeHard}
	result, err := ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Weaviate restored to 1 object
	assert.Len(t, client.Objects, 1)
	assert.Equal(t, 1, result.ObjectsRemoved)

	_, exists := client.Objects["Article/obj-001"]
	assert.True(t, exists)
	_, exists = client.Objects["Article/obj-002"]
	assert.False(t, exists)
}

func TestResetHard_ClearsStagingArea(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Add staged change
	err = st.AddStagedChange(&store.StagedChange{
		ClassName:  "Article",
		ObjectID:   "staged-obj",
		ChangeType: "insert",
	})
	require.NoError(t, err)

	stagedBefore, _ := st.GetStagedChangesCount()
	assert.Equal(t, 1, stagedBefore)

	// Act: Hard reset
	opts := ResetOptions{Mode: ResetModeHard}
	result, err := ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Staging area cleared
	stagedAfter, _ := st.GetStagedChangesCount()
	assert.Equal(t, 0, stagedAfter)
	assert.Equal(t, 1, result.StagedCleared)
}

func TestResetHard_AddsRemovedObjects(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit with 2 objects
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial with 2 objects")
	require.NoError(t, err)

	// Delete one object
	delete(client.Objects, "Article/obj-002")
	_, err = CreateCommit(ctx, cfg, st, client, "Deleted obj-002")
	require.NoError(t, err)

	// Verify only 1 object
	assert.Len(t, client.Objects, 1)

	// Act: Hard reset to first commit
	opts := ResetOptions{Mode: ResetModeHard}
	result, err := ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Object restored
	assert.Len(t, client.Objects, 2)
	assert.Equal(t, 1, result.ObjectsAdded)
}

func TestReset_MovesBranchPointer(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create two commits
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "First")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	commit2, err := CreateCommit(ctx, cfg, st, client, "Second")
	require.NoError(t, err)

	// Verify main branch at commit2
	branch, _ := st.GetBranch("main")
	assert.Equal(t, commit2.ID, branch.CommitID)

	// Act: Reset to commit1
	opts := ResetOptions{Mode: ResetModeSoft}
	result, err := ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Branch moved
	branch, _ = st.GetBranch("main")
	assert.Equal(t, commit1.ID, branch.CommitID)
	assert.Equal(t, "main", result.BranchName)
}

func TestReset_DetachedHEAD_NoBranchMove(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create two commits
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "First")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	commit2, err := CreateCommit(ctx, cfg, st, client, "Second")
	require.NoError(t, err)

	// Checkout commit1 (detached HEAD)
	checkoutOpts := CheckoutOptions{}
	_, err = Checkout(ctx, cfg, st, client, commit1.ID, checkoutOpts)
	require.NoError(t, err)

	// Verify detached state
	currentBranch, _ := st.GetCurrentBranch()
	assert.Empty(t, currentBranch)

	// Act: Reset to commit2 in detached state
	opts := ResetOptions{Mode: ResetModeSoft}
	result, err := ResetToCommit(ctx, cfg, st, client, commit2.ID, opts)
	require.NoError(t, err)

	// Assert: HEAD moved but no branch
	head, _ := st.GetHEAD()
	assert.Equal(t, commit2.ID, head)
	assert.Empty(t, result.BranchName)

	// Main branch should still point to commit2
	branch, _ := st.GetBranch("main")
	assert.Equal(t, commit2.ID, branch.CommitID)
}

func TestReset_ResolveBranchName(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit and branch
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create feature branch at commit1
	err = CreateBranch(st, "feature", commit1.ID)
	require.NoError(t, err)

	// Add another commit on main
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "More"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Another")
	require.NoError(t, err)

	// Act: Reset to "feature" branch by name
	opts := ResetOptions{Mode: ResetModeSoft}
	result, err := ResetToCommit(ctx, cfg, st, client, "feature", opts)
	require.NoError(t, err)

	// Assert: HEAD at commit1
	assert.Equal(t, commit1.ID, result.TargetCommit)
}

func TestReset_ResolveHEADTilde(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create three commits
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "First")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Second")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-003",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Third"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Third")
	require.NoError(t, err)

	// Act: Reset to HEAD~2
	opts := ResetOptions{Mode: ResetModeSoft}
	result, err := ResetToCommit(ctx, cfg, st, client, "HEAD~2", opts)
	require.NoError(t, err)

	// Assert: HEAD at commit1
	assert.Equal(t, commit1.ID, result.TargetCommit)
}

func TestReset_InvalidTarget_Error(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Act: Try to reset to non-existent target
	opts := ResetOptions{Mode: ResetModeSoft}
	_, err = ResetToCommit(ctx, cfg, st, client, "nonexistent", opts)

	// Assert: Error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve")
}

func TestReset_ToSameCommit_ExecutesMode(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Add staged change
	err = st.AddStagedChange(&store.StagedChange{
		ClassName:  "Article",
		ObjectID:   "staged-obj",
		ChangeType: "insert",
	})
	require.NoError(t, err)

	// Act: Mixed reset to same commit (HEAD)
	opts := ResetOptions{Mode: ResetModeMixed}
	result, err := ResetToCommit(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Staging area still cleared
	stagedAfter, _ := st.GetStagedChangesCount()
	assert.Equal(t, 0, stagedAfter)
	assert.Equal(t, 1, result.StagedCleared)
}

func TestResetMode_String(t *testing.T) {
	assert.Equal(t, "soft", ResetModeSoft.String())
	assert.Equal(t, "mixed", ResetModeMixed.String())
	assert.Equal(t, "hard", ResetModeHard.String())
	assert.Equal(t, "unknown", ResetMode(99).String())
}
