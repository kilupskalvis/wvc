package core

import (
	"context"
	"testing"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/weaviate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckout_SwitchBranch(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create initial commit on main
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Main Article"},
	})

	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial commit")
	require.NoError(t, err)

	// Main branch should be created automatically
	currentBranch, _ := st.GetCurrentBranch()
	assert.Equal(t, "main", currentBranch)

	// Create feature branch
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Add more objects and commit on main
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second on Main"},
	})
	commit2, err := CreateCommit(ctx, cfg, st, client, "Second commit on main")
	require.NoError(t, err)

	// Verify main has two commits worth of objects
	assert.Len(t, client.Objects, 2)

	// Act: Checkout feature branch
	opts := CheckoutOptions{}
	result, err := Checkout(ctx, cfg, st, client, "feature", opts)
	require.NoError(t, err)

	// Assert: Should be on feature branch at commit1
	assert.Equal(t, "feature", result.BranchName)
	assert.False(t, result.IsDetached)
	assert.Equal(t, commit1.ID, result.TargetCommit)
	assert.Equal(t, commit2.ID, result.PreviousCommit)

	// Assert: Weaviate should only have one object (obj-001)
	assert.Len(t, client.Objects, 1)
	_, exists := client.Objects["Article/obj-001"]
	assert.True(t, exists)
	_, exists = client.Objects["Article/obj-002"]
	assert.False(t, exists)

	// Assert: HEAD and current branch updated
	head, _ := st.GetHEAD()
	assert.Equal(t, commit1.ID, head)
	currentBranch, _ = st.GetCurrentBranch()
	assert.Equal(t, "feature", currentBranch)
}

func TestCheckout_DetachedHead(t *testing.T) {
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

	// Act: Checkout first commit by ID (detached HEAD)
	opts := CheckoutOptions{}
	result, err := Checkout(ctx, cfg, st, client, commit1.ID, opts)
	require.NoError(t, err)

	// Assert: Should be in detached HEAD state
	assert.True(t, result.IsDetached)
	assert.Empty(t, result.BranchName)

	// Assert: HEAD points to commit1
	head, _ := st.GetHEAD()
	assert.Equal(t, commit1.ID, head)

	// Assert: Current branch is empty (detached)
	currentBranch, _ := st.GetCurrentBranch()
	assert.Empty(t, currentBranch)

	// Assert: Weaviate has only first object
	assert.Len(t, client.Objects, 1)
}

func TestCheckout_CreateBranch(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create initial commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Act: Checkout -b feature
	opts := CheckoutOptions{
		CreateBranch:  true,
		NewBranchName: "feature",
	}
	result, err := Checkout(ctx, cfg, st, client, "feature", opts)
	require.NoError(t, err)

	// Assert: New branch created and checked out
	assert.Equal(t, "feature", result.BranchName)
	assert.False(t, result.IsDetached)

	// Assert: Branch exists
	branch, err := st.GetBranch("feature")
	require.NoError(t, err)
	assert.NotNil(t, branch)

	// Assert: Current branch is feature
	currentBranch, _ := st.GetCurrentBranch()
	assert.Equal(t, "feature", currentBranch)
}

func TestCheckout_WithUncommittedChanges_Error(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit on main
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create feature branch
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Add uncommitted changes
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Uncommitted"},
	})

	// Act: Try to checkout feature without force
	opts := CheckoutOptions{Force: false}
	_, err = Checkout(ctx, cfg, st, client, "feature", opts)

	// Assert: Should fail
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "uncommitted changes")
}

func TestCheckout_WithUncommittedChanges_Force(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit on main
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create feature branch
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Add uncommitted changes
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Uncommitted"},
	})

	// Act: Force checkout
	opts := CheckoutOptions{Force: true}
	result, err := Checkout(ctx, cfg, st, client, "feature", opts)

	// Assert: Should succeed
	require.NoError(t, err)
	assert.Equal(t, "feature", result.BranchName)

	// The uncommitted object should be removed (state restored)
	assert.Len(t, client.Objects, 1)
}

func TestCheckout_SameCommit_JustSwitchBranch(t *testing.T) {
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
	commit, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create feature branch at same commit
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Act: Checkout feature (same commit)
	opts := CheckoutOptions{}
	result, err := Checkout(ctx, cfg, st, client, "feature", opts)
	require.NoError(t, err)

	// Assert: No objects changed
	assert.Equal(t, 0, result.ObjectsAdded)
	assert.Equal(t, 0, result.ObjectsRemoved)
	assert.Equal(t, 0, result.ObjectsUpdated)

	// Assert: On feature branch
	assert.Equal(t, "feature", result.BranchName)
	assert.Equal(t, commit.ID, result.TargetCommit)
}

func TestCheckout_RestoreUpdatedObject(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit with object
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Original"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create feature branch
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Update object on main
	client.Objects["Article/obj-001"].Properties["title"] = "Updated on main"
	_, err = CreateCommit(ctx, cfg, st, client, "Update on main")
	require.NoError(t, err)

	// Verify current state
	obj, _ := client.GetObject(ctx, "Article", "obj-001")
	assert.Equal(t, "Updated on main", obj.Properties["title"])

	// Act: Checkout feature (should restore original)
	opts := CheckoutOptions{}
	result, err := Checkout(ctx, cfg, st, client, "feature", opts)
	require.NoError(t, err)

	// Assert: Object was updated (restored to original)
	assert.Equal(t, 1, result.ObjectsUpdated)

	obj, _ = client.GetObject(ctx, "Article", "obj-001")
	assert.Equal(t, "Original", obj.Properties["title"])

	// Assert: HEAD is at commit1
	head, _ := st.GetHEAD()
	assert.Equal(t, commit1.ID, head)
}

func TestGetCommitPath(t *testing.T) {
	st := newTestStore(t)

	// Create commit chain: c1 <- c2 <- c3
	c1 := &models.Commit{ID: "commit1", Message: "first"}
	c2 := &models.Commit{ID: "commit2", ParentID: "commit1", Message: "second"}
	c3 := &models.Commit{ID: "commit3", ParentID: "commit2", Message: "third"}

	require.NoError(t, st.CreateCommit(c1))
	require.NoError(t, st.CreateCommit(c2))
	require.NoError(t, st.CreateCommit(c3))

	// Act: Get path to commit3
	path, err := getCommitPath(st, "commit3")
	require.NoError(t, err)

	// Assert: Path is [commit1, commit2, commit3]
	assert.Len(t, path, 3)
	assert.Equal(t, "commit1", path[0])
	assert.Equal(t, "commit2", path[1])
	assert.Equal(t, "commit3", path[2])
}

func TestReconstructStateAtCommit(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Create initial commit with 2 objects
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
	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Delete one, add another
	delete(client.Objects, "Article/obj-002")
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-003",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Third"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Second")
	require.NoError(t, err)

	// Act: Reconstruct state at commit1
	state, err := reconstructStateAtCommit(st, commit1.ID)
	require.NoError(t, err)

	// Assert: Should have obj-001 and obj-002, not obj-003
	assert.Len(t, state, 2)
	assert.Contains(t, state, "Article/obj-001")
	assert.Contains(t, state, "Article/obj-002")
	assert.NotContains(t, state, "Article/obj-003")
}

func TestHasUncommittedChanges(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create initial commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Check: No uncommitted changes
	hasChanges, err := HasUncommittedChanges(ctx, cfg, st, client)
	require.NoError(t, err)
	assert.False(t, hasChanges)

	// Add uncommitted object
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "New"},
	})

	// Check: Has uncommitted changes
	hasChanges, err = HasUncommittedChanges(ctx, cfg, st, client)
	require.NoError(t, err)
	assert.True(t, hasChanges)
}

func TestBranchAdvancesOnCommit(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Create first commit - should create main branch
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "First")
	require.NoError(t, err)

	// Verify main branch was created
	branch, err := st.GetBranch("main")
	require.NoError(t, err)
	assert.NotNil(t, branch)
	assert.Equal(t, commit1.ID, branch.CommitID)

	// Create second commit
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	commit2, err := CreateCommit(ctx, cfg, st, client, "Second")
	require.NoError(t, err)

	// Verify main branch advanced
	branch, err = st.GetBranch("main")
	require.NoError(t, err)
	assert.Equal(t, commit2.ID, branch.CommitID)
}
