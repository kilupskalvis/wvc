package core

import (
	"context"
	"testing"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/weaviate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindMergeBase_LinearHistory(t *testing.T) {
	st := newTestStore(t)

	// Create linear commit chain: c1 <- c2 <- c3
	c1 := &models.Commit{ID: "commit1", Message: "first"}
	c2 := &models.Commit{ID: "commit2", ParentID: "commit1", Message: "second"}
	c3 := &models.Commit{ID: "commit3", ParentID: "commit2", Message: "third"}

	require.NoError(t, st.CreateCommit(c1))
	require.NoError(t, st.CreateCommit(c2))
	require.NoError(t, st.CreateCommit(c3))

	// Merge base of commit2 and commit3 should be commit2
	base, err := FindMergeBase(st, "commit2", "commit3")
	require.NoError(t, err)
	assert.Equal(t, "commit2", base)

	// Merge base of commit1 and commit3 should be commit1
	base, err = FindMergeBase(st, "commit1", "commit3")
	require.NoError(t, err)
	assert.Equal(t, "commit1", base)
}

func TestFindMergeBase_DivergedBranches(t *testing.T) {
	st := newTestStore(t)

	// Create diverged history:
	// c1 <- c2 (main)
	//    \- c3 (feature)
	c1 := &models.Commit{ID: "commit1", Message: "first"}
	c2 := &models.Commit{ID: "commit2", ParentID: "commit1", Message: "main"}
	c3 := &models.Commit{ID: "commit3", ParentID: "commit1", Message: "feature"}

	require.NoError(t, st.CreateCommit(c1))
	require.NoError(t, st.CreateCommit(c2))
	require.NoError(t, st.CreateCommit(c3))

	// Merge base should be commit1
	base, err := FindMergeBase(st, "commit2", "commit3")
	require.NoError(t, err)
	assert.Equal(t, "commit1", base)
}

func TestFindMergeBase_WithMergeCommit(t *testing.T) {
	st := newTestStore(t)

	// Create history with merge:
	// c1 <- c2 <- c4 (merge of c2 and c3)
	//    \- c3 -/
	c1 := &models.Commit{ID: "commit1", Message: "first"}
	c2 := &models.Commit{ID: "commit2", ParentID: "commit1", Message: "main"}
	c3 := &models.Commit{ID: "commit3", ParentID: "commit1", Message: "feature"}
	c4 := &models.Commit{ID: "commit4", ParentID: "commit2", MergeParentID: "commit3", Message: "merge"}

	require.NoError(t, st.CreateCommit(c1))
	require.NoError(t, st.CreateCommit(c2))
	require.NoError(t, st.CreateCommit(c3))
	require.NoError(t, st.CreateCommit(c4))

	// c3 is an ancestor of c4 (via merge parent)
	base, err := FindMergeBase(st, "commit3", "commit4")
	require.NoError(t, err)
	assert.Equal(t, "commit3", base)
}

func TestCanFastForward(t *testing.T) {
	st := newTestStore(t)

	// c1 <- c2 <- c3
	c1 := &models.Commit{ID: "commit1", Message: "first"}
	c2 := &models.Commit{ID: "commit2", ParentID: "commit1", Message: "second"}
	c3 := &models.Commit{ID: "commit3", ParentID: "commit2", Message: "third"}

	require.NoError(t, st.CreateCommit(c1))
	require.NoError(t, st.CreateCommit(c2))
	require.NoError(t, st.CreateCommit(c3))

	// c1 can fast-forward to c3 (c1 is ancestor of c3)
	canFF, err := canFastForward(st, "commit1", "commit3")
	require.NoError(t, err)
	assert.True(t, canFF)

	// c3 cannot fast-forward to c1 (c3 is not ancestor of c1)
	canFF, err = canFastForward(st, "commit3", "commit1")
	require.NoError(t, err)
	assert.False(t, canFF)
}

func TestDetectObjectConflicts_NoConflict(t *testing.T) {
	// Base has obj1
	// Ours adds obj2
	// Theirs adds obj3
	// No conflict - different objects modified
	baseState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Base"}}},
	}
	oursState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Base"}}},
		"Article/obj-002": {Object: &models.WeaviateObject{ID: "obj-002", Class: "Article", Properties: map[string]interface{}{"title": "Ours"}}},
	}
	theirsState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Base"}}},
		"Article/obj-003": {Object: &models.WeaviateObject{ID: "obj-003", Class: "Article", Properties: map[string]interface{}{"title": "Theirs"}}},
	}

	conflicts := detectObjectConflicts(baseState, oursState, theirsState)
	assert.Len(t, conflicts, 0)
}

func TestDetectObjectConflicts_ModifyModify(t *testing.T) {
	// Both modified same object differently
	baseState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Base"}}},
	}
	oursState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Ours"}}},
	}
	theirsState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Theirs"}}},
	}

	conflicts := detectObjectConflicts(baseState, oursState, theirsState)
	require.Len(t, conflicts, 1)
	assert.Equal(t, models.ConflictModifyModify, conflicts[0].Type)
	assert.Equal(t, "Article", conflicts[0].ClassName)
	assert.Equal(t, "obj-001", conflicts[0].ObjectID)
}

func TestDetectObjectConflicts_DeleteModify(t *testing.T) {
	// We deleted, they modified
	baseState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Base"}}},
	}
	oursState := map[string]*objectWithVector{}
	theirsState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Modified"}}},
	}

	conflicts := detectObjectConflicts(baseState, oursState, theirsState)
	require.Len(t, conflicts, 1)
	assert.Equal(t, models.ConflictDeleteModify, conflicts[0].Type)
}

func TestDetectObjectConflicts_AddAdd(t *testing.T) {
	// Both added same object with different data
	baseState := map[string]*objectWithVector{}
	oursState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Ours"}}},
	}
	theirsState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Theirs"}}},
	}

	conflicts := detectObjectConflicts(baseState, oursState, theirsState)
	require.Len(t, conflicts, 1)
	assert.Equal(t, models.ConflictAddAdd, conflicts[0].Type)
}

func TestDetectObjectConflicts_SameChange(t *testing.T) {
	// Both made same change - no conflict
	baseState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Base"}}},
	}
	sameChange := &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Same"}}
	oursState := map[string]*objectWithVector{
		"Article/obj-001": {Object: sameChange},
	}
	theirsState := map[string]*objectWithVector{
		"Article/obj-001": {Object: sameChange},
	}

	conflicts := detectObjectConflicts(baseState, oursState, theirsState)
	assert.Len(t, conflicts, 0)
}

func TestComputeMergedState(t *testing.T) {
	// Base: obj1
	// Ours: obj1 (unchanged), obj2 (added)
	// Theirs: obj1 (modified), obj3 (added)
	// Merged should have: obj1 (theirs), obj2 (ours), obj3 (theirs)
	baseState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Base"}}},
	}
	oursState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Base"}}},
		"Article/obj-002": {Object: &models.WeaviateObject{ID: "obj-002", Class: "Article", Properties: map[string]interface{}{"title": "Ours"}}},
	}
	theirsState := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Theirs"}}},
		"Article/obj-003": {Object: &models.WeaviateObject{ID: "obj-003", Class: "Article", Properties: map[string]interface{}{"title": "Theirs3"}}},
	}

	merged := computeMergedState(baseState, oursState, theirsState)

	// Should have 3 objects
	assert.Len(t, merged, 3)
	// obj-001 should have theirs' value (they changed, we didn't)
	assert.Equal(t, "Theirs", merged["Article/obj-001"].Object.Properties["title"])
	// obj-002 should be ours
	assert.Equal(t, "Ours", merged["Article/obj-002"].Object.Properties["title"])
	// obj-003 should be theirs
	assert.Equal(t, "Theirs3", merged["Article/obj-003"].Object.Properties["title"])
}

func TestResolveConflicts_Ours(t *testing.T) {
	conflicts := []*models.MergeConflict{
		{
			Key:       "Article/obj-001",
			ClassName: "Article",
			ObjectID:  "obj-001",
			Type:      models.ConflictModifyModify,
			Ours:      &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Ours"}},
			Theirs:    &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Theirs"}},
		},
	}
	merged := map[string]*objectWithVector{}

	resolved := resolveConflicts(conflicts, models.ConflictOurs, merged)

	assert.Equal(t, 1, resolved)
	assert.Equal(t, "Ours", merged["Article/obj-001"].Object.Properties["title"])
}

func TestResolveConflicts_Theirs(t *testing.T) {
	conflicts := []*models.MergeConflict{
		{
			Key:       "Article/obj-001",
			ClassName: "Article",
			ObjectID:  "obj-001",
			Type:      models.ConflictModifyModify,
			Ours:      &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Ours"}},
			Theirs:    &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Theirs"}},
		},
	}
	merged := map[string]*objectWithVector{}

	resolved := resolveConflicts(conflicts, models.ConflictTheirs, merged)

	assert.Equal(t, 1, resolved)
	assert.Equal(t, "Theirs", merged["Article/obj-001"].Object.Properties["title"])
}

func TestResolveConflicts_DeleteConflict_Ours(t *testing.T) {
	// We deleted, they modified - with --ours we keep it deleted
	conflicts := []*models.MergeConflict{
		{
			Key:       "Article/obj-001",
			ClassName: "Article",
			ObjectID:  "obj-001",
			Type:      models.ConflictDeleteModify,
			Ours:      nil, // We deleted
			Theirs:    &models.WeaviateObject{ID: "obj-001", Class: "Article", Properties: map[string]interface{}{"title": "Theirs"}},
		},
	}
	merged := map[string]*objectWithVector{
		"Article/obj-001": {Object: &models.WeaviateObject{ID: "obj-001"}}, // Currently exists
	}

	resolveConflicts(conflicts, models.ConflictOurs, merged)

	// Should be deleted (not in merged)
	_, exists := merged["Article/obj-001"]
	assert.False(t, exists)
}

func TestMerge_FastForward(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: main with 1 commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})

	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create feature branch
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Checkout feature
	_, err = Checkout(ctx, cfg, st, client, "feature", CheckoutOptions{})
	require.NoError(t, err)

	// Add object on feature
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Feature"},
	})
	commit2, err := CreateCommit(ctx, cfg, st, client, "Feature commit")
	require.NoError(t, err)

	// Checkout main
	_, err = Checkout(ctx, cfg, st, client, "main", CheckoutOptions{})
	require.NoError(t, err)

	// Merge feature into main (should fast-forward)
	result, err := Merge(ctx, cfg, st, client, "feature", models.MergeOptions{})
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.True(t, result.FastForward)
	assert.Nil(t, result.MergeCommit) // No merge commit for FF

	// HEAD should be at commit2
	head, _ := st.GetHEAD()
	assert.Equal(t, commit2.ID, head)

	// Main branch should point to commit2
	branch, _ := st.GetBranch("main")
	assert.Equal(t, commit2.ID, branch.CommitID)

	// Weaviate should have both objects
	assert.Len(t, client.Objects, 2)

	_ = commit1 // suppress unused warning
}

func TestMerge_ThreeWay_NoConflicts(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: initial commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Initial"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create and checkout feature branch
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)
	_, err = Checkout(ctx, cfg, st, client, "feature", CheckoutOptions{})
	require.NoError(t, err)

	// Add obj-002 on feature
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Feature"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Feature commit")
	require.NoError(t, err)

	// Checkout main
	_, err = Checkout(ctx, cfg, st, client, "main", CheckoutOptions{})
	require.NoError(t, err)

	// Add obj-003 on main (diverged)
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-003",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Main"},
	})
	_, err = CreateCommit(ctx, cfg, st, client, "Main commit")
	require.NoError(t, err)

	// Now merge feature into main
	result, err := Merge(ctx, cfg, st, client, "feature", models.MergeOptions{})
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.False(t, result.FastForward)
	assert.NotNil(t, result.MergeCommit)
	assert.True(t, result.MergeCommit.IsMergeCommit())

	// Should have 3 objects after merge
	assert.Len(t, client.Objects, 3)
}

func TestMerge_WithConflict_Abort(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: initial commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Initial"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create feature branch
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Modify obj-001 on main
	client.Objects["Article/obj-001"].Properties["title"] = "Main version"
	_, err = CreateCommit(ctx, cfg, st, client, "Main modify")
	require.NoError(t, err)

	// Checkout feature
	_, err = Checkout(ctx, cfg, st, client, "feature", CheckoutOptions{})
	require.NoError(t, err)

	// Modify same obj-001 on feature
	client.Objects["Article/obj-001"].Properties["title"] = "Feature version"
	_, err = CreateCommit(ctx, cfg, st, client, "Feature modify")
	require.NoError(t, err)

	// Checkout main
	_, err = Checkout(ctx, cfg, st, client, "main", CheckoutOptions{})
	require.NoError(t, err)

	// Merge should detect conflict and abort
	result, err := Merge(ctx, cfg, st, client, "feature", models.MergeOptions{})
	require.NoError(t, err)

	assert.False(t, result.Success)
	assert.Len(t, result.Conflicts, 1)
	assert.Equal(t, models.ConflictModifyModify, result.Conflicts[0].Type)
}

func TestMerge_WithConflict_ResolveOurs(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: initial commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Initial"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create feature branch
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Modify obj-001 on main
	client.Objects["Article/obj-001"].Properties["title"] = "Main version"
	_, err = CreateCommit(ctx, cfg, st, client, "Main modify")
	require.NoError(t, err)

	// Checkout feature
	_, err = Checkout(ctx, cfg, st, client, "feature", CheckoutOptions{})
	require.NoError(t, err)

	// Modify same obj-001 on feature
	client.Objects["Article/obj-001"].Properties["title"] = "Feature version"
	_, err = CreateCommit(ctx, cfg, st, client, "Feature modify")
	require.NoError(t, err)

	// Checkout main
	_, err = Checkout(ctx, cfg, st, client, "main", CheckoutOptions{})
	require.NoError(t, err)

	// Merge with --ours should succeed
	result, err := Merge(ctx, cfg, st, client, "feature", models.MergeOptions{
		Strategy: models.ConflictOurs,
	})
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.Equal(t, 1, result.ResolvedConflicts)

	// Object should have our version
	obj, _ := client.GetObject(ctx, "Article", "obj-001")
	assert.Equal(t, "Main version", obj.Properties["title"])
}

func TestMerge_AlreadyUpToDate(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: initial commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Initial"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Create feature at same commit
	err = CreateBranch(st, "feature", "")
	require.NoError(t, err)

	// Merge feature (same commit) should say already up to date
	result, err := Merge(ctx, cfg, st, client, "feature", models.MergeOptions{})
	require.NoError(t, err)

	assert.True(t, result.Success)
	assert.Contains(t, result.Warnings, "Already up to date.")
}

func TestMerge_DetachedHead(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: initial commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Initial"},
	})
	commit1, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Checkout commit directly (detached HEAD)
	_, err = Checkout(ctx, cfg, st, client, commit1.ID, CheckoutOptions{})
	require.NoError(t, err)

	// Merge should fail on detached HEAD
	_, err = Merge(ctx, cfg, st, client, "main", models.MergeOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "detached")
}
