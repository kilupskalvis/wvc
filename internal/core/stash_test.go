package core

import (
	"context"
	"testing"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/weaviate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStashPush_SavesAndRestoresCleanState(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create initial commit with one object
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial commit")
	require.NoError(t, err)

	// Add a second object (uncommitted)
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})

	// Act: Stash
	result, err := StashPush(ctx, cfg, st, client, StashPushOptions{})
	require.NoError(t, err)

	// Assert: Weaviate restored to clean state (only obj-001)
	assert.Len(t, client.Objects, 1)
	_, exists := client.Objects["Article/obj-001"]
	assert.True(t, exists)
	_, exists = client.Objects["Article/obj-002"]
	assert.False(t, exists)

	// Assert: Stash created
	assert.Equal(t, 0, result.StashIndex)
	assert.Equal(t, 1, result.UnstagedCount)
	assert.Equal(t, 1, result.TotalCount)

	// Assert: Stash list has 1 entry
	stashes, err := st.ListStashes()
	require.NoError(t, err)
	assert.Len(t, stashes, 1)

	// Assert: Stash changes captured
	changes, err := st.GetStashChanges(stashes[0].ID)
	require.NoError(t, err)
	assert.Len(t, changes, 1)
	assert.Equal(t, "obj-002", changes[0].ObjectID)
	assert.Equal(t, "insert", changes[0].ChangeType)
	assert.False(t, changes[0].WasStaged)
}

func TestStashPush_CapturesStagedAndUnstaged(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create initial commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial commit")
	require.NoError(t, err)

	// Add obj-002 and stage it
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = StageAll(ctx, cfg, st, client)
	require.NoError(t, err)

	// Add obj-003 (unstaged)
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-003",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Third"},
	})

	// Act: Stash
	result, err := StashPush(ctx, cfg, st, client, StashPushOptions{})
	require.NoError(t, err)

	// Assert: Both staged and unstaged captured
	assert.Equal(t, 1, result.StagedCount)
	assert.Equal(t, 1, result.UnstagedCount)
	assert.Equal(t, 2, result.TotalCount)

	// Assert: Staging area cleared
	stagedCount, _ := st.GetStagedChangesCount()
	assert.Equal(t, 0, stagedCount)

	// Verify WasStaged flags
	stashes, err := st.ListStashes()
	require.NoError(t, err)
	changes, err := st.GetStashChanges(stashes[0].ID)
	require.NoError(t, err)
	assert.Len(t, changes, 2)

	stagedFound := false
	unstagedFound := false
	for _, c := range changes {
		if c.ObjectID == "obj-002" && c.WasStaged {
			stagedFound = true
		}
		if c.ObjectID == "obj-003" && !c.WasStaged {
			unstagedFound = true
		}
	}
	assert.True(t, stagedFound, "should find staged change for obj-002")
	assert.True(t, unstagedFound, "should find unstaged change for obj-003")
}

func TestStashPush_NoChanges_Error(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit with no pending changes
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial commit")
	require.NoError(t, err)

	// Act: Stash with no changes
	_, err = StashPush(ctx, cfg, st, client, StashPushOptions{})

	// Assert: Error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no local changes to save")
}

func TestStashPush_DefaultMessage(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial setup")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})

	result, err := StashPush(ctx, cfg, st, client, StashPushOptions{})
	require.NoError(t, err)

	assert.Contains(t, result.Message, "WIP on main:")
	assert.Contains(t, result.Message, "Initial setup")
}

func TestStashPush_CustomMessage(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})

	result, err := StashPush(ctx, cfg, st, client, StashPushOptions{Message: "work in progress"})
	require.NoError(t, err)

	assert.Equal(t, "work in progress", result.Message)
}

func TestStashPop_AppliesAndRemoves(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Commit, add object, stash
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = StashPush(ctx, cfg, st, client, StashPushOptions{})
	require.NoError(t, err)

	// Verify clean state
	assert.Len(t, client.Objects, 1)

	// Act: Pop (default: no restage, like git)
	result, err := StashPop(ctx, cfg, st, client, StashApplyOptions{Index: 0})
	require.NoError(t, err)

	// Assert: Object restored
	assert.Len(t, client.Objects, 2)
	_, exists := client.Objects["Article/obj-002"]
	assert.True(t, exists)
	assert.Equal(t, 1, result.UnstagedCount)

	// Assert: Nothing re-staged (git default)
	stagedCount, _ := st.GetStagedChangesCount()
	assert.Equal(t, 0, stagedCount)

	// Assert: Stash removed
	stashes, err := st.ListStashes()
	require.NoError(t, err)
	assert.Len(t, stashes, 0)
}

func TestStashApply_AppliesWithoutRemoving(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = StashPush(ctx, cfg, st, client, StashPushOptions{})
	require.NoError(t, err)

	// Act: Apply (not pop), default no restage
	_, err = StashApply(ctx, cfg, st, client, StashApplyOptions{Index: 0})
	require.NoError(t, err)

	// Assert: Object restored
	assert.Len(t, client.Objects, 2)

	// Assert: Stash still exists
	stashes, err := st.ListStashes()
	require.NoError(t, err)
	assert.Len(t, stashes, 1)
}

func TestStashApply_DefaultNoRestage(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Add and stage obj-002
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = StageAll(ctx, cfg, st, client)
	require.NoError(t, err)

	// Stash
	_, err = StashPush(ctx, cfg, st, client, StashPushOptions{})
	require.NoError(t, err)

	// Act: Apply without --index (default)
	_, err = StashApply(ctx, cfg, st, client, StashApplyOptions{Index: 0})
	require.NoError(t, err)

	// Assert: obj-002 is NOT in staging area (git default behavior)
	staged, err := st.GetStagedChange("Article", "obj-002")
	require.NoError(t, err)
	assert.Nil(t, staged)

	// Assert: obj-002 is in Weaviate though
	_, exists := client.Objects["Article/obj-002"]
	assert.True(t, exists)
}

func TestStashApply_RestagesPreviouslyStagedChanges(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Add and stage obj-002
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = StageAll(ctx, cfg, st, client)
	require.NoError(t, err)

	// Stash
	_, err = StashPush(ctx, cfg, st, client, StashPushOptions{})
	require.NoError(t, err)

	// Act: Apply with restage
	_, err = StashApply(ctx, cfg, st, client, StashApplyOptions{Index: 0, Restage: true})
	require.NoError(t, err)

	// Assert: obj-002 is re-staged
	staged, err := st.GetStagedChange("Article", "obj-002")
	require.NoError(t, err)
	assert.NotNil(t, staged)
	assert.Equal(t, "insert", staged.ChangeType)
}

func TestStashPop_SpecificIndex(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Stash A: add obj-002
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = StashPush(ctx, cfg, st, client, StashPushOptions{Message: "stash-a"})
	require.NoError(t, err)

	// Stash B: add obj-003
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-003",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Third"},
	})
	_, err = StashPush(ctx, cfg, st, client, StashPushOptions{Message: "stash-b"})
	require.NoError(t, err)

	// stash@{0} = stash-b (newest), stash@{1} = stash-a (oldest)

	// Act: Pop stash@{1} (the older one, stash-a)
	result, err := StashPop(ctx, cfg, st, client, StashApplyOptions{Index: 1})
	require.NoError(t, err)
	assert.Equal(t, "stash-a", result.Message)

	// Assert: Only stash-b remains
	stashes, err := st.ListStashes()
	require.NoError(t, err)
	require.Len(t, stashes, 1)
	assert.Equal(t, "stash-b", stashes[0].Message)
}

func TestStashDrop_RemovesStash(t *testing.T) {
	st := newTestStore(t)

	_, err := st.CreateStash("stash one", "main", "commit-aaa")
	require.NoError(t, err)
	_, err = st.CreateStash("stash two", "main", "commit-bbb")
	require.NoError(t, err)
	_, err = st.CreateStash("stash three", "main", "commit-ccc")
	require.NoError(t, err)

	// Drop middle one (index 1)
	msg, err := StashDrop(st, 1)
	require.NoError(t, err)
	assert.Equal(t, "stash two", msg)

	stashes, err := st.ListStashes()
	require.NoError(t, err)
	assert.Len(t, stashes, 2)
}

func TestStashShow_DisplaysChanges(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Add and stage obj-002
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})
	_, err = StageAll(ctx, cfg, st, client)
	require.NoError(t, err)

	// Add obj-003 (unstaged)
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-003",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Third"},
	})

	// Stash
	_, err = StashPush(ctx, cfg, st, client, StashPushOptions{})
	require.NoError(t, err)

	// Act: Show
	result, err := StashShow(st, 0)
	require.NoError(t, err)

	// Assert: Partitioned correctly
	assert.Len(t, result.StagedChanges, 1)
	assert.Len(t, result.UnstagedChanges, 1)
	assert.Equal(t, "obj-002", result.StagedChanges[0].ObjectID)
	assert.Equal(t, "obj-003", result.UnstagedChanges[0].ObjectID)
}

func TestStashClear_RemovesAll(t *testing.T) {
	st := newTestStore(t)

	_, err := st.CreateStash("one", "main", "aaa")
	require.NoError(t, err)
	_, err = st.CreateStash("two", "main", "bbb")
	require.NoError(t, err)
	_, err = st.CreateStash("three", "main", "ccc")
	require.NoError(t, err)

	count, err := StashClear(st)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	stashes, err := st.ListStashes()
	require.NoError(t, err)
	assert.Len(t, stashes, 0)
}

func TestParseStashRef(t *testing.T) {
	tests := []struct {
		input    string
		expected int
		hasError bool
	}{
		{"", 0, false},
		{"stash@{0}", 0, false},
		{"stash@{3}", 3, false},
		{"2", 2, false},
		{"0", 0, false},
		{"stash@{abc}", 0, true},
		{"invalid", 0, true},
		{"stash@{-1}", 0, true},
	}

	for _, tt := range tests {
		t.Run("ref="+tt.input, func(t *testing.T) {
			result, err := ParseStashRef(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestStashList_EmptyRepo(t *testing.T) {
	st := newTestStore(t)

	entries, err := StashList(st)
	require.NoError(t, err)
	assert.Len(t, entries, 0)
}

func TestStashPush_NoCommits_Error(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	_, err := StashPush(ctx, cfg, st, client, StashPushOptions{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no commits yet")
}
