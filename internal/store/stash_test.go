package store

import (
	"testing"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_CreateAndListStashes(t *testing.T) {
	st := newTestStore(t)

	// Create 3 stashes
	id1, err := st.CreateStash("stash one", "main", "commit-aaa")
	require.NoError(t, err)
	assert.True(t, id1 > 0)

	id2, err := st.CreateStash("stash two", "main", "commit-bbb")
	require.NoError(t, err)

	id3, err := st.CreateStash("stash three", "feature", "commit-ccc")
	require.NoError(t, err)

	// List should return newest first
	stashes, err := st.ListStashes()
	require.NoError(t, err)
	require.Len(t, stashes, 3)
	assert.Equal(t, id3, stashes[0].ID)
	assert.Equal(t, "stash three", stashes[0].Message)
	assert.Equal(t, "feature", stashes[0].BranchName)
	assert.Equal(t, id2, stashes[1].ID)
	assert.Equal(t, id1, stashes[2].ID)

	// GetStashByIndex: 0 = newest
	s, err := st.GetStashByIndex(0)
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, "stash three", s.Message)

	s, err = st.GetStashByIndex(2)
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, "stash one", s.Message)
}

func TestStore_StashChanges(t *testing.T) {
	st := newTestStore(t)

	stashID, err := st.CreateStash("test stash", "main", "commit-aaa")
	require.NoError(t, err)

	// Add changes: 2 staged, 1 unstaged
	changes := []*models.StashChange{
		{
			StashID:    stashID,
			ClassName:  "Article",
			ObjectID:   "obj-001",
			ChangeType: "insert",
			ObjectData: []byte(`{"class":"Article","id":"obj-001"}`),
			WasStaged:  true,
		},
		{
			StashID:    stashID,
			ClassName:  "Article",
			ObjectID:   "obj-002",
			ChangeType: "update",
			ObjectData: []byte(`{"class":"Article","id":"obj-002"}`),
			WasStaged:  true,
			VectorHash: "vec-hash-1",
		},
		{
			StashID:      stashID,
			ClassName:    "Article",
			ObjectID:     "obj-003",
			ChangeType:   "delete",
			PreviousData: []byte(`{"class":"Article","id":"obj-003"}`),
			WasStaged:    false,
		},
	}

	for _, c := range changes {
		require.NoError(t, st.CreateStashChange(c))
	}

	// Retrieve and verify
	retrieved, err := st.GetStashChanges(stashID)
	require.NoError(t, err)
	require.Len(t, retrieved, 3)

	assert.Equal(t, "obj-001", retrieved[0].ObjectID)
	assert.True(t, retrieved[0].WasStaged)
	assert.Equal(t, "insert", retrieved[0].ChangeType)

	assert.Equal(t, "obj-002", retrieved[1].ObjectID)
	assert.True(t, retrieved[1].WasStaged)
	assert.Equal(t, "vec-hash-1", retrieved[1].VectorHash)

	assert.Equal(t, "obj-003", retrieved[2].ObjectID)
	assert.False(t, retrieved[2].WasStaged)
	assert.Equal(t, "delete", retrieved[2].ChangeType)
}

func TestStore_DeleteStash(t *testing.T) {
	st := newTestStore(t)

	id1, err := st.CreateStash("stash one", "main", "commit-aaa")
	require.NoError(t, err)
	id2, err := st.CreateStash("stash two", "main", "commit-bbb")
	require.NoError(t, err)

	// Add changes to both
	require.NoError(t, st.CreateStashChange(&models.StashChange{
		StashID: id1, ClassName: "A", ObjectID: "1", ChangeType: "insert",
	}))
	require.NoError(t, st.CreateStashChange(&models.StashChange{
		StashID: id2, ClassName: "B", ObjectID: "2", ChangeType: "insert",
	}))

	// Delete stash 1
	require.NoError(t, st.DeleteStash(id1))

	// Only stash 2 remains
	stashes, err := st.ListStashes()
	require.NoError(t, err)
	require.Len(t, stashes, 1)
	assert.Equal(t, id2, stashes[0].ID)

	// Stash 1 changes gone
	changes, err := st.GetStashChanges(id1)
	require.NoError(t, err)
	assert.Len(t, changes, 0)

	// Stash 2 changes intact
	changes, err = st.GetStashChanges(id2)
	require.NoError(t, err)
	assert.Len(t, changes, 1)
}

func TestStore_DeleteAllStashes(t *testing.T) {
	st := newTestStore(t)

	id1, err := st.CreateStash("stash one", "main", "commit-aaa")
	require.NoError(t, err)
	id2, err := st.CreateStash("stash two", "main", "commit-bbb")
	require.NoError(t, err)

	require.NoError(t, st.CreateStashChange(&models.StashChange{
		StashID: id1, ClassName: "A", ObjectID: "1", ChangeType: "insert",
	}))
	require.NoError(t, st.CreateStashChange(&models.StashChange{
		StashID: id2, ClassName: "B", ObjectID: "2", ChangeType: "insert",
	}))

	require.NoError(t, st.DeleteAllStashes())

	stashes, err := st.ListStashes()
	require.NoError(t, err)
	assert.Len(t, stashes, 0)

	count, err := st.GetStashCount()
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestStore_GetStashByIndex_OutOfRange(t *testing.T) {
	st := newTestStore(t)

	_, err := st.CreateStash("only stash", "main", "commit-aaa")
	require.NoError(t, err)

	s, err := st.GetStashByIndex(5)
	require.NoError(t, err)
	assert.Nil(t, s)
}

func TestStore_GetStashCount(t *testing.T) {
	st := newTestStore(t)

	count, err := st.GetStashCount()
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	_, err = st.CreateStash("one", "main", "aaa")
	require.NoError(t, err)
	_, err = st.CreateStash("two", "main", "bbb")
	require.NoError(t, err)
	_, err = st.CreateStash("three", "main", "ccc")
	require.NoError(t, err)

	count, err = st.GetStashCount()
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}
