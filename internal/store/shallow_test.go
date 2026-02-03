package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShallowCommit_MarkAndCheck(t *testing.T) {
	st := newTestStore(t)

	// Not shallow initially
	shallow, err := st.IsShallowCommit("abc123")
	require.NoError(t, err)
	assert.False(t, shallow)

	// Mark as shallow
	require.NoError(t, st.MarkShallowCommit("abc123"))

	shallow, err = st.IsShallowCommit("abc123")
	require.NoError(t, err)
	assert.True(t, shallow)
}

func TestShallowCommit_List(t *testing.T) {
	st := newTestStore(t)

	ids, err := st.ListShallowCommits()
	require.NoError(t, err)
	assert.Empty(t, ids)

	require.NoError(t, st.MarkShallowCommit("commit1"))
	require.NoError(t, st.MarkShallowCommit("commit2"))

	ids, err = st.ListShallowCommits()
	require.NoError(t, err)
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, "commit1")
	assert.Contains(t, ids, "commit2")
}

func TestShallowCommit_Remove(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.MarkShallowCommit("abc123"))

	shallow, err := st.IsShallowCommit("abc123")
	require.NoError(t, err)
	assert.True(t, shallow)

	require.NoError(t, st.RemoveShallowCommit("abc123"))

	shallow, err = st.IsShallowCommit("abc123")
	require.NoError(t, err)
	assert.False(t, shallow)
}

func TestShallowCommit_MarkIdempotent(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.MarkShallowCommit("abc123"))
	require.NoError(t, st.MarkShallowCommit("abc123")) // no error on double mark

	ids, err := st.ListShallowCommits()
	require.NoError(t, err)
	assert.Len(t, ids, 1)
}

func TestShallowCommit_RemoveNonexistent(t *testing.T) {
	st := newTestStore(t)

	// Should not error
	err := st.RemoveShallowCommit("nonexistent")
	assert.NoError(t, err)
}
