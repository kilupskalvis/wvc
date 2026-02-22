package store

import (
	"testing"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertCommitBundle_Basic(t *testing.T) {
	st := newTestStore(t)

	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:        "abc123",
			Message:   "test commit",
			Timestamp: time.Now().Truncate(time.Second),
		},
		Operations: []*models.Operation{
			{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-001"},
			{Type: models.OperationUpdate, ClassName: "Article", ObjectID: "obj-002"},
		},
	}

	require.NoError(t, st.InsertCommitBundle(bundle))

	// Verify commit was stored
	commit, err := st.GetCommit("abc123")
	require.NoError(t, err)
	assert.Equal(t, "abc123", commit.ID)
	assert.Equal(t, "test commit", commit.Message)

	// Verify operations were stored
	ops, err := st.GetOperationsByCommit("abc123")
	require.NoError(t, err)
	require.Len(t, ops, 2)
	assert.Equal(t, "obj-001", ops[0].ObjectID)
	assert.Equal(t, "obj-002", ops[1].ObjectID)
	assert.Equal(t, "abc123", ops[0].CommitID)
	assert.Equal(t, 0, ops[0].Seq)
	assert.Equal(t, 1, ops[1].Seq)
}

func TestInsertCommitBundle_WithSchema(t *testing.T) {
	st := newTestStore(t)

	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:        "abc123",
			Message:   "with schema",
			Timestamp: time.Now(),
		},
		Schema: &remote.SchemaSnapshot{
			SchemaJSON: []byte(`{"classes":[]}`),
			SchemaHash: "schemahash123",
		},
	}

	require.NoError(t, st.InsertCommitBundle(bundle))

	// Verify schema was stored
	sv, err := st.GetSchemaVersionByCommit("abc123")
	require.NoError(t, err)
	require.NotNil(t, sv)
	assert.Equal(t, `{"classes":[]}`, string(sv.SchemaJSON))
	assert.Equal(t, "schemahash123", sv.SchemaHash)
	assert.Equal(t, "abc123", sv.CommitID)
}

func TestInsertCommitBundle_Idempotent(t *testing.T) {
	st := newTestStore(t)

	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:        "abc123",
			Message:   "test",
			Timestamp: time.Now(),
		},
		Operations: []*models.Operation{
			{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-001"},
		},
	}

	require.NoError(t, st.InsertCommitBundle(bundle))
	require.NoError(t, st.InsertCommitBundle(bundle)) // no-op

	ops, err := st.GetOperationsByCommit("abc123")
	require.NoError(t, err)
	assert.Len(t, ops, 1) // not duplicated
}

func TestInsertCommitBundle_NilBundle(t *testing.T) {
	st := newTestStore(t)
	assert.Error(t, st.InsertCommitBundle(nil))
}

func TestInsertCommitBundle_NilCommit(t *testing.T) {
	st := newTestStore(t)
	assert.Error(t, st.InsertCommitBundle(&remote.CommitBundle{}))
}

func TestInsertCommitBundle_Chain(t *testing.T) {
	st := newTestStore(t)

	// Insert a chain of commits
	for _, b := range []*remote.CommitBundle{
		{Commit: &models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}},
		{Commit: &models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: time.Now()}},
		{Commit: &models.Commit{ID: "c3", ParentID: "c2", Message: "third", Timestamp: time.Now()}},
	} {
		require.NoError(t, st.InsertCommitBundle(b))
	}

	// Verify chain
	c3, err := st.GetCommit("c3")
	require.NoError(t, err)
	assert.Equal(t, "c2", c3.ParentID)

	c2, err := st.GetCommit("c2")
	require.NoError(t, err)
	assert.Equal(t, "c1", c2.ParentID)

	// Verify ancestors
	ancestors, err := st.GetAllAncestors("c3")
	require.NoError(t, err)
	assert.True(t, ancestors["c3"])
	assert.True(t, ancestors["c2"])
	assert.True(t, ancestors["c1"])
}
