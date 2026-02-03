package metastore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *BboltStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test-meta.db")
	s, err := NewBboltStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func TestBboltStore_HasCommit(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	has, err := s.HasCommit(ctx, "nonexistent")
	require.NoError(t, err)
	assert.False(t, has)

	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:        "abc123",
			Message:   "test",
			Timestamp: time.Now(),
		},
	}
	require.NoError(t, s.InsertCommitBundle(ctx, bundle))

	has, err = s.HasCommit(ctx, "abc123")
	require.NoError(t, err)
	assert.True(t, has)
}

func TestBboltStore_GetCommit(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	_, err := s.GetCommit(ctx, "nonexistent")
	assert.ErrorIs(t, err, ErrNotFound)

	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:        "abc123",
			ParentID:  "parent",
			Message:   "test commit",
			Timestamp: time.Now().Truncate(time.Second),
		},
	}
	require.NoError(t, s.InsertCommitBundle(ctx, bundle))

	commit, err := s.GetCommit(ctx, "abc123")
	require.NoError(t, err)
	assert.Equal(t, "abc123", commit.ID)
	assert.Equal(t, "parent", commit.ParentID)
	assert.Equal(t, "test commit", commit.Message)
}

func TestBboltStore_InsertCommitBundle_Idempotent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

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

	require.NoError(t, s.InsertCommitBundle(ctx, bundle))
	require.NoError(t, s.InsertCommitBundle(ctx, bundle)) // second call is no-op

	ops, err := s.GetOperationsByCommit(ctx, "abc123")
	require.NoError(t, err)
	assert.Len(t, ops, 1) // not duplicated
}

func TestBboltStore_InsertCommitBundle_WithSchema(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:        "abc123",
			Message:   "with schema",
			Timestamp: time.Now(),
		},
		Operations: []*models.Operation{
			{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-001"},
		},
		Schema: &remote.SchemaSnapshot{
			SchemaJSON: []byte(`{"classes":[]}`),
			SchemaHash: "schemahash",
		},
	}

	require.NoError(t, s.InsertCommitBundle(ctx, bundle))

	result, err := s.GetCommitBundle(ctx, "abc123")
	require.NoError(t, err)
	require.NotNil(t, result.Schema)
	assert.Equal(t, `{"classes":[]}`, string(result.Schema.SchemaJSON))
	assert.Equal(t, "schemahash", result.Schema.SchemaHash)
}

func TestBboltStore_GetCommitBundle(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	_, err := s.GetCommitBundle(ctx, "nonexistent")
	assert.ErrorIs(t, err, ErrNotFound)

	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:        "abc123",
			Message:   "test",
			Timestamp: time.Now(),
		},
		Operations: []*models.Operation{
			{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-001"},
			{Type: models.OperationUpdate, ClassName: "Article", ObjectID: "obj-002"},
		},
	}
	require.NoError(t, s.InsertCommitBundle(ctx, bundle))

	result, err := s.GetCommitBundle(ctx, "abc123")
	require.NoError(t, err)
	assert.Equal(t, "abc123", result.Commit.ID)
	assert.Len(t, result.Operations, 2)
	assert.Equal(t, "obj-001", result.Operations[0].ObjectID)
	assert.Equal(t, "obj-002", result.Operations[1].ObjectID)
}

func TestBboltStore_GetAncestors(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// Create a chain: c3 -> c2 -> c1
	for _, b := range []*remote.CommitBundle{
		{Commit: &models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}},
		{Commit: &models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: time.Now()}},
		{Commit: &models.Commit{ID: "c3", ParentID: "c2", Message: "third", Timestamp: time.Now()}},
	} {
		require.NoError(t, s.InsertCommitBundle(ctx, b))
	}

	ancestors, err := s.GetAncestors(ctx, "c3")
	require.NoError(t, err)
	assert.True(t, ancestors["c3"])
	assert.True(t, ancestors["c2"])
	assert.True(t, ancestors["c1"])
	assert.Len(t, ancestors, 3)
}

func TestBboltStore_GetCommitCount(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	count, err := s.GetCommitCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	for i := 0; i < 3; i++ {
		bundle := &remote.CommitBundle{
			Commit: &models.Commit{
				ID:        fmt.Sprintf("commit-%d", i),
				Message:   "test",
				Timestamp: time.Now(),
			},
		}
		require.NoError(t, s.InsertCommitBundle(ctx, bundle))
	}

	count, err = s.GetCommitCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestBboltStore_Branches(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// List empty
	branches, err := s.ListBranches(ctx)
	require.NoError(t, err)
	assert.Len(t, branches, 0)

	// Create
	require.NoError(t, s.CreateBranch(ctx, "main", "abc123"))
	require.NoError(t, s.CreateBranch(ctx, "develop", "def456"))

	// List
	branches, err = s.ListBranches(ctx)
	require.NoError(t, err)
	require.Len(t, branches, 2)
	assert.Equal(t, "develop", branches[0].Name) // sorted
	assert.Equal(t, "main", branches[1].Name)

	// Get
	branch, err := s.GetBranch(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, "abc123", branch.CommitID)

	// Get not found
	_, err = s.GetBranch(ctx, "nonexistent")
	assert.ErrorIs(t, err, ErrNotFound)

	// Delete
	require.NoError(t, s.DeleteBranch(ctx, "develop"))
	branches, err = s.ListBranches(ctx)
	require.NoError(t, err)
	assert.Len(t, branches, 1)
}

func TestBboltStore_UpdateBranchCAS(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// Create via CAS with empty expected
	require.NoError(t, s.UpdateBranchCAS(ctx, "main", "abc123", ""))

	branch, err := s.GetBranch(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, "abc123", branch.CommitID)

	// CAS success
	require.NoError(t, s.UpdateBranchCAS(ctx, "main", "def456", "abc123"))

	branch, err = s.GetBranch(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, "def456", branch.CommitID)

	// CAS conflict
	err = s.UpdateBranchCAS(ctx, "main", "ghi789", "abc123") // wrong expected
	assert.ErrorIs(t, err, ErrConflict)

	// Branch still at def456
	branch, err = s.GetBranch(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, "def456", branch.CommitID)
}

func TestBboltStore_GetAllVectorHashes(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// Empty store
	hashes, err := s.GetAllVectorHashes(ctx)
	require.NoError(t, err)
	assert.Empty(t, hashes)

	// Insert commits with vector hashes
	bundle1 := &remote.CommitBundle{
		Commit: &models.Commit{ID: "c1", Message: "first"},
		Operations: []*models.Operation{
			{Seq: 0, Type: models.OperationInsert, ClassName: "Test", ObjectID: "o1", VectorHash: "hash1"},
			{Seq: 1, Type: models.OperationInsert, ClassName: "Test", ObjectID: "o2", VectorHash: "hash2"},
		},
	}
	bundle2 := &remote.CommitBundle{
		Commit: &models.Commit{ID: "c2", ParentID: "c1", Message: "second"},
		Operations: []*models.Operation{
			{Seq: 0, Type: models.OperationInsert, ClassName: "Test", ObjectID: "o3", VectorHash: "hash1"}, // duplicate
			{Seq: 1, Type: models.OperationInsert, ClassName: "Test", ObjectID: "o4", VectorHash: "hash3"},
			{Seq: 2, Type: models.OperationDelete, ClassName: "Test", ObjectID: "o5"}, // no vector
		},
	}
	require.NoError(t, s.InsertCommitBundle(ctx, bundle1))
	require.NoError(t, s.InsertCommitBundle(ctx, bundle2))

	hashes, err = s.GetAllVectorHashes(ctx)
	require.NoError(t, err)
	assert.Len(t, hashes, 3) // hash1, hash2, hash3 (deduplicated)
	assert.True(t, hashes["hash1"])
	assert.True(t, hashes["hash2"])
	assert.True(t, hashes["hash3"])
}

func TestBboltStore_UpdateBranchCAS_NonExistentWithExpected(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	err := s.UpdateBranchCAS(ctx, "main", "abc123", "some-expected")
	assert.ErrorIs(t, err, ErrConflict)
}
