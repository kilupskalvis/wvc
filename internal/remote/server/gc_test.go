package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"testing"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/kilupskalvis/wvc/internal/remote/blobstore"
	"github.com/kilupskalvis/wvc/internal/remote/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hashTestBytes(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func TestGarbageCollect_NoBlobs(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	meta, err := metastore.NewBboltStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	defer meta.Close()

	blobs, err := blobstore.NewFSStore(t.TempDir())
	require.NoError(t, err)

	result, err := GarbageCollect(ctx, meta, blobs, logger)
	require.NoError(t, err)

	assert.Equal(t, 0, result.BlobsScanned)
	assert.Equal(t, 0, result.BlobsDeleted)
	assert.Equal(t, 0, result.ReferencedBlobs)
}

func TestGarbageCollect_AllReferenced(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	meta, err := metastore.NewBboltStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	defer meta.Close()

	blobs, err := blobstore.NewFSStore(t.TempDir())
	require.NoError(t, err)

	// Create a blob
	data := []byte("vector data")
	hash := hashTestBytes(data)
	require.NoError(t, blobs.Put(ctx, hash, bytes.NewReader(data), 4))

	// Create a commit that references the blob
	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:      "commit1",
			Message: "test",
		},
		Operations: []*models.Operation{
			{Seq: 0, Type: "upsert", ClassName: "Test", VectorHash: hash},
		},
	}
	require.NoError(t, meta.InsertCommitBundle(ctx, bundle))

	result, err := GarbageCollect(ctx, meta, blobs, logger)
	require.NoError(t, err)

	assert.Equal(t, 1, result.BlobsScanned)
	assert.Equal(t, 0, result.BlobsDeleted)
	assert.Equal(t, 1, result.ReferencedBlobs)
}

func TestGarbageCollect_DeletesUnreferenced(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	meta, err := metastore.NewBboltStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	defer meta.Close()

	blobs, err := blobstore.NewFSStore(t.TempDir())
	require.NoError(t, err)

	// Create two blobs
	data1 := []byte("referenced blob")
	hash1 := hashTestBytes(data1)
	require.NoError(t, blobs.Put(ctx, hash1, bytes.NewReader(data1), 4))

	data2 := []byte("orphan blob")
	hash2 := hashTestBytes(data2)
	require.NoError(t, blobs.Put(ctx, hash2, bytes.NewReader(data2), 4))

	// Only reference hash1 in a commit
	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:      "commit1",
			Message: "test",
		},
		Operations: []*models.Operation{
			{Seq: 0, Type: "upsert", ClassName: "Test", VectorHash: hash1},
		},
	}
	require.NoError(t, meta.InsertCommitBundle(ctx, bundle))

	result, err := GarbageCollect(ctx, meta, blobs, logger)
	require.NoError(t, err)

	assert.Equal(t, 2, result.BlobsScanned)
	assert.Equal(t, 1, result.BlobsDeleted)
	assert.Equal(t, 1, result.ReferencedBlobs)

	// Verify orphan is gone
	has, err := blobs.Has(ctx, hash2)
	require.NoError(t, err)
	assert.False(t, has)

	// Verify referenced blob still exists
	has, err = blobs.Has(ctx, hash1)
	require.NoError(t, err)
	assert.True(t, has)
}
