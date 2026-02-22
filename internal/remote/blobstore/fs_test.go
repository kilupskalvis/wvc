package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *FSStore {
	t.Helper()
	s, err := NewFSStore(t.TempDir())
	require.NoError(t, err)
	return s
}

func hashBytes(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func TestFSStore_PutAndGet(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	data := []byte("test vector data")
	hash := hashBytes(data)
	dims := 4

	err := s.Put(ctx, hash, bytes.NewReader(data), dims)
	require.NoError(t, err)

	reader, gotDims, err := s.Get(ctx, hash)
	require.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, dims, gotDims)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestFSStore_Has(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	has, err := s.Has(ctx, "nonexistent")
	require.NoError(t, err)
	assert.False(t, has)

	data := []byte("test")
	hash := hashBytes(data)
	require.NoError(t, s.Put(ctx, hash, bytes.NewReader(data), 1))

	has, err = s.Has(ctx, hash)
	require.NoError(t, err)
	assert.True(t, has)
}

func TestFSStore_Put_Idempotent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	data := []byte("test")
	hash := hashBytes(data)

	require.NoError(t, s.Put(ctx, hash, bytes.NewReader(data), 1))
	require.NoError(t, s.Put(ctx, hash, bytes.NewReader(data), 1)) // no-op
}

func TestFSStore_Put_HashMismatch(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	data := []byte("test")
	wrongHash := "0000000000000000000000000000000000000000000000000000000000000000"

	err := s.Put(ctx, wrongHash, bytes.NewReader(data), 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hash mismatch")
}

func TestFSStore_Get_NotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	_, _, err := s.Get(ctx, "nonexistent")
	assert.ErrorIs(t, err, ErrBlobNotFound)
}

func TestFSStore_Delete(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	data := []byte("test")
	hash := hashBytes(data)
	require.NoError(t, s.Put(ctx, hash, bytes.NewReader(data), 1))

	err := s.Delete(ctx, hash)
	require.NoError(t, err)

	has, err := s.Has(ctx, hash)
	require.NoError(t, err)
	assert.False(t, has)
}

func TestFSStore_Delete_NotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// Should not error when deleting non-existent blob
	err := s.Delete(ctx, "nonexistent")
	assert.NoError(t, err)
}

func TestFSStore_TotalCount(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	count, err := s.TotalCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	for i := 0; i < 3; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		hash := hashBytes(data)
		require.NoError(t, s.Put(ctx, hash, bytes.NewReader(data), 1))
	}

	count, err = s.TotalCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}
