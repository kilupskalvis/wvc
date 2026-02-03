package blobstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSStore_ListHashes_Empty(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	hashes, err := s.ListHashes(ctx)
	require.NoError(t, err)
	assert.Empty(t, hashes)
}

func TestFSStore_ListHashes(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	var expected []string
	for i := 0; i < 3; i++ {
		data := []byte{byte(i), byte(i + 10), byte(i + 20)}
		hash := hashBytes(data)
		require.NoError(t, s.Put(ctx, hash, bytes.NewReader(data), 1))
		expected = append(expected, hash)
	}

	hashes, err := s.ListHashes(ctx)
	require.NoError(t, err)
	assert.Len(t, hashes, 3)

	for _, exp := range expected {
		assert.Contains(t, hashes, exp)
	}
}

func TestFSStore_ListHashes_AfterDelete(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	data1 := []byte("blob1")
	hash1 := hashBytes(data1)
	data2 := []byte("blob2")
	hash2 := hashBytes(data2)

	require.NoError(t, s.Put(ctx, hash1, bytes.NewReader(data1), 1))
	require.NoError(t, s.Put(ctx, hash2, bytes.NewReader(data2), 1))

	require.NoError(t, s.Delete(ctx, hash1))

	hashes, err := s.ListHashes(ctx)
	require.NoError(t, err)
	assert.Len(t, hashes, 1)
	assert.Equal(t, hash2, hashes[0])
}
