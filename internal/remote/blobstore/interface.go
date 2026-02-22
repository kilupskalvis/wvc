// Package blobstore provides content-addressable blob storage for vector data.
package blobstore

import (
	"context"
	"errors"
	"io"
)

// ErrBlobNotFound is returned when a requested blob does not exist.
var ErrBlobNotFound = errors.New("blob not found")

// ErrHashMismatch is returned when the computed hash of blob data does not match the expected hash.
var ErrHashMismatch = errors.New("blob hash mismatch")

// BlobStore defines the contract for content-addressable binary storage.
type BlobStore interface {
	// Has checks whether a blob with the given hash exists.
	Has(ctx context.Context, hash string) (bool, error)

	// Get returns a reader for the blob data and the vector dimensions.
	// Returns ErrBlobNotFound if the blob does not exist.
	Get(ctx context.Context, hash string) (io.ReadCloser, int, error)

	// Put stores a blob. The hash is verified against the data.
	// Idempotent — storing the same blob twice is a no-op.
	Put(ctx context.Context, hash string, r io.Reader, dims int) error

	// Delete removes a blob. No error if it doesn't exist.
	Delete(ctx context.Context, hash string) error

	// TotalCount returns the number of stored blobs.
	TotalCount(ctx context.Context) (int, error)

	// ListHashes returns all blob hashes in the store.
	ListHashes(ctx context.Context) ([]string, error)
}
