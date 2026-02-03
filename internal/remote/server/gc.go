package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/kilupskalvis/wvc/internal/remote/blobstore"
	"github.com/kilupskalvis/wvc/internal/remote/metastore"
)

// GCResult contains the outcome of a garbage collection run.
type GCResult struct {
	BlobsScanned    int
	BlobsDeleted    int
	ReferencedBlobs int
}

// GarbageCollect removes blobs not referenced by any operation in the metastore.
func GarbageCollect(ctx context.Context, meta metastore.MetaStore, blobs blobstore.BlobStore, logger *slog.Logger) (*GCResult, error) {
	result := &GCResult{}

	// Collect all referenced vector hashes
	referenced, err := meta.GetAllVectorHashes(ctx)
	if err != nil {
		return nil, fmt.Errorf("get referenced hashes: %w", err)
	}
	result.ReferencedBlobs = len(referenced)

	// List all blobs in the store
	allHashes, err := blobs.ListHashes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list blob hashes: %w", err)
	}
	result.BlobsScanned = len(allHashes)

	// Delete unreferenced blobs
	for _, hash := range allHashes {
		if referenced[hash] {
			continue
		}
		if err := blobs.Delete(ctx, hash); err != nil {
			logger.Warn("gc: failed to delete blob", "hash", hash, "error", err)
			continue
		}
		result.BlobsDeleted++
	}

	logger.Info("gc complete",
		"scanned", result.BlobsScanned,
		"referenced", result.ReferencedBlobs,
		"deleted", result.BlobsDeleted,
	)

	return result, nil
}
