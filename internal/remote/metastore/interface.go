// Package metastore provides the server-side metadata storage abstraction.
package metastore

import (
	"context"
	"errors"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/remote"
)

// Sentinel errors for expected conditions.
var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

// MetaStore defines the contract for server-side metadata persistence.
type MetaStore interface {
	// Commits
	HasCommit(ctx context.Context, id string) (bool, error)
	GetCommit(ctx context.Context, id string) (*models.Commit, error)
	InsertCommitBundle(ctx context.Context, b *remote.CommitBundle) error
	GetCommitBundle(ctx context.Context, id string) (*remote.CommitBundle, error)
	GetAncestors(ctx context.Context, id string) (map[string]bool, error)
	GetCommitCount(ctx context.Context) (int, error)

	// Branches
	ListBranches(ctx context.Context) ([]*models.Branch, error)
	GetBranch(ctx context.Context, name string) (*models.Branch, error)
	CreateBranch(ctx context.Context, name, commitID string) error
	UpdateBranchCAS(ctx context.Context, name, newCommitID, expectedCommitID string) error
	DeleteBranch(ctx context.Context, name string) error

	// Operations
	GetOperationsByCommit(ctx context.Context, commitID string) ([]*models.Operation, error)

	// GetAllVectorHashes returns all unique vector hashes referenced by operations.
	GetAllVectorHashes(ctx context.Context) (map[string]bool, error)

	// Close releases resources.
	Close() error
}
