package core

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/kilupskalvis/wvc/internal/store"
	"golang.org/x/sync/errgroup"
)

// FetchOptions configures a fetch operation.
type FetchOptions struct {
	RemoteName string
	Branch     string
	Depth      int
}

// FetchResult contains the outcome of a fetch operation.
type FetchResult struct {
	CommitsFetched int
	VectorsFetched int
	UpToDate       bool
	RemoteTip      string
	LocalTip       string
}

// PullOptions configures a pull operation.
type PullOptions struct {
	RemoteName string
	Branch     string
	Depth      int
}

// PullResult contains the outcome of a pull operation.
type PullResult struct {
	FetchResult
	FastForward bool
	Diverged    bool
}

// FetchProgress is called during fetch to report progress.
type FetchProgress func(phase string, current, total int)

// Fetch downloads commits and vectors from a remote without merging.
// It updates the remote-tracking branch but does not modify the local branch.
func Fetch(ctx context.Context, st *store.Store, client remote.RemoteClient, opts FetchOptions, progress FetchProgress) (*FetchResult, error) {
	if progress == nil {
		progress = func(string, int, int) {}
	}

	// Get local tip for this remote branch
	localTip := ""
	rb, err := st.GetRemoteBranch(opts.RemoteName, opts.Branch)
	if err != nil {
		return nil, fmt.Errorf("get remote branch: %w", err)
	}
	if rb != nil {
		localTip = rb.CommitID
	}

	// Negotiate with server
	progress("negotiating", 0, 0)
	negotiation, err := client.NegotiatePull(ctx, opts.Branch, localTip, opts.Depth)
	if err != nil {
		return nil, fmt.Errorf("negotiate pull: %w", err)
	}

	if len(negotiation.MissingCommits) == 0 {
		return &FetchResult{
			UpToDate:  true,
			RemoteTip: negotiation.RemoteTip,
			LocalTip:  localTip,
		}, nil
	}

	// Phase 1: Download all commit bundles into memory (don't persist yet).
	// This ensures that if anything fails during download, the local store
	// remains untouched and consistent.
	progress("downloading commits", 0, len(negotiation.MissingCommits))
	bundles := make([]*remote.CommitBundle, 0, len(negotiation.MissingCommits))
	var allVectorHashes []string
	for i, commitID := range negotiation.MissingCommits {
		progress("downloading commits", i+1, len(negotiation.MissingCommits))

		bundle, err := client.DownloadCommitBundle(ctx, commitID)
		if err != nil {
			return nil, fmt.Errorf("download commit %s: %w", commitID, err)
		}
		bundles = append(bundles, bundle)

		// Collect vector hashes from operations
		for _, op := range bundle.Operations {
			if op.VectorHash != "" {
				allVectorHashes = append(allVectorHashes, op.VectorHash)
			}
		}
	}

	// Phase 2: Download missing vectors BEFORE inserting any commits.
	// If vector download fails, no commits have been persisted, so the store
	// remains in a consistent state. Any already-downloaded vectors are
	// content-addressable and will be reused on the next fetch attempt.
	var vectorsFetched int
	if len(allVectorHashes) > 0 {
		// Deduplicate and filter out vectors we already have
		missingVectors, err := filterMissingLocalVectors(st, allVectorHashes)
		if err != nil {
			return nil, fmt.Errorf("filter vectors: %w", err)
		}

		if len(missingVectors) > 0 {
			progress("downloading vectors", 0, len(missingVectors))
			vectorsFetched, err = downloadMissingVectors(ctx, st, client, missingVectors, progress)
			if err != nil {
				return nil, fmt.Errorf("download vectors: %w", err)
			}
		}
	}

	// Phase 3: Now that all vectors are present locally, insert commit bundles.
	// Each InsertCommitBundle call is individually atomic (single bbolt transaction).
	progress("storing commits", 0, len(bundles))
	for i, bundle := range bundles {
		progress("storing commits", i+1, len(bundles))
		if err := st.InsertCommitBundle(bundle); err != nil {
			return nil, fmt.Errorf("store commit %s: %w", bundle.Commit.ID, err)
		}
	}

	// Mark shallow boundary commits when using depth-limited fetch
	if opts.Depth > 0 && len(negotiation.MissingCommits) > 0 {
		// The oldest fetched commit's parents are the shallow boundary
		oldestID := negotiation.MissingCommits[0]
		oldest, err := st.GetCommit(oldestID)
		if err == nil && oldest != nil {
			if oldest.ParentID != "" {
				has, _ := st.HasCommit(oldest.ParentID)
				if !has {
					if err := st.MarkShallowCommit(oldestID); err != nil {
						return nil, fmt.Errorf("mark shallow commit: %w", err)
					}
				}
			}
		}
	}

	// Update remote-tracking branch
	if err := st.SetRemoteBranch(opts.RemoteName, opts.Branch, negotiation.RemoteTip); err != nil {
		return nil, fmt.Errorf("update remote-tracking branch: %w", err)
	}

	return &FetchResult{
		CommitsFetched: len(negotiation.MissingCommits),
		VectorsFetched: vectorsFetched,
		RemoteTip:      negotiation.RemoteTip,
		LocalTip:       localTip,
	}, nil
}

// Pull fetches from a remote and attempts to fast-forward the local branch.
// If the branches have diverged, it reports divergence without merging.
func Pull(ctx context.Context, st *store.Store, client remote.RemoteClient, opts PullOptions, progress FetchProgress) (*PullResult, error) {
	// Check for uncommitted changes
	uncommitted, err := st.GetUncommittedOperations()
	if err != nil {
		return nil, fmt.Errorf("check uncommitted operations: %w", err)
	}
	if len(uncommitted) > 0 {
		return nil, fmt.Errorf("cannot pull with uncommitted changes; commit or stash them first")
	}

	// Fetch first
	fetchResult, err := Fetch(ctx, st, client, FetchOptions(opts), progress)
	if err != nil {
		return nil, err
	}

	result := &PullResult{
		FetchResult: *fetchResult,
	}

	if fetchResult.UpToDate {
		return result, nil
	}

	// Check if we can fast-forward the local branch
	localBranch, err := st.GetBranch(opts.Branch)
	if err != nil {
		return nil, fmt.Errorf("get local branch: %w", err)
	}

	if localBranch == nil {
		// Local branch doesn't exist (unborn) — create it pointing to remote tip
		if err := st.CreateBranchAndHEAD(opts.Branch, fetchResult.RemoteTip); err != nil {
			return nil, fmt.Errorf("create local branch: %w", err)
		}
		result.FastForward = true
		return result, nil
	}

	localTip := localBranch.CommitID

	// If local branch has no commits yet, fast-forward to remote tip
	if localTip == "" {
		currentBranch, err := st.GetCurrentBranch()
		if err == nil && currentBranch == opts.Branch {
			if err := st.UpdateBranchAndHEAD(opts.Branch, fetchResult.RemoteTip); err != nil {
				return nil, fmt.Errorf("update local branch: %w", err)
			}
		} else {
			if err := st.UpdateBranch(opts.Branch, fetchResult.RemoteTip); err != nil {
				return nil, fmt.Errorf("update local branch: %w", err)
			}
		}
		result.FastForward = true
		return result, nil
	}

	// If local tip equals remote tip, we're up to date
	if localTip == fetchResult.RemoteTip {
		result.UpToDate = true
		return result, nil
	}

	// Check if remote tip is a descendant of local tip (fast-forward possible)
	remoteAncestors, err := st.GetAllAncestors(fetchResult.RemoteTip)
	if err != nil {
		return nil, fmt.Errorf("get remote ancestors: %w", err)
	}

	if remoteAncestors[localTip] {
		// Fast-forward: local tip is an ancestor of remote tip
		currentBranch, err := st.GetCurrentBranch()
		if err == nil && currentBranch == opts.Branch {
			if err := st.UpdateBranchAndHEAD(opts.Branch, fetchResult.RemoteTip); err != nil {
				return nil, fmt.Errorf("update local branch: %w", err)
			}
		} else {
			if err := st.UpdateBranch(opts.Branch, fetchResult.RemoteTip); err != nil {
				return nil, fmt.Errorf("update local branch: %w", err)
			}
		}

		result.FastForward = true
		return result, nil
	}

	// Check if local tip is a descendant of remote tip (we're ahead)
	localAncestors, err := st.GetAllAncestors(localTip)
	if err != nil {
		return nil, fmt.Errorf("get local ancestors: %w", err)
	}

	if localAncestors[fetchResult.RemoteTip] {
		// Local is ahead — nothing to do for the branch
		result.UpToDate = true
		return result, nil
	}

	// Branches have diverged
	result.Diverged = true
	return result, nil
}

// filterMissingLocalVectors returns hashes of vectors not present in the local store.
func filterMissingLocalVectors(st *store.Store, hashes []string) ([]string, error) {
	seen := make(map[string]bool)
	var missing []string

	for _, hash := range hashes {
		if seen[hash] {
			continue
		}
		seen[hash] = true

		_, _, err := st.GetVectorBlob(hash)
		if err != nil {
			if errors.Is(err, store.ErrVectorNotFound) {
				missing = append(missing, hash)
			} else {
				return nil, fmt.Errorf("check local vector %s: %w", hash, err)
			}
		}
	}

	return missing, nil
}

// downloadMissingVectors downloads vector blobs in parallel with bounded concurrency.
func downloadMissingVectors(ctx context.Context, st *store.Store, client remote.RemoteClient, missingHashes []string, progress FetchProgress) (int, error) {
	const maxWorkers = 4

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(maxWorkers)

	for i, hash := range missingHashes {
		progress("downloading vectors", i+1, len(missingHashes))
		h := hash
		g.Go(func() error {
			reader, dims, err := client.DownloadVector(ctx, h)
			if err != nil {
				return fmt.Errorf("download vector %s: %w", h, err)
			}
			defer reader.Close()

			data, err := io.ReadAll(reader)
			if err != nil {
				return fmt.Errorf("read vector %s: %w", h, err)
			}

			// Verify hash
			computed := store.HashVector(data)
			if computed != h {
				return fmt.Errorf("vector hash mismatch for %s: got %s", h, computed)
			}

			// Store locally
			if _, err := st.SaveVectorBlob(data, dims); err != nil {
				return fmt.Errorf("save vector %s: %w", h, err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	return len(missingHashes), nil
}
