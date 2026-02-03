package core

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/kilupskalvis/wvc/internal/store"
	"golang.org/x/sync/errgroup"
)

// PushOptions configures a push operation.
type PushOptions struct {
	RemoteName string
	Branch     string
	Force      bool
}

// PushResult contains the outcome of a push operation.
type PushResult struct {
	CommitsPushed int
	VectorsPushed int
	UpToDate      bool
	BranchCreated bool
}

// PushProgress is called during push to report progress.
type PushProgress func(phase string, current, total int)

// Push transfers local commits and vectors to a remote server.
func Push(ctx context.Context, st *store.Store, client remote.RemoteClient, opts PushOptions, progress PushProgress) (*PushResult, error) {
	if progress == nil {
		progress = func(string, int, int) {}
	}

	// Get local branch tip
	branch, err := st.GetBranch(opts.Branch)
	if err != nil {
		return nil, fmt.Errorf("get branch: %w", err)
	}
	if branch == nil {
		return nil, fmt.Errorf("branch '%s' does not exist", opts.Branch)
	}

	// Collect all commit IDs from tip to root
	commitIDs, err := collectCommitChain(st, branch.CommitID)
	if err != nil {
		return nil, fmt.Errorf("collect commit chain: %w", err)
	}

	// Negotiate with server
	progress("negotiating", 0, 0)
	negotiation, err := client.NegotiatePush(ctx, opts.Branch, commitIDs)
	if err != nil {
		return nil, fmt.Errorf("negotiate push: %w", err)
	}

	if len(negotiation.MissingCommits) == 0 {
		// Check if branch pointer needs updating
		if negotiation.RemoteTip == branch.CommitID {
			return &PushResult{UpToDate: true}, nil
		}
	}

	// Build a set of missing commit IDs for ordering
	missingSet := make(map[string]bool, len(negotiation.MissingCommits))
	for _, id := range negotiation.MissingCommits {
		missingSet[id] = true
	}

	// Collect vector hashes from missing commits
	vectorHashes := make(map[string]bool)
	var orderedMissing []string
	for _, id := range commitIDs {
		if !missingSet[id] {
			continue
		}
		orderedMissing = append(orderedMissing, id)

		ops, err := st.GetOperationsByCommit(id)
		if err != nil {
			return nil, fmt.Errorf("get operations for commit %s: %w", id, err)
		}
		for _, op := range ops {
			if op.VectorHash != "" {
				vectorHashes[op.VectorHash] = true
			}
		}
	}

	// Check which vectors are missing on server
	var vectorsPushed int
	if len(vectorHashes) > 0 {
		hashes := make([]string, 0, len(vectorHashes))
		for h := range vectorHashes {
			hashes = append(hashes, h)
		}

		progress("checking vectors", 0, len(hashes))
		vecCheck, err := client.CheckVectors(ctx, hashes)
		if err != nil {
			return nil, fmt.Errorf("check vectors: %w", err)
		}

		// Upload missing vectors in parallel
		if len(vecCheck.Missing) > 0 {
			vectorsPushed, err = uploadMissingVectors(ctx, st, client, vecCheck.Missing, progress)
			if err != nil {
				return nil, fmt.Errorf("upload vectors: %w", err)
			}
		}
	}

	// Reverse to get topological order (oldest first — parents before children)
	for i, j := 0, len(orderedMissing)-1; i < j; i, j = i+1, j-1 {
		orderedMissing[i], orderedMissing[j] = orderedMissing[j], orderedMissing[i]
	}

	// Upload commits in topological order (oldest first)
	progress("uploading commits", 0, len(orderedMissing))
	for i, commitID := range orderedMissing {
		progress("uploading commits", i+1, len(orderedMissing))

		bundle, err := buildCommitBundle(st, commitID)
		if err != nil {
			return nil, fmt.Errorf("build commit bundle for %s: %w", commitID, err)
		}

		if err := client.UploadCommitBundle(ctx, bundle); err != nil {
			return nil, fmt.Errorf("upload commit %s: %w", commitID, err)
		}
	}

	// Update branch pointer (CAS)
	expectedTip := negotiation.RemoteTip
	if negotiation.RemoteTip != "" && !opts.Force {
		remoteIsAncestor := false
		for _, id := range commitIDs {
			if id == negotiation.RemoteTip {
				remoteIsAncestor = true
				break
			}
		}
		if !remoteIsAncestor {
			tip := negotiation.RemoteTip
			if len(tip) > 8 {
				tip = tip[:8]
			}
			return nil, fmt.Errorf("push rejected: remote has diverged (tip %s not in local history); pull first or use --force", tip)
		}
	}

	progress("updating branch", 0, 0)
	branchCreated := negotiation.RemoteTip == ""
	if err := client.UpdateBranch(ctx, opts.Branch, branch.CommitID, expectedTip); err != nil {
		return nil, fmt.Errorf("update remote branch: %w", err)
	}

	// Update remote-tracking branch locally
	if err := st.SetRemoteBranch(opts.RemoteName, opts.Branch, branch.CommitID); err != nil {
		return nil, fmt.Errorf("update remote-tracking branch: %w", err)
	}

	return &PushResult{
		CommitsPushed: len(orderedMissing),
		VectorsPushed: vectorsPushed,
		BranchCreated: branchCreated,
	}, nil
}

// collectCommitChain walks from tip to root and returns commit IDs in tip-first order.
func collectCommitChain(st *store.Store, tipID string) ([]string, error) {
	var chain []string
	visited := make(map[string]bool)
	queue := []string{tipID}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current == "" || visited[current] {
			continue
		}
		visited[current] = true
		chain = append(chain, current)

		commit, err := st.GetCommit(current)
		if err != nil {
			return nil, fmt.Errorf("get commit %s: %w", current, err)
		}

		if commit.ParentID != "" {
			queue = append(queue, commit.ParentID)
		}
		if commit.MergeParentID != "" {
			queue = append(queue, commit.MergeParentID)
		}
	}

	return chain, nil
}

// uploadMissingVectors uploads vector blobs in parallel with bounded concurrency.
func uploadMissingVectors(ctx context.Context, st *store.Store, client remote.RemoteClient, missingHashes []string, progress PushProgress) (int, error) {
	const maxWorkers = 4

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(maxWorkers)

	for i, hash := range missingHashes {
		progress("uploading vectors", i+1, len(missingHashes))
		h := hash
		g.Go(func() error {
			data, dims, err := st.GetVectorBlob(h)
			if err != nil {
				return fmt.Errorf("get local vector %s: %w", h, err)
			}

			reader := io.NopCloser(bytes.NewReader(data))
			if err := client.UploadVector(ctx, h, reader, dims); err != nil {
				return fmt.Errorf("upload vector %s: %w", h, err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	return len(missingHashes), nil
}

// buildCommitBundle creates a CommitBundle from local store data.
func buildCommitBundle(st *store.Store, commitID string) (*remote.CommitBundle, error) {
	commit, err := st.GetCommit(commitID)
	if err != nil {
		return nil, fmt.Errorf("get commit: %w", err)
	}

	ops, err := st.GetOperationsByCommit(commitID)
	if err != nil {
		return nil, fmt.Errorf("get operations: %w", err)
	}

	bundle := &remote.CommitBundle{
		Commit:     commit,
		Operations: ops,
	}

	// Include schema if available
	sv, err := st.GetSchemaVersionByCommit(commitID)
	if err == nil && sv != nil {
		bundle.Schema = &remote.SchemaSnapshot{
			SchemaJSON: sv.SchemaJSON,
			SchemaHash: sv.SchemaHash,
		}
	}

	return bundle, nil
}

// DeleteRemoteBranch deletes a branch on the remote server.
func DeleteRemoteBranch(ctx context.Context, st *store.Store, client remote.RemoteClient, remoteName, branch string) error {
	if err := client.DeleteBranch(ctx, branch); err != nil {
		return fmt.Errorf("delete remote branch: %w", err)
	}

	// Remove local remote-tracking branch
	return st.DeleteRemoteBranch(remoteName, branch)
}

// ResolveRemoteAndBranch resolves default remote and branch names.
func ResolveRemoteAndBranch(st *store.Store, remoteName, branch string) (string, string, error) {
	// Default remote
	if remoteName == "" {
		remotes, err := st.ListRemotes()
		if err != nil {
			return "", "", fmt.Errorf("list remotes: %w", err)
		}
		if len(remotes) == 0 {
			return "", "", fmt.Errorf("no remotes configured — add one with 'wvc remote add'")
		}
		if len(remotes) == 1 {
			remoteName = remotes[0].Name
		} else {
			return "", "", fmt.Errorf("multiple remotes configured — specify which with 'wvc push <remote>'")
		}
	}

	// Verify remote exists
	r, err := st.GetRemote(remoteName)
	if err != nil {
		return "", "", fmt.Errorf("get remote: %w", err)
	}
	if r == nil {
		return "", "", fmt.Errorf("remote '%s' does not exist", remoteName)
	}

	// Default branch
	if branch == "" {
		branch, err = st.GetCurrentBranch()
		if err != nil {
			return "", "", fmt.Errorf("get current branch: %w", err)
		}
		if branch == "" {
			return "", "", fmt.Errorf("not on any branch — specify branch name explicitly")
		}
	}

	return remoteName, branch, nil
}
