package core

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pushMockClient implements remote.RemoteClient with tracking for push tests.
type pushMockClient struct {
	mu sync.Mutex

	// Negotiate push
	negotiatePushResp *remote.NegotiatePushResponse
	negotiatePushErr  error
	negotiatePushArgs struct {
		branch    string
		commitIDs []string
	}

	// Vectors
	vectorCheckResp *remote.VectorCheckResponse
	uploadedVectors map[string]int // hash -> dims
	uploadVectorErr error

	// Commits
	uploadedBundles []*remote.CommitBundle
	uploadBundleErr error

	// Branch
	updateBranchErr  error
	updateBranchArgs struct {
		branch      string
		newTip      string
		expectedTip string
	}
}

func newPushMockClient() *pushMockClient {
	return &pushMockClient{
		uploadedVectors: make(map[string]int),
	}
}

func (m *pushMockClient) NegotiatePush(_ context.Context, branch string, commitIDs []string) (*remote.NegotiatePushResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.negotiatePushArgs.branch = branch
	m.negotiatePushArgs.commitIDs = commitIDs
	return m.negotiatePushResp, m.negotiatePushErr
}

func (m *pushMockClient) NegotiatePull(_ context.Context, _ string, _ string, _ int) (*remote.NegotiatePullResponse, error) {
	return nil, nil
}

func (m *pushMockClient) CheckVectors(_ context.Context, hashes []string) (*remote.VectorCheckResponse, error) {
	if m.vectorCheckResp != nil {
		return m.vectorCheckResp, nil
	}
	return &remote.VectorCheckResponse{Have: nil, Missing: hashes}, nil
}

func (m *pushMockClient) UploadVector(_ context.Context, hash string, r io.Reader, dims int) error {
	if m.uploadVectorErr != nil {
		return m.uploadVectorErr
	}
	// Consume the reader
	if _, err := io.ReadAll(r); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadedVectors[hash] = dims
	return nil
}

func (m *pushMockClient) DownloadVector(_ context.Context, _ string) (io.ReadCloser, int, error) {
	return nil, 0, fmt.Errorf("not implemented in push mock")
}

func (m *pushMockClient) UploadCommitBundle(_ context.Context, bundle *remote.CommitBundle) error {
	if m.uploadBundleErr != nil {
		return m.uploadBundleErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadedBundles = append(m.uploadedBundles, bundle)
	return nil
}

func (m *pushMockClient) DownloadCommitBundle(_ context.Context, _ string) (*remote.CommitBundle, error) {
	return nil, fmt.Errorf("not implemented in push mock")
}

func (m *pushMockClient) UpdateBranch(_ context.Context, branch, newTip, expectedTip string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateBranchArgs.branch = branch
	m.updateBranchArgs.newTip = newTip
	m.updateBranchArgs.expectedTip = expectedTip
	return m.updateBranchErr
}

func (m *pushMockClient) DeleteBranch(_ context.Context, _ string) error {
	return nil
}

func (m *pushMockClient) ListBranches(_ context.Context) ([]*models.Branch, error) {
	return nil, nil
}

func (m *pushMockClient) GetBranch(_ context.Context, _ string) (*models.Branch, error) {
	return nil, nil
}

func (m *pushMockClient) GetRepoInfo(_ context.Context) (*remote.RepoInfo, error) {
	return nil, nil
}

func newPushTestStore(t *testing.T) *store.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test-push.db")
	st, err := store.New(dbPath)
	require.NoError(t, err)
	require.NoError(t, st.Initialize())
	t.Cleanup(func() { st.Close() })
	return st
}

func TestPush_UpToDate(t *testing.T) {
	st := newPushTestStore(t)

	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := newPushMockClient()
	client.negotiatePushResp = &remote.NegotiatePushResponse{
		MissingCommits: nil,
		RemoteTip:      "c1",
	}

	result, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.True(t, result.UpToDate)
	assert.Equal(t, 0, result.CommitsPushed)
	assert.Equal(t, 0, result.VectorsPushed)
}

func TestPush_WithCommits(t *testing.T) {
	st := newPushTestStore(t)

	now := time.Now()
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: now}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: now.Add(time.Second)}))
	require.NoError(t, st.CreateBranch("main", "c2"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := newPushMockClient()
	client.negotiatePushResp = &remote.NegotiatePushResponse{
		MissingCommits: []string{"c2"},
		RemoteTip:      "c1",
	}
	client.vectorCheckResp = &remote.VectorCheckResponse{Have: nil, Missing: nil}

	result, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.False(t, result.UpToDate)
	assert.Equal(t, 1, result.CommitsPushed)

	// Verify commit was uploaded
	require.Len(t, client.uploadedBundles, 1)
	assert.Equal(t, "c2", client.uploadedBundles[0].Commit.ID)

	// Verify branch update CAS args
	assert.Equal(t, "main", client.updateBranchArgs.branch)
	assert.Equal(t, "c2", client.updateBranchArgs.newTip)
	assert.Equal(t, "c1", client.updateBranchArgs.expectedTip)

	// Verify remote-tracking branch was updated
	rb, err := st.GetRemoteBranch("origin", "main")
	require.NoError(t, err)
	assert.Equal(t, "c2", rb.CommitID)
}

func TestPush_WithVectors(t *testing.T) {
	st := newPushTestStore(t)

	now := time.Now()
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: now}))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	// Store a vector blob and an operation that references it
	vecData := []byte{0, 0, 128, 63, 0, 0, 0, 64} // two float32s: 1.0, 2.0
	vhash, err := st.SaveVectorBlob(vecData, 2)
	require.NoError(t, err)
	require.NoError(t, st.RecordOperation(&models.Operation{
		Type:       models.OperationInsert,
		ClassName:  "Article",
		ObjectID:   "obj1",
		VectorHash: vhash,
	}))
	_, err = st.MarkOperationsCommitted("c1")
	require.NoError(t, err)

	client := newPushMockClient()
	client.negotiatePushResp = &remote.NegotiatePushResponse{
		MissingCommits: []string{"c1"},
		RemoteTip:      "",
	}
	client.vectorCheckResp = &remote.VectorCheckResponse{
		Have:    nil,
		Missing: []string{vhash},
	}

	result, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.Equal(t, 1, result.CommitsPushed)
	assert.Equal(t, 1, result.VectorsPushed)
	assert.True(t, result.BranchCreated)

	// Verify vector was uploaded
	assert.Contains(t, client.uploadedVectors, vhash)
	assert.Equal(t, 2, client.uploadedVectors[vhash])
}

func TestPush_VectorDeduplication(t *testing.T) {
	st := newPushTestStore(t)

	now := time.Now()
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: now}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: now.Add(time.Second)}))
	require.NoError(t, st.CreateBranch("main", "c2"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	// Both commits reference the same vector hash
	vecData := []byte{0, 0, 128, 63}
	sharedHash, err := st.SaveVectorBlob(vecData, 1)
	require.NoError(t, err)

	require.NoError(t, st.RecordOperation(&models.Operation{
		Type: models.OperationInsert, ClassName: "A", ObjectID: "1", VectorHash: sharedHash,
	}))
	_, err = st.MarkOperationsCommitted("c1")
	require.NoError(t, err)

	require.NoError(t, st.RecordOperation(&models.Operation{
		Type: models.OperationUpdate, ClassName: "A", ObjectID: "1", VectorHash: sharedHash,
	}))
	_, err = st.MarkOperationsCommitted("c2")
	require.NoError(t, err)

	client := newPushMockClient()
	client.negotiatePushResp = &remote.NegotiatePushResponse{
		MissingCommits: []string{"c1", "c2"},
		RemoteTip:      "",
	}
	// Server says it's missing the hash once
	client.vectorCheckResp = &remote.VectorCheckResponse{
		Have:    nil,
		Missing: []string{sharedHash},
	}

	result, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.Equal(t, 2, result.CommitsPushed)
	assert.Equal(t, 1, result.VectorsPushed) // Only one upload despite two refs
}

func TestPush_CreatesRemoteBranch(t *testing.T) {
	st := newPushTestStore(t)

	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := newPushMockClient()
	client.negotiatePushResp = &remote.NegotiatePushResponse{
		MissingCommits: []string{"c1"},
		RemoteTip:      "", // Empty tip = new branch
	}

	result, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.True(t, result.BranchCreated)
	assert.Equal(t, "", client.updateBranchArgs.expectedTip)
}

func TestPush_DivergenceRejected(t *testing.T) {
	st := newPushTestStore(t)

	// Local chain: c1 -> c2
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c2"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := newPushMockClient()
	client.negotiatePushResp = &remote.NegotiatePushResponse{
		MissingCommits: []string{"c2"},
		RemoteTip:      "c_diverged", // Not in local chain
	}

	_, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "push rejected")
	assert.Contains(t, err.Error(), "diverged")
}

func TestPush_ForceBypassesDivergence(t *testing.T) {
	st := newPushTestStore(t)

	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c2"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := newPushMockClient()
	client.negotiatePushResp = &remote.NegotiatePushResponse{
		MissingCommits: []string{"c2"},
		RemoteTip:      "c_diverged", // Not in local chain
	}

	result, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
		Force:      true,
	}, nil)

	require.NoError(t, err)
	assert.Equal(t, 1, result.CommitsPushed)
}

func TestPush_BranchNotFound(t *testing.T) {
	st := newPushTestStore(t)
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := newPushMockClient()

	_, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "nonexistent",
	}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestPush_NegotiateError(t *testing.T) {
	st := newPushTestStore(t)

	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := newPushMockClient()
	client.negotiatePushErr = fmt.Errorf("connection refused")

	_, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "negotiate push")
}

func TestPush_UpdateBranchError(t *testing.T) {
	st := newPushTestStore(t)

	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := newPushMockClient()
	client.negotiatePushResp = &remote.NegotiatePushResponse{
		MissingCommits: []string{"c1"},
		RemoteTip:      "",
	}
	client.updateBranchErr = &remote.RemoteError{Code: "conflict", Message: "CAS mismatch", Status: 409}

	_, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update remote branch")
}

func TestPush_CommitOrdering(t *testing.T) {
	st := newPushTestStore(t)

	// Create chain: c1 -> c2 -> c3
	now := time.Now()
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: now}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: now.Add(time.Second)}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c3", ParentID: "c2", Message: "third", Timestamp: now.Add(2 * time.Second)}))
	require.NoError(t, st.CreateBranch("main", "c3"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := newPushMockClient()
	client.negotiatePushResp = &remote.NegotiatePushResponse{
		MissingCommits: []string{"c2", "c3"},
		RemoteTip:      "c1",
	}

	result, err := Push(context.Background(), st, client, PushOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.Equal(t, 2, result.CommitsPushed)

	// Verify commits uploaded in topological order (oldest first — parents before children)
	require.Len(t, client.uploadedBundles, 2)
	ids := make([]string, len(client.uploadedBundles))
	for i, b := range client.uploadedBundles {
		ids[i] = b.Commit.ID
	}
	assert.Equal(t, []string{"c2", "c3"}, ids)
}

func TestDeleteRemoteBranch(t *testing.T) {
	st := newPushTestStore(t)
	require.NoError(t, st.AddRemote("origin", "http://example.com"))
	require.NoError(t, st.SetRemoteBranch("origin", "feature", "c1"))

	client := newPushMockClient()

	err := DeleteRemoteBranch(context.Background(), st, client, "origin", "feature")
	require.NoError(t, err)

	// Verify local remote-tracking branch was deleted
	rb, err := st.GetRemoteBranch("origin", "feature")
	require.NoError(t, err)
	assert.Nil(t, rb)
}

func TestCollectCommitChain(t *testing.T) {
	st := newPushTestStore(t)

	now := time.Now()
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "root", Timestamp: now}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: now}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c3", ParentID: "c2", Message: "third", Timestamp: now}))

	chain, err := collectCommitChain(st, "c3")
	require.NoError(t, err)

	// Should be tip-first: c3, c2, c1
	assert.Equal(t, []string{"c3", "c2", "c1"}, chain)
}

func TestCollectCommitChain_MergeCommit(t *testing.T) {
	st := newPushTestStore(t)

	now := time.Now()
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "root", Timestamp: now}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c2a", ParentID: "c1", Message: "branch a", Timestamp: now}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c2b", ParentID: "c1", Message: "branch b", Timestamp: now}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c3", ParentID: "c2a", MergeParentID: "c2b", Message: "merge", Timestamp: now}))

	chain, err := collectCommitChain(st, "c3")
	require.NoError(t, err)

	// Should include all commits, no duplicates
	assert.Len(t, chain, 4)
	assert.Equal(t, "c3", chain[0])
	// c1 should appear exactly once
	c1Count := 0
	for _, id := range chain {
		if id == "c1" {
			c1Count++
		}
	}
	assert.Equal(t, 1, c1Count)
}

func TestResolveRemoteAndBranch_Defaults(t *testing.T) {
	st := newPushTestStore(t)
	require.NoError(t, st.AddRemote("origin", "http://example.com"))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.SetCurrentBranch("main"))

	remoteName, branch, err := ResolveRemoteAndBranch(st, "", "")
	require.NoError(t, err)
	assert.Equal(t, "origin", remoteName)
	assert.Equal(t, "main", branch)
}

func TestResolveRemoteAndBranch_NoRemotes(t *testing.T) {
	st := newPushTestStore(t)

	_, _, err := ResolveRemoteAndBranch(st, "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no remotes configured")
}

func TestResolveRemoteAndBranch_MultipleRemotes(t *testing.T) {
	st := newPushTestStore(t)
	require.NoError(t, st.AddRemote("origin", "http://a.com"))
	require.NoError(t, st.AddRemote("upstream", "http://b.com"))

	_, _, err := ResolveRemoteAndBranch(st, "", "main")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple remotes")
}
