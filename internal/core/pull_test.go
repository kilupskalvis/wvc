package core

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockRemoteClient implements remote.RemoteClient for testing pull/fetch.
type mockRemoteClient struct {
	negotiatePullResp *remote.NegotiatePullResponse
	negotiatePullErr  error
	commitBundles     map[string]*remote.CommitBundle
	vectorData        map[string]mockVector
	vectorCheckResp   *remote.VectorCheckResponse
}

type mockVector struct {
	data []byte
	dims int
}

func (m *mockRemoteClient) NegotiatePush(_ context.Context, _ string, _ []string) (*remote.NegotiatePushResponse, error) {
	return nil, nil
}

func (m *mockRemoteClient) NegotiatePull(_ context.Context, _ string, _ string, _ int) (*remote.NegotiatePullResponse, error) {
	return m.negotiatePullResp, m.negotiatePullErr
}

func (m *mockRemoteClient) CheckVectors(_ context.Context, hashes []string) (*remote.VectorCheckResponse, error) {
	if m.vectorCheckResp != nil {
		return m.vectorCheckResp, nil
	}
	return &remote.VectorCheckResponse{Have: nil, Missing: hashes}, nil
}

func (m *mockRemoteClient) UploadVector(_ context.Context, _ string, _ io.Reader, _ int) error {
	return nil
}

func (m *mockRemoteClient) DownloadVector(_ context.Context, hash string) (io.ReadCloser, int, error) {
	v, ok := m.vectorData[hash]
	if !ok {
		return nil, 0, &remote.RemoteError{Code: "not_found", Message: "vector not found", Status: 404}
	}
	return io.NopCloser(io.NewSectionReader(readerAt(v.data), 0, int64(len(v.data)))), v.dims, nil
}

func (m *mockRemoteClient) UploadCommitBundle(_ context.Context, _ *remote.CommitBundle) error {
	return nil
}

func (m *mockRemoteClient) DownloadCommitBundle(_ context.Context, commitID string) (*remote.CommitBundle, error) {
	b, ok := m.commitBundles[commitID]
	if !ok {
		return nil, &remote.RemoteError{Code: "not_found", Message: "commit not found", Status: 404}
	}
	return b, nil
}

func (m *mockRemoteClient) UpdateBranch(_ context.Context, _, _, _ string) error {
	return nil
}

func (m *mockRemoteClient) DeleteBranch(_ context.Context, _ string) error {
	return nil
}

func (m *mockRemoteClient) ListBranches(_ context.Context) ([]*models.Branch, error) {
	return nil, nil
}

func (m *mockRemoteClient) GetBranch(_ context.Context, _ string) (*models.Branch, error) {
	return nil, nil
}

func (m *mockRemoteClient) GetRepoInfo(_ context.Context) (*remote.RepoInfo, error) {
	return nil, nil
}

// readerAt wraps a byte slice to implement io.ReaderAt.
type readerAt []byte

func (r readerAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(r)) {
		return 0, io.EOF
	}
	n = copy(p, r[off:])
	if n < len(p) {
		err = io.EOF
	}
	return
}

func TestFetch_UpToDate(t *testing.T) {
	st := newPullTestStore(t)

	// Setup: local has commit c1, remote-tracking is at c1
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.SetRemoteBranch("origin", "main", "c1"))

	client := &mockRemoteClient{
		negotiatePullResp: &remote.NegotiatePullResponse{
			MissingCommits: nil,
			RemoteTip:      "c1",
		},
	}

	result, err := Fetch(context.Background(), st, client, FetchOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.True(t, result.UpToDate)
	assert.Equal(t, 0, result.CommitsFetched)
}

func TestFetch_DownloadsCommits(t *testing.T) {
	st := newPullTestStore(t)

	// Setup: local has c1
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.SetRemoteBranch("origin", "main", "c1"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := &mockRemoteClient{
		negotiatePullResp: &remote.NegotiatePullResponse{
			MissingCommits: []string{"c2", "c3"},
			RemoteTip:      "c3",
		},
		commitBundles: map[string]*remote.CommitBundle{
			"c2": {
				Commit:     &models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: time.Now()},
				Operations: []*models.Operation{{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-1"}},
			},
			"c3": {
				Commit:     &models.Commit{ID: "c3", ParentID: "c2", Message: "third", Timestamp: time.Now()},
				Operations: []*models.Operation{{Type: models.OperationUpdate, ClassName: "Article", ObjectID: "obj-1"}},
			},
		},
	}

	result, err := Fetch(context.Background(), st, client, FetchOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.False(t, result.UpToDate)
	assert.Equal(t, 2, result.CommitsFetched)
	assert.Equal(t, "c3", result.RemoteTip)

	// Verify commits were stored locally
	c2, err := st.GetCommit("c2")
	require.NoError(t, err)
	assert.Equal(t, "c1", c2.ParentID)

	c3, err := st.GetCommit("c3")
	require.NoError(t, err)
	assert.Equal(t, "c2", c3.ParentID)

	// Verify remote-tracking branch was updated
	rb, err := st.GetRemoteBranch("origin", "main")
	require.NoError(t, err)
	assert.Equal(t, "c3", rb.CommitID)
}

func TestPull_FastForward(t *testing.T) {
	st := newPullTestStore(t)

	// Setup: local branch at c1, remote has c2, c3
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.SetCurrentBranch("main"))
	require.NoError(t, st.SetHEAD("c1"))
	require.NoError(t, st.SetRemoteBranch("origin", "main", "c1"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := &mockRemoteClient{
		negotiatePullResp: &remote.NegotiatePullResponse{
			MissingCommits: []string{"c2", "c3"},
			RemoteTip:      "c3",
		},
		commitBundles: map[string]*remote.CommitBundle{
			"c2": {Commit: &models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: time.Now()}},
			"c3": {Commit: &models.Commit{ID: "c3", ParentID: "c2", Message: "third", Timestamp: time.Now()}},
		},
	}

	result, err := Pull(context.Background(), st, client, PullOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.True(t, result.FastForward)
	assert.False(t, result.Diverged)
	assert.Equal(t, 2, result.CommitsFetched)

	// Verify local branch was updated
	branch, err := st.GetBranch("main")
	require.NoError(t, err)
	assert.Equal(t, "c3", branch.CommitID)

	// Verify HEAD was updated
	head, err := st.GetHEAD()
	require.NoError(t, err)
	assert.Equal(t, "c3", head)
}

func TestPull_Diverged(t *testing.T) {
	st := newPullTestStore(t)

	// Setup: local has c1 -> c2local, remote has c1 -> c2remote
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c2local", ParentID: "c1", Message: "local change", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c2local"))
	require.NoError(t, st.SetCurrentBranch("main"))
	require.NoError(t, st.SetHEAD("c2local"))
	require.NoError(t, st.SetRemoteBranch("origin", "main", "c1"))
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	client := &mockRemoteClient{
		negotiatePullResp: &remote.NegotiatePullResponse{
			MissingCommits: []string{"c2remote"},
			RemoteTip:      "c2remote",
		},
		commitBundles: map[string]*remote.CommitBundle{
			"c2remote": {Commit: &models.Commit{ID: "c2remote", ParentID: "c1", Message: "remote change", Timestamp: time.Now()}},
		},
	}

	result, err := Pull(context.Background(), st, client, PullOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.True(t, result.Diverged)
	assert.False(t, result.FastForward)

	// Local branch should NOT be modified
	branch, err := st.GetBranch("main")
	require.NoError(t, err)
	assert.Equal(t, "c2local", branch.CommitID)
}

func TestPull_UpToDate(t *testing.T) {
	st := newPullTestStore(t)

	require.NoError(t, st.CreateCommit(&models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}))
	require.NoError(t, st.CreateBranch("main", "c1"))
	require.NoError(t, st.SetRemoteBranch("origin", "main", "c1"))

	client := &mockRemoteClient{
		negotiatePullResp: &remote.NegotiatePullResponse{
			MissingCommits: nil,
			RemoteTip:      "c1",
		},
	}

	result, err := Pull(context.Background(), st, client, PullOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.True(t, result.UpToDate)
}

func TestPull_CreatesBranch(t *testing.T) {
	st := newPullTestStore(t)
	require.NoError(t, st.AddRemote("origin", "http://example.com"))

	// No local branch exists
	client := &mockRemoteClient{
		negotiatePullResp: &remote.NegotiatePullResponse{
			MissingCommits: []string{"c1"},
			RemoteTip:      "c1",
		},
		commitBundles: map[string]*remote.CommitBundle{
			"c1": {Commit: &models.Commit{ID: "c1", Message: "initial", Timestamp: time.Now()}},
		},
	}

	result, err := Pull(context.Background(), st, client, PullOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.True(t, result.FastForward)

	// Branch should now exist
	branch, err := st.GetBranch("main")
	require.NoError(t, err)
	assert.Equal(t, "c1", branch.CommitID)
}

func TestFetch_WithSchema(t *testing.T) {
	st := newPullTestStore(t)
	require.NoError(t, st.AddRemote("origin", "http://example.com"))
	require.NoError(t, st.SetRemoteBranch("origin", "main", ""))

	client := &mockRemoteClient{
		negotiatePullResp: &remote.NegotiatePullResponse{
			MissingCommits: []string{"c1"},
			RemoteTip:      "c1",
		},
		commitBundles: map[string]*remote.CommitBundle{
			"c1": {
				Commit: &models.Commit{ID: "c1", Message: "with schema", Timestamp: time.Now()},
				Schema: &remote.SchemaSnapshot{
					SchemaJSON: []byte(`{"classes":["Article"]}`),
					SchemaHash: "hash123",
				},
			},
		},
	}

	result, err := Fetch(context.Background(), st, client, FetchOptions{
		RemoteName: "origin",
		Branch:     "main",
	}, nil)

	require.NoError(t, err)
	assert.Equal(t, 1, result.CommitsFetched)

	// Verify schema was stored
	sv, err := st.GetSchemaVersionByCommit("c1")
	require.NoError(t, err)
	require.NotNil(t, sv)
	assert.Equal(t, "hash123", sv.SchemaHash)
}

func newPullTestStore(t *testing.T) *store.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test-pull.db")
	st, err := store.New(dbPath)
	require.NoError(t, err)
	require.NoError(t, st.Initialize())
	t.Cleanup(func() { st.Close() })
	return st
}

func TestResolveRef_RemoteTracking(t *testing.T) {
	st := newPullTestStore(t)
	require.NoError(t, st.AddRemote("origin", "http://example.com"))
	require.NoError(t, st.SetRemoteBranch("origin", "main", "c1"))

	commitID, _, err := ResolveRef(st, "origin/main")
	require.NoError(t, err)
	assert.Equal(t, "c1", commitID)

	_, _, err = ResolveRef(st, "nonexistent/main")
	assert.Error(t, err)
}
