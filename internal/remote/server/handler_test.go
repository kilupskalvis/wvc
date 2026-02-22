package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/kilupskalvis/wvc/internal/remote/blobstore"
	"github.com/kilupskalvis/wvc/internal/remote/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testRepoOpener implements RepoOpener for tests.
type testRepoOpener struct {
	meta  metastore.MetaStore
	blobs blobstore.BlobStore
}

func (t *testRepoOpener) Open(name string) (metastore.MetaStore, blobstore.BlobStore, error) {
	return t.meta, t.blobs, nil
}

// testRepoManager implements RepoManager for tests.
type testRepoManager struct {
	repos []string
}

func (m *testRepoManager) Create(name string) error {
	for _, r := range m.repos {
		if r == name {
			return fmt.Errorf("repository '%s' already exists", name)
		}
	}
	m.repos = append(m.repos, name)
	return nil
}

func (m *testRepoManager) Delete(name string) error {
	for i, r := range m.repos {
		if r == name {
			m.repos = append(m.repos[:i], m.repos[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("repository '%s' not found", name)
}

func (m *testRepoManager) List() ([]string, error) {
	return m.repos, nil
}

// testTokenStore implements TokenStore for tests.
type testTokenStore struct {
	tokens map[string]*TokenInfo
}

func (t *testTokenStore) GetByHash(hash string) (*TokenInfo, error) {
	return t.tokens[hash], nil
}

func (t *testTokenStore) UpdateLastUsed(_ string) error {
	return nil
}

func (t *testTokenStore) ListTokens() ([]*TokenInfo, error) {
	tokens := make([]*TokenInfo, 0, len(t.tokens))
	for _, tok := range t.tokens {
		tokens = append(tokens, tok)
	}
	return tokens, nil
}

func (t *testTokenStore) DeleteToken(id string) error {
	for hash, tok := range t.tokens {
		if tok.ID == id {
			delete(t.tokens, hash)
			return nil
		}
	}
	return fmt.Errorf("token '%s' not found", id)
}

func (t *testTokenStore) CreateToken(desc string, repos []string, permission string) (string, *TokenInfo, error) {
	rawToken := "test-created-token"
	tokenHash := HashToken(rawToken)
	info := &TokenInfo{
		ID:         "tok-new",
		TokenHash:  tokenHash,
		Desc:       desc,
		Repos:      repos,
		Permission: permission,
	}
	t.tokens[tokenHash] = info
	return rawToken, info, nil
}

func newTestServer(t *testing.T) (*httptest.Server, metastore.MetaStore, blobstore.BlobStore, string) {
	t.Helper()

	tmpDir := t.TempDir()
	meta, err := metastore.NewBboltStore(filepath.Join(tmpDir, "meta.db"))
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	blobs, err := blobstore.NewFSStore(filepath.Join(tmpDir, "blobs"))
	require.NoError(t, err)

	repos := &testRepoOpener{meta: meta, blobs: blobs}

	rawToken := "test-token-123"
	tokenHash := HashToken(rawToken)
	tokens := &testTokenStore{
		tokens: map[string]*TokenInfo{
			tokenHash: {
				ID:         "tok-1",
				TokenHash:  tokenHash,
				Desc:       "test token",
				Repos:      []string{"*"},
				Permission: "rw",
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := DefaultServerConfig()

	h, cleanup := Handler(repos, tokens, cfg, logger, nil, nil)
	t.Cleanup(cleanup)
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)

	return ts, meta, blobs, rawToken
}

func authReq(method, url, token string, body io.Reader) *http.Request {
	req, _ := http.NewRequest(method, url, body)
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func TestHealthz(t *testing.T) {
	ts, _, _, _ := newTestServer(t)

	resp, err := http.Get(ts.URL + "/healthz")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestReadyz(t *testing.T) {
	ts, _, _, _ := newTestServer(t)

	resp, err := http.Get(ts.URL + "/readyz")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuth_MissingToken(t *testing.T) {
	ts, _, _, _ := newTestServer(t)

	resp, err := http.Get(ts.URL + "/api/v1/repos/test/branches")
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_InvalidToken(t *testing.T) {
	ts, _, _, _ := newTestServer(t)

	req := authReq("GET", ts.URL+"/api/v1/repos/test/branches", "wrong-token", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestBranches_ListEmpty(t *testing.T) {
	ts, _, _, token := newTestServer(t)

	req := authReq("GET", ts.URL+"/api/v1/repos/test/branches", token, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var branches []*models.Branch
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&branches))
	assert.Len(t, branches, 0)
}

func TestCommitBundle_UploadAndDownload(t *testing.T) {
	ts, _, _, token := newTestServer(t)

	msg := "test commit"
	ts0 := time.Now().Truncate(time.Second)
	ops := []*models.Operation{
		{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-001"},
	}
	commitID := models.GenerateCommitID(msg, ts0, "", ops)

	bundle := &remote.CommitBundle{
		Commit: &models.Commit{
			ID:        commitID,
			Message:   msg,
			Timestamp: ts0,
		},
		Operations: ops,
	}

	// Upload
	data, _ := json.Marshal(bundle)
	req := authReq("POST", ts.URL+"/api/v1/repos/test/commits", token, bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Download
	req = authReq("GET", ts.URL+"/api/v1/repos/test/commits/"+commitID+"/bundle", token, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result remote.CommitBundle
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, commitID, result.Commit.ID)
	assert.Len(t, result.Operations, 1)
}

func TestBranchUpdate_CAS(t *testing.T) {
	ts, meta, _, token := newTestServer(t)
	ctx := context.Background()

	// Insert a commit
	bundle := &remote.CommitBundle{
		Commit: &models.Commit{ID: "commit1", Message: "first", Timestamp: time.Now()},
	}
	require.NoError(t, meta.InsertCommitBundle(ctx, bundle))

	// Create branch via CAS (empty expected = new branch)
	updateReq := &remote.BranchUpdateRequest{CommitID: "commit1", Expected: ""}
	data, _ := json.Marshal(updateReq)
	req := authReq("PUT", ts.URL+"/api/v1/repos/test/branches/main", token, bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify branch was created
	req = authReq("GET", ts.URL+"/api/v1/repos/test/branches/main", token, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var branch models.Branch
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&branch))
	assert.Equal(t, "commit1", branch.CommitID)

	// CAS conflict
	updateReq = &remote.BranchUpdateRequest{CommitID: "commit2", Expected: "wrong"}
	data, _ = json.Marshal(updateReq)
	req = authReq("PUT", ts.URL+"/api/v1/repos/test/branches/main", token, bytes.NewReader(data))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestNegotiatePush(t *testing.T) {
	ts, meta, _, token := newTestServer(t)
	ctx := context.Background()

	// Insert one commit
	bundle := &remote.CommitBundle{
		Commit: &models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()},
	}
	require.NoError(t, meta.InsertCommitBundle(ctx, bundle))
	require.NoError(t, meta.CreateBranch(ctx, "main", "c1"))

	// Negotiate: client has c1, c2, c3
	negotiateReq := &remote.NegotiatePushRequest{
		Branch:  "main",
		Commits: []string{"c3", "c2", "c1"},
	}
	data, _ := json.Marshal(negotiateReq)
	req := authReq("POST", ts.URL+"/api/v1/repos/test/negotiate/push", token, bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result remote.NegotiatePushResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, "c1", result.RemoteTip)
	assert.ElementsMatch(t, []string{"c3", "c2"}, result.MissingCommits)
}

func TestVectorUploadAndDownload(t *testing.T) {
	ts, _, _, token := newTestServer(t)

	data := []byte("vector-data-here")
	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])

	// Upload
	req, _ := http.NewRequest("POST", ts.URL+"/api/v1/repos/test/vectors/"+hash, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-WVC-Dimensions", "4")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Download
	req = authReq("GET", ts.URL+"/api/v1/repos/test/vectors/"+hash, token, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "4", resp.Header.Get("X-WVC-Dimensions"))

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestVectorsHave(t *testing.T) {
	ts, _, blobs, token := newTestServer(t)
	ctx := context.Background()

	// Store one blob directly
	data := []byte("existing-blob")
	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])
	require.NoError(t, blobs.Put(ctx, hash, bytes.NewReader(data), 3))

	// Check
	checkReq := &remote.VectorCheckRequest{
		Hashes: []string{hash, "nonexistent"},
	}
	reqData, _ := json.Marshal(checkReq)
	req := authReq("POST", ts.URL+"/api/v1/repos/test/vectors/have", token, bytes.NewReader(reqData))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result remote.VectorCheckResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, []string{hash}, result.Have)
	assert.Equal(t, []string{"nonexistent"}, result.Missing)
}

func TestNegotiatePull(t *testing.T) {
	ts, meta, _, token := newTestServer(t)
	ctx := context.Background()

	// Insert a chain: c1 -> c2 -> c3
	for _, b := range []*remote.CommitBundle{
		{Commit: &models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()}},
		{Commit: &models.Commit{ID: "c2", ParentID: "c1", Message: "second", Timestamp: time.Now()}},
		{Commit: &models.Commit{ID: "c3", ParentID: "c2", Message: "third", Timestamp: time.Now()}},
	} {
		require.NoError(t, meta.InsertCommitBundle(ctx, b))
	}
	require.NoError(t, meta.CreateBranch(ctx, "main", "c3"))

	// Client has c1, wants to pull
	negotiateReq := &remote.NegotiatePullRequest{
		Branch:   "main",
		LocalTip: "c1",
	}
	data, _ := json.Marshal(negotiateReq)
	req := authReq("POST", ts.URL+"/api/v1/repos/test/negotiate/pull", token, bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result remote.NegotiatePullResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, "c3", result.RemoteTip)
	// Missing should be c2 and c3, in topological order (oldest first)
	assert.Equal(t, []string{"c2", "c3"}, result.MissingCommits)
}

func TestNegotiatePull_BranchNotFound(t *testing.T) {
	ts, _, _, token := newTestServer(t)

	negotiateReq := &remote.NegotiatePullRequest{Branch: "nonexistent"}
	data, _ := json.Marshal(negotiateReq)
	req := authReq("POST", ts.URL+"/api/v1/repos/test/negotiate/pull", token, bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestNegotiatePull_Fresh(t *testing.T) {
	ts, meta, _, token := newTestServer(t)
	ctx := context.Background()

	// Setup: single commit on remote
	bundle := &remote.CommitBundle{
		Commit: &models.Commit{ID: "c1", Message: "initial", Timestamp: time.Now()},
	}
	require.NoError(t, meta.InsertCommitBundle(ctx, bundle))
	require.NoError(t, meta.CreateBranch(ctx, "main", "c1"))

	// Client has nothing (empty local tip)
	negotiateReq := &remote.NegotiatePullRequest{Branch: "main", LocalTip: ""}
	data, _ := json.Marshal(negotiateReq)
	req := authReq("POST", ts.URL+"/api/v1/repos/test/negotiate/pull", token, bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result remote.NegotiatePullResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, "c1", result.RemoteTip)
	assert.Equal(t, []string{"c1"}, result.MissingCommits)
}

// newAdminTestServer creates a test server with admin auth and a testRepoManager.
// Returns the server, the repo manager, and the raw admin token.
func newAdminTestServer(t *testing.T) (*httptest.Server, *testRepoManager, string) {
	t.Helper()

	tmpDir := t.TempDir()
	meta, err := metastore.NewBboltStore(filepath.Join(tmpDir, "meta.db"))
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	blobs, err := blobstore.NewFSStore(filepath.Join(tmpDir, "blobs"))
	require.NoError(t, err)

	repos := &testRepoOpener{meta: meta, blobs: blobs}
	manager := &testRepoManager{}
	tokens := &testTokenStore{tokens: map[string]*TokenInfo{}}

	rawAdminToken := "admin-test-token-123"
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := DefaultServerConfig()
	cfg.AdminToken = rawAdminToken

	h, cleanup := Handler(repos, tokens, cfg, logger, nil, manager)
	t.Cleanup(cleanup)
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)

	return ts, manager, rawAdminToken
}

func adminReq(method, url, adminToken string, body io.Reader) *http.Request {
	req, _ := http.NewRequest(method, url, body)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func TestAdminRepos_ListEmpty(t *testing.T) {
	ts, _, adminToken := newAdminTestServer(t)

	req := adminReq("GET", ts.URL+"/admin/repos", adminToken, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string][]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Empty(t, result["repos"])
}

func TestAdminRepos_CreateAndList(t *testing.T) {
	ts, _, adminToken := newAdminTestServer(t)

	// Create a repo
	body, _ := json.Marshal(map[string]string{"name": "myrepo"})
	req := adminReq("POST", ts.URL+"/admin/repos", adminToken, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// List should now contain the repo
	req = adminReq("GET", ts.URL+"/admin/repos", adminToken, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string][]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, []string{"myrepo"}, result["repos"])
}

func TestAdminRepos_CreateDuplicate(t *testing.T) {
	ts, manager, adminToken := newAdminTestServer(t)
	manager.repos = []string{"existing"}

	body, _ := json.Marshal(map[string]string{"name": "existing"})
	req := adminReq("POST", ts.URL+"/admin/repos", adminToken, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestAdminRepos_CreateInvalidName(t *testing.T) {
	ts, _, adminToken := newAdminTestServer(t)

	body, _ := json.Marshal(map[string]string{"name": "bad/name"})
	req := adminReq("POST", ts.URL+"/admin/repos", adminToken, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAdminRepos_Delete(t *testing.T) {
	ts, manager, adminToken := newAdminTestServer(t)
	manager.repos = []string{"todelete"}

	req := adminReq("DELETE", ts.URL+"/admin/repos/todelete", adminToken, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Empty(t, manager.repos)
}

func TestAdminRepos_DeleteNotFound(t *testing.T) {
	ts, _, adminToken := newAdminTestServer(t)

	req := adminReq("DELETE", ts.URL+"/admin/repos/ghost", adminToken, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestAdminRepos_AuthRequired(t *testing.T) {
	ts, _, _ := newAdminTestServer(t)

	resp, err := http.Get(ts.URL + "/admin/repos")
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestRepoInfo(t *testing.T) {
	ts, meta, _, token := newTestServer(t)
	ctx := context.Background()

	// Create some data
	bundle := &remote.CommitBundle{
		Commit: &models.Commit{ID: "c1", Message: "first", Timestamp: time.Now()},
	}
	require.NoError(t, meta.InsertCommitBundle(ctx, bundle))
	require.NoError(t, meta.CreateBranch(ctx, "main", "c1"))

	req := authReq("GET", ts.URL+"/api/v1/repos/test/info", token, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var info remote.RepoInfo
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&info))
	assert.Equal(t, 1, info.BranchCount)
	assert.Equal(t, 1, info.CommitCount)
}
