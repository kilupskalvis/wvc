package remote

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/kilupskalvis/wvc/internal/models"
)

// RemoteClient defines the contract for communicating with a wvc-server.
type RemoteClient interface {
	NegotiatePush(ctx context.Context, branch string, commitIDs []string) (*NegotiatePushResponse, error)
	NegotiatePull(ctx context.Context, branch string, localTip string, depth int) (*NegotiatePullResponse, error)

	CheckVectors(ctx context.Context, hashes []string) (*VectorCheckResponse, error)
	UploadVector(ctx context.Context, hash string, r io.Reader, dims int) error
	DownloadVector(ctx context.Context, hash string) (io.ReadCloser, int, error)

	UploadCommitBundle(ctx context.Context, bundle *CommitBundle) error
	DownloadCommitBundle(ctx context.Context, commitID string) (*CommitBundle, error)

	UpdateBranch(ctx context.Context, branch, newTip, expectedTip string) error
	DeleteBranch(ctx context.Context, branch string) error
	ListBranches(ctx context.Context) ([]*models.Branch, error)
	GetBranch(ctx context.Context, branch string) (*models.Branch, error)

	GetRepoInfo(ctx context.Context) (*RepoInfo, error)
}

// HTTPClient implements RemoteClient over HTTP.
type HTTPClient struct {
	baseURL    string
	repoName   string
	token      string
	httpClient *http.Client
}

// NewHTTPClient creates an HTTP-based remote client.
func NewHTTPClient(baseURL, repoName, token string) *HTTPClient {
	return &HTTPClient{
		baseURL:    baseURL,
		repoName:   repoName,
		token:      token,
		httpClient: &http.Client{},
	}
}

func (c *HTTPClient) repoURL(path string) string {
	return fmt.Sprintf("%s/api/v1/repos/%s%s", c.baseURL, c.repoName, path)
}

func (c *HTTPClient) do(ctx context.Context, method, url string, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}

	return resp, nil
}

func (c *HTTPClient) doJSON(ctx context.Context, method, url string, reqBody, respBody interface{}) error {
	var body io.Reader
	headers := map[string]string{"Content-Type": "application/json"}

	if reqBody != nil {
		data, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(data)
	}

	resp, err := c.do(ctx, method, url, body, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return decodeError(resp)
	}

	if respBody != nil {
		if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}

	return nil
}

// NegotiatePush asks the server which commits it needs.
func (c *HTTPClient) NegotiatePush(ctx context.Context, branch string, commitIDs []string) (*NegotiatePushResponse, error) {
	req := &NegotiatePushRequest{Branch: branch, Commits: commitIDs}
	var resp NegotiatePushResponse
	if err := c.doJSON(ctx, "POST", c.repoURL("/negotiate/push"), req, &resp); err != nil {
		return nil, fmt.Errorf("negotiate push: %w", err)
	}
	return &resp, nil
}

// NegotiatePull asks the server which commits the client needs.
func (c *HTTPClient) NegotiatePull(ctx context.Context, branch string, localTip string, depth int) (*NegotiatePullResponse, error) {
	req := &NegotiatePullRequest{Branch: branch, LocalTip: localTip, Depth: depth}
	var resp NegotiatePullResponse
	if err := c.doJSON(ctx, "POST", c.repoURL("/negotiate/pull"), req, &resp); err != nil {
		return nil, fmt.Errorf("negotiate pull: %w", err)
	}
	return &resp, nil
}

// CheckVectors asks the server which vector blobs it already has.
func (c *HTTPClient) CheckVectors(ctx context.Context, hashes []string) (*VectorCheckResponse, error) {
	req := &VectorCheckRequest{Hashes: hashes}
	var resp VectorCheckResponse
	if err := c.doJSON(ctx, "POST", c.repoURL("/vectors/have"), req, &resp); err != nil {
		return nil, fmt.Errorf("check vectors: %w", err)
	}
	return &resp, nil
}

// UploadVector streams a vector blob to the server.
func (c *HTTPClient) UploadVector(ctx context.Context, hash string, r io.Reader, dims int) error {
	url := c.repoURL("/vectors/" + hash)
	headers := map[string]string{
		"Content-Type":     "application/octet-stream",
		"X-WVC-Dimensions": strconv.Itoa(dims),
	}

	resp, err := c.do(ctx, "POST", url, r, headers)
	if err != nil {
		return fmt.Errorf("upload vector %s: %w", hash, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return decodeError(resp)
	}

	return nil
}

// DownloadVector streams a vector blob from the server.
func (c *HTTPClient) DownloadVector(ctx context.Context, hash string) (io.ReadCloser, int, error) {
	url := c.repoURL("/vectors/" + hash)

	resp, err := c.do(ctx, "GET", url, nil, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("download vector %s: %w", hash, err)
	}

	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return nil, 0, decodeError(resp)
	}

	dims := 0
	if d := resp.Header.Get("X-WVC-Dimensions"); d != "" {
		dims, _ = strconv.Atoi(d)
	}

	return resp.Body, dims, nil
}

// UploadCommitBundle sends a commit bundle to the server with gzip compression.
func (c *HTTPClient) UploadCommitBundle(ctx context.Context, bundle *CommitBundle) error {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(bundle); err != nil {
		gz.Close()
		return fmt.Errorf("encode commit bundle: %w", err)
	}
	gz.Close()

	headers := map[string]string{
		"Content-Type":     "application/json",
		"Content-Encoding": "gzip",
	}

	resp, err := c.do(ctx, "POST", c.repoURL("/commits"), &buf, headers)
	if err != nil {
		return fmt.Errorf("upload commit bundle: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return decodeError(resp)
	}

	return nil
}

// DownloadCommitBundle retrieves a commit bundle from the server.
func (c *HTTPClient) DownloadCommitBundle(ctx context.Context, commitID string) (*CommitBundle, error) {
	url := c.repoURL("/commits/" + commitID + "/bundle")
	headers := map[string]string{"Accept-Encoding": "gzip"}

	resp, err := c.do(ctx, "GET", url, nil, headers)
	if err != nil {
		return nil, fmt.Errorf("download commit bundle %s: %w", commitID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, decodeError(resp)
	}

	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("decompress response: %w", err)
		}
		defer gz.Close()
		reader = gz
	}

	var bundle CommitBundle
	if err := json.NewDecoder(reader).Decode(&bundle); err != nil {
		return nil, fmt.Errorf("decode commit bundle: %w", err)
	}

	return &bundle, nil
}

// UpdateBranch performs a CAS update on a remote branch.
func (c *HTTPClient) UpdateBranch(ctx context.Context, branch, newTip, expectedTip string) error {
	req := &BranchUpdateRequest{CommitID: newTip, Expected: expectedTip}
	if err := c.doJSON(ctx, "PUT", c.repoURL("/branches/"+branch), req, nil); err != nil {
		return fmt.Errorf("update branch %s: %w", branch, err)
	}
	return nil
}

// DeleteBranch removes a remote branch.
func (c *HTTPClient) DeleteBranch(ctx context.Context, branch string) error {
	resp, err := c.do(ctx, "DELETE", c.repoURL("/branches/"+branch), nil, nil)
	if err != nil {
		return fmt.Errorf("delete branch %s: %w", branch, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return decodeError(resp)
	}

	return nil
}

// ListBranches returns all branches on the remote.
func (c *HTTPClient) ListBranches(ctx context.Context) ([]*models.Branch, error) {
	var branches []*models.Branch
	if err := c.doJSON(ctx, "GET", c.repoURL("/branches"), nil, &branches); err != nil {
		return nil, fmt.Errorf("list branches: %w", err)
	}
	return branches, nil
}

// GetBranch returns a single remote branch.
func (c *HTTPClient) GetBranch(ctx context.Context, branch string) (*models.Branch, error) {
	var b models.Branch
	if err := c.doJSON(ctx, "GET", c.repoURL("/branches/"+branch), nil, &b); err != nil {
		return nil, fmt.Errorf("get branch %s: %w", branch, err)
	}
	return &b, nil
}

// GetRepoInfo returns summary info about the remote repository.
func (c *HTTPClient) GetRepoInfo(ctx context.Context) (*RepoInfo, error) {
	var info RepoInfo
	if err := c.doJSON(ctx, "GET", c.repoURL("/info"), nil, &info); err != nil {
		return nil, fmt.Errorf("get repo info: %w", err)
	}
	return &info, nil
}

// RemoteError represents a structured error from the server.
type RemoteError struct {
	Code    string
	Message string
	Status  int
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("remote error (%d): %s — %s", e.Status, e.Code, e.Message)
}

func decodeError(resp *http.Response) error {
	var errResp ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		return &RemoteError{
			Code:    "unknown",
			Message: fmt.Sprintf("HTTP %d", resp.StatusCode),
			Status:  resp.StatusCode,
		}
	}

	return &RemoteError{
		Code:    errResp.Error,
		Message: errResp.Message,
		Status:  resp.StatusCode,
	}
}
