package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// AdminClient communicates with the wvc-server admin API.
// It is distinct from HTTPClient: not repo-scoped and does not implement RemoteClient.
type AdminClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewAdminClient creates an admin API client. Warns if baseURL uses http://.
func NewAdminClient(baseURL, token string) *AdminClient {
	if strings.HasPrefix(baseURL, "http://") {
		fmt.Fprintf(os.Stderr, "warning: sending credentials over unencrypted HTTP connection\n")
	}
	return &AdminClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		token:      token,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// adminTokenCreateReq is the request body for POST /admin/tokens.
type adminTokenCreateReq struct {
	Description string   `json:"description"`
	Repos       []string `json:"repos"`
	Permission  string   `json:"permission"`
}

// AdminTokenCreateResponse is the decoded response from POST /admin/tokens.
// Exported so callers can read the raw token and its metadata.
type AdminTokenCreateResponse struct {
	Token       string   `json:"token"`
	ID          string   `json:"id"`
	Description string   `json:"description"`
	Repos       []string `json:"repos"`
	Permission  string   `json:"permission"`
}

// AdminTokenInfo is one entry in the GET /admin/tokens response.
type AdminTokenInfo struct {
	ID          string   `json:"id"`
	Description string   `json:"description"`
	Repos       []string `json:"repos"`
	Permission  string   `json:"permission"`
}

// adminReposListResp is the decoded response from GET /admin/repos.
type adminReposListResp struct {
	Repos []string `json:"repos"`
}

func (c *AdminClient) do(ctx context.Context, method, url string, body io.Reader, headers map[string]string) (*http.Response, error) {
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

func (c *AdminClient) doJSON(ctx context.Context, method, url string, reqBody, respBody interface{}) error {
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

// CreateToken calls POST /admin/tokens and returns the newly created token.
// The raw token value is only available in the response — it is never stored by the server.
func (c *AdminClient) CreateToken(ctx context.Context, desc string, repos []string, permission string) (*AdminTokenCreateResponse, error) {
	req := adminTokenCreateReq{Description: desc, Repos: repos, Permission: permission}
	var resp AdminTokenCreateResponse
	if err := c.doJSON(ctx, "POST", c.baseURL+"/admin/tokens", req, &resp); err != nil {
		return nil, fmt.Errorf("create token: %w", err)
	}
	return &resp, nil
}

// ListTokens calls GET /admin/tokens and returns all token metadata.
// Raw token values are never returned.
func (c *AdminClient) ListTokens(ctx context.Context) ([]AdminTokenInfo, error) {
	var tokens []AdminTokenInfo
	if err := c.doJSON(ctx, "GET", c.baseURL+"/admin/tokens", nil, &tokens); err != nil {
		return nil, fmt.Errorf("list tokens: %w", err)
	}
	return tokens, nil
}

// DeleteToken calls DELETE /admin/tokens/{id}.
func (c *AdminClient) DeleteToken(ctx context.Context, id string) error {
	resp, err := c.do(ctx, "DELETE", c.baseURL+"/admin/tokens/"+id, nil, nil)
	if err != nil {
		return fmt.Errorf("delete token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("delete token: %w", decodeError(resp))
	}
	return nil
}

// CreateRepo calls POST /admin/repos to create a new repository.
func (c *AdminClient) CreateRepo(ctx context.Context, name string) error {
	req := struct {
		Name string `json:"name"`
	}{Name: name}
	if err := c.doJSON(ctx, "POST", c.baseURL+"/admin/repos", req, nil); err != nil {
		return fmt.Errorf("create repo: %w", err)
	}
	return nil
}

// DeleteRepo calls DELETE /admin/repos/{name} to remove a repository.
func (c *AdminClient) DeleteRepo(ctx context.Context, name string) error {
	resp, err := c.do(ctx, "DELETE", c.baseURL+"/admin/repos/"+name, nil, nil)
	if err != nil {
		return fmt.Errorf("delete repo: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("delete repo: %w", decodeError(resp))
	}
	return nil
}

// ListRepos calls GET /admin/repos and returns all repository names.
func (c *AdminClient) ListRepos(ctx context.Context) ([]string, error) {
	var resp adminReposListResp
	if err := c.doJSON(ctx, "GET", c.baseURL+"/admin/repos", nil, &resp); err != nil {
		return nil, fmt.Errorf("list repos: %w", err)
	}
	return resp.Repos, nil
}
