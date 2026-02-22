// Package core contains business logic for WVC operations.
package core

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
)

// AddRemote validates and stores a new remote configuration.
func AddRemote(st *store.Store, name, rawURL string) error {
	if err := validateRemoteName(name); err != nil {
		return err
	}

	if err := validateRemoteURL(rawURL); err != nil {
		return err
	}

	return st.AddRemote(name, rawURL)
}

// RemoveRemote removes a remote and all its associated data.
func RemoveRemote(st *store.Store, name string) error {
	return st.RemoveRemote(name)
}

// ListRemotesResult contains formatted remote information for display.
type ListRemotesResult struct {
	Remotes []*models.Remote
}

// ListRemotes returns all configured remotes.
func ListRemotes(st *store.Store) (*ListRemotesResult, error) {
	remotes, err := st.ListRemotes()
	if err != nil {
		return nil, fmt.Errorf("list remotes: %w", err)
	}

	return &ListRemotesResult{Remotes: remotes}, nil
}

// GetRemote returns a single remote by name.
func GetRemote(st *store.Store, name string) (*models.Remote, error) {
	remote, err := st.GetRemote(name)
	if err != nil {
		return nil, fmt.Errorf("get remote: %w", err)
	}
	if remote == nil {
		return nil, fmt.Errorf("remote '%s' does not exist", name)
	}
	return remote, nil
}

// SetRemoteToken stores an authentication token for a remote.
// If token is empty, deletes the stored token.
func SetRemoteToken(st *store.Store, remoteName, token string) error {
	// Verify the remote exists
	remote, err := st.GetRemote(remoteName)
	if err != nil {
		return fmt.Errorf("get remote: %w", err)
	}
	if remote == nil {
		return fmt.Errorf("remote '%s' does not exist", remoteName)
	}

	if token == "" {
		return st.DeleteRemoteToken(remoteName)
	}

	return st.SetRemoteToken(remoteName, token)
}

// sanitizeEnvName replaces non-alphanumeric characters with underscores.
var nonAlphanumeric = regexp.MustCompile(`[^A-Za-z0-9]`)

// GetRemoteToken retrieves the token for a remote. It checks:
// 1. Per-remote env var WVC_REMOTE_TOKEN_<UPPER_NAME>
// 2. Global env var WVC_REMOTE_TOKEN
// 3. Stored token
func GetRemoteToken(st *store.Store, remoteName string) (string, error) {
	// Per-remote environment variable takes highest precedence
	sanitized := nonAlphanumeric.ReplaceAllString(strings.ToUpper(remoteName), "_")
	if envToken := os.Getenv("WVC_REMOTE_TOKEN_" + sanitized); envToken != "" {
		return envToken, nil
	}

	// Global environment variable
	if envToken := os.Getenv("WVC_REMOTE_TOKEN"); envToken != "" {
		return envToken, nil
	}

	return st.GetRemoteToken(remoteName)
}

// SetRemoteURL updates the URL of an existing remote.
func SetRemoteURL(st *store.Store, name, rawURL string) error {
	if err := validateRemoteURL(rawURL); err != nil {
		return err
	}
	return st.UpdateRemoteURL(name, rawURL)
}

// validateRemoteName checks that a remote name is valid.
func validateRemoteName(name string) error {
	if name == "" {
		return fmt.Errorf("remote name cannot be empty")
	}

	if strings.ContainsAny(name, " \t\n:/\\") {
		return fmt.Errorf("remote name '%s' contains invalid characters", name)
	}

	// Prevent names that conflict with built-in refs
	reserved := []string{"HEAD", "MERGE_HEAD", "FETCH_HEAD"}
	for _, r := range reserved {
		if strings.EqualFold(name, r) {
			return fmt.Errorf("remote name '%s' is reserved", name)
		}
	}

	return nil
}

// ParseRemoteURL splits a remote URL like "http://host:port/reponame" into
// the base server URL and the repository name.
func ParseRemoteURL(rawURL string) (baseURL, repoName string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("invalid remote URL: %w", err)
	}

	path := strings.TrimSuffix(u.Path, "/")
	if path == "" {
		return "", "", fmt.Errorf("remote URL must include a repository name (e.g., https://host/myrepo)")
	}

	lastSlash := strings.LastIndex(path, "/")
	repoName = path[lastSlash+1:]
	if repoName == "" {
		return "", "", fmt.Errorf("remote URL must include a repository name (e.g., https://host/myrepo)")
	}

	u.Path = path[:lastSlash]
	baseURL = u.String()
	return baseURL, repoName, nil
}

// validateRemoteURL checks that a remote URL is syntactically valid.
func validateRemoteURL(rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("remote URL cannot be empty")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid remote URL: %w", err)
	}

	if u.Scheme == "" {
		return fmt.Errorf("remote URL must include a scheme (e.g., https://)")
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("remote URL scheme must be http or https, got '%s'", u.Scheme)
	}

	if u.Host == "" {
		return fmt.Errorf("remote URL must include a host")
	}

	// Must have a repo name in the path
	path := strings.TrimSuffix(u.Path, "/")
	if path == "" || strings.LastIndex(path, "/") == len(path)-1 {
		return fmt.Errorf("remote URL must include a repository name (e.g., https://host/myrepo)")
	}

	return nil
}
