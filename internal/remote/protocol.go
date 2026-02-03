// Package remote defines the protocol types and client for wvc-server communication.
package remote

import (
	"github.com/kilupskalvis/wvc/internal/models"
)

// NegotiatePushRequest is sent by the client to discover which commits the server needs.
type NegotiatePushRequest struct {
	Branch  string   `json:"branch"`
	Commits []string `json:"commits"`
}

// NegotiatePushResponse tells the client which commits are missing on the server.
type NegotiatePushResponse struct {
	MissingCommits []string `json:"missing_commits"`
	RemoteTip      string   `json:"remote_tip"`
}

// NegotiatePullRequest is sent by the client to discover which commits it needs.
type NegotiatePullRequest struct {
	Branch   string `json:"branch"`
	LocalTip string `json:"local_tip"`
	Depth    int    `json:"depth,omitempty"`
}

// NegotiatePullResponse tells the client which commits to download.
type NegotiatePullResponse struct {
	MissingCommits []string `json:"missing_commits"`
	RemoteTip      string   `json:"remote_tip"`
}

// VectorCheckRequest asks the server which vector blobs it already has.
type VectorCheckRequest struct {
	Hashes []string `json:"hashes"`
}

// VectorCheckResponse indicates which vector blobs the server has and which are missing.
type VectorCheckResponse struct {
	Have    []string `json:"have"`
	Missing []string `json:"missing"`
}

// CommitBundle contains a commit with its operations and optional schema version,
// serialized together for transfer between client and server.
type CommitBundle struct {
	Commit     *models.Commit      `json:"commit"`
	Operations []*models.Operation `json:"operations"`
	Schema     *SchemaSnapshot     `json:"schema,omitempty"`
}

// SchemaSnapshot is the schema state at a particular commit.
type SchemaSnapshot struct {
	SchemaJSON []byte `json:"schema_json"`
	SchemaHash string `json:"schema_hash"`
}

// BranchUpdateRequest is a compare-and-swap update for a branch pointer.
type BranchUpdateRequest struct {
	CommitID string `json:"commit_id"`
	Expected string `json:"expected"`
}

// RepoInfo contains summary information about a remote repository.
type RepoInfo struct {
	BranchCount int `json:"branch_count"`
	CommitCount int `json:"commit_count"`
	TotalBlobs  int `json:"total_blobs"`
}

// ErrorResponse is the structured error format returned by the server.
type ErrorResponse struct {
	Error   string            `json:"error"`
	Message string            `json:"message"`
	Detail  map[string]string `json:"detail,omitempty"`
}
