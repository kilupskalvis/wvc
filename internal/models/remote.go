package models

import "time"

// Remote represents a configured remote server.
type Remote struct {
	Name      string    `json:"name"`
	URL       string    `json:"url"`
	CreatedAt time.Time `json:"created_at"`
}

// RemoteBranch represents a remote-tracking branch reference.
type RemoteBranch struct {
	RemoteName string    `json:"remote_name"`
	BranchName string    `json:"branch_name"`
	CommitID   string    `json:"commit_id"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// RemoteBranchKey returns the bbolt key for a remote branch: "remote:branch".
func RemoteBranchKey(remoteName, branchName string) string {
	return remoteName + ":" + branchName
}
