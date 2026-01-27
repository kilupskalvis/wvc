package models

import "time"

// Commit represents a version control commit
type Commit struct {
	ID             string    `json:"id"`
	ParentID       string    `json:"parent_id,omitempty"`
	MergeParentID  string    `json:"merge_parent_id,omitempty"`
	Message        string    `json:"message"`
	Timestamp      time.Time `json:"timestamp"`
	OperationCount int       `json:"operation_count"`
}

// ShortID returns a shortened commit ID (first 7 characters)
func (c *Commit) ShortID() string {
	if len(c.ID) > 7 {
		return c.ID[:7]
	}
	return c.ID
}

// IsMergeCommit returns true if this commit has two parents
func (c *Commit) IsMergeCommit() bool {
	return c.MergeParentID != ""
}
