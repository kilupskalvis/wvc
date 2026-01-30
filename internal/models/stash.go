package models

import "time"

// Stash represents a saved stash entry
type Stash struct {
	ID         int64     `json:"id"`
	Message    string    `json:"message"`
	BranchName string    `json:"branch_name"`
	CommitID   string    `json:"commit_id"`
	CreatedAt  time.Time `json:"created_at"`
}

// StashChange represents a single object change within a stash
type StashChange struct {
	ID                 int64  `json:"id"`
	StashID            int64  `json:"stash_id"`
	ClassName          string `json:"class_name"`
	ObjectID           string `json:"object_id"`
	ChangeType         string `json:"change_type"` // "insert", "update", "delete"
	ObjectData         []byte `json:"object_data,omitempty"`
	PreviousData       []byte `json:"previous_data,omitempty"`
	WasStaged          bool   `json:"was_staged"`
	VectorHash         string `json:"vector_hash,omitempty"`
	PreviousVectorHash string `json:"previous_vector_hash,omitempty"`
}
