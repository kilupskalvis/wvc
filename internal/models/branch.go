package models

import "time"

// Branch represents a named reference to a commit
type Branch struct {
	Name      string    `json:"name"`
	CommitID  string    `json:"commit_id"`
	CreatedAt time.Time `json:"created_at"`
}
