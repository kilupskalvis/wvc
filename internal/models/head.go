package models

// HeadState represents the current HEAD position
type HeadState struct {
	CommitID   string // Current commit ID
	BranchName string // Empty if detached HEAD
	IsDetached bool   // True if not on a branch
}
