package models

// ConflictStrategy defines how to handle merge conflicts
type ConflictStrategy string

const (
	ConflictAbort  ConflictStrategy = "abort"  // Default: abort on conflict
	ConflictOurs   ConflictStrategy = "ours"   // Prefer our version
	ConflictTheirs ConflictStrategy = "theirs" // Prefer their version
)

// MergeConflictType identifies the type of merge conflict
type MergeConflictType string

const (
	ConflictModifyModify MergeConflictType = "modify-modify" // Both modified differently
	ConflictDeleteModify MergeConflictType = "delete-modify" // We deleted, they modified
	ConflictModifyDelete MergeConflictType = "modify-delete" // We modified, they deleted
	ConflictAddAdd       MergeConflictType = "add-add"       // Both added with different data
)

// MergeConflict represents a conflict during merge
type MergeConflict struct {
	Key       string            // "ClassName/ObjectID"
	ClassName string            // Weaviate class name
	ObjectID  string            // Object UUID
	Type      MergeConflictType // Type of conflict
	Base      *WeaviateObject   // State at common ancestor (nil for add-add)
	Ours      *WeaviateObject   // State in our branch (nil for delete-modify)
	Theirs    *WeaviateObject   // State in their branch (nil for modify-delete)
}

// SchemaConflict represents a schema-level conflict
type SchemaConflict struct {
	ClassName    string      // Class involved
	PropertyName string      // Property name (empty for class-level conflict)
	Type         string      // Conflict type description
	Ours         interface{} // Our schema definition
	Theirs       interface{} // Their schema definition
}

// MergeResult contains the outcome of a merge operation
type MergeResult struct {
	Success           bool              // Whether merge succeeded
	FastForward       bool              // Whether this was a fast-forward merge
	MergeCommit       *Commit           // The merge commit (nil for fast-forward)
	Conflicts         []*MergeConflict  // Object conflicts (if any)
	SchemaConflicts   []*SchemaConflict // Schema conflicts (if any)
	ResolvedConflicts int               // Count of auto-resolved conflicts via --ours/--theirs
	ObjectsAdded      int               // Objects added during merge
	ObjectsUpdated    int               // Objects updated during merge
	ObjectsDeleted    int               // Objects deleted during merge
	Warnings          []string          // Non-fatal warnings
}

// MergeOptions configures merge behavior
type MergeOptions struct {
	NoFastForward bool             // Force creation of merge commit even if FF possible
	Message       string           // Custom merge commit message
	Strategy      ConflictStrategy // How to handle conflicts
}
