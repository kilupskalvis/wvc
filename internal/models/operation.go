package models

import "time"

// OperationType represents the type of database operation.
type OperationType string

const (
	OperationInsert OperationType = "insert"
	OperationUpdate OperationType = "update"
	OperationDelete OperationType = "delete"
)

// Operation represents a single database operation.
// Operations are identified by (CommitID, Seq) — the 0-based index within their commit.
type Operation struct {
	Seq                int           `json:"seq"`
	Timestamp          time.Time     `json:"timestamp"`
	Type               OperationType `json:"operation_type"`
	ClassName          string        `json:"class_name"`
	ObjectID           string        `json:"object_id"`
	ObjectData         []byte        `json:"object_data,omitempty"`   // JSON data for insert/update
	PreviousData       []byte        `json:"previous_data,omitempty"` // Previous state for revert
	CommitID           string        `json:"commit_id,omitempty"`
	Reverted           bool          `json:"reverted"`
	VectorHash         string        `json:"vector_hash,omitempty"`          // Hash reference to vector_blobs
	PreviousVectorHash string        `json:"previous_vector_hash,omitempty"` // Previous vector hash for revert
}
