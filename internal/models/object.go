// Package models defines the core data structures used throughout WVC
// including Weaviate objects, operations, and commits.
package models

// WeaviateObject represents an object stored in Weaviate
type WeaviateObject struct {
	ID                 string                 `json:"id"`
	Class              string                 `json:"class"`
	Properties         map[string]interface{} `json:"properties"`
	Vector             interface{}            `json:"vector,omitempty"`             // interface{} to support multi-vectors (ColBERT) in Weaviate v5+
	CreationTimeUnix   int64                  `json:"creationTimeUnix,omitempty"`   // Object creation timestamp (ms)
	LastUpdateTimeUnix int64                  `json:"lastUpdateTimeUnix,omitempty"` // Last modification timestamp (ms)
}

// ObjectState represents the state of objects at a point in time
type ObjectState struct {
	Objects map[string]*WeaviateObject // key: "ClassName/ObjectID"
}

// NewObjectState creates a new empty ObjectState
func NewObjectState() *ObjectState {
	return &ObjectState{
		Objects: make(map[string]*WeaviateObject),
	}
}

// Key returns the unique key for an object
func ObjectKey(className, objectID string) string {
	return className + "/" + objectID
}

// KnownObjectInfo holds a known object along with its hashes for diff computation
type KnownObjectInfo struct {
	Object     *WeaviateObject
	ObjectHash string
	VectorHash string
}
