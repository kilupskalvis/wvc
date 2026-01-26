package models

// SchemaChangeType represents the type of schema change
type SchemaChangeType string

const (
	SchemaChangeClassAdded        SchemaChangeType = "class_added"
	SchemaChangeClassDeleted      SchemaChangeType = "class_deleted"
	SchemaChangePropertyAdded     SchemaChangeType = "property_added"
	SchemaChangePropertyDeleted   SchemaChangeType = "property_deleted"
	SchemaChangePropertyModified  SchemaChangeType = "property_modified"
	SchemaChangeVectorizerChanged SchemaChangeType = "vectorizer_changed"
)

// SchemaChange represents a single change to the schema
type SchemaChange struct {
	Type          SchemaChangeType
	ClassName     string
	PropertyName  string                 // For property-level changes
	CurrentValue  map[string]interface{} // Current state (for adds/modifies)
	PreviousValue map[string]interface{} // Previous state (for deletes/modifies)
}

// WeaviateSchema represents the complete Weaviate schema
type WeaviateSchema struct {
	Classes []*WeaviateClass `json:"classes"`
}

// WeaviateClass represents a class definition in Weaviate
type WeaviateClass struct {
	Class             string                 `json:"class"`
	Properties        []*WeaviateProperty    `json:"properties"`
	Vectorizer        string                 `json:"vectorizer,omitempty"`
	VectorIndexConfig map[string]interface{} `json:"vectorIndexConfig,omitempty"`
	ModuleConfig      map[string]interface{} `json:"moduleConfig,omitempty"`
	Description       string                 `json:"description,omitempty"`
	VectorIndexType   string                 `json:"vectorIndexType,omitempty"`
	Replication       map[string]interface{} `json:"replicationConfig,omitempty"`
	ShardingConfig    map[string]interface{} `json:"shardingConfig,omitempty"`
	InvertedIndex     map[string]interface{} `json:"invertedIndexConfig,omitempty"`
	MultiTenancy      map[string]interface{} `json:"multiTenancyConfig,omitempty"`
}

// WeaviateProperty represents a property definition in a class
type WeaviateProperty struct {
	Name            string   `json:"name"`
	DataType        []string `json:"dataType"`
	Description     string   `json:"description,omitempty"`
	Tokenization    string   `json:"tokenization,omitempty"`
	IndexFilterable *bool    `json:"indexFilterable,omitempty"`
	IndexSearchable *bool    `json:"indexSearchable,omitempty"`
	IndexInverted   *bool    `json:"indexInverted,omitempty"`
}

// SchemaVersion represents a stored schema snapshot
type SchemaVersion struct {
	ID         int64
	Timestamp  string
	SchemaJSON []byte
	SchemaHash string
	CommitID   string
}
