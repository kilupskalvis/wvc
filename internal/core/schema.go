package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
)

// SchemaDiffResult represents the differences between two schema versions
type SchemaDiffResult struct {
	ClassesAdded       []*models.SchemaChange
	ClassesDeleted     []*models.SchemaChange
	PropertiesAdded    []*models.SchemaChange
	PropertiesDeleted  []*models.SchemaChange
	PropertiesModified []*models.SchemaChange
	VectorizersChanged []*models.SchemaChange
}

// HasChanges returns true if there are any schema changes
func (s *SchemaDiffResult) HasChanges() bool {
	return s.TotalChanges() > 0
}

// TotalChanges returns the total number of schema changes
func (s *SchemaDiffResult) TotalChanges() int {
	return len(s.ClassesAdded) +
		len(s.ClassesDeleted) +
		len(s.PropertiesAdded) +
		len(s.PropertiesDeleted) +
		len(s.PropertiesModified) +
		len(s.VectorizersChanged)
}

// ComputeSchemaDiff compares the current Weaviate schema against the last known schema
func ComputeSchemaDiff(ctx context.Context, st *store.Store, client weaviate.ClientInterface) (*SchemaDiffResult, error) {
	// Get current schema from Weaviate
	currentSchema, err := client.GetSchemaTyped(ctx)
	if err != nil {
		return nil, err
	}

	// Get last known schema from store
	previousVersion, err := st.GetLatestSchemaVersion()
	if err != nil {
		return nil, err
	}

	var previousSchema *models.WeaviateSchema
	if previousVersion != nil {
		var prev models.WeaviateSchema
		if err := json.Unmarshal(previousVersion.SchemaJSON, &prev); err != nil {
			return nil, err
		}
		previousSchema = &prev
	}

	return diffSchemas(currentSchema, previousSchema), nil
}

// ComputeSchemaDiffBetweenVersions compares two schema versions by their JSON
func ComputeSchemaDiffBetweenVersions(currentJSON, previousJSON []byte) (*SchemaDiffResult, error) {
	var currentSchema, previousSchema *models.WeaviateSchema

	if len(currentJSON) > 0 {
		var curr models.WeaviateSchema
		if err := json.Unmarshal(currentJSON, &curr); err != nil {
			return nil, err
		}
		currentSchema = &curr
	}

	if len(previousJSON) > 0 {
		var prev models.WeaviateSchema
		if err := json.Unmarshal(previousJSON, &prev); err != nil {
			return nil, err
		}
		previousSchema = &prev
	}

	return diffSchemas(currentSchema, previousSchema), nil
}

// diffSchemas computes the diff between two schema structs
func diffSchemas(current, previous *models.WeaviateSchema) *SchemaDiffResult {
	result := &SchemaDiffResult{}

	currentClasses := buildClassMap(current)
	previousClasses := buildClassMap(previous)

	// Find added and modified classes
	for name, currentClass := range currentClasses {
		prevClass, exists := previousClasses[name]
		if !exists {
			result.ClassesAdded = append(result.ClassesAdded, &models.SchemaChange{
				Type:         models.SchemaChangeClassAdded,
				ClassName:    name,
				CurrentValue: toMap(currentClass),
			})
		} else {
			compareClasses(name, prevClass, currentClass, result)
		}
	}

	// Find deleted classes
	for name, prevClass := range previousClasses {
		if _, exists := currentClasses[name]; !exists {
			result.ClassesDeleted = append(result.ClassesDeleted, &models.SchemaChange{
				Type:          models.SchemaChangeClassDeleted,
				ClassName:     name,
				PreviousValue: toMap(prevClass),
			})
		}
	}

	return result
}

// buildClassMap creates a map of class name to class definition
func buildClassMap(schema *models.WeaviateSchema) map[string]*models.WeaviateClass {
	m := make(map[string]*models.WeaviateClass)
	if schema == nil {
		return m
	}
	for _, class := range schema.Classes {
		if class != nil {
			m[class.Class] = class
		}
	}
	return m
}

// buildPropertyMap creates a map of property name to property definition
func buildPropertyMap(class *models.WeaviateClass) map[string]*models.WeaviateProperty {
	m := make(map[string]*models.WeaviateProperty)
	if class == nil {
		return m
	}
	for _, prop := range class.Properties {
		if prop != nil {
			m[prop.Name] = prop
		}
	}
	return m
}

// compareClasses compares two class definitions and records differences
func compareClasses(className string, prev, curr *models.WeaviateClass, result *SchemaDiffResult) {
	// Compare vectorizer
	if prev.Vectorizer != curr.Vectorizer {
		result.VectorizersChanged = append(result.VectorizersChanged, &models.SchemaChange{
			Type:          models.SchemaChangeVectorizerChanged,
			ClassName:     className,
			CurrentValue:  map[string]interface{}{"vectorizer": curr.Vectorizer},
			PreviousValue: map[string]interface{}{"vectorizer": prev.Vectorizer},
		})
	}

	// Compare properties
	prevProps := buildPropertyMap(prev)
	currProps := buildPropertyMap(curr)

	// Find added and modified properties
	for propName, currProp := range currProps {
		prevProp, exists := prevProps[propName]
		if !exists {
			// Property was added
			propMap := toMap(currProp)
			result.PropertiesAdded = append(result.PropertiesAdded, &models.SchemaChange{
				Type:         models.SchemaChangePropertyAdded,
				ClassName:    className,
				PropertyName: propName,
				CurrentValue: propMap,
			})
		} else {
			// Check if property was modified
			if !propertiesEqual(prevProp, currProp) {
				result.PropertiesModified = append(result.PropertiesModified, &models.SchemaChange{
					Type:          models.SchemaChangePropertyModified,
					ClassName:     className,
					PropertyName:  propName,
					CurrentValue:  toMap(currProp),
					PreviousValue: toMap(prevProp),
				})
			}
		}
	}

	// Find deleted properties
	for propName, prevProp := range prevProps {
		if _, exists := currProps[propName]; !exists {
			propMap := toMap(prevProp)
			result.PropertiesDeleted = append(result.PropertiesDeleted, &models.SchemaChange{
				Type:          models.SchemaChangePropertyDeleted,
				ClassName:     className,
				PropertyName:  propName,
				PreviousValue: propMap,
			})
		}
	}
}

// propertiesEqual compares two property definitions
func propertiesEqual(a, b *models.WeaviateProperty) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Tokenization != b.Tokenization {
		return false
	}
	if !stringSlicesEqual(a.DataType, b.DataType) {
		return false
	}
	if !boolPtrEqual(a.IndexFilterable, b.IndexFilterable) {
		return false
	}
	if !boolPtrEqual(a.IndexSearchable, b.IndexSearchable) {
		return false
	}
	return true
}

// stringSlicesEqual compares two string slices
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// boolPtrEqual compares two bool pointers
func boolPtrEqual(a, b *bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// toMap converts any struct to a map for storage/display
func toMap(v interface{}) map[string]interface{} {
	if v == nil {
		return nil
	}
	data, _ := json.Marshal(v)
	var m map[string]interface{}
	_ = json.Unmarshal(data, &m)
	return m
}

// HashSchema creates a deterministic hash of a schema
func HashSchema(schema *models.WeaviateSchema) string {
	if schema == nil {
		return ""
	}

	// Sort classes by name for deterministic ordering
	classes := make([]*models.WeaviateClass, len(schema.Classes))
	copy(classes, schema.Classes)
	sort.Slice(classes, func(i, j int) bool {
		return classes[i].Class < classes[j].Class
	})

	// Deep-copy class pointers so we don't mutate the caller's property order
	for i, class := range classes {
		cp := *class
		if class.Properties != nil {
			cp.Properties = make([]*models.WeaviateProperty, len(class.Properties))
			copy(cp.Properties, class.Properties)
			sort.Slice(cp.Properties, func(a, b int) bool {
				return cp.Properties[a].Name < cp.Properties[b].Name
			})
		}
		classes[i] = &cp
	}

	sortedSchema := &models.WeaviateSchema{Classes: classes}
	data, _ := json.Marshal(sortedSchema)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
