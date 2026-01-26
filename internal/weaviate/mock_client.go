package weaviate

import (
	"context"
	"fmt"

	"github.com/kilupskalvis/wvc/internal/models"
)

// MockClient is a mock implementation of ClientInterface for testing.
type MockClient struct {
	// Objects stores objects by "ClassName/ObjectID" key
	Objects map[string]*models.WeaviateObject
	// Schema is the current mock schema
	Schema *models.WeaviateSchema
	// Err can be set to make methods return an error
	Err error
	// ClassCounts can be set to return specific counts (otherwise computed from Objects)
	ClassCounts map[string]int
}

// NewMockClient creates a new MockClient for testing.
func NewMockClient() *MockClient {
	return &MockClient{
		Objects: make(map[string]*models.WeaviateObject),
		Schema: &models.WeaviateSchema{
			Classes: []*models.WeaviateClass{},
		},
		ClassCounts: make(map[string]int),
	}
}

// AddObject adds an object to the mock store.
func (m *MockClient) AddObject(obj *models.WeaviateObject) {
	key := models.ObjectKey(obj.Class, obj.ID)
	m.Objects[key] = obj
}

// AddClass adds a class to the mock schema.
func (m *MockClient) AddClass(class *models.WeaviateClass) {
	m.Schema.Classes = append(m.Schema.Classes, class)
}

// GetSchemaTyped returns the mock schema.
func (m *MockClient) GetSchemaTyped(ctx context.Context) (*models.WeaviateSchema, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return m.Schema, nil
}

// CreateClass adds a class to the mock schema.
func (m *MockClient) CreateClass(ctx context.Context, class *models.WeaviateClass) error {
	if m.Err != nil {
		return m.Err
	}
	m.Schema.Classes = append(m.Schema.Classes, class)
	return nil
}

// DeleteClass removes a class from the mock schema.
func (m *MockClient) DeleteClass(ctx context.Context, className string) error {
	if m.Err != nil {
		return m.Err
	}
	for i, c := range m.Schema.Classes {
		if c.Class == className {
			m.Schema.Classes = append(m.Schema.Classes[:i], m.Schema.Classes[i+1:]...)
			break
		}
	}
	// Also delete objects of this class
	for key := range m.Objects {
		if obj := m.Objects[key]; obj.Class == className {
			delete(m.Objects, key)
		}
	}
	return nil
}

// AddProperty adds a property to a class in the mock schema.
func (m *MockClient) AddProperty(ctx context.Context, className string, property *models.WeaviateProperty) error {
	if m.Err != nil {
		return m.Err
	}
	for _, c := range m.Schema.Classes {
		if c.Class == className {
			c.Properties = append(c.Properties, property)
			return nil
		}
	}
	return fmt.Errorf("class %s not found", className)
}

// GetClasses returns all class names from the mock schema.
func (m *MockClient) GetClasses(ctx context.Context) ([]string, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	var classes []string
	for _, c := range m.Schema.Classes {
		classes = append(classes, c.Class)
	}
	return classes, nil
}

// GetAllObjectsAllClasses returns all objects in the mock store.
func (m *MockClient) GetAllObjectsAllClasses(ctx context.Context, useCursor bool) (map[string]*models.WeaviateObject, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	result := make(map[string]*models.WeaviateObject)
	for k, v := range m.Objects {
		result[k] = v
	}
	return result, nil
}

// GetAllObjects returns all objects of a specific class.
func (m *MockClient) GetAllObjects(ctx context.Context, className string, useCursor bool) ([]*models.WeaviateObject, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	var result []*models.WeaviateObject
	for _, obj := range m.Objects {
		if obj.Class == className {
			result = append(result, obj)
		}
	}
	return result, nil
}

// GetObject returns a specific object from the mock store.
func (m *MockClient) GetObject(ctx context.Context, className, objectID string) (*models.WeaviateObject, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	key := models.ObjectKey(className, objectID)
	obj, ok := m.Objects[key]
	if !ok {
		return nil, fmt.Errorf("object not found: %s/%s", className, objectID)
	}
	return obj, nil
}

// CreateObject adds an object to the mock store.
func (m *MockClient) CreateObject(ctx context.Context, obj *models.WeaviateObject) error {
	if m.Err != nil {
		return m.Err
	}
	key := models.ObjectKey(obj.Class, obj.ID)
	m.Objects[key] = obj
	return nil
}

// UpdateObject updates an object in the mock store.
func (m *MockClient) UpdateObject(ctx context.Context, obj *models.WeaviateObject) error {
	if m.Err != nil {
		return m.Err
	}
	key := models.ObjectKey(obj.Class, obj.ID)
	if _, ok := m.Objects[key]; !ok {
		return fmt.Errorf("object not found: %s/%s", obj.Class, obj.ID)
	}
	m.Objects[key] = obj
	return nil
}

// DeleteObject removes an object from the mock store.
func (m *MockClient) DeleteObject(ctx context.Context, className, objectID string) error {
	if m.Err != nil {
		return m.Err
	}
	key := models.ObjectKey(className, objectID)
	delete(m.Objects, key)
	return nil
}

// GetClassCount returns the count of objects in a class.
func (m *MockClient) GetClassCount(ctx context.Context, className string) (int, error) {
	if m.Err != nil {
		return 0, m.Err
	}
	// Check if a specific count was set
	if count, ok := m.ClassCounts[className]; ok {
		return count, nil
	}
	// Otherwise compute from objects
	count := 0
	for _, obj := range m.Objects {
		if obj.Class == className {
			count++
		}
	}
	return count, nil
}

// Verify MockClient implements ClientInterface
var _ ClientInterface = (*MockClient)(nil)
