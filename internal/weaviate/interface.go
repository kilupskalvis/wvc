package weaviate

import (
	"context"

	"github.com/kilupskalvis/wvc/internal/models"
)

// ClientInterface defines the contract for Weaviate client operations.
// This interface enables mocking for testing the core package.
type ClientInterface interface {
	// Schema operations
	GetSchemaTyped(ctx context.Context) (*models.WeaviateSchema, error)
	CreateClass(ctx context.Context, class *models.WeaviateClass) error
	DeleteClass(ctx context.Context, className string) error
	AddProperty(ctx context.Context, className string, property *models.WeaviateProperty) error
	GetClasses(ctx context.Context) ([]string, error)

	// Object operations
	GetAllObjectsAllClasses(ctx context.Context, useCursor bool) (map[string]*models.WeaviateObject, error)
	GetAllObjects(ctx context.Context, className string, useCursor bool) ([]*models.WeaviateObject, error)
	GetObject(ctx context.Context, className, objectID string) (*models.WeaviateObject, error)
	CreateObject(ctx context.Context, obj *models.WeaviateObject) error
	UpdateObject(ctx context.Context, obj *models.WeaviateObject) error
	DeleteObject(ctx context.Context, className, objectID string) error

	// Query operations
	GetClassCount(ctx context.Context, className string) (int, error)
}

// Verify that *Client implements ClientInterface at compile time
var _ ClientInterface = (*Client)(nil)
