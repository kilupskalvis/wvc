// Package weaviate provides a client wrapper for interacting with Weaviate.
// It handles object fetching, creation, updates, and deletion with support
// for multiple Weaviate server versions.
package weaviate

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	weaviatemodels "github.com/weaviate/weaviate/entities/models"
)

// ServerVersion holds parsed Weaviate version info
type ServerVersion struct {
	Version string // e.g., "1.25.0"
	Major   int
	Minor   int
	Patch   int
}

// parseVersion parses a version string like "1.25.0" into ServerVersion
func parseVersion(version string) (*ServerVersion, error) {
	re := regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(version)
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid version format: %s", version)
	}

	major, _ := strconv.Atoi(matches[1])
	minor, _ := strconv.Atoi(matches[2])
	patch, _ := strconv.Atoi(matches[3])

	return &ServerVersion{
		Version: version,
		Major:   major,
		Minor:   minor,
		Patch:   patch,
	}, nil
}

// SupportsFeature checks if the server supports a specific feature
func (v *ServerVersion) SupportsFeature(feature string) bool {
	switch feature {
	case "cursor_pagination":
		return v.Major > 1 || (v.Major == 1 && v.Minor >= 18)
	case "multi_vector":
		return v.Major > 1 || (v.Major == 1 && v.Minor >= 24)
	default:
		return true
	}
}

// Client wraps the Weaviate client with WVC-specific functionality
type Client struct {
	client *weaviate.Client
	url    string
}

// NewClient creates a new Weaviate client
func NewClient(url string) (*Client, error) {
	cfg := weaviate.Config{
		Host:   url,
		Scheme: "http",
	}

	// Handle URL parsing
	if len(url) > 7 && url[:7] == "http://" {
		cfg.Host = url[7:]
		cfg.Scheme = "http"
	} else if len(url) > 8 && url[:8] == "https://" {
		cfg.Host = url[8:]
		cfg.Scheme = "https"
	}

	client, err := weaviate.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Weaviate client: %w", err)
	}

	return &Client{
		client: client,
		url:    url,
	}, nil
}

// Ping checks if Weaviate is reachable
func (c *Client) Ping(ctx context.Context) error {
	live, err := c.client.Misc().LiveChecker().Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Weaviate: %w", err)
	}
	if !live {
		return fmt.Errorf("weaviate is not live")
	}
	return nil
}

// GetServerVersion fetches and parses the Weaviate server version
func (c *Client) GetServerVersion(ctx context.Context) (*ServerVersion, error) {
	meta, err := c.client.Misc().MetaGetter().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get server metadata: %w", err)
	}
	return parseVersion(meta.Version)
}

// GetSchema retrieves the current Weaviate schema as JSON
func (c *Client) GetSchema(ctx context.Context) ([]byte, error) {
	schema, err := c.client.Schema().Getter().Do(ctx)
	if err != nil {
		return nil, err
	}
	return json.Marshal(schema)
}

// GetSchemaTyped retrieves the current Weaviate schema as a typed struct
func (c *Client) GetSchemaTyped(ctx context.Context) (*models.WeaviateSchema, error) {
	schema, err := c.client.Schema().Getter().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	result := &models.WeaviateSchema{
		Classes: make([]*models.WeaviateClass, 0, len(schema.Classes)),
	}

	for _, class := range schema.Classes {
		wc := &models.WeaviateClass{
			Class:       class.Class,
			Vectorizer:  class.Vectorizer,
			Description: class.Description,
			Properties:  make([]*models.WeaviateProperty, 0),
		}

		// Convert vector index config
		if class.VectorIndexConfig != nil {
			data, _ := json.Marshal(class.VectorIndexConfig)
			_ = json.Unmarshal(data, &wc.VectorIndexConfig)
		}

		// Convert module config
		if class.ModuleConfig != nil {
			data, _ := json.Marshal(class.ModuleConfig)
			_ = json.Unmarshal(data, &wc.ModuleConfig)
		}

		// Convert properties
		for _, prop := range class.Properties {
			wp := &models.WeaviateProperty{
				Name:            prop.Name,
				DataType:        prop.DataType,
				Description:     prop.Description,
				Tokenization:    string(prop.Tokenization),
				IndexFilterable: prop.IndexFilterable,
				IndexSearchable: prop.IndexSearchable,
				IndexInverted:   prop.IndexInverted,
			}
			wc.Properties = append(wc.Properties, wp)
		}

		result.Classes = append(result.Classes, wc)
	}

	return result, nil
}

// CreateClass creates a new class in Weaviate
func (c *Client) CreateClass(ctx context.Context, class *models.WeaviateClass) error {
	creator := c.client.Schema().ClassCreator()

	// Build the class object
	classObj := &weaviatemodels.Class{
		Class:       class.Class,
		Description: class.Description,
		Vectorizer:  class.Vectorizer,
	}

	// Add properties
	for _, prop := range class.Properties {
		p := &weaviatemodels.Property{
			Name:            prop.Name,
			DataType:        prop.DataType,
			Description:     prop.Description,
			IndexFilterable: prop.IndexFilterable,
			IndexSearchable: prop.IndexSearchable,
			Tokenization:    prop.Tokenization,
		}
		classObj.Properties = append(classObj.Properties, p)
	}

	return creator.WithClass(classObj).Do(ctx)
}

// DeleteClass deletes a class from Weaviate
func (c *Client) DeleteClass(ctx context.Context, className string) error {
	return c.client.Schema().ClassDeleter().WithClassName(className).Do(ctx)
}

// AddProperty adds a property to an existing class
func (c *Client) AddProperty(ctx context.Context, className string, property *models.WeaviateProperty) error {
	prop := &weaviatemodels.Property{
		Name:            property.Name,
		DataType:        property.DataType,
		Description:     property.Description,
		IndexFilterable: property.IndexFilterable,
		IndexSearchable: property.IndexSearchable,
		Tokenization:    property.Tokenization,
	}

	return c.client.Schema().PropertyCreator().
		WithClassName(className).
		WithProperty(prop).
		Do(ctx)
}

// GetClasses returns all class names in the schema
func (c *Client) GetClasses(ctx context.Context) ([]string, error) {
	schema, err := c.client.Schema().Getter().Do(ctx)
	if err != nil {
		return nil, err
	}

	var classes []string
	for _, class := range schema.Classes {
		classes = append(classes, class.Class)
	}
	return classes, nil
}

// GetClassCount returns the number of objects in a class using aggregate query
func (c *Client) GetClassCount(ctx context.Context, className string) (int, error) {
	metaField := graphql.Field{
		Name: "meta",
		Fields: []graphql.Field{
			{Name: "count"},
		},
	}

	result, err := c.client.GraphQL().Aggregate().
		WithClassName(className).
		WithFields(metaField).
		Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get count for %s: %w", className, err)
	}

	// Parse the aggregate result
	data, ok := result.Data["Aggregate"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("unexpected aggregate response format")
	}

	classData, ok := data[className].([]interface{})
	if !ok || len(classData) == 0 {
		return 0, nil
	}

	first, ok := classData[0].(map[string]interface{})
	if !ok {
		return 0, nil
	}

	meta, ok := first["meta"].(map[string]interface{})
	if !ok {
		return 0, nil
	}

	count, ok := meta["count"].(float64)
	if !ok {
		return 0, nil
	}

	return int(count), nil
}

// CheckObjectExists checks if an object exists in Weaviate
func (c *Client) CheckObjectExists(ctx context.Context, className, objectID string) (bool, error) {
	objs, err := c.client.Data().ObjectsGetter().
		WithClassName(className).
		WithID(objectID).
		Do(ctx)
	if err != nil {
		// Weaviate returns error for not found in some versions
		return false, nil
	}
	return len(objs) > 0, nil
}

// GetAllObjects fetches all objects from a class with pagination method based on useCursor flag
func (c *Client) GetAllObjects(ctx context.Context, className string, useCursor bool) ([]*models.WeaviateObject, error) {
	if useCursor {
		return c.getAllObjectsCursor(ctx, className)
	}
	return c.getAllObjectsOffset(ctx, className)
}

// getAllObjectsCursor uses WithAfter cursor pagination (Weaviate 1.18+)
func (c *Client) getAllObjectsCursor(ctx context.Context, className string) ([]*models.WeaviateObject, error) {
	var allObjects []*models.WeaviateObject
	limit := 100
	afterCursor := ""

	for {
		getter := c.client.Data().ObjectsGetter().
			WithClassName(className).
			WithVector().
			WithLimit(limit)

		// Use cursor-based pagination with WithAfter
		if afterCursor != "" {
			getter = getter.WithAfter(afterCursor)
		}

		objs, err := getter.Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch objects from %s: %w", className, err)
		}

		if len(objs) == 0 {
			break
		}

		for _, obj := range objs {
			wvcObj := convertToWVCObject(obj)
			if wvcObj != nil {
				allObjects = append(allObjects, wvcObj)
			}
		}

		// Set cursor for next page (use the last object's ID)
		if len(objs) < limit {
			break
		}
		afterCursor = objs[len(objs)-1].ID.String()
	}

	return allObjects, nil
}

// getAllObjectsOffset uses offset/limit pagination (older Weaviate versions)
func (c *Client) getAllObjectsOffset(ctx context.Context, className string) ([]*models.WeaviateObject, error) {
	var allObjects []*models.WeaviateObject
	limit := 100
	offset := 0

	for {
		objs, err := c.client.Data().ObjectsGetter().
			WithClassName(className).
			WithVector().
			WithLimit(limit).
			WithOffset(offset).
			Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch objects from %s: %w", className, err)
		}

		if len(objs) == 0 {
			break
		}

		for _, obj := range objs {
			wvcObj := convertToWVCObject(obj)
			if wvcObj != nil {
				allObjects = append(allObjects, wvcObj)
			}
		}

		if len(objs) < limit {
			break
		}
		offset += limit
	}

	return allObjects, nil
}

// GetAllObjectsAllClasses fetches all objects from all classes
func (c *Client) GetAllObjectsAllClasses(ctx context.Context, useCursor bool) (map[string]*models.WeaviateObject, error) {
	classes, err := c.GetClasses(ctx)
	if err != nil {
		return nil, err
	}

	allObjects := make(map[string]*models.WeaviateObject)

	for _, className := range classes {
		objects, err := c.GetAllObjects(ctx, className, useCursor)
		if err != nil {
			return nil, err
		}

		for _, obj := range objects {
			key := models.ObjectKey(className, obj.ID)
			allObjects[key] = obj
		}
	}

	return allObjects, nil
}

// GetObject fetches a single object by class and ID
func (c *Client) GetObject(ctx context.Context, className, objectID string) (*models.WeaviateObject, error) {
	objs, err := c.client.Data().ObjectsGetter().
		WithClassName(className).
		WithID(objectID).
		WithVector().
		Do(ctx)

	if err != nil {
		return nil, err
	}

	if len(objs) == 0 {
		return nil, fmt.Errorf("object not found")
	}

	return convertToWVCObject(objs[0]), nil
}

// DeleteObject deletes an object by class and ID
func (c *Client) DeleteObject(ctx context.Context, className, objectID string) error {
	return c.client.Data().Deleter().
		WithClassName(className).
		WithID(objectID).
		Do(ctx)
}

// CreateObject creates a new object
func (c *Client) CreateObject(ctx context.Context, obj *models.WeaviateObject) error {
	creator := c.client.Data().Creator().
		WithClassName(obj.Class).
		WithID(obj.ID).
		WithProperties(obj.Properties)

	if vec := vectorToFloat32(obj.Vector); vec != nil {
		creator = creator.WithVector(vec)
	}

	_, err := creator.Do(ctx)
	return err
}

// UpdateObject updates an existing object
func (c *Client) UpdateObject(ctx context.Context, obj *models.WeaviateObject) error {
	updater := c.client.Data().Updater().
		WithClassName(obj.Class).
		WithID(obj.ID).
		WithProperties(obj.Properties)

	if vec := vectorToFloat32(obj.Vector); vec != nil {
		updater = updater.WithVector(vec)
	}

	return updater.Do(ctx)
}

// vectorToFloat32 converts various vector representations to []float32
func vectorToFloat32(v interface{}) []float32 {
	if v == nil {
		return nil
	}

	switch vec := v.(type) {
	case []float32:
		return vec
	case []float64:
		result := make([]float32, len(vec))
		for i, f := range vec {
			result[i] = float32(f)
		}
		return result
	case []interface{}:
		if len(vec) == 0 {
			return nil
		}
		result := make([]float32, len(vec))
		for i, val := range vec {
			switch f := val.(type) {
			case float64:
				result[i] = float32(f)
			case float32:
				result[i] = f
			default:
				return nil
			}
		}
		return result
	default:
		return nil
	}
}

// convertToWVCObject converts a Weaviate API object to our internal model
func convertToWVCObject(obj interface{}) *models.WeaviateObject {
	// Use JSON marshaling/unmarshaling for safe conversion
	// This handles the interface{} vector type in v5 properly
	data, err := json.Marshal(obj)
	if err != nil {
		return nil
	}

	var raw struct {
		ID         string                 `json:"id"`
		Class      string                 `json:"class"`
		Properties map[string]interface{} `json:"properties"`
		Vector     interface{}            `json:"vector"`
		Additional struct {
			CreationTimeUnix   int64 `json:"creationTimeUnix"`
			LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix"`
		} `json:"additional"`
		// Also check top-level fields (some Weaviate versions)
		CreationTimeUnix   int64 `json:"creationTimeUnix"`
		LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil
	}

	// Get timestamps from additional section or top-level
	creationTime := raw.Additional.CreationTimeUnix
	if creationTime == 0 {
		creationTime = raw.CreationTimeUnix
	}
	lastUpdateTime := raw.Additional.LastUpdateTimeUnix
	if lastUpdateTime == 0 {
		lastUpdateTime = raw.LastUpdateTimeUnix
	}

	return &models.WeaviateObject{
		ID:                 raw.ID,
		Class:              raw.Class,
		Properties:         raw.Properties,
		Vector:             raw.Vector,
		CreationTimeUnix:   creationTime,
		LastUpdateTimeUnix: lastUpdateTime,
	}
}

// HashObject creates a hash of an object's properties (excluding vector)
func HashObject(obj *models.WeaviateObject) string {
	// Sort property keys for deterministic hashing
	keys := make([]string, 0, len(obj.Properties))
	for k := range obj.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build sorted properties map
	sortedProps := make([]byte, 0, 256)
	sortedProps = append(sortedProps, '{')
	for i, k := range keys {
		if i > 0 {
			sortedProps = append(sortedProps, ',')
		}
		keyJSON, _ := json.Marshal(k)
		valJSON, _ := json.Marshal(obj.Properties[k])
		sortedProps = append(sortedProps, keyJSON...)
		sortedProps = append(sortedProps, ':')
		sortedProps = append(sortedProps, valJSON...)
	}
	sortedProps = append(sortedProps, '}')

	// Build final deterministic JSON: class, id, properties (alphabetically)
	classJSON, _ := json.Marshal(obj.Class)
	idJSON, _ := json.Marshal(obj.ID)

	var buf []byte
	buf = append(buf, `{"class":`...)
	buf = append(buf, classJSON...)
	buf = append(buf, `,"id":`...)
	buf = append(buf, idJSON...)
	buf = append(buf, `,"properties":`...)
	buf = append(buf, sortedProps...)
	buf = append(buf, '}')

	hash := sha256.Sum256(buf)
	return hex.EncodeToString(hash[:])
}

// HashObjectFull creates hashes for both properties and vector.
// Returns (objectHash, vectorHash) where objectHash is the property-only hash
// and vectorHash is the SHA256 of the raw vector bytes.
func HashObjectFull(obj *models.WeaviateObject) (objectHash string, vectorHash string) {
	// Get property-only hash using existing function
	objectHash = HashObject(obj)

	// Compute vector hash if present
	if obj.Vector != nil {
		vectorBytes, _ := vectorToBytes(obj.Vector)
		if len(vectorBytes) > 0 {
			hash := sha256.Sum256(vectorBytes)
			vectorHash = hex.EncodeToString(hash[:])
		}
	}

	return objectHash, vectorHash
}

// vectorToBytes converts a vector to raw binary float32 bytes (little-endian).
// Returns (bytes, error). On error, returns (nil, error).
func vectorToBytes(v interface{}) ([]byte, error) {
	if v == nil {
		return nil, nil
	}

	var floats []float32

	switch vec := v.(type) {
	case []float32:
		floats = vec
	case []float64:
		floats = make([]float32, len(vec))
		for i, f := range vec {
			floats[i] = float32(f)
		}
	case []interface{}:
		floats = make([]float32, len(vec))
		for i, val := range vec {
			switch n := val.(type) {
			case float64:
				floats[i] = float32(n)
			case float32:
				floats[i] = n
			case int:
				floats[i] = float32(n)
			case int64:
				floats[i] = float32(n)
			default:
				return nil, fmt.Errorf("unsupported element type %T at index %d", val, i)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported vector type %T", v)
	}

	if len(floats) == 0 {
		return nil, nil
	}

	// Convert to little-endian binary
	buf := make([]byte, len(floats)*4)
	for i, f := range floats {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}

	return buf, nil
}
