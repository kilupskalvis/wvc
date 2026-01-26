package core

import (
	"testing"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiffSchemas_ClassAdded(t *testing.T) {
	prev := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{},
	}

	curr := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Article", Vectorizer: "text2vec-openai"},
		},
	}

	diff := diffSchemas(curr, prev)

	assert.Len(t, diff.ClassesAdded, 1)
	assert.Equal(t, "Article", diff.ClassesAdded[0].ClassName)
	assert.Empty(t, diff.ClassesDeleted)
}

func TestDiffSchemas_ClassDeleted(t *testing.T) {
	prev := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Article", Vectorizer: "text2vec-openai"},
		},
	}

	curr := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{},
	}

	diff := diffSchemas(curr, prev)

	assert.Empty(t, diff.ClassesAdded)
	assert.Len(t, diff.ClassesDeleted, 1)
	assert.Equal(t, "Article", diff.ClassesDeleted[0].ClassName)
}

func TestDiffSchemas_PropertyAdded(t *testing.T) {
	prev := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{
				Class:      "Article",
				Properties: []*models.WeaviateProperty{},
			},
		},
	}

	curr := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{
				Class: "Article",
				Properties: []*models.WeaviateProperty{
					{Name: "title", DataType: []string{"text"}},
				},
			},
		},
	}

	diff := diffSchemas(curr, prev)

	assert.Empty(t, diff.ClassesAdded)
	assert.Empty(t, diff.ClassesDeleted)
	assert.Len(t, diff.PropertiesAdded, 1)
	assert.Equal(t, "Article", diff.PropertiesAdded[0].ClassName)
	assert.Equal(t, "title", diff.PropertiesAdded[0].PropertyName)
}

func TestDiffSchemas_PropertyDeleted(t *testing.T) {
	prev := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{
				Class: "Article",
				Properties: []*models.WeaviateProperty{
					{Name: "title", DataType: []string{"text"}},
					{Name: "content", DataType: []string{"text"}},
				},
			},
		},
	}

	curr := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{
				Class: "Article",
				Properties: []*models.WeaviateProperty{
					{Name: "title", DataType: []string{"text"}},
				},
			},
		},
	}

	diff := diffSchemas(curr, prev)

	assert.Len(t, diff.PropertiesDeleted, 1)
	assert.Equal(t, "content", diff.PropertiesDeleted[0].PropertyName)
}

func TestDiffSchemas_PropertyModified(t *testing.T) {
	prev := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{
				Class: "Article",
				Properties: []*models.WeaviateProperty{
					{Name: "title", DataType: []string{"text"}, Tokenization: "word"},
				},
			},
		},
	}

	curr := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{
				Class: "Article",
				Properties: []*models.WeaviateProperty{
					{Name: "title", DataType: []string{"text"}, Tokenization: "field"},
				},
			},
		},
	}

	diff := diffSchemas(curr, prev)

	assert.Len(t, diff.PropertiesModified, 1)
	assert.Equal(t, "title", diff.PropertiesModified[0].PropertyName)
}

func TestDiffSchemas_VectorizerChanged(t *testing.T) {
	prev := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Article", Vectorizer: "text2vec-openai"},
		},
	}

	curr := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Article", Vectorizer: "text2vec-cohere"},
		},
	}

	diff := diffSchemas(curr, prev)

	assert.Len(t, diff.VectorizersChanged, 1)
	assert.Equal(t, "Article", diff.VectorizersChanged[0].ClassName)
}

func TestDiffSchemas_NoChanges(t *testing.T) {
	schema := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{
				Class:      "Article",
				Vectorizer: "text2vec-openai",
				Properties: []*models.WeaviateProperty{
					{Name: "title", DataType: []string{"text"}},
				},
			},
		},
	}

	diff := diffSchemas(schema, schema)

	assert.False(t, diff.HasChanges())
	assert.Equal(t, 0, diff.TotalChanges())
}

func TestDiffSchemas_NilPrevious(t *testing.T) {
	curr := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Article"},
		},
	}

	diff := diffSchemas(curr, nil)

	assert.Len(t, diff.ClassesAdded, 1)
}

func TestDiffSchemas_NilCurrent(t *testing.T) {
	prev := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Article"},
		},
	}

	diff := diffSchemas(nil, prev)

	assert.Len(t, diff.ClassesDeleted, 1)
}

func TestHashSchema_Deterministic(t *testing.T) {
	schema := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Article", Vectorizer: "text2vec-openai"},
			{Class: "Product", Vectorizer: "none"},
		},
	}

	hash1 := HashSchema(schema)
	hash2 := HashSchema(schema)

	assert.Equal(t, hash1, hash2)
	assert.NotEmpty(t, hash1)
}

func TestHashSchema_DifferentOrder(t *testing.T) {
	// Classes in different order should produce same hash
	schema1 := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Article"},
			{Class: "Product"},
		},
	}

	schema2 := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Product"},
			{Class: "Article"},
		},
	}

	hash1 := HashSchema(schema1)
	hash2 := HashSchema(schema2)

	assert.Equal(t, hash1, hash2, "Hash should be same regardless of class order")
}

func TestHashSchema_DifferentSchemas(t *testing.T) {
	schema1 := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Article"},
		},
	}

	schema2 := &models.WeaviateSchema{
		Classes: []*models.WeaviateClass{
			{Class: "Product"},
		},
	}

	hash1 := HashSchema(schema1)
	hash2 := HashSchema(schema2)

	assert.NotEqual(t, hash1, hash2, "Different schemas should have different hashes")
}

func TestHashSchema_Nil(t *testing.T) {
	hash := HashSchema(nil)
	assert.Empty(t, hash)
}

func TestSchemaDiffResult_HasChanges(t *testing.T) {
	tests := []struct {
		name       string
		diff       *SchemaDiffResult
		wantChange bool
	}{
		{
			name:       "empty diff",
			diff:       &SchemaDiffResult{},
			wantChange: false,
		},
		{
			name: "class added",
			diff: &SchemaDiffResult{
				ClassesAdded: []*models.SchemaChange{{ClassName: "Test"}},
			},
			wantChange: true,
		},
		{
			name: "property deleted",
			diff: &SchemaDiffResult{
				PropertiesDeleted: []*models.SchemaChange{{ClassName: "Test"}},
			},
			wantChange: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantChange, tt.diff.HasChanges())
		})
	}
}

func TestComputeSchemaDiffBetweenVersions(t *testing.T) {
	prevJSON := []byte(`{"classes": [{"class": "Article"}]}`)
	currJSON := []byte(`{"classes": [{"class": "Article"}, {"class": "Product"}]}`)

	diff, err := ComputeSchemaDiffBetweenVersions(currJSON, prevJSON)
	require.NoError(t, err)

	assert.Len(t, diff.ClassesAdded, 1)
	assert.Equal(t, "Product", diff.ClassesAdded[0].ClassName)
}

func TestComputeSchemaDiffBetweenVersions_EmptyPrevious(t *testing.T) {
	currJSON := []byte(`{"classes": [{"class": "Article"}]}`)

	diff, err := ComputeSchemaDiffBetweenVersions(currJSON, nil)
	require.NoError(t, err)

	assert.Len(t, diff.ClassesAdded, 1)
}

func TestPropertiesEqual(t *testing.T) {
	filterableTrue := true
	filterableFalse := false

	tests := []struct {
		name  string
		a, b  *models.WeaviateProperty
		equal bool
	}{
		{
			name:  "identical",
			a:     &models.WeaviateProperty{Name: "title", DataType: []string{"text"}},
			b:     &models.WeaviateProperty{Name: "title", DataType: []string{"text"}},
			equal: true,
		},
		{
			name:  "different name",
			a:     &models.WeaviateProperty{Name: "title"},
			b:     &models.WeaviateProperty{Name: "content"},
			equal: false,
		},
		{
			name:  "different datatype",
			a:     &models.WeaviateProperty{Name: "title", DataType: []string{"text"}},
			b:     &models.WeaviateProperty{Name: "title", DataType: []string{"int"}},
			equal: false,
		},
		{
			name:  "different tokenization",
			a:     &models.WeaviateProperty{Name: "title", Tokenization: "word"},
			b:     &models.WeaviateProperty{Name: "title", Tokenization: "field"},
			equal: false,
		},
		{
			name:  "different index filterable",
			a:     &models.WeaviateProperty{Name: "title", IndexFilterable: &filterableTrue},
			b:     &models.WeaviateProperty{Name: "title", IndexFilterable: &filterableFalse},
			equal: false,
		},
		{
			name:  "nil vs set",
			a:     &models.WeaviateProperty{Name: "title", IndexFilterable: nil},
			b:     &models.WeaviateProperty{Name: "title", IndexFilterable: &filterableTrue},
			equal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := propertiesEqual(tt.a, tt.b)
			assert.Equal(t, tt.equal, result)
		})
	}
}
