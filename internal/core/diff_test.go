package core

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStore creates a new bbolt store in a temp directory for testing.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.New(dbPath)
	require.NoError(t, err)
	require.NoError(t, st.Initialize())
	require.NoError(t, st.SetCurrentBranch("main"))
	t.Cleanup(func() { st.Close() })
	return st
}

// newTestConfig creates a config for testing
func newTestConfig() *config.Config {
	return &config.Config{
		WeaviateURL:   "localhost:8080",
		ServerVersion: "1.25.0",
	}
}

func TestComputeDiff_NewObjects(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Add object to Weaviate (current state)
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})

	// No known objects in store (initial state)

	diff, err := ComputeDiff(ctx, cfg, st, client)
	require.NoError(t, err)

	assert.Len(t, diff.Inserted, 1)
	assert.Equal(t, "Article", diff.Inserted[0].ClassName)
	assert.Equal(t, "obj-001", diff.Inserted[0].ObjectID)
	assert.Empty(t, diff.Updated)
	assert.Empty(t, diff.Deleted)
}

func TestComputeDiff_DeletedObjects(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Add known object to store
	objData := []byte(`{"id":"obj-001","class":"Article","properties":{"title":"Test"}}`)
	require.NoError(t, st.SaveKnownObject("Article", "obj-001", "hash123", objData))

	// Client has empty state (object was deleted)
	client.AddClass(&models.WeaviateClass{Class: "Article"})

	diff, err := ComputeDiff(ctx, cfg, st, client)
	require.NoError(t, err)

	assert.Empty(t, diff.Inserted)
	assert.Empty(t, diff.Updated)
	assert.Len(t, diff.Deleted, 1)
	assert.Equal(t, "obj-001", diff.Deleted[0].ObjectID)
}

func TestComputeDiff_UpdatedObjects(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Add known object with old content
	oldData := []byte(`{"id":"obj-001","class":"Article","properties":{"title":"Old Title"}}`)
	oldHash := weaviate.HashObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Old Title"},
	})
	require.NoError(t, st.SaveKnownObjectWithVector("Article", "obj-001", oldHash, "", oldData))

	// Add updated object to client
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "New Title"},
	})

	diff, err := ComputeDiff(ctx, cfg, st, client)
	require.NoError(t, err)

	assert.Empty(t, diff.Inserted)
	assert.Len(t, diff.Updated, 1)
	assert.Equal(t, "obj-001", diff.Updated[0].ObjectID)
	assert.Empty(t, diff.Deleted)
}

func TestComputeDiff_NoChanges(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	obj := &models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Same"},
	}

	// Add to both client and store with same hash
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(obj)

	objData := []byte(`{"id":"obj-001","class":"Article","properties":{"title":"Same"}}`)
	hash := weaviate.HashObject(obj)
	require.NoError(t, st.SaveKnownObjectWithVector("Article", "obj-001", hash, "", objData))

	diff, err := ComputeDiff(ctx, cfg, st, client)
	require.NoError(t, err)

	assert.Equal(t, 0, diff.TotalChanges())
}

func TestComputeDiff_MultipleChanges(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	client.AddClass(&models.WeaviateClass{Class: "Article"})

	// Add new object
	client.AddObject(&models.WeaviateObject{
		ID:         "new-obj",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "New"},
	})

	// Add unchanged object to both
	unchangedObj := &models.WeaviateObject{
		ID:         "unchanged",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Unchanged"},
	}
	client.AddObject(unchangedObj)
	unchangedData := []byte(`{"id":"unchanged","class":"Article","properties":{"title":"Unchanged"}}`)
	require.NoError(t, st.SaveKnownObjectWithVector("Article", "unchanged", weaviate.HashObject(unchangedObj), "", unchangedData))

	// Add deleted object only to store
	deletedData := []byte(`{"id":"deleted","class":"Article","properties":{"title":"Deleted"}}`)
	require.NoError(t, st.SaveKnownObject("Article", "deleted", "hash", deletedData))

	diff, err := ComputeDiff(ctx, cfg, st, client)
	require.NoError(t, err)

	assert.Len(t, diff.Inserted, 1)
	assert.Len(t, diff.Deleted, 1)
	assert.Equal(t, 2, diff.TotalChanges())
}

func TestDiffResult_TotalChanges(t *testing.T) {
	diff := &DiffResult{
		Inserted: []*ObjectChange{{}, {}},
		Updated:  []*ObjectChange{{}},
		Deleted:  []*ObjectChange{{}, {}, {}},
	}

	assert.Equal(t, 6, diff.TotalChanges())
}

func TestRecordDiffAsOperations(t *testing.T) {
	st := newTestStore(t)

	diff := &DiffResult{
		Inserted: []*ObjectChange{
			{
				ClassName: "Article",
				ObjectID:  "obj-001",
				CurrentData: &models.WeaviateObject{
					ID:         "obj-001",
					Class:      "Article",
					Properties: map[string]interface{}{"title": "Test"},
				},
			},
		},
		Updated: []*ObjectChange{
			{
				ClassName: "Article",
				ObjectID:  "obj-002",
				CurrentData: &models.WeaviateObject{
					ID:         "obj-002",
					Class:      "Article",
					Properties: map[string]interface{}{"title": "Updated"},
				},
				PreviousData: &models.WeaviateObject{
					ID:         "obj-002",
					Class:      "Article",
					Properties: map[string]interface{}{"title": "Original"},
				},
			},
		},
		Deleted: []*ObjectChange{
			{
				ClassName: "Article",
				ObjectID:  "obj-003",
				PreviousData: &models.WeaviateObject{
					ID:         "obj-003",
					Class:      "Article",
					Properties: map[string]interface{}{"title": "Deleted"},
				},
			},
		},
	}

	err := RecordDiffAsOperations(st, diff)
	require.NoError(t, err)

	ops, err := st.GetUncommittedOperations()
	require.NoError(t, err)
	assert.Len(t, ops, 3)

	// Verify operation types
	var insertCount, updateCount, deleteCount int
	for _, op := range ops {
		switch op.Type {
		case models.OperationInsert:
			insertCount++
		case models.OperationUpdate:
			updateCount++
		case models.OperationDelete:
			deleteCount++
		}
	}
	assert.Equal(t, 1, insertCount)
	assert.Equal(t, 1, updateCount)
	assert.Equal(t, 1, deleteCount)
}

func TestUpdateKnownState(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	client := weaviate.NewMockClient()

	// Add objects to client
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test 1"},
	})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test 2"},
	})

	err := UpdateKnownState(ctx, st, client, true)
	require.NoError(t, err)

	// Verify objects are in known state
	objects, err := st.GetAllKnownObjects()
	require.NoError(t, err)
	assert.Len(t, objects, 2)
}
