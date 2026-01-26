package core

import (
	"context"
	"testing"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/weaviate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests that verify full workflows

func TestCreateCommit_FullWorkflow(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Add objects to Weaviate
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First Article"},
	})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second Article"},
	})

	// Act: Create commit
	commit, err := CreateCommit(ctx, cfg, st, client, "Initial commit")
	require.NoError(t, err)

	// Assert: Commit was created
	assert.NotEmpty(t, commit.ID)
	assert.Equal(t, "Initial commit", commit.Message)
	assert.Equal(t, 2, commit.OperationCount)

	// Assert: HEAD was updated
	head, err := st.GetHEAD()
	require.NoError(t, err)
	assert.Equal(t, commit.ID, head)

	// Assert: Operations were recorded and committed
	ops, err := st.GetOperationsByCommit(commit.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 2)

	// Assert: Known state was updated
	knownObjects, err := st.GetAllKnownObjects()
	require.NoError(t, err)
	assert.Len(t, knownObjects, 2)

	// Assert: No uncommitted operations remain
	uncommitted, err := st.GetUncommittedOperations()
	require.NoError(t, err)
	assert.Len(t, uncommitted, 0)
}

func TestCreateCommit_NoChanges(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create initial commit with schema and object
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})
	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Act: Try to create another commit with no changes
	_, err = CreateCommit(ctx, cfg, st, client, "Empty commit")

	// Assert: Should fail because nothing changed
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no changes to commit")
}

func TestCreateCommit_SecondCommit(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create first commit
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})

	commit1, err := CreateCommit(ctx, cfg, st, client, "First commit")
	require.NoError(t, err)

	// Add more objects
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})

	// Act: Create second commit
	commit2, err := CreateCommit(ctx, cfg, st, client, "Second commit")
	require.NoError(t, err)

	// Assert: Second commit has correct parent
	assert.Equal(t, commit1.ID, commit2.ParentID)
	assert.Equal(t, 1, commit2.OperationCount) // Only the new object

	// Assert: Commit log shows both commits
	log, err := st.GetCommitLog(0)
	require.NoError(t, err)
	assert.Len(t, log, 2)
}

func TestCreateCommit_UpdateAndDelete(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create initial state
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Original"},
	})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "To Delete"},
	})

	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Modify state: update one, delete one
	client.Objects["Article/obj-001"].Properties["title"] = "Updated"
	delete(client.Objects, "Article/obj-002")

	// Act: Create commit with changes
	commit, err := CreateCommit(ctx, cfg, st, client, "Update and delete")
	require.NoError(t, err)

	// Assert: Both operations recorded
	assert.Equal(t, 2, commit.OperationCount)

	ops, err := st.GetOperationsByCommit(commit.ID)
	require.NoError(t, err)

	var hasUpdate, hasDelete bool
	for _, op := range ops {
		if op.Type == models.OperationUpdate {
			hasUpdate = true
		}
		if op.Type == models.OperationDelete {
			hasDelete = true
		}
	}
	assert.True(t, hasUpdate, "Should have update operation")
	assert.True(t, hasDelete, "Should have delete operation")
}

func TestStageAndCommit_Workflow(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Add objects
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})

	// Act: Stage all changes
	count, err := StageAll(ctx, cfg, st, client)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Verify staging
	staged, err := st.GetAllStagedChanges()
	require.NoError(t, err)
	assert.Len(t, staged, 2)

	// Act: Commit from staging
	commit, err := CreateCommitFromStaging(ctx, cfg, st, client, "Staged commit")
	require.NoError(t, err)

	// Assert
	assert.Equal(t, 2, commit.OperationCount)

	// Staging should be cleared
	staged, err = st.GetAllStagedChanges()
	require.NoError(t, err)
	assert.Len(t, staged, 0)
}

func TestStageObject_Selective(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Add multiple objects
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})

	// Act: Stage only one object
	err := StageObject(ctx, cfg, st, client, "Article", "obj-001")
	require.NoError(t, err)

	// Assert: Only one object staged
	staged, err := st.GetAllStagedChanges()
	require.NoError(t, err)
	assert.Len(t, staged, 1)
	assert.Equal(t, "obj-001", staged[0].ObjectID)
}

func TestUnstageAll(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Stage some objects
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})

	_, err := StageAll(ctx, cfg, st, client)
	require.NoError(t, err)

	// Act: Unstage all
	err = UnstageAll(st)
	require.NoError(t, err)

	// Assert: Nothing staged
	staged, err := st.GetAllStagedChanges()
	require.NoError(t, err)
	assert.Len(t, staged, 0)
}

func TestRevertCommit_Insert(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create commit with insert
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})

	commit, err := CreateCommit(ctx, cfg, st, client, "Add object")
	require.NoError(t, err)

	// Act: Revert the commit
	revertCommit, err := RevertCommit(ctx, cfg, st, client, commit.ID)
	require.NoError(t, err)

	// Assert: Revert commit was created
	assert.Contains(t, revertCommit.Message, "Revert")
	assert.Equal(t, commit.ID, revertCommit.ParentID)

	// Assert: Object was deleted from Weaviate
	_, err = client.GetObject(ctx, "Article", "obj-001")
	assert.Error(t, err, "Object should be deleted after revert")
}

func TestRevertCommit_Delete(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Create initial state with object
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})

	_, err := CreateCommit(ctx, cfg, st, client, "Initial")
	require.NoError(t, err)

	// Delete the object and commit
	delete(client.Objects, "Article/obj-001")
	deleteCommit, err := CreateCommit(ctx, cfg, st, client, "Delete object")
	require.NoError(t, err)

	// Act: Revert the delete
	_, err = RevertCommit(ctx, cfg, st, client, deleteCommit.ID)
	require.NoError(t, err)

	// Assert: Object was restored
	obj, err := client.GetObject(ctx, "Article", "obj-001")
	require.NoError(t, err)
	assert.Equal(t, "Test", obj.Properties["title"])
}

func TestIncrementalDiff_StagedVsUnstaged(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Add objects
	client.AddClass(&models.WeaviateClass{Class: "Article"})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "First"},
	})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-002",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Second"},
	})

	// Stage only one object
	err := StageObject(ctx, cfg, st, client, "Article", "obj-001")
	require.NoError(t, err)

	// Act: Get incremental diff
	diff, err := ComputeIncrementalDiff(ctx, cfg, st, client)
	require.NoError(t, err)

	// Assert: One staged, one unstaged
	assert.Equal(t, 1, diff.TotalStagedChanges())
	assert.Equal(t, 1, diff.TotalUnstagedChanges())
}

func TestSchemaChangeWithCommit(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	// Setup: Add class and object
	client.AddClass(&models.WeaviateClass{
		Class:      "Article",
		Vectorizer: "text2vec-openai",
	})
	client.AddObject(&models.WeaviateObject{
		ID:         "obj-001",
		Class:      "Article",
		Properties: map[string]interface{}{"title": "Test"},
	})

	// Act: Create commit
	commit, err := CreateCommit(ctx, cfg, st, client, "Initial with schema")
	require.NoError(t, err)

	// Assert: Schema was captured
	schemaVersion, err := st.GetSchemaVersionByCommit(commit.ID)
	require.NoError(t, err)
	assert.NotNil(t, schemaVersion)
	assert.NotEmpty(t, schemaVersion.SchemaJSON)
	assert.NotEmpty(t, schemaVersion.SchemaHash)
}

func TestCommitLog_Order(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	cfg := newTestConfig()
	client := weaviate.NewMockClient()

	client.AddClass(&models.WeaviateClass{Class: "Article"})

	// Create multiple commits
	for i := 1; i <= 5; i++ {
		client.AddObject(&models.WeaviateObject{
			ID:         string(rune('a' + i)),
			Class:      "Article",
			Properties: map[string]interface{}{"num": i},
		})
		_, err := CreateCommit(ctx, cfg, st, client, "Commit "+string(rune('0'+i)))
		require.NoError(t, err)
	}

	// Get log
	log, err := st.GetCommitLog(0)
	require.NoError(t, err)
	assert.Len(t, log, 5)

	// Most recent should be first
	assert.Equal(t, "Commit 5", log[0].Message)
	assert.Equal(t, "Commit 1", log[4].Message)
}
