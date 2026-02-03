package core

import (
	"testing"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestGenerateCommitID_Deterministic(t *testing.T) {
	message := "Test commit"
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	parentID := "parent123"

	id1 := generateCommitID(message, timestamp, parentID, nil)
	id2 := generateCommitID(message, timestamp, parentID, nil)

	assert.Equal(t, id1, id2, "Same inputs should produce same commit ID")
	assert.Len(t, id1, 64, "Commit ID should be SHA256 hex (64 chars)")
}

func TestGenerateCommitID_DifferentInputs(t *testing.T) {
	timestamp := time.Now()

	id1 := generateCommitID("Message 1", timestamp, "", nil)
	id2 := generateCommitID("Message 2", timestamp, "", nil)

	assert.NotEqual(t, id1, id2, "Different messages should produce different IDs")
}

func TestGenerateCommitID_DifferentParents(t *testing.T) {
	message := "Test"
	timestamp := time.Now()

	id1 := generateCommitID(message, timestamp, "parent1", nil)
	id2 := generateCommitID(message, timestamp, "parent2", nil)

	assert.NotEqual(t, id1, id2, "Different parents should produce different IDs")
}

func TestGenerateCommitID_EmptyParent(t *testing.T) {
	message := "Initial commit"
	timestamp := time.Now()

	// Should work with empty parent (initial commit)
	id := generateCommitID(message, timestamp, "", nil)
	assert.NotEmpty(t, id)
	assert.Len(t, id, 64)
}

func TestGenerateCommitID_DifferentOperations(t *testing.T) {
	message := "Test"
	timestamp := time.Now()
	parentID := "parent123"

	ops1 := []*models.Operation{
		{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-001"},
	}
	ops2 := []*models.Operation{
		{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-002"},
	}

	id1 := generateCommitID(message, timestamp, parentID, ops1)
	id2 := generateCommitID(message, timestamp, parentID, ops2)

	assert.NotEqual(t, id1, id2, "Different operations should produce different commit IDs")
}

func TestGenerateCommitID_OperationsVsNoOperations(t *testing.T) {
	message := "Test"
	timestamp := time.Now()
	parentID := "parent123"

	ops := []*models.Operation{
		{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-001"},
	}

	idNoOps := generateCommitID(message, timestamp, parentID, nil)
	idWithOps := generateCommitID(message, timestamp, parentID, ops)

	assert.NotEqual(t, idNoOps, idWithOps, "Commits with vs without operations should differ")
}

func TestComputeOperationsHash_Deterministic(t *testing.T) {
	ops := []*models.Operation{
		{Type: models.OperationInsert, ClassName: "Article", ObjectID: "obj-001", ObjectData: []byte(`{"title":"A"}`)},
		{Type: models.OperationUpdate, ClassName: "Article", ObjectID: "obj-002", ObjectData: []byte(`{"title":"B"}`)},
	}

	hash1 := computeOperationsHash(ops)
	hash2 := computeOperationsHash(ops)

	assert.Equal(t, hash1, hash2)
	assert.Len(t, hash1, 64)
}

func TestComputeOperationsHash_EmptyOperations(t *testing.T) {
	hash := computeOperationsHash(nil)
	assert.Equal(t, "", hash)

	hash = computeOperationsHash([]*models.Operation{})
	assert.Equal(t, "", hash)
}
