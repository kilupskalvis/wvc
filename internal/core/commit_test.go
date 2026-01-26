package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenerateCommitID_Deterministic(t *testing.T) {
	message := "Test commit"
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	parentID := "parent123"

	id1 := generateCommitID(message, timestamp, parentID)
	id2 := generateCommitID(message, timestamp, parentID)

	assert.Equal(t, id1, id2, "Same inputs should produce same commit ID")
	assert.Len(t, id1, 64, "Commit ID should be SHA256 hex (64 chars)")
}

func TestGenerateCommitID_DifferentInputs(t *testing.T) {
	timestamp := time.Now()

	id1 := generateCommitID("Message 1", timestamp, "")
	id2 := generateCommitID("Message 2", timestamp, "")

	assert.NotEqual(t, id1, id2, "Different messages should produce different IDs")
}

func TestGenerateCommitID_DifferentParents(t *testing.T) {
	message := "Test"
	timestamp := time.Now()

	id1 := generateCommitID(message, timestamp, "parent1")
	id2 := generateCommitID(message, timestamp, "parent2")

	assert.NotEqual(t, id1, id2, "Different parents should produce different IDs")
}

func TestGenerateCommitID_EmptyParent(t *testing.T) {
	message := "Initial commit"
	timestamp := time.Now()

	// Should work with empty parent (initial commit)
	id := generateCommitID(message, timestamp, "")
	assert.NotEmpty(t, id)
	assert.Len(t, id, 64)
}
