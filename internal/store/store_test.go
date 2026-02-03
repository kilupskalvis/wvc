package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStore creates a new bbolt store in a temp directory for testing.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := New(dbPath)
	require.NoError(t, err)
	require.NoError(t, st.Initialize())
	t.Cleanup(func() { st.Close() })
	return st
}

// ==================== Store Tests ====================

func TestStore_Initialize(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := New(dbPath)
	require.NoError(t, err)
	defer st.Close()

	err = st.Initialize()
	assert.NoError(t, err)

	// Verify buckets exist by checking we can read from them
	_, err = st.GetHEAD()
	assert.NoError(t, err)

	_, err = st.GetUncommittedOperations()
	assert.NoError(t, err)
}

func TestStore_GetSetValue(t *testing.T) {
	st := newTestStore(t)

	// Set a value
	err := st.SetValue("test_key", "test_value")
	require.NoError(t, err)

	// Get the value back
	val, err := st.GetValue("test_key")
	require.NoError(t, err)
	assert.Equal(t, "test_value", val)

	// Get non-existent key returns empty
	val, err = st.GetValue("nonexistent")
	require.NoError(t, err)
	assert.Equal(t, "", val)

	// Update existing value
	err = st.SetValue("test_key", "new_value")
	require.NoError(t, err)

	val, err = st.GetValue("test_key")
	require.NoError(t, err)
	assert.Equal(t, "new_value", val)
}

// ==================== Commits Tests ====================

func TestStore_CreateAndGetCommit(t *testing.T) {
	st := newTestStore(t)

	commit := &models.Commit{
		ID:             "abc123",
		ParentID:       "",
		Message:        "Initial commit",
		Timestamp:      time.Now(),
		OperationCount: 5,
	}

	err := st.CreateCommit(commit)
	require.NoError(t, err)

	// Get by full ID
	retrieved, err := st.GetCommit("abc123")
	require.NoError(t, err)
	assert.Equal(t, commit.ID, retrieved.ID)
	assert.Equal(t, commit.Message, retrieved.Message)
	assert.Equal(t, commit.OperationCount, retrieved.OperationCount)
}

func TestStore_GetCommitByShortID(t *testing.T) {
	st := newTestStore(t)

	commit := &models.Commit{
		ID:             "abc123def456",
		Message:        "Test commit",
		Timestamp:      time.Now(),
		OperationCount: 1,
	}

	err := st.CreateCommit(commit)
	require.NoError(t, err)

	// Get by short ID
	retrieved, err := st.GetCommitByShortID("abc123")
	require.NoError(t, err)
	assert.Equal(t, commit.ID, retrieved.ID)
}

func TestStore_GetCommitLog(t *testing.T) {
	st := newTestStore(t)

	// Create multiple commits
	for i := 0; i < 5; i++ {
		commit := &models.Commit{
			ID:        string(rune('a' + i)),
			Message:   "Commit " + string(rune('1'+i)),
			Timestamp: time.Now().Add(time.Duration(i) * time.Hour),
		}
		require.NoError(t, st.CreateCommit(commit))
	}

	// Get all commits
	log, err := st.GetCommitLog(0)
	require.NoError(t, err)
	assert.Len(t, log, 5)

	// Get limited commits
	log, err = st.GetCommitLog(3)
	require.NoError(t, err)
	assert.Len(t, log, 3)
}

func TestStore_HEAD(t *testing.T) {
	st := newTestStore(t)

	// Initially empty
	head, err := st.GetHEAD()
	require.NoError(t, err)
	assert.Equal(t, "", head)

	// Set HEAD
	err = st.SetHEAD("commit123")
	require.NoError(t, err)

	head, err = st.GetHEAD()
	require.NoError(t, err)
	assert.Equal(t, "commit123", head)
}

// ==================== Operations Tests ====================

func TestStore_RecordOperation(t *testing.T) {
	st := newTestStore(t)

	op := &models.Operation{
		Timestamp:  time.Now(),
		Type:       models.OperationInsert,
		ClassName:  "Article",
		ObjectID:   "obj-001",
		ObjectData: []byte(`{"title": "Test"}`),
	}

	err := st.RecordOperation(op)
	require.NoError(t, err)

	// Verify operation was recorded
	ops, err := st.GetUncommittedOperations()
	require.NoError(t, err)
	assert.Len(t, ops, 1)
	assert.Equal(t, "Article", ops[0].ClassName)
	assert.Equal(t, "obj-001", ops[0].ObjectID)
}

func TestStore_MarkOperationsCommitted(t *testing.T) {
	st := newTestStore(t)

	// Create some operations
	for i := 0; i < 3; i++ {
		op := &models.Operation{
			Timestamp: time.Now(),
			Type:      models.OperationInsert,
			ClassName: "Article",
			ObjectID:  string(rune('a' + i)),
		}
		require.NoError(t, st.RecordOperation(op))
	}

	// Mark as committed
	count, err := st.MarkOperationsCommitted("commit-001")
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// Verify no uncommitted operations remain
	ops, err := st.GetUncommittedOperations()
	require.NoError(t, err)
	assert.Len(t, ops, 0)

	// Verify we can get by commit ID
	ops, err = st.GetOperationsByCommit("commit-001")
	require.NoError(t, err)
	assert.Len(t, ops, 3)
}

// ==================== Known Objects Tests ====================

func TestStore_SaveAndGetKnownObject(t *testing.T) {
	st := newTestStore(t)

	data := []byte(`{"class": "Article", "id": "obj-001"}`)
	err := st.SaveKnownObject("Article", "obj-001", "hash123", data)
	require.NoError(t, err)

	hash, retrieved, err := st.GetKnownObject("Article", "obj-001")
	require.NoError(t, err)
	assert.Equal(t, "hash123", hash)
	assert.Equal(t, data, retrieved)
}

func TestStore_DeleteKnownObject(t *testing.T) {
	st := newTestStore(t)

	data := []byte(`{"class": "Article", "id": "obj-001"}`)
	require.NoError(t, st.SaveKnownObject("Article", "obj-001", "hash123", data))

	err := st.DeleteKnownObject("Article", "obj-001")
	require.NoError(t, err)

	// Should not be found now
	_, _, err = st.GetKnownObject("Article", "obj-001")
	assert.Error(t, err)
}

func TestStore_ClearKnownObjects(t *testing.T) {
	st := newTestStore(t)

	// Add multiple objects
	for i := 0; i < 3; i++ {
		data := []byte(`{}`)
		require.NoError(t, st.SaveKnownObject("Article", string(rune('a'+i)), "hash", data))
	}

	err := st.ClearKnownObjects()
	require.NoError(t, err)

	// Verify all are gone
	objects, err := st.GetAllKnownObjects()
	require.NoError(t, err)
	assert.Len(t, objects, 0)
}

// ==================== Staging Tests ====================

func TestStore_StagedChanges(t *testing.T) {
	st := newTestStore(t)

	sc := &StagedChange{
		ClassName:  "Article",
		ObjectID:   "obj-001",
		ChangeType: "insert",
		ObjectData: []byte(`{"title": "Test"}`),
		StagedAt:   time.Now(),
	}

	err := st.AddStagedChange(sc)
	require.NoError(t, err)

	// Get all staged changes
	changes, err := st.GetAllStagedChanges()
	require.NoError(t, err)
	assert.Len(t, changes, 1)
	assert.Equal(t, "Article", changes[0].ClassName)

	// Remove the staged change
	err = st.RemoveStagedChange("Article", "obj-001")
	require.NoError(t, err)

	changes, err = st.GetAllStagedChanges()
	require.NoError(t, err)
	assert.Len(t, changes, 0)
}

func TestStore_ClearStagedChanges(t *testing.T) {
	st := newTestStore(t)

	// Add multiple staged changes
	for i := 0; i < 3; i++ {
		sc := &StagedChange{
			ClassName:  "Article",
			ObjectID:   string(rune('a' + i)),
			ChangeType: "insert",
			StagedAt:   time.Now(),
		}
		require.NoError(t, st.AddStagedChange(sc))
	}

	err := st.ClearStagedChanges()
	require.NoError(t, err)

	changes, err := st.GetAllStagedChanges()
	require.NoError(t, err)
	assert.Len(t, changes, 0)
}

// ==================== Vector Blob Tests ====================

func TestStore_VectorBlob(t *testing.T) {
	st := newTestStore(t)

	// Create test vector bytes (4 floats = 16 bytes)
	vectorData := []byte{0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64, 0, 0, 128, 64} // [1.0, 2.0, 3.0, 4.0]
	dims := 4

	// Save vector
	hash, err := st.SaveVectorBlob(vectorData, dims)
	require.NoError(t, err)
	assert.NotEmpty(t, hash)

	// Retrieve vector
	retrieved, retrievedDims, err := st.GetVectorBlob(hash)
	require.NoError(t, err)
	assert.Equal(t, vectorData, retrieved)
	assert.Equal(t, dims, retrievedDims)
}

func TestStore_VectorBlobRefCount(t *testing.T) {
	st := newTestStore(t)

	vectorData := []byte{0, 0, 128, 63} // Single float32
	hash, err := st.SaveVectorBlob(vectorData, 1)
	require.NoError(t, err)

	// Save same vector again - should increment ref count
	hash2, err := st.SaveVectorBlob(vectorData, 1)
	require.NoError(t, err)
	assert.Equal(t, hash, hash2)

	// Decrement ref count twice - should delete on second
	deleted, err := st.DecrementVectorRefCount(hash)
	require.NoError(t, err)
	assert.False(t, deleted)

	deleted, err = st.DecrementVectorRefCount(hash)
	require.NoError(t, err)
	assert.True(t, deleted)

	// Should not exist now
	_, _, err = st.GetVectorBlob(hash)
	assert.Equal(t, ErrVectorNotFound, err)
}

// ==================== Schema Version Tests ====================

func TestStore_SchemaVersion(t *testing.T) {
	st := newTestStore(t)

	schemaJSON := []byte(`{"classes": []}`)
	schemaHash := "hash123"

	id, err := st.SaveSchemaVersion(schemaJSON, schemaHash)
	require.NoError(t, err)
	assert.Greater(t, id, int64(0))

	// Mark as committed
	err = st.MarkSchemaVersionCommitted(id, "commit-001")
	require.NoError(t, err)

	// Get by commit
	sv, err := st.GetSchemaVersionByCommit("commit-001")
	require.NoError(t, err)
	assert.Equal(t, schemaJSON, sv.SchemaJSON)
	assert.Equal(t, schemaHash, sv.SchemaHash)
}

func TestStore_GetLatestSchemaVersion(t *testing.T) {
	st := newTestStore(t)

	// Create and commit two schema versions
	id1, _ := st.SaveSchemaVersion([]byte(`{"v": 1}`), "hash1")
	st.MarkSchemaVersionCommitted(id1, "commit-001")

	id2, _ := st.SaveSchemaVersion([]byte(`{"v": 2}`), "hash2")
	st.MarkSchemaVersionCommitted(id2, "commit-002")

	latest, err := st.GetLatestSchemaVersion()
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"v": 2}`), latest.SchemaJSON)
}

// ==================== Migration Tests ====================

func TestStore_Migrations(t *testing.T) {
	st := newTestStore(t)

	// Migrations should run without error (no-op for bbolt)
	err := st.RunMigrations()
	assert.NoError(t, err)

	// Running again should be idempotent
	err = st.RunMigrations()
	assert.NoError(t, err)
}

// ==================== Helper Function Tests ====================

func TestVectorToBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		wantDims int
		wantErr  bool
	}{
		{
			name:     "float32 slice",
			input:    []float32{1.0, 2.0, 3.0},
			wantDims: 3,
		},
		{
			name:     "float64 slice",
			input:    []float64{1.0, 2.0},
			wantDims: 2,
		},
		{
			name:     "interface slice with float64",
			input:    []interface{}{1.0, 2.0, 3.0, 4.0},
			wantDims: 4,
		},
		{
			name:    "nil input",
			input:   nil,
			wantErr: false,
		},
		{
			name:    "unsupported type",
			input:   "not a vector",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bytes, dims, err := VectorToBytes(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.input != nil && tt.wantDims > 0 {
				assert.Equal(t, tt.wantDims, dims)
				assert.Len(t, bytes, tt.wantDims*4) // 4 bytes per float32
			}
		})
	}
}

func TestBytesToVector(t *testing.T) {
	// Create known vector bytes for [1.0, 2.0, 3.0]
	original := []float32{1.0, 2.0, 3.0}
	bytes, dims, err := VectorToBytes(original)
	require.NoError(t, err)

	// Convert back
	result, err := BytesToVector(bytes, dims)
	require.NoError(t, err)
	assert.Equal(t, original, result)
}

func TestHashVector(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	hash := HashVector(data)
	assert.NotEmpty(t, hash)
	assert.Len(t, hash, 64) // SHA256 hex is 64 chars

	// Same input should produce same hash
	hash2 := HashVector(data)
	assert.Equal(t, hash, hash2)

	// Different input should produce different hash
	hash3 := HashVector([]byte{5, 6, 7, 8})
	assert.NotEqual(t, hash, hash3)
}
