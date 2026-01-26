package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
)

// CreateCommit creates a new commit from current changes
func CreateCommit(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, message string) (*models.Commit, error) {
	diff, err := ComputeDiff(ctx, cfg, st, client)
	if err != nil {
		return nil, err
	}

	schemaDiff, err := ComputeSchemaDiff(ctx, st, client)
	if err != nil {
		schemaDiff = &SchemaDiffResult{}
	}

	if diff.TotalChanges() == 0 && !schemaDiff.HasChanges() {
		return nil, fmt.Errorf("no changes to commit")
	}

	if diff.TotalChanges() > 0 {
		if err := RecordDiffAsOperations(st, diff); err != nil {
			return nil, err
		}
	}

	parentID, err := st.GetHEAD()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	commitID := generateCommitID(message, now, parentID)

	if err := captureSchemaSnapshot(ctx, st, client, commitID); err != nil {
		return nil, fmt.Errorf("failed to capture schema: %w", err)
	}

	commit := &models.Commit{
		ID:             commitID,
		ParentID:       parentID,
		Message:        message,
		Timestamp:      now,
		OperationCount: diff.TotalChanges(),
	}

	if _, err := st.MarkOperationsCommitted(commitID); err != nil {
		return nil, err
	}

	if err := st.CreateCommit(commit); err != nil {
		return nil, err
	}

	if err := st.SetHEAD(commitID); err != nil {
		return nil, err
	}

	useCursor := cfg.SupportsCursorPagination()
	if err := UpdateKnownState(ctx, st, client, useCursor); err != nil {
		return nil, err
	}

	return commit, nil
}

// CreateCommitFromStaging creates a commit from staged changes only
func CreateCommitFromStaging(ctx context.Context, cfg *config.Config, st *store.Store, client weaviate.ClientInterface, message string) (*models.Commit, error) {
	stagedChanges, err := st.GetAllStagedChanges()
	if err != nil {
		return nil, err
	}

	schemaDiff, err := ComputeSchemaDiff(ctx, st, client)
	if err != nil {
		schemaDiff = &SchemaDiffResult{}
	}

	if len(stagedChanges) == 0 && !schemaDiff.HasChanges() {
		return nil, fmt.Errorf("nothing to commit (use \"wvc add\" to stage changes)")
	}

	now := time.Now()
	for _, sc := range stagedChanges {
		op := &models.Operation{
			Timestamp:    now,
			Type:         models.OperationType(sc.ChangeType),
			ClassName:    sc.ClassName,
			ObjectID:     sc.ObjectID,
			ObjectData:   sc.ObjectData,
			PreviousData: sc.PreviousData,
		}
		if err := st.RecordOperation(op); err != nil {
			return nil, err
		}
	}

	parentID, err := st.GetHEAD()
	if err != nil {
		return nil, err
	}

	commitID := generateCommitID(message, now, parentID)

	if err := captureSchemaSnapshot(ctx, st, client, commitID); err != nil {
		return nil, fmt.Errorf("failed to capture schema: %w", err)
	}

	commit := &models.Commit{
		ID:             commitID,
		ParentID:       parentID,
		Message:        message,
		Timestamp:      now,
		OperationCount: len(stagedChanges),
	}

	if _, err := st.MarkOperationsCommitted(commitID); err != nil {
		return nil, err
	}

	if err := st.CreateCommit(commit); err != nil {
		return nil, err
	}

	if err := st.SetHEAD(commitID); err != nil {
		return nil, err
	}

	if err := updateKnownStateForStagedChanges(ctx, st, client, stagedChanges); err != nil {
		return nil, err
	}

	if err := st.ClearStagedChanges(); err != nil {
		return nil, err
	}

	return commit, nil
}

// updateKnownStateForStagedChanges updates known_objects only for the committed changes
func updateKnownStateForStagedChanges(ctx context.Context, st *store.Store, client weaviate.ClientInterface, changes []*store.StagedChange) error {
	for _, sc := range changes {
		switch sc.ChangeType {
		case "insert", "update":
			obj, err := client.GetObject(ctx, sc.ClassName, sc.ObjectID)
			if err != nil {
				return fmt.Errorf("failed to fetch object %s/%s: %w", sc.ClassName, sc.ObjectID, err)
			}
			objectHash, vectorHash := weaviate.HashObjectFull(obj)
			data, _ := json.Marshal(obj)
			if err := st.SaveKnownObjectWithVector(obj.Class, obj.ID, objectHash, vectorHash, data); err != nil {
				return err
			}
		case "delete":
			if err := st.DeleteKnownObject(sc.ClassName, sc.ObjectID); err != nil {
				return err
			}
		}
	}
	return nil
}

// generateCommitID generates a unique commit ID
func generateCommitID(message string, timestamp time.Time, parentID string) string {
	data := fmt.Sprintf("%s|%s|%s", message, timestamp.Format(time.RFC3339Nano), parentID)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// captureSchemaSnapshot fetches current schema and saves it with the commit
func captureSchemaSnapshot(ctx context.Context, st *store.Store, client weaviate.ClientInterface, commitID string) error {
	schema, err := client.GetSchemaTyped(ctx)
	if err != nil {
		return err
	}

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	schemaHash := HashSchema(schema)
	schemaVersionID, err := st.SaveSchemaVersion(schemaJSON, schemaHash)
	if err != nil {
		return err
	}

	return st.MarkSchemaVersionCommitted(schemaVersionID, commitID)
}
