package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
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

	commit, err := finalizeCommit(ctx, st, client, message, diff.TotalChanges())
	if err != nil {
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

	for _, sc := range stagedChanges {
		op := &models.Operation{
			Timestamp:    time.Now(),
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

	commit, err := finalizeCommit(ctx, st, client, message, len(stagedChanges))
	if err != nil {
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

// finalizeCommit performs the shared commit finalization: generate ID, capture
// schema, mark operations, create commit, set HEAD, and update branch pointer.
func finalizeCommit(ctx context.Context, st *store.Store, client weaviate.ClientInterface, message string, opCount int) (*models.Commit, error) {
	parentID, err := st.GetHEAD()
	if err != nil {
		return nil, err
	}

	uncommittedOps, err := st.GetUncommittedOperations()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	commitID := generateCommitID(message, now, parentID, uncommittedOps)

	if err := captureSchemaSnapshot(ctx, st, client, commitID); err != nil {
		return nil, fmt.Errorf("capture schema: %w", err)
	}

	commit := &models.Commit{
		ID:             commitID,
		ParentID:       parentID,
		Message:        message,
		Timestamp:      now,
		OperationCount: opCount,
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

	if branch, _ := st.GetCurrentBranch(); branch != "" {
		existing, _ := st.GetBranch(branch)
		if existing != nil {
			if err := st.UpdateBranch(branch, commitID); err != nil {
				return nil, fmt.Errorf("update branch %s: %w", branch, err)
			}
		} else {
			// Unborn branch — create it on first commit
			if err := st.CreateBranch(branch, commitID); err != nil {
				return nil, fmt.Errorf("create branch %s: %w", branch, err)
			}
		}
	}

	return commit, nil
}

// generateCommitID generates a content-addressable commit ID.
// The ID includes a Merkle hash of operations so that two commits with
// identical metadata but different operations produce different IDs.
func generateCommitID(message string, timestamp time.Time, parentID string, operations []*models.Operation) string {
	opsHash := computeOperationsHash(operations)
	data := fmt.Sprintf("%s|%s|%s|%s", message, timestamp.Format(time.RFC3339Nano), parentID, opsHash)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// computeOperationsHash computes a Merkle hash over a set of operations.
// Each operation is hashed individually, the hashes are sorted, and then
// hashed together to produce a deterministic digest.
func computeOperationsHash(operations []*models.Operation) string {
	if len(operations) == 0 {
		return ""
	}

	hashes := make([]string, len(operations))
	for i, op := range operations {
		opData := fmt.Sprintf("%s|%s|%s|%s|%s",
			op.Type, op.ClassName, op.ObjectID,
			string(op.ObjectData), op.VectorHash)
		h := sha256.Sum256([]byte(opData))
		hashes[i] = hex.EncodeToString(h[:])
	}

	// Sort for deterministic ordering
	sort.Strings(hashes)

	combined := strings.Join(hashes, "")
	final := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(final[:])
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
