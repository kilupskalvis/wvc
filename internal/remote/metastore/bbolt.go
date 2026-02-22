package metastore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/remote"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketCommits    = []byte("commits")
	bucketOperations = []byte("operations")
	bucketBranches   = []byte("branches")
	bucketSchemaVers = []byte("schema_versions")
)

// BboltStore implements MetaStore using bbolt.
type BboltStore struct {
	db *bolt.DB
}

// NewBboltStore opens or creates a bbolt database at the given path.
func NewBboltStore(dbPath string) (*BboltStore, error) {
	dir := filepath.Dir(dbPath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create meta directory: %w", err)
		}
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open meta database: %w", err)
	}

	// Create buckets
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketCommits, bucketOperations, bucketBranches, bucketSchemaVers} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("create bucket %s: %w", name, err)
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}

	return &BboltStore{db: db}, nil
}

// Close releases the bbolt database.
func (s *BboltStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// HasCommit checks if a commit exists.
func (s *BboltStore) HasCommit(_ context.Context, id string) (bool, error) {
	var exists bool
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommits)
		exists = b.Get([]byte(id)) != nil
		return nil
	})
	return exists, err
}

// GetCommit retrieves a commit by ID. Returns ErrNotFound if missing.
func (s *BboltStore) GetCommit(_ context.Context, id string) (*models.Commit, error) {
	var commit *models.Commit
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketCommits).Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		commit = &models.Commit{}
		return json.Unmarshal(data, commit)
	})
	if err != nil {
		return nil, err
	}
	return commit, nil
}

// InsertCommitBundle atomically stores a commit with its operations and schema.
func (s *BboltStore) InsertCommitBundle(_ context.Context, b *remote.CommitBundle) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		commitBucket := tx.Bucket(bucketCommits)

		// Skip if commit already exists (idempotent)
		if commitBucket.Get([]byte(b.Commit.ID)) != nil {
			return nil
		}

		// Store commit
		commitData, err := json.Marshal(b.Commit)
		if err != nil {
			return fmt.Errorf("marshal commit: %w", err)
		}
		if err := commitBucket.Put([]byte(b.Commit.ID), commitData); err != nil {
			return fmt.Errorf("store commit: %w", err)
		}

		// Store operations
		opBucket := tx.Bucket(bucketOperations)
		for i, op := range b.Operations {
			op.CommitID = b.Commit.ID
			op.Seq = i
			opData, err := json.Marshal(op)
			if err != nil {
				return fmt.Errorf("marshal operation: %w", err)
			}
			key := fmt.Sprintf("%s:%08d", b.Commit.ID, i)
			if err := opBucket.Put([]byte(key), opData); err != nil {
				return fmt.Errorf("store operation: %w", err)
			}
		}

		// Store schema if present
		if b.Schema != nil {
			schemaBucket := tx.Bucket(bucketSchemaVers)
			schemaData, err := json.Marshal(b.Schema)
			if err != nil {
				return fmt.Errorf("marshal schema: %w", err)
			}
			if err := schemaBucket.Put([]byte(b.Commit.ID), schemaData); err != nil {
				return fmt.Errorf("store schema: %w", err)
			}
		}

		return nil
	})
}

// GetCommitBundle retrieves a commit with its operations and schema.
func (s *BboltStore) GetCommitBundle(_ context.Context, id string) (*remote.CommitBundle, error) {
	bundle := &remote.CommitBundle{}

	err := s.db.View(func(tx *bolt.Tx) error {
		// Get commit
		commitData := tx.Bucket(bucketCommits).Get([]byte(id))
		if commitData == nil {
			return ErrNotFound
		}
		bundle.Commit = &models.Commit{}
		if err := json.Unmarshal(commitData, bundle.Commit); err != nil {
			return fmt.Errorf("unmarshal commit: %w", err)
		}

		// Get operations by prefix scan
		opBucket := tx.Bucket(bucketOperations)
		prefix := id + ":"
		c := opBucket.Cursor()
		for k, v := c.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
			var op models.Operation
			if err := json.Unmarshal(v, &op); err != nil {
				return fmt.Errorf("unmarshal operation: %w", err)
			}
			bundle.Operations = append(bundle.Operations, &op)
		}

		// Get schema if present
		schemaData := tx.Bucket(bucketSchemaVers).Get([]byte(id))
		if schemaData != nil {
			bundle.Schema = &remote.SchemaSnapshot{}
			if err := json.Unmarshal(schemaData, bundle.Schema); err != nil {
				return fmt.Errorf("unmarshal schema: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return bundle, nil
}

// GetAncestors returns all ancestor commit IDs reachable from the given commit.
func (s *BboltStore) GetAncestors(_ context.Context, id string) (map[string]bool, error) {
	ancestors := make(map[string]bool)

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommits)
		queue := []string{id}

		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]

			if ancestors[current] {
				continue
			}
			ancestors[current] = true

			data := b.Get([]byte(current))
			if data == nil {
				continue
			}

			var commit models.Commit
			if err := json.Unmarshal(data, &commit); err != nil {
				return fmt.Errorf("unmarshal commit %s: %w", current, err)
			}

			if commit.ParentID != "" {
				queue = append(queue, commit.ParentID)
			}
			if commit.MergeParentID != "" {
				queue = append(queue, commit.MergeParentID)
			}
		}

		return nil
	})

	return ancestors, err
}

// GetCommitCount returns the total number of commits.
func (s *BboltStore) GetCommitCount(_ context.Context) (int, error) {
	var count int
	err := s.db.View(func(tx *bolt.Tx) error {
		count = tx.Bucket(bucketCommits).Stats().KeyN
		return nil
	})
	return count, err
}

// ListBranches returns all branches sorted by name.
func (s *BboltStore) ListBranches(_ context.Context) ([]*models.Branch, error) {
	var branches []*models.Branch

	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketBranches).ForEach(func(k, v []byte) error {
			var branch models.Branch
			if err := json.Unmarshal(v, &branch); err != nil {
				return fmt.Errorf("unmarshal branch: %w", err)
			}
			branches = append(branches, &branch)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	sort.Slice(branches, func(i, j int) bool {
		return branches[i].Name < branches[j].Name
	})

	return branches, nil
}

// GetBranch retrieves a branch by name. Returns ErrNotFound if missing.
func (s *BboltStore) GetBranch(_ context.Context, name string) (*models.Branch, error) {
	var branch *models.Branch

	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketBranches).Get([]byte(name))
		if data == nil {
			return ErrNotFound
		}
		branch = &models.Branch{}
		return json.Unmarshal(data, branch)
	})

	if err != nil {
		return nil, err
	}
	return branch, nil
}

// CreateBranch creates a new branch pointing to the given commit.
func (s *BboltStore) CreateBranch(_ context.Context, name, commitID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBranches)

		if b.Get([]byte(name)) != nil {
			return fmt.Errorf("branch '%s' already exists", name)
		}

		branch := &models.Branch{
			Name:      name,
			CommitID:  commitID,
			CreatedAt: time.Now(),
		}

		data, err := json.Marshal(branch)
		if err != nil {
			return fmt.Errorf("marshal branch: %w", err)
		}

		return b.Put([]byte(name), data)
	})
}

// UpdateBranchCAS performs a compare-and-swap update on a branch pointer.
// If the branch doesn't exist and expectedCommitID is empty, it creates the branch.
// Returns ErrConflict if the current tip doesn't match expectedCommitID.
func (s *BboltStore) UpdateBranchCAS(_ context.Context, name, newCommitID, expectedCommitID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBranches)

		data := b.Get([]byte(name))

		if data == nil {
			// Branch doesn't exist
			if expectedCommitID != "" {
				return ErrConflict
			}
			// Create new branch
			branch := &models.Branch{
				Name:      name,
				CommitID:  newCommitID,
				CreatedAt: time.Now(),
			}
			newData, err := json.Marshal(branch)
			if err != nil {
				return fmt.Errorf("marshal branch: %w", err)
			}
			return b.Put([]byte(name), newData)
		}

		var branch models.Branch
		if err := json.Unmarshal(data, &branch); err != nil {
			return fmt.Errorf("unmarshal branch: %w", err)
		}

		if expectedCommitID != "" && branch.CommitID != expectedCommitID {
			return ErrConflict
		}

		branch.CommitID = newCommitID

		newData, err := json.Marshal(&branch)
		if err != nil {
			return fmt.Errorf("marshal branch: %w", err)
		}

		return b.Put([]byte(name), newData)
	})
}

// DeleteBranch removes a branch. Returns ErrNotFound if it doesn't exist.
func (s *BboltStore) DeleteBranch(_ context.Context, name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBranches)

		if b.Get([]byte(name)) == nil {
			return ErrNotFound
		}

		return b.Delete([]byte(name))
	})
}

// GetAllVectorHashes scans all operations and returns every unique VectorHash.
func (s *BboltStore) GetAllVectorHashes(_ context.Context) (map[string]bool, error) {
	hashes := make(map[string]bool)

	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOperations).ForEach(func(_, v []byte) error {
			var op models.Operation
			if err := json.Unmarshal(v, &op); err != nil {
				return nil // skip malformed entries
			}
			if op.VectorHash != "" {
				hashes[op.VectorHash] = true
			}
			return nil
		})
	})

	return hashes, err
}

// GetOperationsByCommit returns all operations for a commit, ordered by sequence.
func (s *BboltStore) GetOperationsByCommit(_ context.Context, commitID string) ([]*models.Operation, error) {
	var ops []*models.Operation

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOperations)
		prefix := commitID + ":"
		c := b.Cursor()
		for k, v := c.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
			var op models.Operation
			if err := json.Unmarshal(v, &op); err != nil {
				return fmt.Errorf("unmarshal operation: %w", err)
			}
			ops = append(ops, &op)
		}
		return nil
	})

	return ops, err
}
