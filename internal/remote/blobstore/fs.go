package blobstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// validHash matches a lowercase hex-encoded SHA256 hash (64 characters).
var validHash = regexp.MustCompile(`^[0-9a-f]{64}$`)

// FSStore implements BlobStore using the local filesystem.
// Blobs are stored in a two-level directory structure using the first two
// characters of the hash as a prefix directory.
type FSStore struct {
	root string
}

// NewFSStore creates a filesystem-backed blob store rooted at the given directory.
func NewFSStore(root string) (*FSStore, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("create blob root: %w", err)
	}
	return &FSStore{root: root}, nil
}

// Has checks whether a blob exists.
func (s *FSStore) Has(_ context.Context, hash string) (bool, error) {
	if !validHash.MatchString(hash) {
		return false, nil
	}
	_, err := os.Stat(s.blobPath(hash))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat blob %s: %w", hash, err)
	}
	return true, nil
}

// Get opens a blob for reading. Returns the data reader and dimensions.
// Returns ErrBlobNotFound if the blob does not exist.
func (s *FSStore) Get(_ context.Context, hash string) (io.ReadCloser, int, error) {
	if !validHash.MatchString(hash) {
		return nil, 0, ErrBlobNotFound
	}
	metaPath := s.metaPath(hash)
	dims, err := s.readDims(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrBlobNotFound
		}
		return nil, 0, fmt.Errorf("read blob meta %s: %w", hash, err)
	}

	f, err := os.Open(s.blobPath(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrBlobNotFound
		}
		return nil, 0, fmt.Errorf("open blob %s: %w", hash, err)
	}

	return f, dims, nil
}

// Put stores a blob. The data is read from r and verified against the hash.
// Idempotent — if the blob exists, this is a no-op.
func (s *FSStore) Put(_ context.Context, hash string, r io.Reader, dims int) error {
	if !validHash.MatchString(hash) {
		return fmt.Errorf("invalid blob hash: %q", hash)
	}
	blobPath := s.blobPath(hash)

	// Check if already exists
	if _, err := os.Stat(blobPath); err == nil {
		return nil // idempotent
	}

	// Create directory
	dir := filepath.Dir(blobPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create blob dir: %w", err)
	}

	// Write to temp file, verify hash, rename
	tmpFile, err := os.CreateTemp(dir, ".blob-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Hash data as we write
	hasher := sha256.New()
	writer := io.MultiWriter(tmpFile, hasher)

	if _, err := io.Copy(writer, r); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write blob data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp file: %w", err)
	}

	// Verify hash
	computedHash := hex.EncodeToString(hasher.Sum(nil))
	if computedHash != hash {
		os.Remove(tmpPath)
		return fmt.Errorf("expected %s, got %s: %w", hash, computedHash, ErrHashMismatch)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, blobPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename blob: %w", err)
	}

	// Write meta file with dimensions
	metaPath := s.metaPath(hash)
	if err := os.WriteFile(metaPath, []byte(strconv.Itoa(dims)), 0644); err != nil {
		return fmt.Errorf("write blob meta: %w", err)
	}

	return nil
}

// Delete removes a blob and its metadata file.
func (s *FSStore) Delete(_ context.Context, hash string) error {
	if !validHash.MatchString(hash) {
		return nil
	}
	os.Remove(s.blobPath(hash))
	os.Remove(s.metaPath(hash))
	return nil
}

// TotalCount returns the number of stored blobs by scanning the directory tree.
func (s *FSStore) TotalCount(_ context.Context) (int, error) {
	var count int

	err := filepath.Walk(s.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && !strings.HasSuffix(path, ".meta") && !strings.HasPrefix(info.Name(), ".") {
			count++
		}
		return nil
	})

	return count, err
}

// ListHashes returns all blob hashes by scanning the directory tree.
func (s *FSStore) ListHashes(_ context.Context) ([]string, error) {
	var hashes []string

	err := filepath.Walk(s.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || strings.HasSuffix(path, ".meta") || strings.HasPrefix(info.Name(), ".") {
			return nil
		}
		// Reconstruct hash from path: root/ab/cd... -> abcd...
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return nil
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) == 2 {
			hashes = append(hashes, parts[0]+parts[1])
		}
		return nil
	})

	return hashes, err
}

// blobPath returns the filesystem path for a blob.
func (s *FSStore) blobPath(hash string) string {
	if len(hash) < 2 {
		return filepath.Join(s.root, hash)
	}
	return filepath.Join(s.root, hash[:2], hash[2:])
}

// metaPath returns the filesystem path for a blob's metadata.
func (s *FSStore) metaPath(hash string) string {
	return s.blobPath(hash) + ".meta"
}

// readDims reads dimensions from a metadata file.
func (s *FSStore) readDims(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}
