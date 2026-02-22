package models

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// GenerateCommitID generates a content-addressable commit ID.
// The ID includes a Merkle hash of operations so that two commits with
// identical metadata but different operations produce different IDs.
func GenerateCommitID(message string, timestamp time.Time, parentID string, operations []*Operation) string {
	opsHash := ComputeOperationsHash(operations)
	data := fmt.Sprintf("%s|%s|%s|%s", message, timestamp.Format(time.RFC3339Nano), parentID, opsHash)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// GenerateMergeCommitID generates a content-addressable commit ID for merge commits.
// Includes both parent IDs and the operations Merkle hash.
func GenerateMergeCommitID(message string, timestamp time.Time, parent1, parent2 string, operations []*Operation) string {
	opsHash := ComputeOperationsHash(operations)
	data := fmt.Sprintf("%s|%s|%s|%s|%s", message, timestamp.Format(time.RFC3339Nano), parent1, parent2, opsHash)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// ComputeOperationsHash computes a Merkle hash over a set of operations.
// Each operation is hashed individually, the hashes are sorted, and then
// hashed together to produce a deterministic digest.
func ComputeOperationsHash(operations []*Operation) string {
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
