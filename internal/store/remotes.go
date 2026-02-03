package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
	bolt "go.etcd.io/bbolt"
)

// remoteTokenKeyPrefix is the prefix for storing remote tokens in the kv bucket.
const remoteTokenKeyPrefix = "remote."

// AddRemote stores a new remote. Returns an error if a remote with the same name exists.
func (s *Store) AddRemote(name, url string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRemotes)
		if bucket == nil {
			return fmt.Errorf("remotes bucket not found")
		}

		if bucket.Get([]byte(name)) != nil {
			return fmt.Errorf("remote '%s' already exists", name)
		}

		remote := &models.Remote{
			Name:      name,
			URL:       url,
			CreatedAt: time.Now(),
		}

		data, err := json.Marshal(remote)
		if err != nil {
			return fmt.Errorf("marshal remote: %w", err)
		}

		return bucket.Put([]byte(name), data)
	})
}

// GetRemote retrieves a remote by name. Returns (nil, nil) if not found.
func (s *Store) GetRemote(name string) (*models.Remote, error) {
	var remote *models.Remote

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRemotes)
		if bucket == nil {
			return nil
		}

		data := bucket.Get([]byte(name))
		if data == nil {
			return nil
		}

		remote = &models.Remote{}
		return json.Unmarshal(data, remote)
	})

	return remote, err
}

// ListRemotes returns all remotes sorted by name.
func (s *Store) ListRemotes() ([]*models.Remote, error) {
	var remotes []*models.Remote

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRemotes)
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(k, v []byte) error {
			var r models.Remote
			if err := json.Unmarshal(v, &r); err != nil {
				return fmt.Errorf("unmarshal remote: %w", err)
			}
			remotes = append(remotes, &r)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	sort.Slice(remotes, func(i, j int) bool {
		return remotes[i].Name < remotes[j].Name
	})

	return remotes, nil
}

// RemoveRemote deletes a remote and all its remote-tracking branches and stored token.
func (s *Store) RemoveRemote(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		// Delete the remote itself
		remoteBucket := tx.Bucket(bucketRemotes)
		if remoteBucket == nil {
			return fmt.Errorf("remotes bucket not found")
		}

		if remoteBucket.Get([]byte(name)) == nil {
			return fmt.Errorf("remote '%s' does not exist", name)
		}

		if err := remoteBucket.Delete([]byte(name)); err != nil {
			return fmt.Errorf("delete remote: %w", err)
		}

		// Delete all remote-tracking branches for this remote
		rbBucket := tx.Bucket(bucketRemoteBranch)
		if rbBucket != nil {
			prefix := name + ":"
			var toDelete [][]byte
			rbBucket.ForEach(func(k, v []byte) error {
				if bytes.HasPrefix(k, []byte(prefix)) {
					toDelete = append(toDelete, append([]byte(nil), k...))
				}
				return nil
			})
			for _, k := range toDelete {
				if err := rbBucket.Delete(k); err != nil {
					return fmt.Errorf("delete remote branch: %w", err)
				}
			}
		}

		// Delete stored token
		kvBucket := tx.Bucket(bucketKV)
		if kvBucket != nil {
			tokenKey := remoteTokenKey(name)
			kvBucket.Delete([]byte(tokenKey))
		}

		return nil
	})
}

// UpdateRemoteURL updates the URL of an existing remote.
func (s *Store) UpdateRemoteURL(name, url string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRemotes)
		if bucket == nil {
			return fmt.Errorf("remotes bucket not found")
		}

		data := bucket.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("remote '%s' does not exist", name)
		}

		var remote models.Remote
		if err := json.Unmarshal(data, &remote); err != nil {
			return fmt.Errorf("unmarshal remote: %w", err)
		}

		remote.URL = url

		updatedData, err := json.Marshal(&remote)
		if err != nil {
			return fmt.Errorf("marshal remote: %w", err)
		}

		return bucket.Put([]byte(name), updatedData)
	})
}

// SetRemoteToken stores a token for a remote in the kv bucket.
func (s *Store) SetRemoteToken(remoteName, token string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		kvBucket := tx.Bucket(bucketKV)
		if kvBucket == nil {
			return fmt.Errorf("kv bucket not found")
		}

		return kvBucket.Put([]byte(remoteTokenKey(remoteName)), []byte(token))
	})
}

// GetRemoteToken retrieves the stored token for a remote.
// Returns ("", nil) if no token is stored.
func (s *Store) GetRemoteToken(remoteName string) (string, error) {
	var token string

	err := s.db.View(func(tx *bolt.Tx) error {
		kvBucket := tx.Bucket(bucketKV)
		if kvBucket == nil {
			return nil
		}

		data := kvBucket.Get([]byte(remoteTokenKey(remoteName)))
		if data != nil {
			token = string(data)
		}
		return nil
	})

	return token, err
}

// DeleteRemoteToken removes the stored token for a remote.
func (s *Store) DeleteRemoteToken(remoteName string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		kvBucket := tx.Bucket(bucketKV)
		if kvBucket == nil {
			return nil
		}
		return kvBucket.Delete([]byte(remoteTokenKey(remoteName)))
	})
}

// SetRemoteBranch updates or creates a remote-tracking branch reference.
func (s *Store) SetRemoteBranch(remoteName, branchName, commitID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRemoteBranch)
		if bucket == nil {
			return fmt.Errorf("remote_branches bucket not found")
		}

		rb := &models.RemoteBranch{
			RemoteName: remoteName,
			BranchName: branchName,
			CommitID:   commitID,
			UpdatedAt:  time.Now(),
		}

		data, err := json.Marshal(rb)
		if err != nil {
			return fmt.Errorf("marshal remote branch: %w", err)
		}

		key := models.RemoteBranchKey(remoteName, branchName)
		return bucket.Put([]byte(key), data)
	})
}

// GetRemoteBranch retrieves a remote-tracking branch. Returns (nil, nil) if not found.
func (s *Store) GetRemoteBranch(remoteName, branchName string) (*models.RemoteBranch, error) {
	var rb *models.RemoteBranch

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRemoteBranch)
		if bucket == nil {
			return nil
		}

		key := models.RemoteBranchKey(remoteName, branchName)
		data := bucket.Get([]byte(key))
		if data == nil {
			return nil
		}

		rb = &models.RemoteBranch{}
		return json.Unmarshal(data, rb)
	})

	return rb, err
}

// ListRemoteBranches returns all remote-tracking branches for a given remote, sorted by name.
func (s *Store) ListRemoteBranches(remoteName string) ([]*models.RemoteBranch, error) {
	var branches []*models.RemoteBranch

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRemoteBranch)
		if bucket == nil {
			return nil
		}

		prefix := []byte(remoteName + ":")
		c := bucket.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var rb models.RemoteBranch
			if err := json.Unmarshal(v, &rb); err != nil {
				return fmt.Errorf("unmarshal remote branch: %w", err)
			}
			branches = append(branches, &rb)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	sort.Slice(branches, func(i, j int) bool {
		return branches[i].BranchName < branches[j].BranchName
	})

	return branches, nil
}

// DeleteRemoteBranch removes a remote-tracking branch.
func (s *Store) DeleteRemoteBranch(remoteName, branchName string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRemoteBranch)
		if bucket == nil {
			return nil
		}

		key := models.RemoteBranchKey(remoteName, branchName)
		return bucket.Delete([]byte(key))
	})
}

// remoteTokenKey returns the kv key for a remote's token.
func remoteTokenKey(remoteName string) string {
	return remoteTokenKeyPrefix + remoteName + ".token"
}
