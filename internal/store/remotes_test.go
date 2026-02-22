package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_AddRemote(t *testing.T) {
	st := newTestStore(t)

	err := st.AddRemote("origin", "https://example.com/repo")
	require.NoError(t, err)

	remote, err := st.GetRemote("origin")
	require.NoError(t, err)
	require.NotNil(t, remote)
	assert.Equal(t, "origin", remote.Name)
	assert.Equal(t, "https://example.com/repo", remote.URL)
	assert.False(t, remote.CreatedAt.IsZero())
}

func TestStore_AddRemote_Duplicate(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.AddRemote("origin", "https://example.com/repo"))

	err := st.AddRemote("origin", "https://other.com/repo")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestStore_GetRemote_NotFound(t *testing.T) {
	st := newTestStore(t)

	remote, err := st.GetRemote("nonexistent")
	require.NoError(t, err)
	assert.Nil(t, remote)
}

func TestStore_ListRemotes(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.AddRemote("origin", "https://example.com/repo"))
	require.NoError(t, st.AddRemote("upstream", "https://upstream.com/repo"))
	require.NoError(t, st.AddRemote("backup", "https://backup.com/repo"))

	remotes, err := st.ListRemotes()
	require.NoError(t, err)
	require.Len(t, remotes, 3)

	// Should be sorted by name
	assert.Equal(t, "backup", remotes[0].Name)
	assert.Equal(t, "origin", remotes[1].Name)
	assert.Equal(t, "upstream", remotes[2].Name)
}

func TestStore_ListRemotes_Empty(t *testing.T) {
	st := newTestStore(t)

	remotes, err := st.ListRemotes()
	require.NoError(t, err)
	assert.Len(t, remotes, 0)
}

func TestStore_RemoveRemote(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.AddRemote("origin", "https://example.com/repo"))

	// Add a remote branch and token
	require.NoError(t, st.SetRemoteBranch("origin", "main", "abc123"))
	require.NoError(t, st.SetRemoteToken("origin", "secret-token"))

	// Remove the remote
	err := st.RemoveRemote("origin")
	require.NoError(t, err)

	// Verify remote is gone
	remote, err := st.GetRemote("origin")
	require.NoError(t, err)
	assert.Nil(t, remote)

	// Verify remote branches are gone
	branches, err := st.ListRemoteBranches("origin")
	require.NoError(t, err)
	assert.Len(t, branches, 0)

	// Verify token is gone
	token, err := st.GetRemoteToken("origin")
	require.NoError(t, err)
	assert.Equal(t, "", token)
}

func TestStore_RemoveRemote_NotFound(t *testing.T) {
	st := newTestStore(t)

	err := st.RemoveRemote("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestStore_UpdateRemoteURL(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.AddRemote("origin", "https://old.com/repo"))

	err := st.UpdateRemoteURL("origin", "https://new.com/repo")
	require.NoError(t, err)

	remote, err := st.GetRemote("origin")
	require.NoError(t, err)
	assert.Equal(t, "https://new.com/repo", remote.URL)
}

func TestStore_UpdateRemoteURL_NotFound(t *testing.T) {
	st := newTestStore(t)

	err := st.UpdateRemoteURL("nonexistent", "https://new.com/repo")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestStore_RemoteToken(t *testing.T) {
	st := newTestStore(t)

	// Initially no token
	token, err := st.GetRemoteToken("origin")
	require.NoError(t, err)
	assert.Equal(t, "", token)

	// Set token
	require.NoError(t, st.SetRemoteToken("origin", "my-secret-token"))

	token, err = st.GetRemoteToken("origin")
	require.NoError(t, err)
	assert.Equal(t, "my-secret-token", token)

	// Update token
	require.NoError(t, st.SetRemoteToken("origin", "new-token"))

	token, err = st.GetRemoteToken("origin")
	require.NoError(t, err)
	assert.Equal(t, "new-token", token)

	// Delete token
	require.NoError(t, st.DeleteRemoteToken("origin"))

	token, err = st.GetRemoteToken("origin")
	require.NoError(t, err)
	assert.Equal(t, "", token)
}

func TestStore_SetRemoteBranch(t *testing.T) {
	st := newTestStore(t)

	err := st.SetRemoteBranch("origin", "main", "abc123")
	require.NoError(t, err)

	rb, err := st.GetRemoteBranch("origin", "main")
	require.NoError(t, err)
	require.NotNil(t, rb)
	assert.Equal(t, "origin", rb.RemoteName)
	assert.Equal(t, "main", rb.BranchName)
	assert.Equal(t, "abc123", rb.CommitID)
	assert.False(t, rb.UpdatedAt.IsZero())
}

func TestStore_SetRemoteBranch_Update(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.SetRemoteBranch("origin", "main", "abc123"))
	require.NoError(t, st.SetRemoteBranch("origin", "main", "def456"))

	rb, err := st.GetRemoteBranch("origin", "main")
	require.NoError(t, err)
	assert.Equal(t, "def456", rb.CommitID)
}

func TestStore_GetRemoteBranch_NotFound(t *testing.T) {
	st := newTestStore(t)

	rb, err := st.GetRemoteBranch("origin", "main")
	require.NoError(t, err)
	assert.Nil(t, rb)
}

func TestStore_ListRemoteBranches(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.SetRemoteBranch("origin", "main", "abc123"))
	require.NoError(t, st.SetRemoteBranch("origin", "develop", "def456"))
	require.NoError(t, st.SetRemoteBranch("upstream", "main", "ghi789"))

	// List only origin's branches
	branches, err := st.ListRemoteBranches("origin")
	require.NoError(t, err)
	require.Len(t, branches, 2)

	// Should be sorted by branch name
	assert.Equal(t, "develop", branches[0].BranchName)
	assert.Equal(t, "main", branches[1].BranchName)

	// List upstream's branches
	branches, err = st.ListRemoteBranches("upstream")
	require.NoError(t, err)
	require.Len(t, branches, 1)
	assert.Equal(t, "main", branches[0].BranchName)
}

func TestStore_ListRemoteBranches_Empty(t *testing.T) {
	st := newTestStore(t)

	branches, err := st.ListRemoteBranches("origin")
	require.NoError(t, err)
	assert.Len(t, branches, 0)
}

func TestStore_DeleteRemoteBranch(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.SetRemoteBranch("origin", "main", "abc123"))

	err := st.DeleteRemoteBranch("origin", "main")
	require.NoError(t, err)

	rb, err := st.GetRemoteBranch("origin", "main")
	require.NoError(t, err)
	assert.Nil(t, rb)
}

func TestStore_RemoveRemote_CascadesRemoteBranches(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, st.AddRemote("origin", "https://example.com/repo"))
	require.NoError(t, st.SetRemoteBranch("origin", "main", "abc123"))
	require.NoError(t, st.SetRemoteBranch("origin", "develop", "def456"))

	// Also set a branch for a different remote to ensure it's not affected
	require.NoError(t, st.AddRemote("upstream", "https://upstream.com/repo"))
	require.NoError(t, st.SetRemoteBranch("upstream", "main", "ghi789"))

	err := st.RemoveRemote("origin")
	require.NoError(t, err)

	// Origin branches should be gone
	branches, err := st.ListRemoteBranches("origin")
	require.NoError(t, err)
	assert.Len(t, branches, 0)

	// Upstream branches should still exist
	branches, err = st.ListRemoteBranches("upstream")
	require.NoError(t, err)
	assert.Len(t, branches, 1)
}
