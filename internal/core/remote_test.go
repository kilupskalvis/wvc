package core

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddRemote(t *testing.T) {
	st := newTestStore(t)

	err := AddRemote(st, "origin", "https://example.com/repo")
	require.NoError(t, err)

	remote, err := st.GetRemote("origin")
	require.NoError(t, err)
	require.NotNil(t, remote)
	assert.Equal(t, "origin", remote.Name)
	assert.Equal(t, "https://example.com/repo", remote.URL)
}

func TestAddRemote_InvalidName(t *testing.T) {
	st := newTestStore(t)

	tests := []struct {
		name    string
		wantErr string
	}{
		{"", "cannot be empty"},
		{"has space", "invalid characters"},
		{"has\ttab", "invalid characters"},
		{"has:colon", "invalid characters"},
		{"has/slash", "invalid characters"},
		{"has\\backslash", "invalid characters"},
		{"HEAD", "reserved"},
		{"head", "reserved"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AddRemote(st, tt.name, "https://example.com/repo")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestAddRemote_InvalidURL(t *testing.T) {
	st := newTestStore(t)

	tests := []struct {
		url     string
		wantErr string
	}{
		{"", "cannot be empty"},
		{"no-scheme.com/repo", "must include a scheme"},
		{"ftp://example.com/repo", "must be http or https"},
		{"https://", "must include a host"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			err := AddRemote(st, "origin", tt.url)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestAddRemote_Duplicate(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://example.com/repo"))

	err := AddRemote(st, "origin", "https://other.com/repo")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestRemoveRemote(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://example.com/repo"))

	err := RemoveRemote(st, "origin")
	require.NoError(t, err)

	remote, err := st.GetRemote("origin")
	require.NoError(t, err)
	assert.Nil(t, remote)
}

func TestRemoveRemote_NotFound(t *testing.T) {
	st := newTestStore(t)

	err := RemoveRemote(st, "nonexistent")
	assert.Error(t, err)
}

func TestListRemotes(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://example.com/repo"))
	require.NoError(t, AddRemote(st, "upstream", "https://upstream.com/repo"))

	result, err := ListRemotes(st)
	require.NoError(t, err)
	require.Len(t, result.Remotes, 2)
	assert.Equal(t, "origin", result.Remotes[0].Name)
	assert.Equal(t, "upstream", result.Remotes[1].Name)
}

func TestListRemotes_Empty(t *testing.T) {
	st := newTestStore(t)

	result, err := ListRemotes(st)
	require.NoError(t, err)
	assert.Len(t, result.Remotes, 0)
}

func TestGetRemote(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://example.com/repo"))

	remote, err := GetRemote(st, "origin")
	require.NoError(t, err)
	assert.Equal(t, "origin", remote.Name)
	assert.Equal(t, "https://example.com/repo", remote.URL)
}

func TestGetRemote_NotFound(t *testing.T) {
	st := newTestStore(t)

	_, err := GetRemote(st, "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestSetRemoteToken(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://example.com/repo"))

	err := SetRemoteToken(st, "origin", "my-token")
	require.NoError(t, err)

	token, err := st.GetRemoteToken("origin")
	require.NoError(t, err)
	assert.Equal(t, "my-token", token)
}

func TestSetRemoteToken_EmptyDeletesToken(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://example.com/repo"))
	require.NoError(t, SetRemoteToken(st, "origin", "my-token"))

	err := SetRemoteToken(st, "origin", "")
	require.NoError(t, err)

	token, err := st.GetRemoteToken("origin")
	require.NoError(t, err)
	assert.Equal(t, "", token)
}

func TestSetRemoteToken_RemoteNotFound(t *testing.T) {
	st := newTestStore(t)

	err := SetRemoteToken(st, "nonexistent", "token")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestGetRemoteToken_EnvVarOverride(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://example.com/repo"))
	require.NoError(t, SetRemoteToken(st, "origin", "stored-token"))

	// Set env var
	t.Setenv("WVC_REMOTE_TOKEN", "env-token")

	token, err := GetRemoteToken(st, "origin")
	require.NoError(t, err)
	assert.Equal(t, "env-token", token)
}

func TestGetRemoteToken_FallbackToStored(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://example.com/repo"))
	require.NoError(t, SetRemoteToken(st, "origin", "stored-token"))

	// Ensure env var is not set
	os.Unsetenv("WVC_REMOTE_TOKEN")

	token, err := GetRemoteToken(st, "origin")
	require.NoError(t, err)
	assert.Equal(t, "stored-token", token)
}

func TestSetRemoteURL(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://old.com/repo"))

	err := SetRemoteURL(st, "origin", "https://new.com/repo")
	require.NoError(t, err)

	remote, err := GetRemote(st, "origin")
	require.NoError(t, err)
	assert.Equal(t, "https://new.com/repo", remote.URL)
}

func TestSetRemoteURL_InvalidURL(t *testing.T) {
	st := newTestStore(t)

	require.NoError(t, AddRemote(st, "origin", "https://old.com/repo"))

	err := SetRemoteURL(st, "origin", "not-a-url")
	assert.Error(t, err)
}

func TestValidateRemoteName(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"origin", false},
		{"upstream", false},
		{"my-remote", false},
		{"remote_1", false},
		{"", true},
		{"has space", true},
		{"HEAD", true},
		{"MERGE_HEAD", true},
		{"FETCH_HEAD", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRemoteName(tt.name)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateRemoteURL(t *testing.T) {
	tests := []struct {
		url     string
		wantErr bool
	}{
		{"https://example.com/repo", false},
		{"http://localhost:8720/repo", false},
		{"https://example.com:8080/path/to/repo", false},
		{"", true},
		{"no-scheme", true},
		{"ftp://example.com", true},
		{"https://", true},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			err := validateRemoteURL(tt.url)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
