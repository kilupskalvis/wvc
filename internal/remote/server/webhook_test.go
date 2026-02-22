package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWebhookNotifier_NilConfig(t *testing.T) {
	wn := NewWebhookNotifier(nil, slog.Default())
	assert.Nil(t, wn)
}

func TestNewWebhookNotifier_EmptyURLs(t *testing.T) {
	wn := NewWebhookNotifier(&WebhookConfig{URLs: nil}, slog.Default())
	assert.Nil(t, wn)
}

func TestWebhookNotifier_NotifyPush_NilReceiver(t *testing.T) {
	// Should not panic
	var wn *WebhookNotifier
	wn.NotifyPush("repo", "main", "abc123")
}

func TestWebhookNotifier_NotifyPush(t *testing.T) {
	var mu sync.Mutex
	var received []WebhookEvent

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event WebhookEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	wn := NewWebhookNotifier(&WebhookConfig{URLs: []string{ts.URL}, AllowPrivate: true}, slog.Default())
	require.NotNil(t, wn)

	wn.NotifyPush("myrepo", "main", "commit123")

	// Wait for async delivery
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 1)
	assert.Equal(t, "push", received[0].Event)
	assert.Equal(t, "myrepo", received[0].Repo)
	assert.Equal(t, "main", received[0].Branch)
	assert.Equal(t, "commit123", received[0].CommitID)
	assert.NotEmpty(t, received[0].Timestamp)
}

func TestWebhookNotifier_NotifyPush_MultipleURLs(t *testing.T) {
	var mu sync.Mutex
	callCount := 0

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts2.Close()

	wn := NewWebhookNotifier(&WebhookConfig{URLs: []string{ts1.URL, ts2.URL}, AllowPrivate: true}, slog.Default())
	require.NotNil(t, wn)

	wn.NotifyPush("repo", "main", "abc")

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, callCount)
}

func TestWebhookNotifier_Post_4xxNoRetry(t *testing.T) {
	callCount := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	wn := NewWebhookNotifier(&WebhookConfig{URLs: []string{ts.URL}, AllowPrivate: true}, slog.Default())
	require.NotNil(t, wn)

	err := wn.post(ts.URL, []byte(`{}`))
	assert.Error(t, err)
	assert.Equal(t, 1, callCount) // no retry for 4xx
}
