package remote

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsTransient_NilError(t *testing.T) {
	assert.False(t, isTransient(nil))
}

func TestIsTransient_ServerError(t *testing.T) {
	err := &RemoteError{Status: 500, Code: "internal_error", Message: "server error"}
	assert.True(t, isTransient(err))
}

func TestIsTransient_TooManyRequests(t *testing.T) {
	err := &RemoteError{Status: http.StatusTooManyRequests, Code: "rate_limited", Message: "too many"}
	assert.True(t, isTransient(err))
}

func TestIsTransient_ClientError(t *testing.T) {
	err := &RemoteError{Status: 404, Code: "not_found", Message: "not found"}
	assert.False(t, isTransient(err))
}

func TestIsTransient_NetworkError(t *testing.T) {
	err := &http.MaxBytesError{Limit: 100}
	assert.True(t, isTransient(err))
}

func TestRetryClient_Backoff(t *testing.T) {
	rc := NewRetryClient(nil, &RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		JitterFraction: 0.0, // no jitter for deterministic test
	})

	d0 := rc.backoff(0)
	d1 := rc.backoff(1)
	d2 := rc.backoff(2)

	assert.Equal(t, 100*time.Millisecond, d0)
	assert.Equal(t, 200*time.Millisecond, d1)
	assert.Equal(t, 400*time.Millisecond, d2)
}

func TestRetryClient_BackoffCapped(t *testing.T) {
	rc := NewRetryClient(nil, &RetryConfig{
		MaxRetries:     10,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     5 * time.Second,
		JitterFraction: 0.0,
	})

	d := rc.backoff(10)
	assert.Equal(t, 5*time.Second, d)
}

func TestRetryClient_RetrySuccess(t *testing.T) {
	rc := NewRetryClient(nil, &RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		JitterFraction: 0.0,
	})

	attempts := 0
	err := rc.retry(context.Background(), "test", func() error {
		attempts++
		if attempts < 3 {
			return &RemoteError{Status: 500, Code: "internal", Message: "fail"}
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestRetryClient_RetryExhausted(t *testing.T) {
	rc := NewRetryClient(nil, &RetryConfig{
		MaxRetries:     2,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		JitterFraction: 0.0,
	})

	attempts := 0
	err := rc.retry(context.Background(), "test", func() error {
		attempts++
		return &RemoteError{Status: 500, Code: "internal", Message: "fail"}
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "after 2 retries")
	assert.Equal(t, 3, attempts) // initial + 2 retries
}

func TestRetryClient_NoRetryOn4xx(t *testing.T) {
	rc := NewRetryClient(nil, &RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		JitterFraction: 0.0,
	})

	attempts := 0
	err := rc.retry(context.Background(), "test", func() error {
		attempts++
		return &RemoteError{Status: 404, Code: "not_found", Message: "not found"}
	})

	assert.Error(t, err)
	assert.Equal(t, 1, attempts) // no retry
}

func TestRetryClient_ContextCancellation(t *testing.T) {
	rc := NewRetryClient(nil, &RetryConfig{
		MaxRetries:     5,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     10 * time.Second,
		JitterFraction: 0.0,
	})

	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := rc.retry(ctx, "test", func() error {
		attempts++
		return &RemoteError{Status: 500, Code: "internal", Message: "fail"}
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "retry cancelled")
}

func TestSleep_ContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sleep(ctx, 10*time.Second)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSleep_Normal(t *testing.T) {
	err := sleep(context.Background(), 1*time.Millisecond)
	assert.NoError(t, err)
}
