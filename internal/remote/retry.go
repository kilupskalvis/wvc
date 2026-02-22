package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/kilupskalvis/wvc/internal/models"
)

// RetryConfig configures retry behavior for transient errors.
type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	JitterFraction float64 // 0.0 to 1.0
}

// DefaultRetryConfig returns sensible retry defaults.
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		JitterFraction: 0.25,
	}
}

// RetryClient wraps a RemoteClient with automatic retry on transient errors.
type RetryClient struct {
	inner  RemoteClient
	config *RetryConfig
}

// NewRetryClient creates a RetryClient that wraps the given RemoteClient.
func NewRetryClient(inner RemoteClient, cfg *RetryConfig) *RetryClient {
	if cfg == nil {
		cfg = DefaultRetryConfig()
	}
	return &RetryClient{inner: inner, config: cfg}
}

// isTransient returns true for errors that are worth retrying.
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	var re *RemoteError
	if errors.As(err, &re) {
		return re.Status >= 500 || re.Status == http.StatusTooManyRequests
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true // network errors are transient
}

// backoff computes the delay for the given attempt with jitter.
func (rc *RetryClient) backoff(attempt int) time.Duration {
	base := float64(rc.config.InitialBackoff) * math.Pow(2, float64(attempt))
	if base > float64(rc.config.MaxBackoff) {
		base = float64(rc.config.MaxBackoff)
	}
	jitter := base * rc.config.JitterFraction * (rand.Float64()*2 - 1) // +/- jitter
	d := time.Duration(base + jitter)
	if d < 0 {
		d = 0
	}
	return d
}

// sleep waits for the given duration or until the context is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// retry executes fn with retry logic. Only retries transient errors.
func (rc *RetryClient) retry(ctx context.Context, operation string, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= rc.config.MaxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isTransient(lastErr) {
			return lastErr
		}
		if attempt < rc.config.MaxRetries {
			d := rc.backoff(attempt)
			if err := sleep(ctx, d); err != nil {
				return fmt.Errorf("%s: %w (retry cancelled)", operation, lastErr)
			}
		}
	}
	return fmt.Errorf("%s: %w (after %d retries)", operation, lastErr, rc.config.MaxRetries)
}

// --- Delegate all RemoteClient methods through retry logic ---

func (rc *RetryClient) NegotiatePush(ctx context.Context, branch string, commitIDs []string) (resp *NegotiatePushResponse, err error) {
	err = rc.retry(ctx, "negotiate push", func() error {
		resp, err = rc.inner.NegotiatePush(ctx, branch, commitIDs)
		return err
	})
	return
}

func (rc *RetryClient) NegotiatePull(ctx context.Context, branch string, localTip string, depth int) (resp *NegotiatePullResponse, err error) {
	err = rc.retry(ctx, "negotiate pull", func() error {
		resp, err = rc.inner.NegotiatePull(ctx, branch, localTip, depth)
		return err
	})
	return
}

func (rc *RetryClient) CheckVectors(ctx context.Context, hashes []string) (resp *VectorCheckResponse, err error) {
	err = rc.retry(ctx, "check vectors", func() error {
		resp, err = rc.inner.CheckVectors(ctx, hashes)
		return err
	})
	return
}

func (rc *RetryClient) UploadVector(ctx context.Context, hash string, r io.Reader, dims int) error {
	// Note: Cannot retry uploads with io.Reader (consumed on first attempt).
	// Callers should buffer if they need retry.
	return rc.inner.UploadVector(ctx, hash, r, dims)
}

func (rc *RetryClient) DownloadVector(ctx context.Context, hash string) (reader io.ReadCloser, dims int, err error) {
	err = rc.retry(ctx, "download vector", func() error {
		if reader != nil {
			reader.Close()
			reader = nil
		}
		reader, dims, err = rc.inner.DownloadVector(ctx, hash)
		return err
	})
	return
}

func (rc *RetryClient) UploadCommitBundle(ctx context.Context, bundle *CommitBundle) error {
	// Commit bundle upload is already gzip-buffered, so retry is safe.
	return rc.retry(ctx, "upload commit bundle", func() error {
		return rc.inner.UploadCommitBundle(ctx, bundle)
	})
}

func (rc *RetryClient) DownloadCommitBundle(ctx context.Context, commitID string) (bundle *CommitBundle, err error) {
	err = rc.retry(ctx, "download commit bundle", func() error {
		bundle, err = rc.inner.DownloadCommitBundle(ctx, commitID)
		return err
	})
	return
}

func (rc *RetryClient) UpdateBranch(ctx context.Context, branch, newTip, expectedTip string) error {
	// CAS operations are NOT retried — conflict errors are not transient.
	return rc.inner.UpdateBranch(ctx, branch, newTip, expectedTip)
}

func (rc *RetryClient) DeleteBranch(ctx context.Context, branch string) error {
	return rc.retry(ctx, "delete branch", func() error {
		return rc.inner.DeleteBranch(ctx, branch)
	})
}

func (rc *RetryClient) ListBranches(ctx context.Context) (branches []*models.Branch, err error) {
	err = rc.retry(ctx, "list branches", func() error {
		branches, err = rc.inner.ListBranches(ctx)
		return err
	})
	return
}

func (rc *RetryClient) GetBranch(ctx context.Context, branch string) (b *models.Branch, err error) {
	err = rc.retry(ctx, "get branch", func() error {
		b, err = rc.inner.GetBranch(ctx, branch)
		return err
	})
	return
}

func (rc *RetryClient) GetRepoInfo(ctx context.Context) (info *RepoInfo, err error) {
	err = rc.retry(ctx, "get repo info", func() error {
		info, err = rc.inner.GetRepoInfo(ctx)
		return err
	})
	return
}
