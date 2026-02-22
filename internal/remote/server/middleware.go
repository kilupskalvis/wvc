// Package server implements the wvc-server HTTP handlers and middleware.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type contextKey string

const (
	contextKeyRequestID  contextKey = "request_id"
	contextKeyTokenID    contextKey = "token_id"
	contextKeyRepos      contextKey = "repos"
	contextKeyPermission contextKey = "permission"
)

// TokenInfo holds the metadata for an authenticated token.
type TokenInfo struct {
	ID         string   `json:"id"`
	TokenHash  string   `json:"token_hash"`
	Desc       string   `json:"description"`
	Repos      []string `json:"repos"`
	Permission string   `json:"permission"` // "ro" or "rw"
}

// TokenStore is the interface for managing authentication tokens.
type TokenStore interface {
	GetByHash(hash string) (*TokenInfo, error)
	UpdateLastUsed(id string) error
	ListTokens() ([]*TokenInfo, error)
	DeleteToken(id string) error
	CreateToken(desc string, repos []string, permission string) (rawToken string, info *TokenInfo, err error)
}

// requestIDMiddleware generates a UUID per request and adds it to the context.
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := uuid.New().String()
		ctx := context.WithValue(r.Context(), contextKeyRequestID, reqID)
		w.Header().Set("X-Request-ID", reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// loggingMiddleware logs request method, path, status, and latency.
func loggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(rw, r)

			reqID, _ := r.Context().Value(contextKeyRequestID).(string)
			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.statusCode,
				"latency_ms", time.Since(start).Milliseconds(),
				"request_id", reqID,
			)
		})
	}
}

// recoveryMiddleware catches panics and returns 500.
func recoveryMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw := &responseWriter{ResponseWriter: w, statusCode: 0}
			defer func() {
				if rec := recover(); rec != nil {
					reqID, _ := r.Context().Value(contextKeyRequestID).(string)
					logger.Error("panic recovered", "error", rec, "request_id", reqID)
					if rw.statusCode == 0 {
						http.Error(rw, `{"error":"internal_error","message":"internal server error"}`, http.StatusInternalServerError)
					}
				}
			}()
			next.ServeHTTP(rw, r)
		})
	}
}

// authMiddleware validates bearer tokens and sets permissions in context.
func authMiddleware(tokens TokenStore, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		sem := make(chan struct{}, 20)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, "Bearer ") {
				writeJSON(w, http.StatusUnauthorized, map[string]string{
					"error":   "auth_failed",
					"message": "missing or invalid Authorization header",
				})
				return
			}

			rawToken := strings.TrimPrefix(auth, "Bearer ")
			tokenHash := HashToken(rawToken)

			info, err := tokens.GetByHash(tokenHash)
			if err != nil || info == nil {
				writeJSON(w, http.StatusUnauthorized, map[string]string{
					"error":   "auth_failed",
					"message": "invalid token",
				})
				return
			}

			// Async update last_used_at
			select {
			case sem <- struct{}{}:
				go func() {
					defer func() { <-sem }()
					if err := tokens.UpdateLastUsed(info.ID); err != nil {
						logger.Warn("failed to update token last_used_at", "error", err, "token_id", info.ID)
					}
				}()
			default:
				// Drop update if too many in flight
			}

			ctx := r.Context()
			ctx = context.WithValue(ctx, contextKeyTokenID, info.ID)
			ctx = context.WithValue(ctx, contextKeyRepos, info.Repos)
			ctx = context.WithValue(ctx, contextKeyPermission, info.Permission)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// requireRepo checks that the token has access to the requested repo.
func requireRepo(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		repo := r.PathValue("repo")
		if repo == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error":   "bad_request",
				"message": "missing repository name in path",
			})
			return
		}

		repos, _ := r.Context().Value(contextKeyRepos).([]string)
		allowed := false
		for _, rp := range repos {
			if rp == "*" || rp == repo {
				allowed = true
				break
			}
		}

		if !allowed {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error":   "forbidden",
				"message": "token does not have access to repository '" + repo + "'",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

// requireWrite checks that the token has "rw" permission.
func requireWrite(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		perm, _ := r.Context().Value(contextKeyPermission).(string)
		if perm != "rw" {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error":   "forbidden",
				"message": "read-only token cannot perform write operations",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// rateLimitMiddleware implements a per-token sliding window rate limiter.
type rateLimiter struct {
	mu      sync.Mutex
	windows map[string]*window
	limit   int
	done    chan struct{}
}

type window struct {
	count   int
	resetAt time.Time
}

func newRateLimiter(requestsPerMinute int) *rateLimiter {
	rl := &rateLimiter{
		windows: make(map[string]*window),
		limit:   requestsPerMinute,
		done:    make(chan struct{}),
	}
	go rl.cleanup()
	return rl
}

func (rl *rateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rl.mu.Lock()
			now := time.Now()
			for k, w := range rl.windows {
				if now.After(w.resetAt) {
					delete(rl.windows, k)
				}
			}
			rl.mu.Unlock()
		case <-rl.done:
			return
		}
	}
}

func (rl *rateLimiter) Stop() {
	close(rl.done)
}

func (rl *rateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rl.limit <= 0 {
			next.ServeHTTP(w, r)
			return
		}

		key, _ := r.Context().Value(contextKeyTokenID).(string)
		if key == "" {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				host = r.RemoteAddr
			}
			key = host
		}

		rl.mu.Lock()
		win, ok := rl.windows[key]
		now := time.Now()
		if !ok || now.After(win.resetAt) {
			win = &window{count: 0, resetAt: now.Add(time.Minute)}
			rl.windows[key] = win
		}
		win.count++
		count := win.count
		rl.mu.Unlock()

		if count > rl.limit {
			w.Header().Set("Retry-After", "60")
			writeJSON(w, http.StatusTooManyRequests, map[string]string{
				"error":   "rate_limited",
				"message": "rate limit exceeded",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// HashToken returns the SHA256 hex digest of a raw token string.
func HashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}
