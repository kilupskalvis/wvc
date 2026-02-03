package server

import (
	"compress/gzip"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/kilupskalvis/wvc/internal/remote/blobstore"
	"github.com/kilupskalvis/wvc/internal/remote/metastore"
)

// RepoOpener returns the MetaStore and BlobStore for a given repo name.
type RepoOpener interface {
	Open(name string) (metastore.MetaStore, blobstore.BlobStore, error)
}

// ServerConfig holds configurable limits for the server.
type ServerConfig struct {
	MaxRequestBody    int64  // bytes, for JSON endpoints
	MaxBlobSize       int64  // bytes, for vector uploads
	RequestsPerMinute int    // per-token rate limit
	AdminToken        string // for admin endpoints
	Webhooks          *WebhookNotifier
}

// DefaultServerConfig returns reasonable defaults.
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		MaxRequestBody:    64 * 1024 * 1024,  // 64MB
		MaxBlobSize:       512 * 1024 * 1024, // 512MB
		RequestsPerMinute: 300,
	}
}

// Handler creates the HTTP handler with all routes and middleware.
// The returned cleanup function stops background goroutines and should be
// called on server shutdown.
func Handler(repos RepoOpener, tokens TokenStore, cfg *ServerConfig, logger *slog.Logger) (http.Handler, func()) {
	if cfg == nil {
		cfg = DefaultServerConfig()
	}
	if logger == nil {
		logger = slog.Default()
	}

	rl := newRateLimiter(cfg.RequestsPerMinute)
	auth := authMiddleware(tokens, logger)

	// Wrap a handler with auth + repo check + rate limit.
	// applyMiddleware reverses the list, so the last item runs outermost (first).
	// Execution order: auth -> requireRepo -> rl -> handler
	withAuth := func(h http.HandlerFunc) http.Handler {
		return applyMiddleware(h, auth, requireRepo, rl.middleware)
	}
	// Execution order: auth -> requireRepo -> requireWrite -> rl -> handler
	withAuthWrite := func(h http.HandlerFunc) http.Handler {
		return applyMiddleware(h, auth, requireRepo, requireWrite, rl.middleware)
	}

	mux := http.NewServeMux()

	// Health endpoints (no auth)
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if _, err := tokens.ListTokens(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("not ready: token store unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Admin endpoints
	if cfg.AdminToken != "" {
		adminMux := http.NewServeMux()
		adminMux.HandleFunc("POST /admin/tokens", makeAdminCreateTokenHandler(tokens, logger))
		adminMux.HandleFunc("DELETE /admin/tokens/{id}", makeAdminDeleteTokenHandler(tokens, logger))
		adminMux.HandleFunc("GET /admin/tokens", makeAdminListTokensHandler(tokens, logger))
		adminMux.HandleFunc("POST /admin/repos", handleNotImplemented)
		adminMux.HandleFunc("DELETE /admin/repos/{name}", handleNotImplemented)
		adminMux.HandleFunc("POST /admin/repos/{repo}/gc", makeAdminGCHandler(repos, logger))
		mux.Handle("/admin/", adminAuth(cfg.AdminToken, adminMux))
	}

	// Negotiation
	mux.Handle("POST /api/v1/repos/{repo}/negotiate/push", withAuth(makeRepoHandler(repos, cfg, handleNegotiatePush)))
	mux.Handle("POST /api/v1/repos/{repo}/negotiate/pull", withAuth(makeRepoHandler(repos, cfg, handleNegotiatePull)))
	mux.Handle("POST /api/v1/repos/{repo}/vectors/have", withAuth(makeRepoHandler(repos, cfg, handleVectorsHave)))

	// Commits
	mux.Handle("GET /api/v1/repos/{repo}/commits/{id}/bundle", withAuth(makeRepoHandler(repos, cfg, handleGetCommitBundle)))
	mux.Handle("POST /api/v1/repos/{repo}/commits", withAuthWrite(makeRepoHandler(repos, cfg, handlePostCommitBundle)))

	// Vectors
	mux.Handle("GET /api/v1/repos/{repo}/vectors/{hash}", withAuth(makeRepoHandler(repos, cfg, handleGetVector)))
	mux.Handle("POST /api/v1/repos/{repo}/vectors/{hash}", withAuthWrite(makeRepoHandler(repos, cfg, handlePostVector)))

	// Branches
	mux.Handle("GET /api/v1/repos/{repo}/branches", withAuth(makeRepoHandler(repos, cfg, handleListBranches)))
	mux.Handle("GET /api/v1/repos/{repo}/branches/{name}", withAuth(makeRepoHandler(repos, cfg, handleGetBranch)))
	mux.Handle("PUT /api/v1/repos/{repo}/branches/{name}", withAuthWrite(makeRepoHandler(repos, cfg, handleUpdateBranch)))
	mux.Handle("DELETE /api/v1/repos/{repo}/branches/{name}", withAuthWrite(makeRepoHandler(repos, cfg, handleDeleteBranch)))

	// Info
	mux.Handle("GET /api/v1/repos/{repo}/info", withAuth(makeRepoHandler(repos, cfg, handleRepoInfo)))

	// Apply global middleware
	handler := applyMiddleware(mux,
		recoveryMiddleware(logger),
		loggingMiddleware(logger),
		requestIDMiddleware,
	)

	cleanup := func() {
		rl.Stop()
	}

	return handler, cleanup
}

// applyMiddleware applies middleware in reverse order so the first in the list runs first.
func applyMiddleware(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

type repoHandlerFunc func(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, blobs blobstore.BlobStore, cfg *ServerConfig)

// makeRepoHandler resolves the repo and calls the handler with MetaStore and BlobStore.
func makeRepoHandler(repos RepoOpener, cfg *ServerConfig, fn repoHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repoName := r.PathValue("repo")
		if repoName == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error":   "bad_request",
				"message": "missing repository name in path",
			})
			return
		}

		meta, blobs, err := repos.Open(repoName)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error":   "not_found",
				"message": fmt.Sprintf("repository '%s' not found", repoName),
			})
			return
		}
		fn(w, r, meta, blobs, cfg)
	}
}

// --- Negotiate Handlers ---

func handleNegotiatePush(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, _ blobstore.BlobStore, cfg *ServerConfig) {
	var req remote.NegotiatePushRequest
	if err := readJSON(r, cfg.MaxRequestBody, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": err.Error()})
		return
	}

	if req.Branch == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "branch is required"})
		return
	}

	// Find remote tip
	var remoteTip string
	branch, err := meta.GetBranch(r.Context(), req.Branch)
	if err != nil && !errors.Is(err, metastore.ErrNotFound) {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}
	if branch != nil {
		remoteTip = branch.CommitID
	}

	// Find missing commits
	var missing []string
	for _, commitID := range req.Commits {
		has, err := meta.HasCommit(r.Context(), commitID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
			return
		}
		if !has {
			missing = append(missing, commitID)
		}
	}

	writeJSON(w, http.StatusOK, &remote.NegotiatePushResponse{
		MissingCommits: missing,
		RemoteTip:      remoteTip,
	})
}

func handleNegotiatePull(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, _ blobstore.BlobStore, cfg *ServerConfig) {
	var req remote.NegotiatePullRequest
	if err := readJSON(r, cfg.MaxRequestBody, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": err.Error()})
		return
	}

	if req.Branch == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "branch is required"})
		return
	}

	branch, err := meta.GetBranch(r.Context(), req.Branch)
	if err != nil {
		if errors.Is(err, metastore.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "branch not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	// Walk commits from tip backwards to find what client is missing
	localAncestors := make(map[string]bool)
	if req.LocalTip != "" {
		localAncestors[req.LocalTip] = true
		anc, err := meta.GetAncestors(r.Context(), req.LocalTip)
		if err == nil {
			for k, v := range anc {
				localAncestors[k] = v
			}
		}
	}

	// Walk from remote tip
	type queueItem struct {
		id    string
		depth int
	}
	var missing []string
	queue := []queueItem{{id: branch.CommitID, depth: 0}}
	visited := make(map[string]bool)

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if visited[item.id] || localAncestors[item.id] {
			continue
		}
		if req.Depth > 0 && item.depth >= req.Depth {
			continue
		}
		visited[item.id] = true
		missing = append(missing, item.id)

		commit, err := meta.GetCommit(r.Context(), item.id)
		if err != nil {
			continue
		}
		if commit.ParentID != "" {
			queue = append(queue, queueItem{id: commit.ParentID, depth: item.depth + 1})
		}
		if commit.MergeParentID != "" {
			queue = append(queue, queueItem{id: commit.MergeParentID, depth: item.depth + 1})
		}
	}

	// Reverse so oldest is first (topological order)
	for i, j := 0, len(missing)-1; i < j; i, j = i+1, j-1 {
		missing[i], missing[j] = missing[j], missing[i]
	}

	writeJSON(w, http.StatusOK, &remote.NegotiatePullResponse{
		MissingCommits: missing,
		RemoteTip:      branch.CommitID,
	})
}

func handleVectorsHave(w http.ResponseWriter, r *http.Request, _ metastore.MetaStore, blobs blobstore.BlobStore, cfg *ServerConfig) {
	var req remote.VectorCheckRequest
	if err := readJSON(r, cfg.MaxRequestBody, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": err.Error()})
		return
	}

	var have, missing []string
	for _, hash := range req.Hashes {
		exists, err := blobs.Has(r.Context(), hash)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
			return
		}
		if exists {
			have = append(have, hash)
		} else {
			missing = append(missing, hash)
		}
	}

	writeJSON(w, http.StatusOK, &remote.VectorCheckResponse{
		Have:    have,
		Missing: missing,
	})
}

// --- Commit Handlers ---

func handleGetCommitBundle(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, _ blobstore.BlobStore, _ *ServerConfig) {
	commitID := r.PathValue("id")
	if commitID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "commit ID required"})
		return
	}

	bundle, err := meta.GetCommitBundle(r.Context(), commitID)
	if err != nil {
		if errors.Is(err, metastore.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "commit not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	// Respond with gzip if client accepts it
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		gz := gzip.NewWriter(w)
		if err := json.NewEncoder(gz).Encode(bundle); err != nil {
			// Headers already sent — can't change status. Best effort close.
			gz.Close()
			return
		}
		gz.Close()
		return
	}

	writeJSON(w, http.StatusOK, bundle)
}

func handlePostCommitBundle(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, _ blobstore.BlobStore, cfg *ServerConfig) {
	var bundle remote.CommitBundle

	// Handle gzip'd body
	body := io.Reader(r.Body)
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "invalid gzip body"})
			return
		}
		defer gz.Close()
		body = gz
	}

	limited := io.LimitReader(body, cfg.MaxRequestBody)
	if err := json.NewDecoder(limited).Decode(&bundle); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": fmt.Sprintf("invalid JSON: %v", err)})
		return
	}

	if bundle.Commit == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "commit is required"})
		return
	}

	// Validate parent exists (unless initial commit)
	if bundle.Commit.ParentID != "" {
		has, err := meta.HasCommit(r.Context(), bundle.Commit.ParentID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
			return
		}
		if !has {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
				"error":   "validation_failed",
				"message": fmt.Sprintf("parent commit %s does not exist", bundle.Commit.ParentID),
			})
			return
		}
	}

	// Validate merge parent
	if bundle.Commit.MergeParentID != "" {
		has, err := meta.HasCommit(r.Context(), bundle.Commit.MergeParentID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
			return
		}
		if !has {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
				"error":   "validation_failed",
				"message": fmt.Sprintf("merge parent commit %s does not exist", bundle.Commit.MergeParentID),
			})
			return
		}
	}

	if err := meta.InsertCommitBundle(r.Context(), &bundle); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// --- Vector Handlers ---

func handleGetVector(w http.ResponseWriter, r *http.Request, _ metastore.MetaStore, blobs blobstore.BlobStore, _ *ServerConfig) {
	hash := r.PathValue("hash")
	if hash == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "vector hash required"})
		return
	}

	reader, dims, err := blobs.Get(r.Context(), hash)
	if err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "vector not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-WVC-Dimensions", strconv.Itoa(dims))
	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}

func handlePostVector(w http.ResponseWriter, r *http.Request, _ metastore.MetaStore, blobs blobstore.BlobStore, cfg *ServerConfig) {
	hash := r.PathValue("hash")
	if hash == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "vector hash required"})
		return
	}

	dimsStr := r.Header.Get("X-WVC-Dimensions")
	if dimsStr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "X-WVC-Dimensions header required"})
		return
	}
	dims, err := strconv.Atoi(dimsStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "invalid X-WVC-Dimensions value"})
		return
	}

	limited := io.LimitReader(r.Body, cfg.MaxBlobSize)
	if err := blobs.Put(r.Context(), hash, limited, dims); err != nil {
		if errors.Is(err, blobstore.ErrHashMismatch) {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "hash_mismatch", "message": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// --- Branch Handlers ---

func handleListBranches(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, _ blobstore.BlobStore, _ *ServerConfig) {
	branches, err := meta.ListBranches(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, branches)
}

func handleGetBranch(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, _ blobstore.BlobStore, _ *ServerConfig) {
	name := r.PathValue("name")
	branch, err := meta.GetBranch(r.Context(), name)
	if err != nil {
		if errors.Is(err, metastore.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "branch not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, branch)
}

func handleUpdateBranch(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, _ blobstore.BlobStore, cfg *ServerConfig) {
	name := r.PathValue("name")

	var req remote.BranchUpdateRequest
	if err := readJSON(r, cfg.MaxRequestBody, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": err.Error()})
		return
	}

	if req.CommitID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "commit_id is required"})
		return
	}

	err := meta.UpdateBranchCAS(r.Context(), name, req.CommitID, req.Expected)
	if err != nil {
		if errors.Is(err, metastore.ErrConflict) {
			branch, _ := meta.GetBranch(r.Context(), name)
			currentTip := ""
			if branch != nil {
				currentTip = branch.CommitID
			}
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"error":   "push_rejected",
				"message": fmt.Sprintf("remote branch '%s' has diverged — expected tip %s, got %s", name, req.Expected, currentTip),
				"detail":  map[string]string{"remote_tip": currentTip},
			})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	// Fire webhook on successful branch update (push)
	if cfg.Webhooks != nil {
		repoName := r.PathValue("repo")
		cfg.Webhooks.NotifyPush(repoName, name, req.CommitID)
	}

	w.WriteHeader(http.StatusOK)
}

func handleDeleteBranch(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, _ blobstore.BlobStore, _ *ServerConfig) {
	name := r.PathValue("name")

	err := meta.DeleteBranch(r.Context(), name)
	if err != nil {
		if errors.Is(err, metastore.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "branch not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
}

// --- Info Handler ---

func handleRepoInfo(w http.ResponseWriter, r *http.Request, meta metastore.MetaStore, blobs blobstore.BlobStore, _ *ServerConfig) {
	branches, err := meta.ListBranches(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	commitCount, err := meta.GetCommitCount(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	blobCount, err := blobs.TotalCount(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, &remote.RepoInfo{
		BranchCount: len(branches),
		CommitCount: commitCount,
		TotalBlobs:  blobCount,
	})
}

// --- Health Handlers ---

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func handleNotImplemented(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "not_implemented", "message": "endpoint not yet implemented"})
}

// --- Admin Auth ---

func adminAuth(adminToken string, next http.Handler) http.Handler {
	expected := "Bearer " + adminToken
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(auth), []byte(expected)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "auth_failed", "message": "invalid admin token"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, maxSize int64, v interface{}) error {
	limited := io.LimitReader(r.Body, maxSize)
	if err := json.NewDecoder(limited).Decode(v); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

// --- Admin Token Handlers ---

func makeAdminCreateTokenHandler(tokens TokenStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Description string   `json:"description"`
			Repos       []string `json:"repos"`
			Permission  string   `json:"permission"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "invalid JSON"})
			return
		}
		if req.Permission == "" {
			req.Permission = "ro"
		}
		if req.Permission != "ro" && req.Permission != "rw" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "permission must be 'ro' or 'rw'"})
			return
		}

		rawToken, info, err := tokens.CreateToken(req.Description, req.Repos, req.Permission)
		if err != nil {
			logger.Error("create token", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
			return
		}

		writeJSON(w, http.StatusCreated, map[string]interface{}{
			"token":       rawToken,
			"id":          info.ID,
			"description": info.Desc,
			"repos":       info.Repos,
			"permission":  info.Permission,
		})
	}
}

func makeAdminListTokensHandler(tokens TokenStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		list, err := tokens.ListTokens()
		if err != nil {
			logger.Error("list tokens", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
			return
		}

		// Return metadata only — no hashes
		type tokenEntry struct {
			ID          string   `json:"id"`
			Description string   `json:"description"`
			Repos       []string `json:"repos"`
			Permission  string   `json:"permission"`
		}
		entries := make([]tokenEntry, len(list))
		for i, t := range list {
			entries[i] = tokenEntry{
				ID:          t.ID,
				Description: t.Desc,
				Repos:       t.Repos,
				Permission:  t.Permission,
			}
		}

		writeJSON(w, http.StatusOK, entries)
	}
}

func makeAdminDeleteTokenHandler(tokens TokenStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "token ID required"})
			return
		}

		if err := tokens.DeleteToken(id); err != nil {
			logger.Error("delete token", "error", err, "token_id", id)
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": err.Error()})
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

// makeAdminGCHandler creates a handler for garbage collecting a repo's unreferenced blobs.
func makeAdminGCHandler(repos RepoOpener, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repoName := r.PathValue("repo")
		if repoName == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "repo name required"})
			return
		}

		meta, blobs, err := repos.Open(repoName)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": fmt.Sprintf("repository '%s' not found", repoName)})
			return
		}

		result, err := GarbageCollect(r.Context(), meta, blobs, logger)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error", "message": err.Error()})
			return
		}

		writeJSON(w, http.StatusOK, result)
	}
}
