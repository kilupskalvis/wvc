// Command wvc-server runs the WVC remote server.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kilupskalvis/wvc/internal/remote/blobstore"
	"github.com/kilupskalvis/wvc/internal/remote/metastore"
	"github.com/kilupskalvis/wvc/internal/remote/server"
)

func main() {
	listen := flag.String("listen", envOrDefault("WVC_LISTEN", "0.0.0.0:8720"), "Listen address")
	dataDir := flag.String("data-dir", envOrDefault("WVC_DATA_DIR", "/var/lib/wvc-server"), "Data directory")
	adminToken := flag.String("admin-token", os.Getenv("WVC_ADMIN_TOKEN"), "Admin API token")
	logLevel := flag.String("log-level", envOrDefault("WVC_LOG_LEVEL", "info"), "Log level (debug, info, warn, error)")
	logFormat := flag.String("log-format", envOrDefault("WVC_LOG_FORMAT", "json"), "Log format (json, text)")
	tlsCert := flag.String("tls-cert", os.Getenv("WVC_TLS_CERT"), "TLS certificate file")
	tlsKey := flag.String("tls-key", os.Getenv("WVC_TLS_KEY"), "TLS key file")
	webhookURLs := flag.String("webhook-urls", os.Getenv("WVC_WEBHOOK_URLS"), "Comma-separated webhook URLs to notify on push")
	flag.Parse()

	// Setup logger
	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	if *logFormat == "text" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	logger := slog.New(handler)

	// Validate data dir
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		logger.Error("failed to create data directory", "error", err, "path", *dataDir)
		os.Exit(1)
	}

	reposDir := filepath.Join(*dataDir, "repos")
	if err := os.MkdirAll(reposDir, 0755); err != nil {
		logger.Error("failed to create repos directory", "error", err, "path", reposDir)
		os.Exit(1)
	}

	// Token store (in-memory, loaded from JSON file)
	tokens := newFileTokenStore(filepath.Join(*dataDir, "tokens.json"), logger)
	if err := tokens.Load(); err != nil {
		logger.Warn("no token store loaded — creating empty", "error", err)
	}

	// Repo opener
	repos := &diskRepoOpener{
		reposDir: reposDir,
		stores:   make(map[string]*repoEntry),
		logger:   logger,
	}

	// Server config
	cfg := server.DefaultServerConfig()
	cfg.AdminToken = *adminToken

	// Webhooks
	if *webhookURLs != "" {
		urls := strings.Split(*webhookURLs, ",")
		var trimmed []string
		for _, u := range urls {
			u = strings.TrimSpace(u)
			if u != "" {
				trimmed = append(trimmed, u)
			}
		}
		if len(trimmed) > 0 {
			cfg.Webhooks = server.NewWebhookNotifier(&server.WebhookConfig{URLs: trimmed}, logger)
			logger.Info("webhooks configured", "count", len(trimmed))
		}
	}

	// Handler
	h, handlerCleanup := server.Handler(repos, tokens, cfg, logger)
	defer handlerCleanup()

	// HTTP server
	srv := &http.Server{
		Addr:         *listen,
		Handler:      h,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  120 * time.Second,
		BaseContext:  func(_ net.Listener) context.Context { return context.Background() },
	}

	// Graceful shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("starting wvc-server", "listen", *listen, "data_dir", *dataDir)
		var err error
		if *tlsCert != "" && *tlsKey != "" {
			err = srv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-done
	logger.Info("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown error", "error", err)
	}

	repos.CloseAll()
	logger.Info("server stopped")
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// diskRepoOpener manages bbolt + filesystem stores per repository.
type diskRepoOpener struct {
	reposDir string
	mu       sync.RWMutex
	stores   map[string]*repoEntry
	logger   *slog.Logger
}

type repoEntry struct {
	meta  metastore.MetaStore
	blobs blobstore.BlobStore
}

func (d *diskRepoOpener) Open(name string) (metastore.MetaStore, blobstore.BlobStore, error) {
	d.mu.RLock()
	entry, ok := d.stores[name]
	d.mu.RUnlock()
	if ok {
		return entry.meta, entry.blobs, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Double-check after write lock
	if entry, ok := d.stores[name]; ok {
		return entry.meta, entry.blobs, nil
	}

	// Validate repo name to prevent path traversal
	if strings.ContainsAny(name, "/\\") || name == ".." || name == "." || name == "" {
		return nil, nil, fmt.Errorf("invalid repository name: %q", name)
	}

	repoDir := filepath.Join(d.reposDir, name)
	if _, err := os.Stat(repoDir); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("repository '%s' not found", name)
	}

	meta, err := metastore.NewBboltStore(filepath.Join(repoDir, "meta.db"))
	if err != nil {
		return nil, nil, fmt.Errorf("open metastore for %s: %w", name, err)
	}

	blobs, err := blobstore.NewFSStore(filepath.Join(repoDir, "blobs"))
	if err != nil {
		meta.Close()
		return nil, nil, fmt.Errorf("open blobstore for %s: %w", name, err)
	}

	d.stores[name] = &repoEntry{meta: meta, blobs: blobs}
	d.logger.Info("opened repository", "name", name)

	return meta, blobs, nil
}

func (d *diskRepoOpener) CloseAll() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for name, entry := range d.stores {
		if err := entry.meta.Close(); err != nil {
			d.logger.Error("close metastore", "repo", name, "error", err)
		}
	}
	d.stores = make(map[string]*repoEntry)
}

// fileTokenStore is a JSON-file-based token store.
type fileTokenStore struct {
	path   string
	mu     sync.RWMutex
	tokens map[string]*server.TokenInfo // keyed by token_hash
	logger *slog.Logger
}

func newFileTokenStore(path string, logger *slog.Logger) *fileTokenStore {
	return &fileTokenStore{
		path:   path,
		tokens: make(map[string]*server.TokenInfo),
		logger: logger,
	}
}

func (s *fileTokenStore) Load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}

	var tokens []*server.TokenInfo
	if err := json.Unmarshal(data, &tokens); err != nil {
		return fmt.Errorf("parse token store: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tokens = make(map[string]*server.TokenInfo)
	for _, t := range tokens {
		s.tokens[t.TokenHash] = t
	}

	s.logger.Info("loaded tokens", "count", len(tokens))
	return nil
}

func (s *fileTokenStore) GetByHash(hash string) (*server.TokenInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, ok := s.tokens[hash]
	if !ok {
		return nil, nil
	}
	return info, nil
}

func (s *fileTokenStore) UpdateLastUsed(_ string) error {
	return nil
}

func (s *fileTokenStore) Save() error {
	s.mu.RLock()
	tokens := make([]*server.TokenInfo, 0, len(s.tokens))
	for _, t := range s.tokens {
		tokens = append(tokens, t)
	}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(tokens, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal tokens: %w", err)
	}
	return os.WriteFile(s.path, data, 0600)
}

func (s *fileTokenStore) CreateToken(desc string, repos []string, permission string) (string, *server.TokenInfo, error) {
	rawToken := fmt.Sprintf("wvc_%s", generateID())
	tokenHash := server.HashToken(rawToken)

	info := &server.TokenInfo{
		ID:         generateID(),
		TokenHash:  tokenHash,
		Desc:       desc,
		Repos:      repos,
		Permission: permission,
	}

	s.mu.Lock()
	s.tokens[tokenHash] = info
	s.mu.Unlock()

	if err := s.Save(); err != nil {
		return "", nil, fmt.Errorf("persist token: %w", err)
	}

	return rawToken, info, nil
}

func (s *fileTokenStore) ListTokens() ([]*server.TokenInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tokens := make([]*server.TokenInfo, 0, len(s.tokens))
	for _, t := range s.tokens {
		tokens = append(tokens, t)
	}
	return tokens, nil
}

func (s *fileTokenStore) DeleteToken(id string) error {
	s.mu.Lock()
	found := false
	for hash, t := range s.tokens {
		if t.ID == id {
			delete(s.tokens, hash)
			found = true
			break
		}
	}
	s.mu.Unlock()

	if !found {
		return fmt.Errorf("token '%s' not found", id)
	}

	return s.Save()
}

func generateID() string {
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
