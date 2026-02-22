package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/kilupskalvis/wvc/internal/remote/blobstore"
	"github.com/kilupskalvis/wvc/internal/remote/metastore"
	"github.com/kilupskalvis/wvc/internal/remote/server"
	"github.com/spf13/cobra"
)

var (
	serverListen        string
	serverDataDir       string
	serverLogLevel      string
	serverLogFormat     string
	serverTLSCert       string
	serverTLSKey        string
	serverWebhookURLs   string
	serverWebhookSecret string

	serverAdminURL        string
	serverAdminToken      string
	serverTokenDesc       string
	serverTokenRepos      []string
	serverTokenPermission string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the WVC remote server",
	Long:  "Commands for running the WVC remote server.",
}

var serverStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the WVC remote server",
	Long: `Start the WVC remote server.

The server stores commit metadata in bbolt and vector blobs on the local
filesystem. Bearer token authentication is required for all repo endpoints.

The admin token is read from the WVC_ADMIN_TOKEN environment variable and
enables the /admin/ endpoints for token management and garbage collection.

Examples:
  wvc server start
  wvc server start --listen 0.0.0.0:8720 --data-dir /var/lib/wvc
  wvc server start --tls-cert server.crt --tls-key server.key`,
	Run: runServerStart,
}

func init() {
	serverCmd.AddCommand(serverStartCmd)
	serverCmd.AddCommand(serverTokensCmd)
	serverCmd.AddCommand(serverReposCmd)

	f := serverStartCmd.Flags()
	f.StringVar(&serverListen, "listen", envOrDefault("WVC_LISTEN", "127.0.0.1:8720"), "Listen address (host:port)")
	f.StringVar(&serverDataDir, "data-dir", envOrDefault("WVC_DATA_DIR", defaultDataDir()), "Directory for repo data")
	f.StringVar(&serverLogLevel, "log-level", envOrDefault("WVC_LOG_LEVEL", "info"), "Log level (debug|info|warn|error)")
	f.StringVar(&serverLogFormat, "log-format", envOrDefault("WVC_LOG_FORMAT", "json"), "Log format (json|text)")
	f.StringVar(&serverTLSCert, "tls-cert", os.Getenv("WVC_TLS_CERT"), "TLS certificate file")
	f.StringVar(&serverTLSKey, "tls-key", os.Getenv("WVC_TLS_KEY"), "TLS key file")
	f.StringVar(&serverWebhookURLs, "webhook-urls", os.Getenv("WVC_WEBHOOK_URLS"), "Comma-separated webhook URLs to notify on push")
	f.StringVar(&serverWebhookSecret, "webhook-secret", os.Getenv("WVC_WEBHOOK_SECRET"), "HMAC secret for signing webhook payloads")

	// Shared admin connection flags. PersistentFlags are inherited by all subcommands.
	// Both parents bind the same package-level vars — safe because only one command
	// path executes at runtime.
	for _, cmd := range []*cobra.Command{serverTokensCmd, serverReposCmd} {
		cmd.PersistentFlags().StringVar(&serverAdminURL, "url",
			envOrDefault("WVC_SERVER_URL", ""),
			"Server base URL (env: WVC_SERVER_URL)")
		cmd.PersistentFlags().StringVar(&serverAdminToken, "admin-token",
			os.Getenv("WVC_ADMIN_TOKEN"),
			"Admin token (env: WVC_ADMIN_TOKEN)")
	}

	serverTokensCmd.AddCommand(serverTokensCreateCmd, serverTokensListCmd, serverTokensDeleteCmd)
	serverReposCmd.AddCommand(serverReposCreateCmd, serverReposListCmd, serverReposDeleteCmd)

	tf := serverTokensCreateCmd.Flags()
	tf.StringVar(&serverTokenDesc, "desc", "", "Token description")
	tf.StringArrayVar(&serverTokenRepos, "repo", nil,
		"Repos to grant access to, repeat for multiple (default: *)")
	tf.StringVar(&serverTokenPermission, "permission", "rw", "Permission level: ro or rw")
}

func runServerStart(_ *cobra.Command, _ []string) {
	var level slog.Level
	switch serverLogLevel {
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
	if serverLogFormat == "text" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	logger := slog.New(handler)

	if err := os.MkdirAll(serverDataDir, 0755); err != nil {
		logger.Error("failed to create data directory", "error", err, "path", serverDataDir)
		os.Exit(1)
	}

	reposDir := filepath.Join(serverDataDir, "repos")
	if err := os.MkdirAll(reposDir, 0755); err != nil {
		logger.Error("failed to create repos directory", "error", err, "path", reposDir)
		os.Exit(1)
	}

	tokens := newFileTokenStore(filepath.Join(serverDataDir, "tokens.json"), logger)
	if err := tokens.Load(); err != nil {
		logger.Warn("no token store loaded — creating empty", "error", err)
	}

	repos := &diskRepoOpener{
		reposDir: reposDir,
		stores:   make(map[string]*repoEntry),
		logger:   logger,
	}

	cfg := server.DefaultServerConfig()
	cfg.AdminToken = os.Getenv("WVC_ADMIN_TOKEN")

	if serverWebhookURLs != "" {
		urls := strings.Split(serverWebhookURLs, ",")
		var trimmed []string
		for _, u := range urls {
			u = strings.TrimSpace(u)
			if u != "" {
				trimmed = append(trimmed, u)
			}
		}
		if len(trimmed) > 0 {
			cfg.Webhooks = server.NewWebhookNotifier(&server.WebhookConfig{
				URLs:   trimmed,
				Secret: serverWebhookSecret,
			}, logger)
			logger.Info("webhooks configured", "count", len(trimmed))
		}
	}

	h, handlerCleanup := server.Handler(repos, tokens, cfg, logger, repos, repos)
	defer handlerCleanup()

	srv := &http.Server{
		Addr:              serverListen,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       120 * time.Second,
		BaseContext:       func(_ net.Listener) context.Context { return context.Background() },
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("starting wvc server", "listen", serverListen, "data_dir", serverDataDir)
		var err error
		if serverTLSCert != "" && serverTLSKey != "" {
			err = srv.ListenAndServeTLS(serverTLSCert, serverTLSKey)
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

// defaultDataDir returns the default server data directory (~/.wvc-server).
func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/var/lib/wvc-server"
	}
	return filepath.Join(home, ".wvc-server")
}

// envOrDefault returns the value of the environment variable key, or defaultVal if unset.
func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// diskRepoOpener manages bbolt + filesystem stores per repository, opening them lazily.
type diskRepoOpener struct {
	reposDir string
	mu       sync.RWMutex
	stores   map[string]*repoEntry
	logger   *slog.Logger
}

type repoEntry struct {
	meta    metastore.MetaStore
	blobs   blobstore.BlobStore
	writeMu sync.Mutex
}

// Open returns the MetaStore and BlobStore for the named repository.
// The repository directory must already exist under reposDir.
func (d *diskRepoOpener) Open(name string) (metastore.MetaStore, blobstore.BlobStore, error) {
	d.mu.RLock()
	entry, ok := d.stores[name]
	d.mu.RUnlock()
	if ok {
		return entry.meta, entry.blobs, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Double-check after acquiring write lock.
	if entry, ok := d.stores[name]; ok {
		return entry.meta, entry.blobs, nil
	}

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

// LockWrite acquires the per-repo write mutex, blocking concurrent GC and push operations.
func (d *diskRepoOpener) LockWrite(name string) {
	d.mu.RLock()
	entry, ok := d.stores[name]
	d.mu.RUnlock()
	if ok {
		entry.writeMu.Lock()
	}
}

// UnlockWrite releases the per-repo write mutex.
func (d *diskRepoOpener) UnlockWrite(name string) {
	d.mu.RLock()
	entry, ok := d.stores[name]
	d.mu.RUnlock()
	if ok {
		entry.writeMu.Unlock()
	}
}

// CloseAll closes all open repository stores.
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

// Create initialises a new repository directory under reposDir.
// Returns an error containing "already exists" if the repo is present.
func (d *diskRepoOpener) Create(name string) error {
	if strings.ContainsAny(name, "/\\") || name == ".." || name == "." || name == "" {
		return fmt.Errorf("invalid repository name: %q", name)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	repoDir := filepath.Join(d.reposDir, name)
	if _, err := os.Stat(repoDir); err == nil {
		return fmt.Errorf("repository '%s' already exists", name)
	}

	if err := os.MkdirAll(repoDir, 0755); err != nil {
		return fmt.Errorf("create repository directory: %w", err)
	}

	d.logger.Info("created repository", "name", name)
	return nil
}

// Delete removes a repository, closing and evicting any open stores first.
// Returns an error containing "not found" if the repo directory does not exist.
func (d *diskRepoOpener) Delete(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	repoDir := filepath.Join(d.reposDir, name)
	if _, err := os.Stat(repoDir); os.IsNotExist(err) {
		return fmt.Errorf("repository '%s' not found", name)
	}

	// Close and evict cached entry before removing files.
	if entry, ok := d.stores[name]; ok {
		// Acquire the per-repo write lock to block any in-flight request.
		entry.writeMu.Lock()
		defer entry.writeMu.Unlock()

		if err := entry.meta.Close(); err != nil {
			d.logger.Error("close metastore on delete", "repo", name, "error", err)
		}
		delete(d.stores, name)
	}

	if err := os.RemoveAll(repoDir); err != nil {
		return fmt.Errorf("remove repository directory: %w", err)
	}

	d.logger.Info("deleted repository", "name", name)
	return nil
}

// List returns all repository names by scanning the repos directory.
func (d *diskRepoOpener) List() ([]string, error) {
	entries, err := os.ReadDir(d.reposDir)
	if err != nil {
		return nil, fmt.Errorf("list repositories: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// fileTokenStore is a JSON-file-backed implementation of server.TokenStore.
// Tokens are stored as hashed values; the raw token is only returned on creation.
type fileTokenStore struct {
	path   string
	mu     sync.RWMutex
	tokens map[string]*server.TokenInfo // keyed by token hash
	logger *slog.Logger
}

func newFileTokenStore(path string, logger *slog.Logger) *fileTokenStore {
	return &fileTokenStore{
		path:   path,
		tokens: make(map[string]*server.TokenInfo),
		logger: logger,
	}
}

// Load reads the token store from disk.
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

// GetByHash returns the token info for the given SHA256 hash, or nil if not found.
func (s *fileTokenStore) GetByHash(hash string) (*server.TokenInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, ok := s.tokens[hash]
	if !ok {
		return nil, nil
	}
	return info, nil
}

// UpdateLastUsed is a no-op for the file-based store (no last-used tracking).
func (s *fileTokenStore) UpdateLastUsed(_ string) error {
	return nil
}

// Save persists all tokens to disk atomically.
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

// CreateToken generates a new bearer token, persists it, and returns the raw value.
// The raw token is only available at creation time; only its hash is stored.
func (s *fileTokenStore) CreateToken(desc string, repos []string, permission string) (string, *server.TokenInfo, error) {
	rawToken := fmt.Sprintf("wvc_%s", generateServerID())
	tokenHash := server.HashToken(rawToken)

	info := &server.TokenInfo{
		ID:         generateServerID(),
		TokenHash:  tokenHash,
		Desc:       desc,
		Repos:      repos,
		Permission: permission,
	}

	s.mu.Lock()
	s.tokens[tokenHash] = info
	s.mu.Unlock()

	if err := s.Save(); err != nil {
		// Save failed — remove from in-memory map to stay consistent.
		s.mu.Lock()
		delete(s.tokens, tokenHash)
		s.mu.Unlock()
		return "", nil, fmt.Errorf("persist token: %w", err)
	}

	return rawToken, info, nil
}

// ListTokens returns all token metadata. Raw token values are never returned.
func (s *fileTokenStore) ListTokens() ([]*server.TokenInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tokens := make([]*server.TokenInfo, 0, len(s.tokens))
	for _, t := range s.tokens {
		tokens = append(tokens, t)
	}
	return tokens, nil
}

// DeleteToken removes the token with the given ID. Returns an error if not found.
func (s *fileTokenStore) DeleteToken(id string) error {
	s.mu.Lock()
	var foundHash string
	var foundToken *server.TokenInfo
	for hash, t := range s.tokens {
		if t.ID == id {
			foundHash = hash
			foundToken = t
			break
		}
	}
	if foundHash == "" {
		s.mu.Unlock()
		return fmt.Errorf("token '%s' not found", id)
	}

	delete(s.tokens, foundHash)
	s.mu.Unlock()

	if err := s.Save(); err != nil {
		// Save failed — restore the token to keep in-memory and on-disk state consistent.
		s.mu.Lock()
		s.tokens[foundHash] = foundToken
		s.mu.Unlock()
		return err
	}

	return nil
}

// generateServerID returns a cryptographically random 16-byte hex string.
func generateServerID() string {
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// --- wvc server tokens ---

var serverTokensCmd = &cobra.Command{
	Use:   "tokens",
	Short: "Manage server tokens",
	Long:  "Commands for managing authentication tokens on a running wvc server.",
}

var serverTokensCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new authentication token",
	Run:   runServerTokensCreate,
}

var serverTokensListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all authentication tokens",
	Run:   runServerTokensList,
}

var serverTokensDeleteCmd = &cobra.Command{
	Use:   "delete <id>",
	Short: "Delete an authentication token",
	Args:  cobra.ExactArgs(1),
	Run:   runServerTokensDelete,
}

// --- wvc server repos ---

var serverReposCmd = &cobra.Command{
	Use:   "repos",
	Short: "Manage server repositories",
	Long:  "Commands for managing repositories on a running wvc server.",
}

var serverReposCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new repository",
	Args:  cobra.ExactArgs(1),
	Run:   runServerReposCreate,
}

var serverReposListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all repositories",
	Run:   runServerReposList,
}

var serverReposDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a repository",
	Args:  cobra.ExactArgs(1),
	Run:   runServerReposDelete,
}

// resolveAdminClient builds an AdminClient from the package-level admin flag vars.
func resolveAdminClient() *remote.AdminClient {
	if serverAdminURL == "" {
		exitError("--url or WVC_SERVER_URL is required")
	}
	if serverAdminToken == "" {
		exitError("--admin-token or WVC_ADMIN_TOKEN is required")
	}
	return remote.NewAdminClient(serverAdminURL, serverAdminToken)
}

func runServerTokensCreate(_ *cobra.Command, _ []string) {
	c := resolveAdminClient()
	ctx := context.Background()

	repos := serverTokenRepos
	if len(repos) == 0 {
		repos = []string{"*"}
	}

	resp, err := c.CreateToken(ctx, serverTokenDesc, repos, serverTokenPermission)
	if err != nil {
		exitError("%v", err)
	}

	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)

	fmt.Println("Token created.")
	fmt.Printf("  ID:          %s\n", resp.ID)
	fmt.Printf("  Description: %s\n", resp.Description)
	fmt.Printf("  Repos:       %s\n", strings.Join(resp.Repos, ", "))
	fmt.Printf("  Permission:  %s\n", resp.Permission)
	fmt.Println()
	green.Printf("Token: %s\n", resp.Token)
	yellow.Println("Save this token — it will not be shown again.")
}

func runServerTokensList(_ *cobra.Command, _ []string) {
	c := resolveAdminClient()
	ctx := context.Background()

	tokens, err := c.ListTokens(ctx)
	if err != nil {
		exitError("%v", err)
	}

	if len(tokens) == 0 {
		return
	}

	fmt.Printf("  %-32s  %-20s  %-16s  %s\n", "ID", "Description", "Repos", "Permission")
	for _, t := range tokens {
		fmt.Printf("  %-32s  %-20s  %-16s  %s\n",
			t.ID,
			t.Description,
			strings.Join(t.Repos, ","),
			t.Permission,
		)
	}
}

func runServerTokensDelete(_ *cobra.Command, args []string) {
	c := resolveAdminClient()
	ctx := context.Background()

	if err := c.DeleteToken(ctx, args[0]); err != nil {
		exitError("%v", err)
	}

	fmt.Printf("Deleted token '%s'\n", args[0])
}

func runServerReposCreate(_ *cobra.Command, args []string) {
	c := resolveAdminClient()
	ctx := context.Background()

	if err := c.CreateRepo(ctx, args[0]); err != nil {
		exitError("%v", err)
	}

	green := color.New(color.FgGreen)
	green.Printf("Created repository '%s'\n", args[0])
}

func runServerReposList(_ *cobra.Command, _ []string) {
	c := resolveAdminClient()
	ctx := context.Background()

	repos, err := c.ListRepos(ctx)
	if err != nil {
		exitError("%v", err)
	}

	for _, r := range repos {
		fmt.Printf("  %s\n", r)
	}
}

func runServerReposDelete(_ *cobra.Command, args []string) {
	c := resolveAdminClient()
	ctx := context.Background()

	if err := c.DeleteRepo(ctx, args[0]); err != nil {
		exitError("%v", err)
	}

	fmt.Printf("Deleted repository '%s'\n", args[0])
}
