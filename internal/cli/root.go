// Package cli implements the command-line interface for WVC.
package cli

import (
	"fmt"
	"os"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
	"github.com/spf13/cobra"
)

// cmdContext holds common resources for CLI commands
type cmdContext struct {
	Config *config.Config
	Store  *store.Store
	Client weaviate.ClientInterface
}

// Close releases resources held by cmdContext
func (c *cmdContext) Close() {
	if c.Store != nil {
		c.Store.Close()
	}
}

// initContext initializes config and store (no client)
func initContext() *cmdContext {
	cfg, err := config.Load()
	if err != nil {
		exitError("%v", err)
	}

	st, err := store.New(cfg.DatabasePath())
	if err != nil {
		exitError("failed to open store: %v", err)
	}

	return &cmdContext{Config: cfg, Store: st}
}

// initContextWithMigrations initializes config, store, and runs migrations
func initContextWithMigrations() *cmdContext {
	ctx := initContext()

	if err := ctx.Store.RunMigrations(); err != nil {
		ctx.Close()
		exitError("failed to run migrations: %v", err)
	}

	return ctx
}

// initFullContext initializes config, store, migrations, and weaviate client
func initFullContext() *cmdContext {
	ctx := initContextWithMigrations()

	client, err := weaviate.NewClient(ctx.Config.WeaviateURL)
	if err != nil {
		ctx.Close()
		exitError("failed to create Weaviate client: %v", err)
	}
	ctx.Client = client

	return ctx
}

var rootCmd = &cobra.Command{
	Use:   "wvc",
	Short: "Weaviate Version Control",
	Long: `WVC (Weaviate Version Control) is a git-like CLI tool for version controlling
Weaviate databases. Track changes, revert commits, and maintain
a full history of your vector database.`,
}

// Execute runs the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(addCmd)
	rootCmd.AddCommand(resetCmd)
	rootCmd.AddCommand(commitCmd)
	rootCmd.AddCommand(logCmd)
	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(revertCmd)
	rootCmd.AddCommand(showCmd)
	rootCmd.AddCommand(branchCmd)
	rootCmd.AddCommand(checkoutCmd)
}

// exitError prints an error and exits
func exitError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

// shortID returns first 8 characters of an ID
func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
