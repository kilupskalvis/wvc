package cli

import (
	"context"
	"fmt"

	"github.com/kilupskalvis/wvc/internal/config"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/kilupskalvis/wvc/internal/weaviate"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a new WVC repository",
	Long: `Initialize a new WVC repository in the current directory.
This creates a .wvc directory to store version control data.`,
	Run: runInit,
}

var initURL string

func init() {
	initCmd.Flags().StringVar(&initURL, "url", "http://localhost:8080", "Weaviate server URL")
}

func runInit(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// Check if already initialized
	if _, err := config.FindWVCRoot(); err == nil {
		exitError("wvc repository already exists")
	}

	fmt.Printf("Initializing WVC repository...\n")
	fmt.Printf("Weaviate URL: %s\n", initURL)

	// Test connection to Weaviate
	client, err := weaviate.NewClient(initURL)
	if err != nil {
		exitError("failed to create Weaviate client: %v", err)
	}

	fmt.Printf("Connecting to Weaviate...\n")
	if err := client.Ping(ctx); err != nil {
		exitError("failed to connect to Weaviate: %v", err)
	}

	// Detect server version
	var serverVersion string
	version, err := client.GetServerVersion(ctx)
	if err != nil {
		fmt.Printf("Warning: Could not detect Weaviate version\n")
	} else {
		serverVersion = version.Version
		fmt.Printf("Weaviate version: %s\n", version.Version)

		if !version.SupportsFeature("cursor_pagination") {
			fmt.Printf("Warning: Server < 1.18, using offset pagination (slower for large datasets)\n")
		}
	}

	// Initialize config
	cfg, err := config.Initialize(initURL)
	if err != nil {
		exitError("failed to initialize config: %v", err)
	}

	// Store detected server version
	if serverVersion != "" {
		cfg.ServerVersion = serverVersion
		if err := cfg.Save(); err != nil {
			fmt.Printf("Warning: Could not save server version to config: %v\n", err)
		}
	}

	// Initialize store
	st, err := store.New(cfg.DatabasePath())
	if err != nil {
		exitError("failed to create store: %v", err)
	}
	defer st.Close()

	if err := st.Initialize(); err != nil {
		exitError("failed to initialize store: %v", err)
	}

	// Set up initial branch state (HEAD_BRANCH will be set after first commit)
	// For now, just ensure we're ready for branching
	_ = st.SetCurrentBranch("")

	// Take initial snapshot of current state
	fmt.Printf("Taking initial snapshot...\n")
	useCursor := cfg.SupportsCursorPagination()
	if err := core.UpdateKnownState(ctx, st, client, useCursor); err != nil {
		exitError("failed to take initial snapshot: %v", err)
	}

	// Get object count for initial commit message
	objects, _ := st.GetAllKnownObjects()
	objectCount := len(objects)

	// Create initial commit if there are objects
	if objectCount > 0 {
		fmt.Printf("Found %d existing objects\n", objectCount)
	}

	fmt.Printf("\nInitialized empty WVC repository in .wvc/\n")
	fmt.Printf("Tracking Weaviate at %s\n", initURL)

	if objectCount > 0 {
		fmt.Printf("\nRun 'wvc commit -m \"Initial state\"' to create the first commit.\n")
	}
}
