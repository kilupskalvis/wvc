package cli

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/spf13/cobra"
)

var mergeCmd = &cobra.Command{
	Use:   "merge <branch>",
	Short: "Merge a branch into the current branch",
	Long: `Merge the specified branch into the current branch.

If there are no conflicts, a merge commit will be created.
If conflicts are detected, the merge will abort unless --ours or --theirs is specified.

Examples:
  wvc merge feature           # Merge 'feature' into current branch
  wvc merge --no-ff main      # Force merge commit even if fast-forward possible
  wvc merge -m "msg" feature  # Use custom merge commit message
  wvc merge --ours feature    # On conflict, prefer our version
  wvc merge --theirs feature  # On conflict, prefer their version`,
	Args: cobra.ExactArgs(1),
	Run:  runMerge,
}

var (
	mergeNoFF    bool
	mergeMessage string
	mergeOurs    bool
	mergeTheirs  bool
)

func init() {
	mergeCmd.Flags().BoolVar(&mergeNoFF, "no-ff", false, "Create a merge commit even when fast-forward is possible")
	mergeCmd.Flags().StringVarP(&mergeMessage, "message", "m", "", "Custom merge commit message")
	mergeCmd.Flags().BoolVar(&mergeOurs, "ours", false, "On conflict, prefer our version")
	mergeCmd.Flags().BoolVar(&mergeTheirs, "theirs", false, "On conflict, prefer their version")
}

func runMerge(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	c := initFullContext()
	defer c.Close()

	targetBranch := args[0]

	// Validate flags
	if mergeOurs && mergeTheirs {
		exitError("cannot use --ours and --theirs together")
	}

	// Determine conflict strategy
	strategy := models.ConflictAbort
	if mergeOurs {
		strategy = models.ConflictOurs
	} else if mergeTheirs {
		strategy = models.ConflictTheirs
	}

	opts := models.MergeOptions{
		NoFastForward: mergeNoFF,
		Message:       mergeMessage,
		Strategy:      strategy,
	}

	result, err := core.Merge(ctx, c.Config, c.Store, c.Client, targetBranch, opts)
	if err != nil {
		exitError("%v", err)
	}

	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)
	red := color.New(color.FgRed, color.Bold)

	// Handle conflicts
	if !result.Success {
		printMergeConflicts(result, red)
		exitError("Automatic merge failed; fix conflicts and then commit the result.")
	}

	// Success output
	if result.FastForward {
		green.Println("Fast-forward")
	} else {
		fmt.Println("Merge made by the 'recursive' strategy.")
		if result.MergeCommit != nil {
			fmt.Printf("  Merge commit: %s\n", shortID(result.MergeCommit.ID))
		}
	}

	// Show resolved conflicts if any
	if result.ResolvedConflicts > 0 {
		yellow.Printf("Auto-resolved %d conflict(s) using '%s' strategy\n", result.ResolvedConflicts, strategy)
	}

	// Show statistics
	if result.ObjectsAdded > 0 {
		green.Printf("  %d objects added\n", result.ObjectsAdded)
	}
	if result.ObjectsUpdated > 0 {
		yellow.Printf("  %d objects updated\n", result.ObjectsUpdated)
	}
	if result.ObjectsDeleted > 0 {
		red.Printf("  %d objects deleted\n", result.ObjectsDeleted)
	}

	// Show warnings
	for _, warning := range result.Warnings {
		yellow.Printf("  Warning: %s\n", warning)
	}
}

func printMergeConflicts(result *models.MergeResult, red *color.Color) {
	if len(result.Conflicts) > 0 {
		red.Println("\nCONFLICTS (object data):")
		for _, c := range result.Conflicts {
			fmt.Printf("  %s: %s/%s\n", c.Type, c.ClassName, c.ObjectID)
		}
	}

	if len(result.SchemaConflicts) > 0 {
		red.Println("\nCONFLICTS (schema):")
		for _, c := range result.SchemaConflicts {
			if c.PropertyName != "" {
				fmt.Printf("  %s: %s.%s\n", c.Type, c.ClassName, c.PropertyName)
			} else {
				fmt.Printf("  %s: %s\n", c.Type, c.ClassName)
			}
		}
	}
}
