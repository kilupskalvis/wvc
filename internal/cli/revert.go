package cli

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var revertCmd = &cobra.Command{
	Use:   "revert <commit>",
	Short: "Revert a commit",
	Long: `Revert the changes made by a commit.
This creates a new commit that undoes all changes from the specified commit.`,
	Args: cobra.ExactArgs(1),
	Run:  runRevert,
}

func runRevert(cmd *cobra.Command, args []string) {
	bgCtx := context.Background()
	commitRef := args[0]

	c := initFullContext()
	defer c.Close()

	cfg, st, client := c.Config, c.Store, c.Client
	fmt.Printf("Reverting commit %s...\n", commitRef)

	var warnings []core.SchemaRevertWarning
	revertCommit, err := core.RevertCommitWithWarnings(bgCtx, cfg, st, client, commitRef, &warnings)
	if err != nil {
		exitError("failed to revert: %v", err)
	}

	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)

	green.Printf("[%s] %s\n", revertCommit.ShortID(), revertCommit.Message)
	fmt.Printf(" %d operation(s) reverted\n", revertCommit.OperationCount)

	// Display schema warnings
	if len(warnings) > 0 {
		fmt.Println()
		yellow.Println("Schema revert warnings:")
		for _, w := range warnings {
			if w.PropertyName != "" {
				yellow.Printf("  - %s %s.%s: %s\n", w.Operation, w.ClassName, w.PropertyName, w.Reason)
			} else {
				yellow.Printf("  - %s %s: %s\n", w.Operation, w.ClassName, w.Reason)
			}
		}
	}
}
