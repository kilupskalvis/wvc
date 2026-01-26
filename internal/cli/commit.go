package cli

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/spf13/cobra"
)

var commitCmd = &cobra.Command{
	Use:   "commit",
	Short: "Record changes to the repository",
	Long: `Create a new commit with staged changes.

By default, only staged changes are committed. Use -a to automatically
stage all changes before committing.`,
	Run: runCommit,
}

var (
	commitMessage string
	commitAll     bool
)

func init() {
	commitCmd.Flags().StringVarP(&commitMessage, "message", "m", "", "Commit message (required)")
	commitCmd.Flags().BoolVarP(&commitAll, "all", "a", false, "Automatically stage all changes before committing")
	commitCmd.MarkFlagRequired("message")
}

func runCommit(cmd *cobra.Command, args []string) {
	bgCtx := context.Background()
	c := initFullContext()
	defer c.Close()

	cfg, st, client := c.Config, c.Store, c.Client
	var commit *models.Commit

	if commitAll {
		_, err := core.StageAll(bgCtx, cfg, st, client)
		if err != nil {
			exitError("failed to stage changes: %v", err)
		}
	}

	// Check if there are staged changes
	stagedCount, err := st.GetStagedChangesCount()
	if err != nil {
		exitError("failed to check staged changes: %v", err)
	}

	if stagedCount == 0 {
		commit, err = core.CreateCommit(bgCtx, cfg, st, client, commitMessage)
		if err != nil {
			exitError("%v", err)
		}
	} else {
		commit, err = core.CreateCommitFromStaging(bgCtx, cfg, st, client, commitMessage)
		if err != nil {
			exitError("%v", err)
		}
	}

	green := color.New(color.FgGreen)
	green.Printf("[%s] %s\n", commit.ShortID(), commit.Message)
	fmt.Printf(" %d operation(s)\n", commit.OperationCount)
}
