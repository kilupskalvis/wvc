package cli

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var pullDepth int

var pullCmd = &cobra.Command{
	Use:   "pull [<remote>] [<branch>]",
	Short: "Fetch and fast-forward from a remote",
	Long: `Download commits and vectors from a remote and fast-forward the local branch.

If the remote branch has diverged from the local branch, the command reports
the divergence and suggests running 'wvc merge'.

Defaults to the only configured remote and the current branch.

Examples:
  wvc pull                          Pull current branch from default remote
  wvc pull origin main              Pull 'main' from 'origin'
  wvc pull --depth 10 origin main   Pull only the last 10 commits`,
	Args: cobra.MaximumNArgs(2),
	Run:  runPull,
}

func init() {
	pullCmd.Flags().IntVar(&pullDepth, "depth", 0, "Limit number of commits to fetch (0 = all)")
}

func runPull(cmd *cobra.Command, args []string) {
	c := initFullContext()
	defer c.Close()

	ctx := context.Background()

	remoteName := ""
	branch := ""
	if len(args) >= 1 {
		remoteName = args[0]
	}
	if len(args) >= 2 {
		branch = args[1]
	}

	client, remoteInfo, remoteName, branch := resolveRemoteClient(c.Store, remoteName, branch)

	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)

	fmt.Printf("Pulling from %s (%s)...\n", remoteName, remoteInfo.URL)

	result, err := core.Pull(ctx, c.Config, c.Store, c.Client, client, core.PullOptions{
		RemoteName: remoteName,
		Branch:     branch,
		Depth:      pullDepth,
	}, func(phase string, current, total int) {
		if total > 0 {
			fmt.Printf("\r  %s %d/%d", phase, current, total)
		}
	})
	if err != nil {
		fmt.Println()
		exitError("%v", err)
	}

	fmt.Println()
	if result.UpToDate {
		fmt.Println("Already up-to-date.")
		return
	}

	if result.CommitsFetched > 0 {
		green.Printf("Fetched %d commit(s)", result.CommitsFetched)
		if result.VectorsFetched > 0 {
			fmt.Printf(", %d vector(s)", result.VectorsFetched)
		}
		fmt.Println()
	}

	if result.FastForward {
		green.Printf("Fast-forwarded '%s' to %s\n", branch, shortID(result.RemoteTip))
		if result.ObjectsAdded > 0 || result.ObjectsUpdated > 0 || result.ObjectsRemoved > 0 {
			fmt.Printf("  %d added, %d updated, %d removed\n",
				result.ObjectsAdded, result.ObjectsUpdated, result.ObjectsRemoved)
		}
	}

	if result.Diverged {
		yellow.Printf("Your branch and '%s/%s' have diverged.\n", remoteName, branch)
		yellow.Printf("Run 'wvc merge %s/%s' to integrate remote changes.\n", remoteName, branch)
	}

	if len(result.Warnings) > 0 {
		yellow.Println("\nWarnings:")
		for _, w := range result.Warnings {
			yellow.Printf("  - %s\n", w.Message)
		}
	}
}
