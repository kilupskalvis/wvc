package cli

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var fetchDepth int

var fetchCmd = &cobra.Command{
	Use:   "fetch [<remote>] [<branch>]",
	Short: "Download commits and vectors from a remote",
	Long: `Download objects and refs from a remote repository without modifying
the local branch. Updates the remote-tracking branch only.

Defaults to the only configured remote and the current branch.

Examples:
  wvc fetch                         Fetch current branch from default remote
  wvc fetch origin                  Fetch current branch from 'origin'
  wvc fetch origin main             Fetch 'main' from 'origin'
  wvc fetch --depth 5 origin main   Fetch only the last 5 commits`,
	Args: cobra.MaximumNArgs(2),
	Run:  runFetch,
}

func init() {
	fetchCmd.Flags().IntVar(&fetchDepth, "depth", 0, "Limit number of commits to fetch (0 = all)")
}

func runFetch(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
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

	fmt.Printf("Fetching from %s (%s)...\n", remoteName, remoteInfo.URL)

	result, err := core.Fetch(ctx, c.Store, client, core.FetchOptions{
		RemoteName: remoteName,
		Branch:     branch,
		Depth:      fetchDepth,
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

	green.Printf("Fetched %d commit(s)", result.CommitsFetched)
	if result.VectorsFetched > 0 {
		fmt.Printf(", %d vector(s)", result.VectorsFetched)
	}
	fmt.Println()

	fmt.Printf("Updated %s/%s -> %s\n", remoteName, branch, shortID(result.RemoteTip))
}
