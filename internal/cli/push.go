package cli

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var pushForce bool
var pushDelete string

var pushCmd = &cobra.Command{
	Use:   "push [<remote>] [<branch>]",
	Short: "Push commits and vectors to a remote",
	Long: `Upload local commits and vector data to a remote wvc-server.

Defaults to the only configured remote and the current branch.

Examples:
  wvc push                          Push current branch to default remote
  wvc push origin main              Push 'main' branch to 'origin'
  wvc push --force origin main      Force push (overwrites remote)
  wvc push --delete origin feature  Delete 'feature' branch on 'origin'`,
	Args: cobra.MaximumNArgs(2),
	Run:  runPush,
}

func init() {
	pushCmd.Flags().BoolVarP(&pushForce, "force", "f", false, "Force push (overwrite remote branch)")
	pushCmd.Flags().StringVar(&pushDelete, "delete", "", "Delete a remote branch")
}

func runPush(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	ctx := context.Background()

	// Parse args
	remoteName := ""
	branch := ""
	if len(args) >= 1 {
		remoteName = args[0]
	}
	if len(args) >= 2 {
		branch = args[1]
	}

	// Handle --delete
	if pushDelete != "" {
		if remoteName == "" {
			var err error
			remoteName, _, err = core.ResolveRemoteAndBranch(c.Store, "", "")
			if err != nil {
				exitError("%v", err)
			}
		}
		handlePushDelete(ctx, c, remoteName, pushDelete)
		return
	}

	client, remoteInfo, remoteName, branch := resolveRemoteClient(c.Store, remoteName, branch)

	// Push
	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)

	fmt.Printf("Pushing to %s (%s)...\n", remoteName, remoteInfo.URL)

	result, err := core.Push(ctx, c.Store, client, core.PushOptions{
		RemoteName: remoteName,
		Branch:     branch,
		Force:      pushForce,
	}, func(phase string, current, total int) {
		if total > 0 {
			fmt.Printf("\r  %s %d/%d", phase, current, total)
		}
	})
	if err != nil {
		fmt.Println() // newline after progress
		exitError("%v", err)
	}

	fmt.Println() // newline after progress
	if result.UpToDate {
		fmt.Println("Already up-to-date.")
		return
	}

	if result.BranchCreated {
		green.Printf("Created remote branch '%s'\n", branch)
	}

	if result.CommitsPushed > 0 {
		green.Printf("Pushed %d commit(s)", result.CommitsPushed)
		if result.VectorsPushed > 0 {
			fmt.Printf(", %d vector(s)", result.VectorsPushed)
		}
		fmt.Println()
	}

	if pushForce {
		yellow.Println("(force push)")
	}
}

func handlePushDelete(ctx context.Context, c *cmdContext, remoteName, branch string) {
	client := resolveRemoteClientByName(c.Store, remoteName)

	if err := core.DeleteRemoteBranch(ctx, c.Store, client, remoteName, branch); err != nil {
		exitError("%v", err)
	}

	fmt.Printf("Deleted remote branch '%s/%s'\n", remoteName, branch)
}
