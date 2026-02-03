package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/kilupskalvis/wvc/internal/remote"
	"github.com/spf13/cobra"
)

var remoteCmd = &cobra.Command{
	Use:   "remote",
	Short: "Manage remote repositories",
	Long: `Manage the set of remote repositories whose branches you track.

Without a subcommand, lists all configured remotes.

Examples:
  wvc remote                           List all remotes
  wvc remote add origin https://...    Add a remote named 'origin'
  wvc remote remove origin             Remove a remote
  wvc remote set-url origin https://.. Update a remote's URL
  wvc remote set-token origin          Set authentication token for a remote`,
	Run: runRemoteList,
}

var remoteVerbose bool

var remoteAddCmd = &cobra.Command{
	Use:   "add <name> <url>",
	Short: "Add a new remote",
	Long:  `Add a new remote repository with the given name and URL.`,
	Args:  cobra.ExactArgs(2),
	Run:   runRemoteAdd,
}

var remoteRemoveCmd = &cobra.Command{
	Use:     "remove <name>",
	Aliases: []string{"rm"},
	Short:   "Remove a remote",
	Long:    `Remove a remote and all its remote-tracking branches.`,
	Args:    cobra.ExactArgs(1),
	Run:     runRemoteRemove,
}

var remoteSetURLCmd = &cobra.Command{
	Use:   "set-url <name> <url>",
	Short: "Change a remote's URL",
	Args:  cobra.ExactArgs(2),
	Run:   runRemoteSetURL,
}

var remoteInfoCmd = &cobra.Command{
	Use:   "info <name>",
	Short: "Display remote repository stats",
	Long: `Show information about a remote repository including branch count,
commit count, and total stored blobs.

Examples:
  wvc remote info origin`,
	Args: cobra.ExactArgs(1),
	Run:  runRemoteInfo,
}

var remoteSetTokenCmd = &cobra.Command{
	Use:   "set-token <name>",
	Short: "Set authentication token for a remote",
	Long: `Set or update the authentication token for a remote.
The token is read from stdin for security (not passed as an argument).

Examples:
  wvc remote set-token origin                  # prompts for token
  echo "my-token" | wvc remote set-token origin  # pipe token from stdin`,
	Args: cobra.ExactArgs(1),
	Run:  runRemoteSetToken,
}

func init() {
	remoteCmd.Flags().BoolVarP(&remoteVerbose, "verbose", "v", false, "Show remote URLs")

	remoteCmd.AddCommand(remoteAddCmd)
	remoteCmd.AddCommand(remoteRemoveCmd)
	remoteCmd.AddCommand(remoteSetURLCmd)
	remoteCmd.AddCommand(remoteSetTokenCmd)
	remoteCmd.AddCommand(remoteInfoCmd)
}

func runRemoteList(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	result, err := core.ListRemotes(c.Store)
	if err != nil {
		exitError("%v", err)
	}

	if len(result.Remotes) == 0 {
		return
	}

	for _, r := range result.Remotes {
		if remoteVerbose {
			fmt.Printf("%s\t%s\n", r.Name, r.URL)
		} else {
			fmt.Println(r.Name)
		}
	}
}

func runRemoteAdd(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	name := args[0]
	url := args[1]

	if err := core.AddRemote(c.Store, name, url); err != nil {
		exitError("%v", err)
	}

	green := color.New(color.FgGreen)
	green.Printf("Added remote '%s' (%s)\n", name, url)
}

func runRemoteRemove(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	name := args[0]

	if err := core.RemoveRemote(c.Store, name); err != nil {
		exitError("%v", err)
	}

	fmt.Printf("Removed remote '%s'\n", name)
}

func runRemoteSetURL(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	name := args[0]
	url := args[1]

	if err := core.SetRemoteURL(c.Store, name, url); err != nil {
		exitError("%v", err)
	}

	fmt.Printf("Updated remote '%s' URL to %s\n", name, url)
}

func runRemoteSetToken(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	name := args[0]

	// Verify the remote exists before prompting
	if _, err := core.GetRemote(c.Store, name); err != nil {
		exitError("%v", err)
	}

	fmt.Fprintf(os.Stderr, "Enter token for remote '%s': ", name)

	reader := bufio.NewReader(os.Stdin)
	token, err := reader.ReadString('\n')
	if err != nil {
		exitError("failed to read token: %v", err)
	}

	token = strings.TrimSpace(token)
	if token == "" {
		exitError("token cannot be empty")
	}

	if err := core.SetRemoteToken(c.Store, name, token); err != nil {
		exitError("%v", err)
	}

	green := color.New(color.FgGreen)
	green.Printf("Token stored for remote '%s'\n", name)
}

func runRemoteInfo(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	name := args[0]

	remoteInfo, err := core.GetRemote(c.Store, name)
	if err != nil {
		exitError("%v", err)
	}

	token, err := core.GetRemoteToken(c.Store, name)
	if err != nil {
		exitError("get token: %v", err)
	}
	if token == "" {
		exitError("no token configured for remote '%s' — run 'wvc remote set-token %s'", name, name)
	}

	baseURL, repoName, err := core.ParseRemoteURL(remoteInfo.URL)
	if err != nil {
		exitError("%v", err)
	}

	client := remote.NewHTTPClient(baseURL, repoName, token)

	ctx := context.Background()
	info, err := client.GetRepoInfo(ctx)
	if err != nil {
		exitError("failed to get remote info: %v", err)
	}

	fmt.Printf("Remote: %s (%s)\n", name, remoteInfo.URL)
	fmt.Printf("  Branches: %d\n", info.BranchCount)
	fmt.Printf("  Commits:  %d\n", info.CommitCount)
	fmt.Printf("  Blobs:    %d\n", info.TotalBlobs)
}
