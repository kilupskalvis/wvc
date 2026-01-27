package cli

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var checkoutCmd = &cobra.Command{
	Use:   "checkout <branch|commit>",
	Short: "Switch branches or restore working tree",
	Long: `Switch to a branch or checkout a specific commit.

Examples:
  wvc checkout main          # Switch to main branch
  wvc checkout abc1234       # Checkout specific commit (detached HEAD)
  wvc checkout -b feature    # Create and switch to new branch
  wvc checkout -f main       # Force checkout, discarding uncommitted changes`,
	Args: cobra.MaximumNArgs(1),
	Run:  runCheckout,
}

var (
	checkoutCreateBranch bool
	checkoutForce        bool
)

func init() {
	checkoutCmd.Flags().BoolVarP(&checkoutCreateBranch, "branch", "b", false, "Create and checkout a new branch")
	checkoutCmd.Flags().BoolVarP(&checkoutForce, "force", "f", false, "Force checkout, discarding local changes")
}

func runCheckout(cmd *cobra.Command, args []string) {
	bgCtx := context.Background()
	c := initFullContext()
	defer c.Close()

	cfg, st, client := c.Config, c.Store, c.Client

	// Determine target
	var target string
	if len(args) > 0 {
		target = args[0]
	}

	// Validate arguments
	if checkoutCreateBranch {
		if target == "" {
			exitError("branch name required with -b flag")
		}
	} else {
		if target == "" {
			exitError("branch or commit required")
		}
	}

	opts := core.CheckoutOptions{
		Force:         checkoutForce,
		CreateBranch:  checkoutCreateBranch,
		NewBranchName: "",
	}

	// If -b flag, target becomes the new branch name
	if checkoutCreateBranch {
		opts.NewBranchName = target
		// Target will be resolved to current HEAD in Checkout
	}

	result, err := core.Checkout(bgCtx, cfg, st, client, target, opts)
	if err != nil {
		exitError("%v", err)
	}

	yellow := color.New(color.FgYellow)
	green := color.New(color.FgGreen)

	// Print result
	if checkoutCreateBranch {
		green.Printf("Switched to a new branch '%s'\n", result.BranchName)
	} else if result.IsDetached {
		yellow.Printf("HEAD is now at %s\n", shortID(result.TargetCommit))
		fmt.Println("You are in 'detached HEAD' state. You can look around, make experimental")
		fmt.Println("changes and commit them. To create a branch to retain commits, use:")
		fmt.Println("  wvc checkout -b <new-branch-name>")
	} else {
		green.Printf("Switched to branch '%s'\n", result.BranchName)
	}

	// Print stats if there were changes
	if result.ObjectsAdded > 0 || result.ObjectsRemoved > 0 || result.ObjectsUpdated > 0 {
		fmt.Printf("  %d added, %d updated, %d removed\n",
			result.ObjectsAdded, result.ObjectsUpdated, result.ObjectsRemoved)
	}

	// Print warnings
	if len(result.Warnings) > 0 {
		yellow.Println("\nWarnings:")
		for _, w := range result.Warnings {
			yellow.Printf("  - %s\n", w.Message)
		}
	}
}
