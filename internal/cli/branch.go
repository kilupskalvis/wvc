package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var branchCmd = &cobra.Command{
	Use:   "branch [name]",
	Short: "List, create, or delete branches",
	Long: `Manage branches in the WVC repository.

Without arguments, lists all branches.
With a name argument, creates a new branch at HEAD.

Examples:
  wvc branch              # List all branches
  wvc branch feature      # Create 'feature' branch at HEAD
  wvc branch feature abc123  # Create 'feature' branch at commit abc123
  wvc branch -d feature   # Delete 'feature' branch`,
	Run: runBranch,
}

var (
	branchDelete      bool
	branchForceDelete bool
)

func init() {
	branchCmd.Flags().BoolVarP(&branchDelete, "delete", "d", false, "Delete a branch")
	branchCmd.Flags().BoolVarP(&branchForceDelete, "force", "D", false, "Force delete a branch")
}

func runBranch(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	st := c.Store

	// Delete branch
	if branchDelete || branchForceDelete {
		if len(args) == 0 {
			exitError("branch name required for deletion")
		}
		if err := core.DeleteBranch(st, args[0], branchForceDelete); err != nil {
			exitError("%v", err)
		}
		fmt.Printf("Deleted branch '%s'\n", args[0])
		return
	}

	// Create branch
	if len(args) > 0 {
		name := args[0]
		startPoint := ""
		if len(args) > 1 {
			startPoint = args[1]
		}

		if err := core.CreateBranch(st, name, startPoint); err != nil {
			exitError("%v", err)
		}

		// Get the commit ID for display
		branch, _ := st.GetBranch(name)
		if branch != nil {
			fmt.Printf("Created branch '%s' at %s\n", name, shortID(branch.CommitID))
		} else {
			fmt.Printf("Created branch '%s'\n", name)
		}
		return
	}

	// List branches
	branches, currentBranch, err := core.ListBranches(st)
	if err != nil {
		exitError("failed to list branches: %v", err)
	}

	if len(branches) == 0 {
		fmt.Println("No branches yet. Create a commit first, then branches will be available.")
		return
	}

	green := color.New(color.FgGreen)
	for _, branch := range branches {
		if branch.Name == currentBranch {
			green.Printf("* %s\n", branch.Name)
		} else {
			fmt.Printf("  %s\n", branch.Name)
		}
	}
}
