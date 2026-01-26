package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset [<class>/<id>]",
	Short: "Unstage changes",
	Long: `Remove changes from the staging area.

Examples:
  wvc reset                 Unstage all changes
  wvc reset Article         Unstage all Article class changes
  wvc reset Article/abc123  Unstage specific object change`,
	Run: runReset,
}

func runReset(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	st := c.Store
	yellow := color.New(color.FgYellow)

	if len(args) == 0 {
		// Unstage all
		count, err := st.GetStagedChangesCount()
		if err != nil {
			exitError("failed to get staged count: %v", err)
		}

		if count == 0 {
			fmt.Println("Nothing to unstage")
			return
		}

		if err := core.UnstageAll(st); err != nil {
			exitError("failed to unstage: %v", err)
		}
		yellow.Printf("Unstaged %d change(s)\n", count)
		return
	}

	for _, arg := range args {
		className, objectID, err := core.ParseObjectRef(arg)
		if err != nil {
			exitError("%v", err)
		}

		if objectID == "" {
			// Unstage entire class
			changes, err := st.GetStagedChangesByClass(className)
			if err != nil {
				exitError("failed to get staged changes: %v", err)
			}

			if len(changes) == 0 {
				fmt.Printf("No staged changes for %s\n", className)
				continue
			}

			if err := core.UnstageClass(st, className); err != nil {
				exitError("failed to unstage %s: %v", className, err)
			}
			yellow.Printf("Unstaged %d change(s) from %s\n", len(changes), className)
		} else {
			// Unstage specific object
			staged, err := st.GetStagedChange(className, objectID)
			if err != nil {
				exitError("failed to check staged change: %v", err)
			}
			if staged == nil {
				fmt.Printf("No staged changes for %s/%s\n", className, objectID)
				continue
			}

			if err := core.UnstageObject(st, className, objectID); err != nil {
				exitError("failed to unstage %s/%s: %v", className, objectID, err)
			}
			yellow.Printf("Unstaged %s/%s\n", className, objectID)
		}
	}
}
