package cli

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var addCmd = &cobra.Command{
	Use:   "add [<class> | <class>/<id> | .]",
	Short: "Add changes to the staging area",
	Long: `Add file contents to the staging area.

Examples:
  wvc add .                 Stage all changes
  wvc add Article           Stage all Article class changes
  wvc add Article/abc123    Stage specific object change`,
	Args: cobra.MinimumNArgs(1),
	Run:  runAdd,
}

func runAdd(cmd *cobra.Command, args []string) {
	bgCtx := context.Background()
	c := initFullContext()
	defer c.Close()

	cfg, st, client := c.Config, c.Store, c.Client
	green := color.New(color.FgGreen)
	totalStaged := 0

	for _, arg := range args {
		if arg == "." {
			count, err := core.StageAll(bgCtx, cfg, st, client)
			if err != nil {
				exitError("failed to stage changes: %v", err)
			}
			totalStaged += count
		} else {
			// Parse as class or class/id
			className, objectID, err := core.ParseObjectRef(arg)
			if err != nil {
				exitError("%v", err)
			}

			if objectID == "" {
				// Stage entire class
				count, err := core.StageClass(bgCtx, cfg, st, client, className)
				if err != nil {
					exitError("failed to stage %s: %v", className, err)
				}
				totalStaged += count
			} else {
				// Stage specific object
				if err := core.StageObject(bgCtx, cfg, st, client, className, objectID); err != nil {
					exitError("failed to stage %s/%s: %v", className, objectID, err)
				}
				totalStaged++
			}
		}
	}

	if totalStaged == 0 {
		fmt.Println("No changes to stage")
	} else {
		green.Printf("Staged %d change(s)\n", totalStaged)
	}
}
